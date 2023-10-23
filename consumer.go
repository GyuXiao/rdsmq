package rdsmq

import (
	"context"
	"errors"
	"rdsmq/log"
	"rdsmq/redis"
)

// 内置 redis 客户端，通过 XREADGROUP 指令实现消息的消费，通过 XACK 指令实现消息的确认

type Consumer struct {
	// 生命周期管理
	ctx context.Context

	// 停止控制器
	stop context.CancelFunc

	// 接收到 msg 执行的回调函数
	callbackFunc MsgCallback

	// redis 客户端
	client *redis.Client

	// 消费的 topic
	topic string

	// 所属消费者组
	groupID string

	// 当前节点的消费者 ID
	consumerID string

	// 不同消息的累计失败次数
	failureCnt map[redis.MsgEntity]int

	// 用户自定义配置
	opts *ConsumerOptions
}

func NewConsumer(client *redis.Client, topic, groupId, consumerID string, callbackFunc MsgCallback, opts ...ConsumerOption) (*Consumer, error) {
	ctx, stop := context.WithCancel(context.Background())
	c := Consumer{
		ctx:          ctx,
		stop:         stop,
		callbackFunc: callbackFunc,
		client:       client,
		topic:        topic,
		groupID:      groupId,
		consumerID:   consumerID,
		failureCnt:   make(map[redis.MsgEntity]int),
		opts:         &ConsumerOptions{},
	}
	if err := c.checkParams(); err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(c.opts)
	}
	repairConsumerOpts(c.opts)

	go c.run()

	return &c, nil
}

func (c *Consumer) checkParams() error {
	if c.callbackFunc == nil {
		return errors.New("callback function can not be empty")
	}
	if c.client == nil {
		return errors.New("redis client can not be empty")
	}
	if c.topic == "" || c.consumerID == "" || c.groupID == "" {
		return errors.New("redis topic/consumerID/groupID can not be empty")
	}
	return nil
}

type MsgCallback func(ctx context.Context, msg *redis.MsgEntity) error

// 消费新消息
func (c *Consumer) receive() ([]*redis.MsgEntity, error) {
	msgs, err := c.client.XReadGroup(c.ctx, c.groupID, c.consumerID, c.topic, int(c.opts.receiveTimeout.Milliseconds()))
	if err != nil && !errors.Is(err, redis.ErrNoMsg) {
		return nil, err
	}
	return msgs, nil
}

// 消费未经确认的旧消息
func (c *Consumer) receivePending() ([]*redis.MsgEntity, error) {
	pendingMsgs, err := c.client.XReadGroupPending(c.ctx, c.groupID, c.consumerID, c.topic)
	if err != nil && !errors.Is(err, redis.ErrNoMsg) {
		return nil, err
	}
	return pendingMsgs, nil
}

func (c *Consumer) handlerMsgs(ctx context.Context, msgs []*redis.MsgEntity) {
	for _, msg := range msgs {
		// 如果 callback 执行失败，则需要对失败次数进行累加
		if err := c.callbackFunc(ctx, msg); err != nil {
			c.failureCnt[*msg]++
			continue
		}

		// 如果 callback 执行成功，则需要调用 xack 方法确认答复
		if err := c.client.XACK(ctx, c.topic, c.groupID, msg.MsgID); err != nil {
			log.ErrorContextf(ctx, "msg ack failed, msgID: %d, err: %v", msg.MsgID, err)
			continue
		}

		// xack 成功， 从 failureCnt 清零对应消息的 fail 次数
		delete(c.failureCnt, *msg)
	}
}

// 如果某条消息的失败次数达到阈值，则投递到 deadMsg 队列中，再执行 xack
func (c *Consumer) deliverDeadMsg(ctx context.Context) {
	for msg, failCnt := range c.failureCnt {
		if failCnt < c.opts.maxRetryLimit {
			continue
		}

		// 投递 deadMsg 队列
		if err := c.opts.deadMsgMailBox.Deliver(ctx, &msg); err != nil {
			log.ErrorContextf(c.ctx, "dead msg deliver failed, msgID: %d, err: %v", msg.MsgID, err)
		}

		// 对于投递到 deadMsg 队列中的 msg，需要执行 ack 操作，后续就不再重复操作了
		if err := c.client.XACK(ctx, c.topic, c.groupID, msg.MsgID); err != nil {
			log.ErrorContextf(ctx, "msg ack failed, msgID: %d, err: %v", msg.MsgID, err)
			continue
		}

		// xack 成功， 从 failureCnt 清零对应消息的 fail 次数
		delete(c.failureCnt, msg)
	}
}

func (c *Consumer) Stop() {
	c.stop()
}

func (c *Consumer) run() {
	for {
		// 保证在执行 consumer.Stop 方法后，该 goroutine 及时退出
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// 接收到新消息
		msgs, err := c.receive()
		if err != nil {
			log.ErrorContextf(c.ctx, "msg received failed, err: %v", err)
			continue
		}

		// 接收到新消息后，执行回调
		tCtx, _ := context.WithTimeout(c.ctx, c.opts.handlerMsgTimeout)
		c.handlerMsgs(tCtx, msgs)

		// 1,如果失败次数超过阈值，则把旧消息投递到 deadMsg 队列
		// 2,旧消息投递到 deadMsg 队列成功后，执行 xack 操作
		tCtx, _ = context.WithTimeout(c.ctx, c.opts.deadMsgDeliverTimeout)
		c.deliverDeadMsg(tCtx)

		// 处理旧消息
		pendingMsgs, err := c.receivePending()
		if err != nil {
			log.ErrorContextf(c.ctx, "pending msg receive failed, err: %v", err)
			continue
		}

		// 接收到旧消息后，执行回调
		tCtx, _ = context.WithTimeout(c.ctx, c.opts.handlerMsgTimeout)
		c.handlerMsgs(tCtx, pendingMsgs)
	}
}
