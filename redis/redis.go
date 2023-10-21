package redis

import (
	"context"
	"errors"
	"github.com/demdxx/gocast"
	"github.com/gomodule/redigo/redis"
	"time"
)

// 封装 redis Stream 的相关指令，包括 XADD, XREADGROUP, XACK 等

type Client struct {
	pool *redis.Pool
	opts *ClientOptions
}

type MsgEntity struct {
	MsgID string
	Key   string
	Value string
}

func NewClient(network, address, password string, opts ...ClientOption) *Client {
	c := &Client{
		opts: &ClientOptions{
			network:  network,
			address:  address,
			password: password,
		},
	}
	// 自定义参数
	for _, opt := range opts {
		opt(c.opts)
	}
	// 修复非法参数
	repairClient(c.opts)
	// 创建 redis 连接池
	pool := c.getRedisPool()
	// 返回 redis 客户端实例
	return &Client{
		pool: pool,
	}
}

func (c *Client) getRedisPool() *redis.Pool {
	return &redis.Pool{
		// 最大空闲连接数
		MaxIdle: c.opts.maxIdle,
		// 最大活跃连接数
		MaxActive: c.opts.maxActive,
		// 连接最长空闲时间
		IdleTimeout: time.Duration(c.opts.idleTimeoutSeconds) * time.Second,
		// 连接不够时，是阻塞等待还是返回错误
		Wait: c.opts.wait,
		// 创建连接
		Dial: func() (redis.Conn, error) {
			c, err := c.getRedisConn()
			if err != nil {
				return nil, err
			}
			return c, err
		},
		// 测试方法
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// 获取 redis 连接
func (c *Client) getRedisConn() (redis.Conn, error) {
	// address 不能为 ""，否则直接 panic
	if c.opts.address == "" {
		panic(any("Cannot get address from redis config"))
	}
	// 客户端配置
	var diaOpts []redis.DialOption
	if len(c.opts.password) > 0 {
		diaOpts = append(diaOpts, redis.DialPassword(c.opts.password))
	}

	conn, err := redis.DialContext(context.Background(), c.opts.network, c.opts.address, diaOpts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// XADD 投递消息
func (c *Client) XADD(ctx context.Context, topic string, maxLen int, key, value string) (string, error) {
	if topic == "" {
		return "", errors.New("redis XADD topic can not be empty")
	}
	// 从连接池获取连接
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	// 使用完毕后放回连接池
	defer conn.Close()
	return redis.String(conn.Do("XADD", topic, "MAXLEN", maxLen, "*", key, value))
}

// XReadGroup 消费信息
// 分为两种
// 消费新消息
func (c *Client) XReadGroup(ctx context.Context, groupID, consumerID, topic string, timeout int) ([]*MsgEntity, error) {
	return c.xReadGroup(ctx, groupID, consumerID, topic, timeout, false)
}

// XReadGroupPending 消费旧消息
func (c *Client) XReadGroupPending(ctx context.Context, groupID, consumerID, topic string) ([]*MsgEntity, error) {
	return c.xReadGroup(ctx, groupID, consumerID, topic, 0, true)
}

func (c *Client) xReadGroup(ctx context.Context, groupID, consumerID, topic string, timeout int, pending bool) ([]*MsgEntity, error) {
	if groupID == "" || consumerID == "" || topic == "" {
		return nil, errors.New("redis XREADGROUP groupID/consumerID/topic can not be empty")
	}
	// 从连接池获取连接
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	// 使用完毕后放回连接池
	defer conn.Close()

	// 如果 pending 为 true，代表需要消费的是已分配给当前 consumer 但是还未经 xack 确认的老消息. 此时采用非阻塞模式进行处理
	// 如果 pending 为 false，代表需要消费的是尚未分配给任何 consumer 的新消息. 此时采用阻塞模式进行处理
	var rawReply interface{}
	if pending {
		rawReply, err = conn.Do("XREADGROUP", "GROUP", groupID, consumerID, "STREAMS", topic, "0-0")
	} else {
		rawReply, err = conn.Do("XREADGROUP", "GROUP", groupID, consumerID, "BLOCK", timeout, "STREAMS", topic, ">")
	}
	if err != nil {
		return nil, err
	}

	return formatRawReply(rawReply)
}

func formatRawReply(rawReply interface{}) ([]*MsgEntity, error) {
	reply, _ := rawReply.([]interface{})
	if len(reply) == 0 {
		return nil, ErrNoMsg
	}
	// topic = topic 名称 + n 条记录
	replyElement, _ := reply[0].([]interface{})
	if len(replyElement) != 2 {
		return nil, ErrInvalidMsgFormat
	}
	var msgs []*MsgEntity
	// 遍历 n 条记录
	rawMsgs, _ := replyElement[1].([]interface{})
	for _, rawMsg := range rawMsgs {
		// 记录 = ID + msgBody
		_msg, _ := rawMsg.([]interface{})
		if len(_msg) != 2 {
			return nil, ErrInvalidMsgFormat
		}
		msgID := gocast.ToString(_msg[0])
		// msgBody = key + value
		msgBody, _ := _msg[1].([]interface{})
		if len(msgBody) != 2 {
			return nil, ErrInvalidMsgFormat
		}
		msgKey, msgValue := gocast.ToString(msgBody[0]), gocast.ToString(msgBody[1])
		msgs = append(msgs, &MsgEntity{
			MsgID: msgID,
			Key:   msgKey,
			Value: msgValue,
		})
	}
	return msgs, nil
}
