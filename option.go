package rdsmq

import "time"

// Producer 和 Consumer 的配置参数和参数修正

type ProducerOptions struct {
	msgQueueLen int
}

type ProducerOption func(p *ProducerOptions)

func WithMsgQueueLen(queueLen int) ProducerOption {
	return func(p *ProducerOptions) {
		p.msgQueueLen = queueLen
	}
}

func repairMsgQueueLen(p *ProducerOptions) {
	if p.msgQueueLen <= 0 {
		p.msgQueueLen = 500
	}
}

type ConsumerOptions struct {
	// 阻塞消费新消息时等待超时时长
	receiveTimeout time.Duration
	// 投递 deadMsg 流程超时阈值
	deadMsgDeliverTimeout time.Duration
	// 处理消息流程超时阈值
	handlerMsgTimeout time.Duration
	// 处理消息时的最大重试次数
	maxRetryLimit int
	// deadMsg 队列
	deadMsgMailBox DeadMsgMailBox
}

type ConsumerOption func(opts *ConsumerOptions)

func WithReceiveTimeout(timeout time.Duration) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.receiveTimeout = timeout
	}
}

func WithDeadMsgDeliverTimeout(timeout time.Duration) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.deadMsgDeliverTimeout = timeout
	}
}

func WithHandlerMsgTimeout(timeout time.Duration) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.handlerMsgTimeout = timeout
	}
}

func WithMaxRetryLimit(limit int) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.maxRetryLimit = limit
	}
}

func WithDeadMsgMailBox(mailBox DeadMsgMailBox) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.deadMsgMailBox = mailBox
	}
}

// 修复非法配置参数

func repairConsumerOpts(opts *ConsumerOptions) {
	if opts.receiveTimeout < 0 {
		opts.receiveTimeout = time.Second * 2
	}
	if opts.maxRetryLimit < 0 {
		opts.maxRetryLimit = 3
	}
	if opts.deadMsgMailBox == nil {
		opts.deadMsgMailBox = NewDeadMsgLogger()
	}
	if opts.deadMsgDeliverTimeout <= 0 {
		opts.deadMsgDeliverTimeout = time.Second
	}
	if opts.handlerMsgTimeout <= 0 {
		opts.handlerMsgTimeout = time.Second
	}
}
