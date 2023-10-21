package rdsmq

import (
	"context"
	"rdsmq/redis"
)

// 内置 redis 客户端，通过 XADD 指令实现消息的生产投递

type Producer struct {
	client *redis.Client
	opts   *ProducerOptions
}

func NewProducer(client *redis.Client, opts ...ProducerOption) *Producer {
	p := &Producer{
		client: client,
		opts:   &ProducerOptions{},
	}
	for _, opt := range opts {
		opt(p.opts)
	}
	repairMsgQueueLen(p.opts)
	return p
}

// SendMsg 生产一条消息
func (p *Producer) SendMsg(ctx context.Context, topic, key, value string) (string, error) {
	return p.client.XADD(ctx, topic, p.opts.msgQueueLen, key, value)
}
