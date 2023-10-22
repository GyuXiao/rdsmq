package rdsmq

import (
	"context"
	"rdsmq/log"
	"rdsmq/redis"
)

type DeadMsgMailBox interface {
	Deliver(ctx context.Context, msg *redis.MsgEntity) error
}

type DeadMsgLogger struct{}

func NewDeadMsgLogger() *DeadMsgLogger {
	return &DeadMsgLogger{}
}

func (l *DeadMsgLogger) Deliver(ctx context.Context, msg *redis.MsgEntity) error {
	log.ErrorContextf(ctx, "msg exceed retry limit, msg id: %s", msg.MsgID)
	return nil
}
