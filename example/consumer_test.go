package example

import (
	"context"
	"rdsmq"
	"rdsmq/redis"
	"testing"
	"time"
)

const (
	network       = "tcp"
	address       = "127.0.0.1:6379"
	password      = ""
	topic         = "my_test_topic"
	consumerGroup = "my_test_group"
	consumerID    = "consumerTest"
)

// 自定义实现 deadMsg 队列
type DemoDeadMsgMailBox struct {
	do func(msg *redis.MsgEntity)
}

func NewDemoDeadMsgMailBox(do func(msg *redis.MsgEntity)) *DemoDeadMsgMailBox {
	return &DemoDeadMsgMailBox{
		do: do,
	}
}

func (d *DemoDeadMsgMailBox) Deliver(ctx context.Context, msg *redis.MsgEntity) error {
	d.do(msg)
	return nil
}

// 测试前，先在环境里的 redis 创建 my_test_topic 和 my_test_group

func TestConsumer(t *testing.T) {
	client := redis.NewClient(network, address, password)

	// consumer 接收到消息后执行的 callback 回调
	callbackFunc := func(ctx context.Context, msg *redis.MsgEntity) error {
		t.Logf("received msg, msgID: %s, msgKey: %s, msgBody: %s", msg.MsgID, msg.Key, msg.Value)
		return nil
	}

	// deadMsg 队列的实例
	demoDeadMsgMailBox := NewDemoDeadMsgMailBox(func(msg *redis.MsgEntity) {
		t.Logf("received dead msg, msgID: %s, msgKey: %s, msgBody: %s", msg.MsgID, msg.Key, msg.Value)
	})

	// 创建 consumer 实例
	consumer, err := rdsmq.NewConsumer(client, topic, consumerGroup, consumerID, callbackFunc,
		rdsmq.WithMaxRetryLimit(2),
		rdsmq.WithReceiveTimeout(time.Second*2),
		rdsmq.WithDeadMsgMailBox(demoDeadMsgMailBox))
	if err != nil {
		t.Error(err)
		return
	}
	defer consumer.Stop()

	// 10 秒后退出单测程序
	<-time.After(time.Second * 10)
}
