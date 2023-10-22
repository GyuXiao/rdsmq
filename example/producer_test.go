package example

import (
	"context"
	"rdsmq"
	"rdsmq/redis"
	"testing"
)

//const (
//	network  = "tcp"
//	address  = "127.0.0.1:6379"
//	password = ""
//	topic    = "my_test_topic"
//)

func Test_Producer(t *testing.T) {
	client := redis.NewClient(network, address, password)

	producer := rdsmq.NewProducer(client, rdsmq.WithMsgQueueLen(10))
	ctx := context.Background()

	msgID, err := producer.SendMsg(ctx, topic, "test_k", "test_value")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(msgID)
}
