package push

import (
	"context"
	"fmt"
	"github.com/garfieldlw/kafka-delay-queue-golang/proto/push"
	"testing"
)

func TestServiceIns_PushToKafkaDelay(t *testing.T) {
	realTopic := "topic-real"
	delayMsg := "delay message"

	_, err := Client.PushToKafkaDelay(context.Background(), 123456789, realTopic, delayMsg, push.DelayEnum_DelayEnum1m)
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}
}
