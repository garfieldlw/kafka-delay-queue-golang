package handler

import (
	"context"
	"github.com/garfieldlw/kafka-delay-queue-golang/proto/push"
	"github.com/garfieldlw/kafka-delay-queue-golang/push/internal/service/kafka"
)

type PushServer struct {
}

func (*PushServer) PushToKafka(ctx context.Context, req *push.PushKafkaRequest) (*push.PushKafkaResponse, error) {
	return kafka.PushToKafka(ctx, req)
}
func (*PushServer) PushToKafkaDelay(ctx context.Context, req *push.PushKafkaDelayRequest) (*push.PushKafkaDelayResponse, error) {
	return kafka.PushToKafkaDelay(ctx, req)
}
