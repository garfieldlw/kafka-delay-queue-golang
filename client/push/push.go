package push

import (
	"context"
	"github.com/garfieldlw/kafka-delay-queue-golang/pkg/grpc"
	"github.com/garfieldlw/kafka-delay-queue-golang/pkg/log"
	"github.com/garfieldlw/kafka-delay-queue-golang/pkg/pool"
	"github.com/garfieldlw/kafka-delay-queue-golang/proto/push"
	"go.uber.org/zap"
	grpc2 "google.golang.org/grpc"
	"time"
)

const GrpcName = "push"

var (
	Client *serviceIns
)

type grpcClient struct {
	p    pool.Pool
	c    interface{}
	conn push.PushClient
}

func (c *grpcClient) put() {
	err := c.p.Put(c.c)
	if err != nil {
		log.Error("grpc client, put client fail", zap.Error(err))
	}
}

func newGrpcClient() (*grpcClient, error) {
	p, err := grpc.LoadServicePool(GrpcName)
	if err != nil {
		log.Warn("get connect fail", zap.Error(err))
		return nil, err
	}
	client, err := p.Get()
	if err != nil {
		log.Warn("get connect fail", zap.Error(err))
		return nil, err
	}

	return &grpcClient{p: p, c: client, conn: push.NewPushClient(client.(*grpc2.ClientConn))}, nil
}

type serviceIns struct {
}

func (service *serviceIns) PushToKafka(ctx context.Context, id int64, topic, data string) (*push.PushKafkaResponse, error) {
	client, err := newGrpcClient()
	if err != nil {
		log.Warn("get connect fail", zap.Error(err))
		return nil, err
	}
	defer client.put()

	ctx, _ = context.WithTimeout(ctx, 100*time.Millisecond)
	t, err := client.conn.PushToKafka(ctx, &push.PushKafkaRequest{
		Id:    id,
		Topic: topic,
		Data:  data,
	})
	if err != nil {
		log.Warn("grpc error", zap.Error(err))
		return nil, err
	}

	return t, nil
}

func (service *serviceIns) PushToKafkaDelay(ctx context.Context, id int64, topic, data string, delay push.DelayEnum) (*push.PushKafkaDelayResponse, error) {
	client, err := newGrpcClient()
	if err != nil {
		log.Warn("get connect fail", zap.Error(err))
		return nil, err
	}
	defer client.put()

	ctx, _ = context.WithTimeout(ctx, 100*time.Millisecond)
	t, err := client.conn.PushToKafkaDelay(ctx, &push.PushKafkaDelayRequest{
		Id:    id,
		Topic: topic,
		Data:  data,
		Delay: delay,
	})
	if err != nil {
		log.Warn("grpc error", zap.Error(err))
		return nil, err
	}

	return t, nil
}
