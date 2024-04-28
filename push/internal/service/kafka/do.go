package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/garfieldlw/kafka-delay-queue-golang/pkg/delay"
	"github.com/garfieldlw/kafka-delay-queue-golang/proto/push"
	"time"
)

func PushToKafka(ctx context.Context, req *push.PushKafkaRequest) (*push.PushKafkaResponse, error) {
	if req.Id < 1 {
		return nil, errors.New("push to kafka, invalid id")
	}

	if len(req.Topic) == 0 {
		return nil, errors.New("push to kafka, invalid topic")
	}

	if len(req.Data) == 0 {
		return nil, errors.New("push to kafka, invalid data")
	}

	service := LoadKafkaService()
	if service == nil {
		return nil, errors.New("push to kafka, load client fail")
	}

	err := service.PushDataToKafka(req.Id, req.Topic, req.Data)
	if err != nil {
		return nil, err
	}

	resp := new(push.PushKafkaResponse)
	resp.Id = req.Id

	return resp, nil
}
func PushToKafkaDelay(ctx context.Context, req *push.PushKafkaDelayRequest) (*push.PushKafkaDelayResponse, error) {
	if req.Id < 1 {
		return nil, errors.New("push to kafka, invalid id")
	}

	if len(req.Topic) == 0 {
		return nil, errors.New("push to kafka, invalid topic")
	}

	if len(req.Data) == 0 {
		return nil, errors.New("push to kafka, invalid data")
	}

	data := new(delay.PayloadDelay)
	data.Id = req.Id
	data.Topic = req.Topic
	data.Payload = req.Data
	data.Push = time.Now().Unix()

	d, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	var topic delay.Topic
	switch req.Delay {
	case push.DelayEnum_DelayEnum1m:
		{
			topic = delay.Topic1m
		}
	case push.DelayEnum_DelayEnum3m:
		{
			topic = delay.Topic3m
		}
	case push.DelayEnum_DelayEnum10m:
		{
			topic = delay.Topic10m
		}
	case push.DelayEnum_DelayEnum15m:
		{
			topic = delay.Topic15m
		}
	case push.DelayEnum_DelayEnum30m:
		{
			topic = delay.Topic30m
		}
	default:
		{
			return nil, errors.New("push to kafka, invalid delay type")
		}
	}
	service := LoadKafkaService()
	if service == nil {
		return nil, errors.New("push to kafka, load client fail")
	}

	err = service.PushDataToKafka(req.Id, string(topic), string(d[:]))
	if err != nil {
		return nil, err
	}

	resp := new(push.PushKafkaDelayResponse)
	resp.Id = req.Id

	return resp, nil
}
