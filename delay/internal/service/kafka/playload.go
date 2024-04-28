package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/garfieldlw/kafka-delay-queue-golang/client/push"
	"github.com/garfieldlw/kafka-delay-queue-golang/pkg/delay"
	"time"
)

type payloadDelay struct {
	delay.PayloadDelay
}

func loadPayload(payload []byte) (res payloadDelay, err error) {
	err = json.Unmarshal(payload, &res)
	return
}

func newPushUnix() int64 {
	n := time.Now().Unix()
	n = n - n%(60*5)
	return n
}

func (p payloadDelay) delay(topic string) (runNow bool, err error) {
	var duration int64
	switch topic {
	case string(delay.Topic1m):
		duration = delay1m
	case string(delay.Topic3m):
		duration = delay3m
	case string(delay.Topic10m):
		duration = delay10m
	case string(delay.Topic15m):
		duration = delay15m
	case string(delay.Topic30m):
		duration = delay30m
	default:
		return false, errors.New("unsupported topic")
	}

	nowSecond := time.Now().Unix()
	untilSecond := p.Push + duration
	return delayUntil(nowSecond, untilSecond)
}

func (p payloadDelay) deliver() (bool, error) {
	_, err := push.Client.PushToKafka(context.Background(), p.Id, p.Topic, p.Payload)
	if err != nil {
		return false, err
	}
	return true, nil
}
