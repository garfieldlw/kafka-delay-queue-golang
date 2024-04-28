package kafka

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/garfieldlw/kafka-delay-queue-golang/pkg/delay"
	"github.com/garfieldlw/kafka-delay-queue-golang/pkg/log"
	"go.uber.org/zap"
)

type ConsumerDelayConfig struct {
	Group string `json:"group"`
	Topic string `json:"topic"`
}

type HandlerConfig struct {
	Name            string
	ClientConfigMD5 string
	CloseChan       chan *struct{}
}

var closeChan = make(map[string]*HandlerConfig)
var consumers = make(map[string]*kafka.Consumer)

func RunSub() error {
	kafkaConfig := []*ConsumerDelayConfig{
		{
			Group: "group-delay-topic-1m",
			Topic: string(delay.Topic1m),
		},
		{
			Group: "group-delay-topic-3m",
			Topic: string(delay.Topic3m),
		},
		{
			Group: "group-delay-topic-10m",
			Topic: string(delay.Topic10m),
		},
		{
			Group: "group-delay-topic-15m",
			Topic: string(delay.Topic15m),
		},
		{
			Group: "group-delay-topic-30m",
			Topic: string(delay.Topic30m),
		},
	}
	if kafkaConfig == nil || len(kafkaConfig) == 0 {
		return errors.New("get config fail")
	}

	for _, c := range kafkaConfig {
		if c == nil {
			continue
		}

		md5Value := getMD5(c)
		if len(md5Value) == 0 {
			continue
		}

		handlerConfig := closeChan[c.Group]
		if handlerConfig != nil {
			if handlerConfig.ClientConfigMD5 == md5Value {
				continue
			}

			close(handlerConfig.CloseChan)
		}

		cc := new(HandlerConfig)
		cc.Name = c.Group
		cc.CloseChan = make(chan *struct{})
		cc.ClientConfigMD5 = md5Value

		go runHandler(c, cc.CloseChan)

		closeChan[c.Group] = cc
	}

	return nil
}

func Close() {
	log.Info("close consumer")
	for k, c := range consumers {
		if c == nil {
			continue
		}

		err := c.Close()
		if err != nil {
			log.Error("close consumer error", zap.Error(err))
		}

		handlerConfig := closeChan[k]
		if handlerConfig != nil {
			close(handlerConfig.CloseChan)
		}
	}
}

func runHandler(c *ConsumerDelayConfig, closeChan chan *struct{}) {
	errChan := make(chan error)
	go cron(c, closeChan, errChan)

	for {
		select {
		case workErr := <-errChan:
			log.Warn("worker routine stopped", zap.Error(workErr))
			go cron(c, closeChan, errChan)
		case <-closeChan:
			return
		}
	}
}

func cron(c *ConsumerDelayConfig, closeChan chan *struct{}, errChan chan error) {
	if cl, ok := consumers[c.Group]; ok {
		if cl != nil {
			_ = cl.Close()
		}
	}

	client, err := LoadKafkaClientConsumerGroup(c.Group)
	log.Debug("init consumer group", zap.Any("config", *c), zap.Error(err))
	if err != nil {
		errChan <- err
		return
	}

	consumers[c.Group] = client

	err = client.SubscribeTopics([]string{c.Topic}, nil)
	if err != nil {
		errChan <- err
		return
	}

	for {
		select {
		case <-closeChan:
			return

		default:
			ev := client.Poll(1000)
			if ev == nil {
				continue
			}

			switch msg := ev.(type) {
			case *kafka.Message:
				log.Info("consumer group msg", zap.String("offset", msg.TopicPartition.Offset.String()), zap.Int32("partition", msg.TopicPartition.Partition), zap.String("key", string(msg.Key)), zap.String("value", string(msg.Value)))

				p, err := loadPayload(msg.Value)
				if err != nil {
					_, err = client.CommitMessage(msg)
					if err != nil {
						errChan <- err
						return
					}

					continue
				}

				_, err = p.delay(*msg.TopicPartition.Topic)
				if err != nil {
					_, err = client.CommitOffsets([]kafka.TopicPartition{msg.TopicPartition})
					if err != nil {
						errChan <- err
						return
					}

					err = client.Seek(msg.TopicPartition, 0)
					if err != nil {
						errChan <- err
						return
					}
				}

				ok, errDeliver := p.deliver()
				if errDeliver != nil {
					if ok {
						_, err = client.CommitMessage(msg)
						if err != nil {
							errChan <- err
							return
						}
					}
					continue
				}

				_, err = client.CommitMessage(msg)
				if err != nil {
					errChan <- err
					return
				}

			case kafka.Error:
				errChan <- msg
				return
			default:
				log.Warn("kafka message error", zap.Any("value", msg))
			}
		}
	}
}

func getMD5(val interface{}) string {
	data, errData := json.Marshal(val)
	if errData != nil {
		return ""
	}
	has := md5.Sum(data)
	return fmt.Sprintf("%x", has)
}
