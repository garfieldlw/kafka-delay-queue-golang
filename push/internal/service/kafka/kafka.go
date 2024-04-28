package kafka

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/garfieldlw/kafka-delay-queue-golang/pkg/log"
	_var "github.com/garfieldlw/kafka-delay-queue-golang/pkg/var"
	"go.uber.org/zap"
	"sync"
	"time"
)

const PusherMsgLength = 1000

var producerCli *kafka.Producer

var (
	serviceAct *Service
	lock       = &sync.Mutex{}

	pusherMsg = make(chan *kafka.Message, PusherMsgLength)
)

type Service struct {
}

func LoadKafkaService() *Service {
	if serviceAct != nil {
		return serviceAct
	}

	lock.Lock()
	defer lock.Unlock()
	if serviceAct == nil {
		err := initProducer()
		if err != nil {
			log.Error("init producer fail", zap.Error(err))
			return nil
		}

		go pushDataToKafka()

		serviceAct = &Service{}
	}
	return serviceAct
}

func (actService *Service) PushDataToKafka(id int64, topic, message string) error {
	if len(topic) == 0 {
		return errors.New("topic is empty")
	}

	if len(message) == 0 {
		return errors.New("message is empty")
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: hashPartitions(topic, id)},
		Key:            []byte(fmt.Sprintf("%v", id)),
		Value:          []byte(message),
		Timestamp:      time.Now(),
		TimestampType:  kafka.TimestampCreateTime,
	}

	if len(pusherMsg) == PusherMsgLength {
		return errors.New("push msg length is max")
	}

	pusherMsg <- msg

	return nil
}

func Close() {
	if producerCli == nil {
		return
	}

	producerCli.Flush(1000 * 5)
}

func initProducer() (err error) {
	broker := _var.GetKafkaConfig()
	if broker == nil {
		return errors.New("kafka config is empty")
	}

	conf := &kafka.ConfigMap{
		"api.version.request": "true",
		"bootstrap.servers":   broker.Broker,
		"linger.ms":           10,
		"retry.backoff.ms":    1000,
		"acks":                "all",
		"retries":             5,
	}

	if len(broker.User) > 0 {
		_ = conf.SetKey("sasl.mechanism", "PLAIN")
		_ = conf.SetKey("security.protocol", "sasl_plaintext")
		_ = conf.SetKey("sasl.username", broker.User)
		_ = conf.SetKey("sasl.password", broker.Password)
		_ = conf.SetKey("sasl.mechanism", broker.Mechanism)
	}

	producerCli, err = kafka.NewProducer(conf)
	if err != nil {
		return err
	}

	go callback()

	return nil
}

func pushDataToKafka() {
	for {
		select {
		case msg := <-pusherMsg:
			{
				log.Info("push to kafka", zap.Any("msg", msg))
				_ = producerCli.Produce(msg, nil)
			}
		}
	}
}

func callback() {
	for e := range producerCli.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Error("push to kafka error", zap.Error(ev.TopicPartition.Error), zap.Any("msg", ev))
			} else {
				log.Info("push to kafka success", zap.Any("msg", ev))
			}
		}
	}
}

func hashPartitions(topic string, key int64) int32 {
	num := partitionsNumber(topic)

	p := key % int64(num)
	return int32(p)
}

var topicPartition = map[string]int32{}

func partitionsNumber(topic string) int32 {
	if n, ok := topicPartition[topic]; ok {
		return n
	} else {
		return 1
	}
}
