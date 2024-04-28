package kafka

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/garfieldlw/kafka-delay-queue-golang/pkg/utils"
	_var "github.com/garfieldlw/kafka-delay-queue-golang/pkg/var"
)

func LoadKafkaClientConsumerGroup(groupId string) (*kafka.Consumer, error) {
	broker := _var.GetKafkaConfig()
	if broker == nil || len(broker.Broker) == 0 {
		return nil, errors.New("get broker config fail")
	}

	clientId := fmt.Sprintf("%s-%s", groupId, utils.RandomString(3))

	conf := &kafka.ConfigMap{
		"api.version.request": "true",
		"bootstrap.servers":   broker.Broker,
		"group.id":            groupId,
		"session.timeout.ms":  6000,
		"auto.offset.reset":   "latest",
		"client.id":           clientId,
		"enable.auto.commit":  false,
	}

	if len(broker.User) > 0 {
		_ = conf.SetKey("sasl.mechanism", "PLAIN")
		_ = conf.SetKey("security.protocol", "sasl_plaintext")
		_ = conf.SetKey("sasl.username", broker.User)
		_ = conf.SetKey("sasl.password", broker.Password)
		_ = conf.SetKey("sasl.mechanism", broker.Mechanism)
	}

	c, err := kafka.NewConsumer(conf)
	if err != nil {
		return nil, err
	}

	return c, nil
}
