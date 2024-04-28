# kafka-delay-queue-golang

## Introduction
Delay queue base on kafka. This is similar to RocketMQ, the delayed messages would be delivered to the consumer for consumption after delaying a certain period of time. RocketMQ supports 18 levels of delayed messages, and here, you can customize the required delay time level, but like RocketMQ, you need to pre-set the corresponding level.


## Implementation
In Kafka, each delayed message at each time level has a corresponding topic. The delayed message will first be delivered to the corresponding topic mentioned above. When the delay time is reached, the message will be delivered to the real topic and the offset will be manually submitted.
Due to the characteristics of Kafka, messages are ordered within a certain range.

## How To Use
It is very easy to use. Just send the original message to the grpc(PushToKafkaDelay) of the delay queue.

```go
func TestServiceIns_PushToKafkaDelay(t *testing.T) {
	realTopic := "real-topic"
	delayMsg := "real message"

	_, err := Client.PushToKafkaDelay(context.Background(), 123456789, realTopic, delayMsg, push.DelayEnum_DelayEnum1m)
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}
}
```


## Notes
* kafka client base on github.com/confluentinc/confluent-kafka-go
