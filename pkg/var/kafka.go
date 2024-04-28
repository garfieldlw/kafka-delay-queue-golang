package _var

type KafkaConfigItem struct {
	Broker    string `json:"broker"`
	User      string `json:"user"`
	Password  string `json:"password"`
	Mechanism string `json:"mechanism"`
}

func GetKafkaConfig() *KafkaConfigItem {
	return &KafkaConfigItem{
		Broker:    "10.0.0.35:9092",
		User:      "",
		Password:  "",
		Mechanism: "",
	}
}
