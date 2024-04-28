package delay

type PayloadDelay struct {
	Push    int64  `json:"push"`
	Payload string `json:"payload"`
	Topic   string `json:"topic"`
	Id      int64  `json:"id"`
}
