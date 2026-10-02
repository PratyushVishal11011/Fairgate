package wire

type Ack struct {
	ProducerId string `json:"producer_id"`
	Seq        uint64 `json:"seq"`
	Idx        uint16 `json:"idx"`
	Status     string `json:"status"`
	Message    string `json:"message,omitempty"`
}
