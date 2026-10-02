package wire

import (
	"encoding/json"
	"errors"
	"time"
)

type Event struct {
	//identify the producer sending the event
	ProducerId string `json:"producer_id"`
	//timestamp associated with event
	EventTime time.Time `json:"event_time"`
	//sequence number for tracking event
	Seq uint64 `json:"seq"`
	//index for distinguishing events with the same sequence number
	Idx uint16 `json:"idx"`
	//Categorize the event
	EventType string `json:"event_type"`
	//The actual data
	Payload string `json:"payload"`
}

func DecodeEvent(data []byte) (Event, error) {
	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return Event{}, err
	}

	if event.ProducerId == "" {
		return Event{}, errors.New("Producer ID is required")
	}

	if event.EventTime.IsZero() {
		return Event{}, errors.New("Event time is required")
	}

	if event.EventType == "" {
		return Event{}, errors.New("Event type is required")
	}

	return event, nil
}
