package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Event struct {
	//identify the producer sending the event
	ProducerId string `json:"producer_id"`
	//identifies a particular generation of a producer
	//prevents FairGate from confusing new events with old events when a producer restarts and resets its sequence counter
	ProducerEpoch string `json:"producer_epoch,omitempty"`
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

// MaxIdentifierLength bounds producer_id, producer_epoch, and event_type in bytes. They are kept in
// deduplication and queue maps for every event, so an unbounded value would cost unbounded memory.
const MaxIdentifierLength = 256

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

	if event.ProducerEpoch == "" {
		return Event{}, errors.New("Producer epoch is required")
	}

	for _, field := range []struct{ name, value string }{
		{"producer_id", event.ProducerId},
		{"producer_epoch", event.ProducerEpoch},
		{"event_type", event.EventType},
	} {
		if len(field.value) > MaxIdentifierLength {
			return Event{}, fmt.Errorf("%s is longer than %d bytes", field.name, MaxIdentifierLength)
		}
	}

	return event, nil
}
