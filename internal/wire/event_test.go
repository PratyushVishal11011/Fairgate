package wire

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestDecodeEvent(t *testing.T) {
	data := []byte(`{
		"producer_id": "producer-001",
		"producer_epoch": "epoch-001",
		"event_time": "2026-10-02T09:00:00Z",
		"seq": 42,
		"idx": 0,
		"event_type": "bearing_fault",
		"payload": "vibration anomaly"
	}`)

	event, err := DecodeEvent(data)
	if err != nil {
		t.Fatal(err)
	}

	if event.ProducerId != "producer-001" {
		t.Errorf("unexpected producer ID: %s", event.ProducerId)
	}

	if event.ProducerEpoch != "epoch-001" {
		t.Errorf("unexpected producer epoch: %s", event.ProducerEpoch)
	}

	if event.Seq != 42 {
		t.Errorf("expected sequence 42, got %d", event.Seq)
	}

	if event.Idx != 0 {
		t.Errorf("expected index 0, got %d", event.Idx)
	}

	if event.EventType != "bearing_fault" {
		t.Errorf("unexpected event type: %s", event.EventType)
	}

	expectedTime, _ := time.Parse(time.RFC3339, "2026-10-02T09:00:00Z")
	if !event.EventTime.Equal(expectedTime) {
		t.Errorf("unexpected event time: %v", event.EventTime)
	}

	if event.Payload != "vibration anomaly" {
		t.Errorf("unexpected payload: %s", event.Payload)
	}
}

func TestDecodeInvalidEvent(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{
			name: "malformed JSON",
			data: `{"producer_id":`,
		},
		{
			name: "missing producer ID",
			data: `{"event_time":"2026-10-02T09:00:00Z","event_type":"test"}`,
		},
		{
			name: "missing event time",
			data: `{"producer_id":"p1","event_type":"test"}`,
		},
		{
			name: "missing event type",
			data: `{"producer_id":"p1","event_time":"2026-10-02T09:00:00Z"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeEvent([]byte(tt.data))
			if err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestDecodeEventWithoutProducerEpoch(t *testing.T) {
	data := []byte(`{
		"producer_id": "producer-001",
		"event_time": "2026-10-02T09:00:00Z",
		"seq": 42,
		"event_type": "bearing_fault"
	}`)

	_, err := DecodeEvent(data)
	if err == nil {
		t.Fatal("expected an error when producer epoch is missing")
	}
}

func TestDecodeEventRejectsOverlongIdentifiers(t *testing.T) {
	long := strings.Repeat("x", MaxIdentifierLength+1)
	for _, field := range []string{"producer_id", "producer_epoch", "event_type"} {
		event := map[string]any{
			"producer_id": "p", "producer_epoch": "1", "event_time": "2026-10-02T09:00:00Z", "event_type": "t",
		}
		event[field] = long
		data, _ := json.Marshal(event)
		if _, err := DecodeEvent(data); err == nil || !strings.Contains(err.Error(), field) {
			t.Fatalf("%s: expected an error naming the field, got %v", field, err)
		}
	}
}
