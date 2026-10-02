package wire

import (
	"testing"
	"time"
)

func TestDecodeEvent(t *testing.T) {
	data := []byte(`{
        "producer_id": "producer-001",
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

	if event.Seq != 42 {
		t.Errorf("expected sequence 42, got %d", event.Seq)
	}

	if event.EventType != "bearing_fault" {
		t.Errorf("unexpected event type: %s", event.EventType)
	}

	expectedTime, _ := time.Parse(time.RFC3339, "2026-10-02T09:00:00Z")
	if !event.EventTime.Equal(expectedTime) {
		t.Errorf("unexpected event time: %v", event.EventTime)
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
