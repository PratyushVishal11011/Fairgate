package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"FairGate/internal/wire"
)

func TestClickHouseInsertBatchIntegration(t *testing.T) {
	if os.Getenv("FAIRGATE_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set FAIRGATE_CLICKHOUSE_INTEGRATION=1 to run")
	}

	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}

	database := os.Getenv("CLICKHOUSE_DATABASE")
	if database == "" {
		database = "fairgate"
	}

	username := os.Getenv("CLICKHOUSE_USER")
	if username == "" {
		username = "fairgate"
	}

	password := os.Getenv("CLICKHOUSE_PASSWORD")
	if password == "" {
		password = "fairgate_dev_password"
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	ch, err := NewClickHouseStore(
		[]string{addr},
		database,
		username,
		password,
	)
	if err != nil {
		t.Fatalf("connect to ClickHouse: %v", err)
	}
	defer ch.Close()

	producerID := fmt.Sprintf(
		"integration-test-%d",
		time.Now().UnixNano(),
	)

	eventTime := time.Now().UTC().Truncate(time.Millisecond)
	baseSeq := uint64(time.Now().UnixNano())

	events := []wire.Event{
		{
			ProducerId: producerID,
			EventTime:  eventTime,
			Seq:        baseSeq,
			Idx:        0,
			EventType:  "integration_test",
			Payload:    "first test event",
		},
		{
			ProducerId: producerID,
			EventTime:  eventTime,
			Seq:        baseSeq + 1,
			Idx:        1,
			EventType:  "integration_test",
			Payload:    "second test event",
		},
	}

	token := fmt.Sprintf("integration-%s-%d", producerID, baseSeq)

	if err := ch.InsertBatch(ctx, events, token); err != nil {
		t.Fatalf("initial InsertBatch failed: %v", err)
	}

	// Replay the exact same batch with the same idempotency token.
	if err := ch.InsertBatch(ctx, events, token); err != nil {
		t.Fatalf("replayed InsertBatch failed: %v", err)
	}

	for _, want := range events {
		t.Run(fmt.Sprintf("seq_%d", want.Seq), func(t *testing.T) {
			var got wire.Event

			err := ch.conn.QueryRow(
				ctx,
				`SELECT
producer_id,
event_time,
seq,
idx,
event_type,
payload
FROM fairgate.events
WHERE producer_id = ?
AND seq = ?
AND idx = ?
LIMIT 1`,
				want.ProducerId,
				want.Seq,
				want.Idx,
			).Scan(
				&got.ProducerId,
				&got.EventTime,
				&got.Seq,
				&got.Idx,
				&got.EventType,
				&got.Payload,
			)
			if err != nil {
				t.Fatalf("query inserted event: %v", err)
			}

			if got.ProducerId != want.ProducerId {
				t.Errorf("producer_id: got %q, want %q",
					got.ProducerId, want.ProducerId)
			}
			if !got.EventTime.Equal(want.EventTime) {
				t.Errorf("event_time: got %v, want %v",
					got.EventTime, want.EventTime)
			}
			if got.Seq != want.Seq {
				t.Errorf("seq: got %d, want %d", got.Seq, want.Seq)
			}
			if got.Idx != want.Idx {
				t.Errorf("idx: got %d, want %d", got.Idx, want.Idx)
			}
			if got.EventType != want.EventType {
				t.Errorf("event_type: got %q, want %q",
					got.EventType, want.EventType)
			}
			if got.Payload != want.Payload {
				t.Errorf("payload: got %q, want %q",
					got.Payload, want.Payload)
			}
		})
	}
}
