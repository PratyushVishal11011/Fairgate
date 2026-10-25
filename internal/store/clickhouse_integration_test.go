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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch, err := NewClickHouseStore([]string{addr}, database, username, password)
	if err != nil {
		t.Fatalf("connect to ClickHouse: %v", err)
	}
	defer ch.Close()

	now := time.Now().UnixNano()
	producerID := fmt.Sprintf("integration-test-%d", now)
	producerEpoch := fmt.Sprintf("epoch-%d", now)
	eventTime := time.Now().UTC().Truncate(time.Millisecond)
	baseSeq := uint64(now)

	events := []wire.Event{
		{
			ProducerId:    producerID,
			ProducerEpoch: producerEpoch,
			EventTime:     eventTime,
			Seq:           baseSeq,
			Idx:           0,
			EventType:     "integration_test",
			Payload:       "first test event",
		},
		{
			ProducerId:    producerID,
			ProducerEpoch: producerEpoch,
			EventTime:     eventTime,
			Seq:           baseSeq + 1,
			Idx:           1,
			EventType:     "integration_test",
			Payload:       "second test event",
		},
	}

	token := fmt.Sprintf("integration-%s-%d", producerID, baseSeq)

	if err := ch.InsertBatch(ctx, events, token); err != nil {
		t.Fatalf("initial InsertBatch failed: %v", err)
	}
	if err := ch.InsertBatch(ctx, events, token); err != nil {
		t.Fatalf("replayed InsertBatch failed: %v", err)
	}

	for _, want := range events {
		want := want

		t.Run(fmt.Sprintf("seq_%d_idx_%d", want.Seq, want.Idx), func(t *testing.T) {
			var count uint64
			err := ch.conn.QueryRow(
				ctx,
				fmt.Sprintf(
					`SELECT count()
					FROM %s.events
					WHERE producer_id = ?
					  AND producer_epoch = ?
					  AND seq = ?
					  AND idx = ?`,
					quoteIdentifier(database),
				),
				want.ProducerId, want.ProducerEpoch, want.Seq, want.Idx,
			).Scan(&count)
			if err != nil {
				t.Fatalf("count inserted event: %v", err)
			}
			if count != 1 {
				t.Fatalf("expected exactly one copy of event seq=%d idx=%d, got %d",
					want.Seq, want.Idx, count)
			}

			var got wire.Event
			var gotIdx uint64
			err = ch.conn.QueryRow(
				ctx,
				fmt.Sprintf(
					`SELECT
						producer_id,
						producer_epoch,
						event_time,
						seq,
						idx,
						event_type,
						payload
					FROM %s.events
					WHERE producer_id = ?
					  AND producer_epoch = ?
					  AND seq = ?
					  AND idx = ?
					LIMIT 1`,
					quoteIdentifier(database),
				),
				want.ProducerId, want.ProducerEpoch, want.Seq, want.Idx,
			).Scan(
				&got.ProducerId,
				&got.ProducerEpoch,
				&got.EventTime,
				&got.Seq,
				&gotIdx,
				&got.EventType,
				&got.Payload,
			)
			if err != nil {
				t.Fatalf("query inserted event: %v", err)
			}
			if gotIdx > uint64(^uint16(0)) {
				t.Fatalf("idx %d exceeds uint16 range", gotIdx)
			}
			got.Idx = uint16(gotIdx)

			if got.ProducerId != want.ProducerId {
				t.Errorf("producer_id: got %q, want %q", got.ProducerId, want.ProducerId)
			}
			if got.ProducerEpoch != want.ProducerEpoch {
				t.Errorf("producer_epoch: got %q, want %q", got.ProducerEpoch, want.ProducerEpoch)
			}
			if !got.EventTime.Equal(want.EventTime) {
				t.Errorf("event_time: got %v, want %v", got.EventTime, want.EventTime)
			}
			if got.Seq != want.Seq {
				t.Errorf("seq: got %d, want %d", got.Seq, want.Seq)
			}
			if got.Idx != want.Idx {
				t.Errorf("idx: got %d, want %d", got.Idx, want.Idx)
			}
			if got.EventType != want.EventType {
				t.Errorf("event_type: got %q, want %q", got.EventType, want.EventType)
			}
			if got.Payload != want.Payload {
				t.Errorf("payload: got %q, want %q", got.Payload, want.Payload)
			}
		})
	}
}

func quoteIdentifier(identifier string) string {
	for _, r := range identifier {
		if (r < 'a' || r > 'z') &&
			(r < 'A' || r > 'Z') &&
			(r < '0' || r > '9') &&
			r != '_' {
			panic("invalid ClickHouse database identifier")
		}
	}
	if identifier == "" {
		panic("empty ClickHouse database identifier")
	}
	return "`" + identifier + "`"
}
