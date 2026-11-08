package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"FairGate/internal/wire"
)

// integrationConfig returns the ClickHouse settings for integration tests.
// The tests run only when CLICKHOUSE_ADDR is set, so the default suite needs no live ClickHouse.
func integrationConfig(t *testing.T) (addr, database, username, password string) {
	t.Helper()

	addr = os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("set CLICKHOUSE_ADDR to run ClickHouse integration tests")
	}

	database = os.Getenv("CLICKHOUSE_DATABASE")
	if database == "" {
		database = "fairgate"
	}

	username = os.Getenv("CLICKHOUSE_USER")
	if username == "" {
		username = "fairgate"
	}

	password = os.Getenv("CLICKHOUSE_PASSWORD")
	if password == "" {
		password = "fairgate_dev_password"
	}

	return addr, database, username, password
}

func TestClickHouseInsertBatchIntegration(t *testing.T) {
	addr, database, username, password := integrationConfig(t)

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

// TestClickHouseInsertBatchSkipsStoredEventsIntegration replays events under tokens that
// ClickHouse's insert deduplication cannot match, as happens when a replay covers a different
// WAL range or falls outside the deduplication window. Each event must still be stored once.
func TestClickHouseInsertBatchSkipsStoredEventsIntegration(t *testing.T) {
	addr, database, username, password := integrationConfig(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch, err := NewClickHouseStore([]string{addr}, database, username, password)
	if err != nil {
		t.Fatalf("connect to ClickHouse: %v", err)
	}
	defer ch.Close()

	now := time.Now().UnixNano()
	producerID := fmt.Sprintf("exactly-once-test-%d", now)
	eventTime := time.Now().UTC().Truncate(time.Millisecond)

	event := func(seq uint64) wire.Event {
		return wire.Event{
			ProducerId:    producerID,
			ProducerEpoch: "epoch-1",
			EventTime:     eventTime,
			Seq:           seq,
			EventType:     "integration_test",
			Payload:       fmt.Sprintf("event-%d", seq),
		}
	}

	steps := []struct {
		token  string
		events []wire.Event
	}{
		{"token-a", []wire.Event{event(1), event(2)}},
		// A wider replay under a new token: 1 and 2 are already stored.
		{"token-b", []wire.Event{event(1), event(2), event(3)}},
		// Duplicates inside one batch.
		{"token-c", []wire.Event{event(4), event(4)}},
		// The same batch token after only part of the batch landed: 5 is stored, 6 is not.
		// Sending 6 under the original token would be discarded as a duplicate block.
		{"token-d", []wire.Event{event(5)}},
		{"token-d", []wire.Event{event(5), event(6)}},
	}

	for i, step := range steps {
		if err := ch.InsertBatch(ctx, step.events, step.token); err != nil {
			t.Fatalf("InsertBatch step %d failed: %v", i+1, err)
		}
	}

	for seq := uint64(1); seq <= 6; seq++ {
		var count uint64
		err := ch.conn.QueryRow(
			ctx,
			fmt.Sprintf(
				`SELECT count() FROM %s.events WHERE producer_id = ? AND seq = ?`,
				quoteIdentifier(database),
			),
			producerID, seq,
		).Scan(&count)
		if err != nil {
			t.Fatalf("count event seq=%d: %v", seq, err)
		}
		if count != 1 {
			t.Errorf("expected exactly one copy of event seq=%d, got %d", seq, count)
		}
	}
}

// TestClickHouseInsertBatchLargeBatchIntegration covers batches whose existence check spans several
// queries. One query for all 5000 IDs would exceed ClickHouse's default max_query_size. A replay
// under a new token overlaps stored events across query boundaries; each event must be stored once.
func TestClickHouseInsertBatchLargeBatchIntegration(t *testing.T) {
	addr, database, username, password := integrationConfig(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ch, err := NewClickHouseStore([]string{addr}, database, username, password)
	if err != nil {
		t.Fatalf("connect to ClickHouse: %v", err)
	}
	defer ch.Close()

	now := time.Now().UnixNano()
	producerID := fmt.Sprintf("large-batch-test-%d", now)
	eventTime := time.Now().UTC().Truncate(time.Millisecond)

	batch := func(from, to uint64) []wire.Event {
		events := make([]wire.Event, 0, to-from+1)
		for seq := from; seq <= to; seq++ {
			events = append(events, wire.Event{
				ProducerId:    producerID,
				ProducerEpoch: "epoch-1",
				EventTime:     eventTime,
				Seq:           seq,
				EventType:     "integration_test",
				Payload:       fmt.Sprintf("event-%d", seq),
			})
		}
		return events
	}

	if err := ch.InsertBatch(ctx, batch(1, 5000), "large-a"); err != nil {
		t.Fatalf("InsertBatch of 5000 events failed: %v", err)
	}
	// 2500-5000 are stored, 5001-7500 are new.
	if err := ch.InsertBatch(ctx, batch(2500, 7500), "large-b"); err != nil {
		t.Fatalf("overlapping InsertBatch failed: %v", err)
	}

	var count, distinct uint64
	err = ch.conn.QueryRow(
		ctx,
		fmt.Sprintf(
			`SELECT count(), uniqExact(seq) FROM %s.events WHERE producer_id = ?`,
			quoteIdentifier(database),
		),
		producerID,
	).Scan(&count, &distinct)
	if err != nil {
		t.Fatalf("count events: %v", err)
	}
	if count != 7500 || distinct != 7500 {
		t.Errorf("expected 7500 events stored once each, got %d rows for %d distinct seqs", count, distinct)
	}
}
