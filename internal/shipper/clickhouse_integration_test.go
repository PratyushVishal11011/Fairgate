package shipper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"FairGate/internal/store"
	"FairGate/internal/wal"
	"FairGate/internal/wire"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// crashAfterInsertStore forwards to a real store and panics once after the first
// successful insert, before the shipper can commit its checkpoint.
type crashAfterInsertStore struct {
	store.Store
	crashed bool
}

func (s *crashAfterInsertStore) InsertBatch(ctx context.Context, events []wire.Event, token string) error {
	if err := s.Store.InsertBatch(ctx, events, token); err != nil {
		return err
	}
	if !s.crashed {
		s.crashed = true
		panic("simulated crash after ClickHouse accepted batch")
	}
	return nil
}

// TestShipperExactlyOnceToClickHouseIntegration runs the WAL, shipper, and ClickHouse store
// through a crash before checkpoint commit, a restart with more durable records, and finally a
// lost checkpoint that re-ships the whole WAL. Every event must be stored exactly once.
func TestShipperExactlyOnceToClickHouseIntegration(t *testing.T) {
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("set CLICKHOUSE_ADDR to run ClickHouse integration tests")
	}
	database := envOr("CLICKHOUSE_DATABASE", "fairgate")
	username := envOr("CLICKHOUSE_USER", "fairgate")
	password := envOr("CLICKHOUSE_PASSWORD", "fairgate_dev_password")

	ch, err := store.NewClickHouseStore([]string{addr}, database, username, password)
	if err != nil {
		t.Fatalf("connect to ClickHouse: %v", err)
	}
	defer ch.Close()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: database, Username: username, Password: password},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	dir := t.TempDir()
	walPath := filepath.Join(dir, "events.wal")
	checkpointStore := NewCheckpointStore(filepath.Join(dir, "checkpoint.json"))

	producerID := fmt.Sprintf("shipper-exactly-once-%d", time.Now().UnixNano())
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

	appendEvents := func(w *wal.WAL, from, to uint64) {
		t.Helper()
		for seq := from; seq <= to; seq++ {
			if err := w.Append(event(seq)); err != nil {
				t.Fatal(err)
			}
		}
	}

	runUntilShipped := func(storage store.Store, batchSize int) {
		t.Helper()
		w, err := wal.Open(walPath)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()

		s, err := New(storage, w, checkpointStore, newTestBackoff(), Config{
			BatchSize: batchSize, PollInterval: 5 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		defer func() {
			cancel()
			<-done
		}()

		waitForCommittedEnd(t, checkpointStore, w.DurableEnd())
	}

	// Run 1: ship 1-2, then crash after ClickHouse accepted them.
	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	appendEvents(w, 1, 3)

	first, err := New(&crashAfterInsertStore{Store: ch}, w, checkpointStore, newTestBackoff(), Config{
		BatchSize: 2, PollInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	crash := make(chan any, 1)
	go func() {
		defer func() { crash <- recover() }()
		_ = first.Run(context.Background())
	}()
	select {
	case recovered := <-crash:
		if recovered == nil {
			t.Fatal("expected simulated crash")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for simulated crash")
	}

	// More records become durable before the restart.
	appendEvents(w, 4, 5)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Run 2: restart with a larger batch size.
	runUntilShipped(ch, 10)

	// Run 3: the checkpoint is lost, so the whole WAL is shipped again under new batch ranges.
	if err := checkpointStore.Commit(Checkpoint{}); err != nil {
		t.Fatal(err)
	}
	runUntilShipped(ch, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for seq := uint64(1); seq <= 5; seq++ {
		var count uint64
		err := conn.QueryRow(ctx,
			`SELECT count() FROM fairgate.events WHERE producer_id = ? AND seq = ?`,
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

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
