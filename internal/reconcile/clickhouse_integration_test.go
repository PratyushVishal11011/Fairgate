package reconcile

import (
	"FairGate/internal/loadgen"
	"FairGate/internal/store"
	"FairGate/internal/wire"
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestClickHouseSourceIntegration stores events through the gateway's adapter and reconciles them.
// It runs only when CLICKHOUSE_ADDR is set.
func TestClickHouseSourceIntegration(t *testing.T) {
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("set CLICKHOUSE_ADDR to run ClickHouse integration tests")
	}
	env := func(key, fallback string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return fallback
	}
	database := env("CLICKHOUSE_DATABASE", "fairgate")
	username := env("CLICKHOUSE_USER", "fairgate")
	password := env("CLICKHOUSE_PASSWORD", "fairgate_dev_password")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	adapter, err := store.NewClickHouseStore([]string{addr}, database, username, password)
	if err != nil {
		t.Fatalf("connect adapter: %v", err)
	}
	defer adapter.Close()
	src, err := OpenClickHouse([]string{addr}, database, username, password, DefaultTable)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer src.Close()

	epoch := fmt.Sprintf("reconcile-test-%d", time.Now().UnixNano())
	producer := "reconcile-it-" + epoch
	manifest := &loadgen.Manifest{
		Version:      loadgen.ManifestVersion,
		Epoch:        epoch,
		EventType:    "loadgen",
		PayloadBytes: 48,
		Producers:    []loadgen.ManifestProducer{{ID: producer, Accepted: loadgen.Ranges{{1, 5}}}},
	}
	var events []wire.Event
	for seq := uint64(1); seq <= 4; seq++ {
		events = append(events, wire.Event{
			ProducerId:    producer,
			ProducerEpoch: epoch,
			EventTime:     time.Now().UTC().Truncate(time.Millisecond),
			Seq:           seq,
			EventType:     "loadgen",
			Payload:       loadgen.Payload(producer, epoch, seq, 48),
		})
	}
	if err := adapter.InsertBatch(ctx, events, "reconcile-"+epoch); err != nil {
		t.Fatalf("insert: %v", err)
	}

	report, err := Run(ctx, src, manifest, Options{VerifyContent: true})
	if err != nil {
		t.Fatal(err)
	}
	if report.Passed || report.Missing != 1 || report.StoredIdentities != 4 || report.Mismatched != 0 {
		t.Fatalf("expected seq 5 missing and nothing else, got %+v", report)
	}
	if got := report.Producers[0].Missing.String(); got != "5" {
		t.Fatalf("missing ranges %q", got)
	}
}
