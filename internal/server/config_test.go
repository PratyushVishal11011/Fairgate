package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"FairGate/internal/config"
	"FairGate/internal/shipper"
	"FairGate/internal/store"
	"FairGate/internal/wire"
)

type countingStore struct {
	mu     sync.Mutex
	events int
}

func (s *countingStore) InsertBatch(_ context.Context, events []wire.Event, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events += len(events)
	return nil
}

func (s *countingStore) Close() error {
	return nil
}

func (s *countingStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.events
}

func TestRunContextUsesConfiguredSettings(t *testing.T) {
	dir := t.TempDir()

	cfg := config.Default()
	cfg.Server.ListenAddr = testServerAddress(t)
	cfg.Server.MetricsAddr = "127.0.0.1:0"
	cfg.WAL.Path = filepath.Join(dir, "wal", "events.wal")
	// Small enough that the events below span several segments.
	cfg.WAL.SegmentSize = 512
	cfg.Shipper.CheckpointPath = filepath.Join(dir, "state", "checkpoint.json")
	cfg.Shipper.BatchSize = 3
	cfg.Shipper.PollInterval = 10 * time.Millisecond

	storage := &countingStore{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runContext(ctx, cfg, logger, func() (store.Store, error) {
			return storage, nil
		})
	}()

	var conn net.Conn
	deadline := time.Now().Add(5 * time.Second)
	for conn == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not listen on the configured address")
		}
		conn, _ = net.DialTimeout("tcp", cfg.Server.ListenAddr, 100*time.Millisecond)
		if conn == nil {
			time.Sleep(20 * time.Millisecond)
		}
	}
	defer conn.Close()

	const total = 12
	for seq := uint64(1); seq <= total; seq++ {
		payload, err := json.Marshal(wire.Event{
			ProducerId:    "config-test",
			ProducerEpoch: "epoch-1",
			EventTime:     time.Now().UTC(),
			Seq:           seq,
			EventType:     "test",
			Payload:       "payload",
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := wire.WriteFrame(conn, wire.Frame{Type: wire.FrameTypeEvent, Payload: payload}); err != nil {
			t.Fatal(err)
		}
		frame, err := wire.ReadFrame(conn)
		if err != nil {
			t.Fatal(err)
		}
		var ack wire.Ack
		if err := json.Unmarshal(frame.Payload, &ack); err != nil {
			t.Fatal(err)
		}
		if ack.Status != ackAccepted {
			t.Fatalf("event %d not accepted: %+v", seq, ack)
		}
	}

	checkpoints := shipper.NewCheckpointStore(cfg.Shipper.CheckpointPath)
	for storage.count() < total {
		if time.Now().After(deadline.Add(5 * time.Second)) {
			t.Fatalf("shipper delivered %d of %d events", storage.count(), total)
		}
		time.Sleep(10 * time.Millisecond)
	}

	_ = conn.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server returned an error: %v", err)
		}
	case <-time.After(shutdownGracePeriod() + 3*time.Second):
		t.Fatal("server did not stop")
	}

	checkpoint, err := checkpoints.Load()
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Segment == 0 {
		t.Fatalf("expected the configured segment size to rotate the WAL, checkpoint %+v", checkpoint)
	}
	// Reclamation removes shipped segments, so check that the remaining ones live in the configured directory.
	segments, err := filepath.Glob(cfg.WAL.Path + "*")
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) == 0 {
		t.Fatalf("no WAL segments under the configured path %s", cfg.WAL.Path)
	}
}
