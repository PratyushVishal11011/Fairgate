package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"FairGate/internal/admit"
	"FairGate/internal/deduplication"
	"FairGate/internal/observability"
	"FairGate/internal/overload"
	"FairGate/internal/sched"
	"FairGate/internal/wal"
	"FairGate/internal/wire"
)

// A producer whose queue is at its high-water mark gets a retryable rejection before the WAL append,
// instead of a handler that waits for queue space without acknowledging. Identical retries of events
// that are already durable are still acknowledged, and other producers are unaffected.
func TestBackloggedQueueIsRejectedBeforeWALAppend(t *testing.T) {
	walPath := t.TempDir() + "/test.wal"
	walLog, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer walLog.Close()

	// No scheduler drains these queues, so they only fill.
	queues, err := sched.NewQueues(2)
	if err != nil {
		t.Fatal(err)
	}

	admission, err := admit.NewManager(100, 200)
	if err != nil {
		t.Fatal(err)
	}

	dedupManager, err := deduplication.NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}

	overloadCtl, err := overload.New(overload.Config{QueueHighWater: 1, ResumeRatio: 0.8}, queues, nil)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.New()
	metrics.RegisterOverload(overloadCtl)

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	if err := clientConn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		handleConn(context.Background(), serverConn, logger, admission, queues, walLog, metrics, dedupManager, overloadCtl, protocolLimits{})
	}()

	send := func(producerId string, seq uint64) wire.Ack {
		t.Helper()
		payload, err := json.Marshal(wire.Event{
			ProducerId:    producerId,
			ProducerEpoch: "epoch-1",
			EventTime:     time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC),
			Seq:           seq,
			EventType:     "test",
			Payload:       "payload",
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := wire.WriteFrame(clientConn, wire.Frame{Type: wire.FrameTypeEvent, Payload: payload}); err != nil {
			t.Fatalf("failed to send event: %v", err)
		}
		frame, err := wire.ReadFrame(clientConn)
		if err != nil {
			t.Fatalf("failed to read ACK: %v", err)
		}
		var ack wire.Ack
		if err := json.Unmarshal(frame.Payload, &ack); err != nil {
			t.Fatal(err)
		}
		return ack
	}

	for seq := uint64(1); seq <= 2; seq++ {
		if ack := send("busy", seq); ack.Status != ackAccepted {
			t.Fatalf("event %d: got %q/%q, want %q", seq, ack.Status, ack.Message, ackAccepted)
		}
	}

	ack := send("busy", 3)
	if ack.Status != ackRejected || ack.Message != overload.ReasonQueueBacklogged {
		t.Fatalf("event 3: got %q/%q, want %q/%q", ack.Status, ack.Message, ackRejected, overload.ReasonQueueBacklogged)
	}
	if ack.Seq != 3 {
		t.Fatalf("event 3: ACK seq %d, want 3", ack.Seq)
	}

	// A lost ACK must not turn into a rejection: the retry of a durable event is still accepted.
	if ack := send("busy", 2); ack.Status != ackAccepted || ack.Message != "duplicate_event_already_persisted" {
		t.Fatalf("retry of event 2: got %q/%q, want an accepted duplicate", ack.Status, ack.Message)
	}

	if ack := send("quiet", 1); ack.Status != ackAccepted {
		t.Fatalf("other producer: got %q/%q, want %q", ack.Status, ack.Message, ackAccepted)
	}

	server := httptest.NewServer(metrics.Handler())
	defer server.Close()
	body := httpGet(t, server.URL+"/metrics")
	for _, line := range []string{
		`fairgate_events_overloaded_total{reason="queue_backlogged"} 1`,
		`fairgate_events_overloaded_total{reason="storage_backlogged"} 0`,
		`fairgate_overload_active{signal="storage"} 0`,
		`fairgate_events_rejected_total 0`,
	} {
		if !strings.Contains(body, line) {
			t.Errorf("metrics output is missing %q", line)
		}
	}

	_ = clientConn.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not exit after client connection closed")
	}

	// The refused event never reached the WAL.
	if err := walLog.Close(); err != nil {
		t.Fatal(err)
	}
	events, err := wal.Recover(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("WAL holds %d events, want 3 (busy 1-2 and quiet 1)", len(events))
	}
	for _, event := range events {
		if event.ProducerId == "busy" && event.Seq == 3 {
			t.Fatal("the refused event was appended to the WAL")
		}
	}
}
