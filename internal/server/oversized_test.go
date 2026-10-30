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
	"FairGate/internal/sched"
	"FairGate/internal/wal"
	"FairGate/internal/wire"
)

// An event between the WAL record limit (1 MiB) and the frame limit (5 MiB) is a client error:
// it must be rejected as invalid without closing the connection or counting a WAL failure.
func TestOversizedEventIsInvalidNotWALFailure(t *testing.T) {
	walLog, err := wal.Open(t.TempDir() + "/test.wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walLog.Close()

	queues, err := sched.NewQueues(10)
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

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.New()

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
		handleConn(context.Background(), serverConn, logger, admission, queues, walLog, metrics, dedupManager, nil)
	}()

	send := func(event wire.Event) wire.Ack {
		t.Helper()
		payload, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if err := wire.WriteFrame(clientConn, wire.Frame{Type: wire.FrameTypeEvent, Payload: payload}); err != nil {
			t.Fatalf("failed to send event: %v", err)
		}
		ackFrame, err := wire.ReadFrame(clientConn)
		if err != nil {
			t.Fatalf("failed to read ACK: %v", err)
		}
		var ack wire.Ack
		if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
			t.Fatalf("failed to decode ACK: %v", err)
		}
		return ack
	}

	event := wire.Event{
		ProducerId:    "producer-oversized",
		ProducerEpoch: "epoch-001",
		EventTime:     time.Now().UTC(),
		Seq:           1,
		EventType:     "test_event",
		Payload:       strings.Repeat("x", 3<<19), // 1.5 MiB
	}

	ack := send(event)
	if ack.Status != ackInvalid || ack.Message != "payload_too_large" {
		t.Fatalf("oversized event: got %q/%q, want %q/%q", ack.Status, ack.Message, ackInvalid, "payload_too_large")
	}
	if ack.Seq != event.Seq {
		t.Fatalf("oversized event: ACK seq %d, want %d", ack.Seq, event.Seq)
	}

	//the connection stays open and the WAL stays healthy.
	event.Seq = 2
	event.Payload = "small"
	if ack := send(event); ack.Status != ackAccepted {
		t.Fatalf("event after oversized one: got %q/%q, want %q", ack.Status, ack.Message, ackAccepted)
	}

	server := httptest.NewServer(metrics.Handler())
	defer server.Close()
	body := httpGet(t, server.URL+"/metrics")
	for _, line := range []string{
		"fairgate_wal_append_errors_total 0",
		"fairgate_events_invalid_total 1",
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
}
