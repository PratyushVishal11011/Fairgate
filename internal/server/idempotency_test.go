package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"path/filepath"
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

func TestTCPIngestionIdempotency(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "idempotency.wal")

	walLog, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}

	queues, err := sched.NewQueues(10)
	if err != nil {
		_ = walLog.Close()
		t.Fatal(err)
	}

	admission, err := admit.NewManager(100, 200)
	if err != nil {
		_ = walLog.Close()
		t.Fatal(err)
	}

	dedupManager, err := deduplication.NewManager(nil)
	if err != nil {
		_ = walLog.Close()
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.New()

	serverConn, clientConn := net.Pipe()

	done := make(chan struct{})

	go func() {
		defer close(done)
		handleConn(
			context.Background(),
			serverConn,
			logger,
			admission,
			queues,
			walLog,
			metrics,
			dedupManager,
			nil,
		)
	}()

	defer func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
		<-done
		if err := walLog.Close(); err != nil {
			t.Errorf("failed to close WAL: %v", err)
		}
	}()

	if err := clientConn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	event := wire.Event{
		ProducerId:    "producer-idempotency",
		ProducerEpoch: "epoch-001",
		EventTime:     time.Now().UTC(),
		Seq:           1,
		Idx:           0,
		EventType:     "test_event",
		Payload:       "original-payload",
	}

	sendEvent := func(event wire.Event) wire.Ack {
		t.Helper()

		payload, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("failed to marshal event: %v", err)
		}

		if err := wire.WriteFrame(clientConn, wire.Frame{
			Type:    wire.FrameTypeEvent,
			Payload: payload,
		}); err != nil {
			t.Fatalf("failed to send event: %v", err)
		}

		frame, err := wire.ReadFrame(clientConn)
		if err != nil {
			t.Fatalf("failed to read ACK: %v", err)
		}

		if frame.Type != wire.FrameTypeAck {
			t.Fatalf("expected ACK frame, got type %d", frame.Type)
		}

		var ack wire.Ack
		if err := json.Unmarshal(frame.Payload, &ack); err != nil {
			t.Fatalf("failed to decode ACK: %v", err)
		}

		return ack
	}

	// The first submission should be accepted and persisted.
	firstAck := sendEvent(event)
	if firstAck.Status != "accepted" {
		t.Fatalf("expected first event to be accepted, got %q", firstAck.Status)
	}

	// Retrying the same identity and contents should be acknowledged
	// without appending another WAL record or enqueueing another event.
	duplicateAck := sendEvent(event)
	if duplicateAck.Status != "accepted" {
		t.Fatalf(
			"expected duplicate event to be acknowledged, got %q",
			duplicateAck.Status,
		)
	}

	// Reusing the identity with different contents must be rejected.
	conflictingEvent := event
	conflictingEvent.Payload = "different-payload"

	conflictAck := sendEvent(conflictingEvent)
	if conflictAck.Status == "accepted" {
		t.Fatal("expected conflicting event contents to be rejected")
	}

	if !strings.Contains(
		strings.ToLower(conflictAck.Message),
		"conflict",
	) {
		t.Errorf(
			"expected conflict information in ACK message, got %q",
			conflictAck.Message,
		)
	}

	// Close the connection so the handler exits before closing the WAL.
	_ = clientConn.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not exit")
	}

	if err := walLog.Close(); err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	// Recover the WAL and verify that the original event was persisted once.
	recovered, err := wal.Recover(walPath)
	if err != nil {
		t.Fatalf("failed to recover WAL: %v", err)
	}

	if len(recovered) != 1 {
		t.Fatalf("expected 1 WAL event, got %d", len(recovered))
	}

	if recovered[0].Payload != event.Payload {
		t.Fatalf(
			"expected original payload %q, got %q",
			event.Payload,
			recovered[0].Payload,
		)
	}

	// Verify that the event was enqueued exactly once.
	queue := queues.Queue(event.ProducerId)

	select {
	case queued := <-queue:
		if queued.ProducerEpoch != event.ProducerEpoch ||
			queued.Seq != event.Seq ||
			queued.Idx != event.Idx ||
			queued.Payload != event.Payload {
			t.Fatalf("unexpected queued event: %+v", queued)
		}
	default:
		t.Fatal("expected original event in producer queue")
	}

	select {
	case extra := <-queue:
		t.Fatalf("unexpected duplicate event in queue: %+v", extra)
	default:
	}
}
