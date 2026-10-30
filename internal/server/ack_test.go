package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"FairGate/internal/admit"
	"FairGate/internal/deduplication"
	"FairGate/internal/observability"
	"FairGate/internal/sched"
	"FairGate/internal/wal"
	"FairGate/internal/wire"
)

func TestAcknowledgment(t *testing.T) {
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
			protocolLimits{},
		)
	}()

	event := wire.Event{
		ProducerId:    "producer-ack-test",
		ProducerEpoch: "epoch-001",
		EventTime:     time.Now().UTC(),
		Seq:           1,
		Idx:           0,
		EventType:     "test_event",
		Payload:       "ack-test",
	}

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}

	if err := wire.WriteFrame(clientConn, wire.Frame{
		Type:    wire.FrameTypeEvent,
		Payload: payload,
	}); err != nil {
		t.Fatalf("failed to send event: %v", err)
	}

	ackFrame, err := wire.ReadFrame(clientConn)
	if err != nil {
		t.Fatalf("failed to read ACK: %v", err)
	}

	if ackFrame.Type != wire.FrameTypeAck {
		t.Fatalf(
			"expected ACK frame type %d, got %d",
			wire.FrameTypeAck,
			ackFrame.Type,
		)
	}

	var ack wire.Ack
	if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
		t.Fatalf("failed to decode ACK: %v", err)
	}

	if ack.Status != "accepted" {
		t.Fatalf("expected status 'accepted', got %q", ack.Status)
	}

	if ack.ProducerId != event.ProducerId {
		t.Fatalf(
			"expected producer ID %q, got %q",
			event.ProducerId,
			ack.ProducerId,
		)
	}

	if ack.Seq != event.Seq {
		t.Fatalf("expected sequence %d, got %d", event.Seq, ack.Seq)
	}

	if ack.Idx != event.Idx {
		t.Fatalf("expected index %d, got %d", event.Idx, ack.Idx)
	}

	_ = clientConn.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not exit after client connection closed")
	}
}
