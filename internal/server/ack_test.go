package server

import (
	"FairGate/internal/admit"
	"FairGate/internal/sched"
	"FairGate/internal/wal"
	"FairGate/internal/wire"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
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

	admission, _ := admit.NewManager(100, 200)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})

	go func() {
		defer close(done)
		handleConn(serverConn, logger, admission, queues, walLog)
	}()

	event := wire.Event{
		ProducerId: "producer-ack-test",
		EventTime:  time.Now().UTC(),
		Seq:        1,
		Idx:        0,
		EventType:  "test_event",
		Payload:    "ack-test",
	}

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}

	// Send event to the server.
	if err := wire.WriteFrame(clientConn, wire.Frame{
		Type:    wire.FrameTypeEvent,
		Payload: payload,
	}); err != nil {
		t.Fatalf("failed to send event: %v", err)
	}

	// Read the ACK from the server.
	ackFrame, err := wire.ReadFrame(clientConn)
	if err != nil {
		t.Fatalf("failed to read ACK: %v", err)
	}

	if ackFrame.Type != wire.FrameTypeAck {
		t.Fatalf("expected ACK frame type %d, got %d",
			wire.FrameTypeAck, ackFrame.Type)
	}

	var ack wire.Ack
	if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
		t.Fatalf("failed to decode ACK: %v", err)
	}

	if ack.Status != "accepted" {
		t.Fatalf("expected status 'accepted', got %q", ack.Status)
	}

	if ack.ProducerId != event.ProducerId {
		t.Fatalf("expected producer ID %q, got %q",
			event.ProducerId, ack.ProducerId)
	}

	if ack.Seq != event.Seq {
		t.Fatalf("expected sequence %d, got %d",
			event.Seq, ack.Seq)
	}

	if ack.Idx != event.Idx {
		t.Fatalf("expected index %d, got %d",
			event.Idx, ack.Idx)
	}

	// Close the connection so handleConn can finish.
	clientConn.Close()
	<-done
}
