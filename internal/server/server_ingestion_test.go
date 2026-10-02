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
	"path/filepath"
	"testing"
	"time"
)

func TestTCPIngestionPersistsEventsToWAL(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")

	walLog, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}

	queues, err := sched.NewQueues(256)
	if err != nil {
		walLog.Close()
		t.Fatal(err)
	}

	admission, _ := admit.NewManager(100, 200)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		walLog.Close()
		t.Fatal(err)
	}
	defer listener.Close()

	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)

		conn, err := listener.Accept()
		if err != nil {
			return
		}

		handleConn(conn, logger, admission, queues, walLog)
	}()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		walLog.Close()
		t.Fatal(err)
	}

	// Prevent the test from hanging indefinitely if the server
	// does not send an acknowledgment.
	if err := clientConn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		clientConn.Close()
		walLog.Close()
		t.Fatal(err)
	}

	events := []wire.Event{
		{
			ProducerId: "producer-001",
			EventTime:  time.Now().UTC(),
			Seq:        1,
			Idx:        0,
			EventType:  "test_event",
			Payload:    "sample-event-1",
		},
		{
			ProducerId: "producer-001",
			EventTime:  time.Now().UTC(),
			Seq:        2,
			Idx:        0,
			EventType:  "test_event",
			Payload:    "sample-event-2",
		},
		{
			ProducerId: "producer-002",
			EventTime:  time.Now().UTC(),
			Seq:        1,
			Idx:        0,
			EventType:  "test_event",
			Payload:    "sample-event-3",
		},
	}

	for _, event := range events {
		payload, err := json.Marshal(event)
		if err != nil {
			clientConn.Close()
			walLog.Close()
			t.Fatal(err)
		}

		// Send the event frame.
		err = wire.WriteFrame(clientConn, wire.Frame{
			Type:    wire.FrameTypeEvent,
			Payload: payload,
		})
		if err != nil {
			clientConn.Close()
			walLog.Close()
			t.Fatalf("failed to send event: %v", err)
		}

		// Read the acknowledgment.
		ackFrame, err := wire.ReadFrame(clientConn)
		if err != nil {
			clientConn.Close()
			walLog.Close()
			t.Fatalf("failed to read ACK: %v", err)
		}

		if ackFrame.Type != wire.FrameTypeAck {
			clientConn.Close()
			walLog.Close()
			t.Fatalf(
				"expected ACK frame type %d, got %d",
				wire.FrameTypeAck,
				ackFrame.Type,
			)
		}

		var ack wire.Ack
		if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
			clientConn.Close()
			walLog.Close()
			t.Fatalf("failed to decode ACK: %v", err)
		}

		if ack.Status != "accepted" {
			clientConn.Close()
			walLog.Close()
			t.Fatalf("expected accepted status, got %q", ack.Status)
		}

		if ack.ProducerId != event.ProducerId {
			clientConn.Close()
			walLog.Close()
			t.Fatalf(
				"expected producer ID %q, got %q",
				event.ProducerId,
				ack.ProducerId,
			)
		}

		if ack.Seq != event.Seq {
			clientConn.Close()
			walLog.Close()
			t.Fatalf(
				"expected sequence %d, got %d",
				event.Seq,
				ack.Seq,
			)
		}

		if ack.Idx != event.Idx {
			clientConn.Close()
			walLog.Close()
			t.Fatalf(
				"expected index %d, got %d",
				event.Idx,
				ack.Idx,
			)
		}
	}

	// Closing the client allows handleConn to exit.
	clientConn.Close()
	<-serverDone

	// Close the WAL before recovering it.
	if err := walLog.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := wal.Recover(walPath)
	if err != nil {
		t.Fatalf("failed to recover WAL: %v", err)
	}

	if len(recovered) != len(events) {
		t.Fatalf(
			"expected %d recovered events, got %d",
			len(events),
			len(recovered),
		)
	}

	// Verify that all expected events were persisted.
	for i, expected := range events {
		actual := recovered[i]

		if actual.ProducerId != expected.ProducerId {
			t.Errorf(
				"event %d: expected producer %q, got %q",
				i,
				expected.ProducerId,
				actual.ProducerId,
			)
		}

		if actual.Seq != expected.Seq {
			t.Errorf(
				"event %d: expected seq %d, got %d",
				i,
				expected.Seq,
				actual.Seq,
			)
		}

		if actual.Idx != expected.Idx {
			t.Errorf(
				"event %d: expected idx %d, got %d",
				i,
				expected.Idx,
				actual.Idx,
			)
		}

		if actual.EventType != expected.EventType {
			t.Errorf(
				"event %d: expected event type %q, got %q",
				i,
				expected.EventType,
				actual.EventType,
			)
		}

		if actual.Payload != expected.Payload {
			t.Errorf(
				"event %d: expected payload %q, got %q",
				i,
				expected.Payload,
				actual.Payload,
			)
		}
	}
}
