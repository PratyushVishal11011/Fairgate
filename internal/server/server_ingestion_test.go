package server

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"FairGate/internal/admit"
	"FairGate/internal/sched"
	"FairGate/internal/wal"
	"FairGate/internal/wire"
)

func TestTCPIngestionPersistsEventsToWAL(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "fairgate.wal")

	walLog, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = walLog.Close()
		t.Fatalf("failed to start TCP listener: %v", err)
	}
	defer listener.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	admission, err := admit.NewManager(100, 200)
	if err != nil {
		_ = walLog.Close()
		t.Fatalf("failed to create admission manager: %v", err)
	}

	queues, err := sched.NewQueues(256)
	if err != nil {
		_ = walLog.Close()
		t.Fatalf("failed to create queues: %v", err)
	}

	handlerDone := make(chan struct{})

	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}

		handleConn(conn, logger, admission, queues, walLog)
		close(handlerDone)
	}()

	client, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
	if err != nil {
		_ = walLog.Close()
		t.Fatalf("failed to connect to server: %v", err)
	}

	events := []wire.Event{
		{
			ProducerId: "producer-001",
			EventTime:  time.Now().UTC(),
			Seq:        1,
			Idx:        0,
			EventType:  "bearing_fault",
			Payload:    "sample-event-1",
		},
		{
			ProducerId: "producer-001",
			EventTime:  time.Now().UTC(),
			Seq:        2,
			Idx:        0,
			EventType:  "bearing_fault",
			Payload:    "sample-event-2",
		},
		{
			ProducerId: "producer-002",
			EventTime:  time.Now().UTC(),
			Seq:        1,
			Idx:        0,
			EventType:  "bearing_fault",
			Payload:    "sample-event-3",
		},
	}

	for _, event := range events {
		payload, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			_ = client.Close()
			_ = walLog.Close()
			t.Fatalf("failed to marshal event: %v", marshalErr)
		}

		// Frame format: 4-byte length, 1-byte type,
		// 1-byte flags, followed by JSON payload.
		body := make([]byte, 2+len(payload))
		body[0] = 1
		body[1] = 0
		copy(body[2:], payload)

		frame := make([]byte, 4+len(body))
		binary.BigEndian.PutUint32(frame[:4], uint32(len(body)))
		copy(frame[4:], body)

		if _, err := client.Write(frame); err != nil {
			_ = client.Close()
			_ = walLog.Close()
			t.Fatalf("failed to send event: %v", err)
		}
	}

	if err := client.Close(); err != nil {
		_ = walLog.Close()
		t.Fatalf("failed to close client connection: %v", err)
	}

	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		_ = walLog.Close()
		t.Fatal("timed out waiting for connection handler to finish")
	}

	if err := walLog.Close(); err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	recovered, err := wal.Recover(walPath)
	if err != nil {
		t.Fatalf("failed to recover WAL: %v", err)
	}

	if len(recovered) != len(events) {
		t.Fatalf("expected %d recovered events, got %d", len(events), len(recovered))
	}

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
				"event %d: expected sequence %v, got %v",
				i,
				expected.Seq,
				actual.Seq,
			)
		}

		if !actual.EventTime.Equal(expected.EventTime) {
			t.Errorf("event %d: event time does not match", i)
		}
	}
}
