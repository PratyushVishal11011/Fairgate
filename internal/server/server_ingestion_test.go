package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"FairGate/internal/admit"
	"FairGate/internal/deduplication"
	"FairGate/internal/observability"
	"FairGate/internal/sched"
	"FairGate/internal/wal"
	"FairGate/internal/wire"
)

func TestTCPIngestionPersistsEventsToWAL(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")

	walLog, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}

	queues, err := sched.NewQueues(256)
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

	metrics := observability.New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = walLog.Close()
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

		handleConn(
			context.Background(),
			conn,
			logger,
			admission,
			queues,
			walLog,
			metrics,
			dedupManager,
			nil,
		)
	}()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		_ = walLog.Close()
		t.Fatal(err)
	}

	if err := clientConn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		_ = clientConn.Close()
		_ = walLog.Close()
		t.Fatal(err)
	}

	events := []wire.Event{
		{
			ProducerId:    "producer-001",
			ProducerEpoch: "epoch-001",
			EventTime:     time.Now().UTC(),
			Seq:           1,
			Idx:           0,
			EventType:     "test_event",
			Payload:       "sample-event-1",
		},
		{
			ProducerId:    "producer-001",
			ProducerEpoch: "epoch-001",
			EventTime:     time.Now().UTC(),
			Seq:           2,
			Idx:           0,
			EventType:     "test_event",
			Payload:       "sample-event-2",
		},
		{
			ProducerId:    "producer-002",
			ProducerEpoch: "epoch-001",
			EventTime:     time.Now().UTC(),
			Seq:           1,
			Idx:           0,
			EventType:     "test_event",
			Payload:       "sample-event-3",
		},
	}

	for _, event := range events {
		payload, err := json.Marshal(event)
		if err != nil {
			_ = clientConn.Close()
			_ = walLog.Close()
			t.Fatal(err)
		}

		err = wire.WriteFrame(clientConn, wire.Frame{
			Type:    wire.FrameTypeEvent,
			Payload: payload,
		})
		if err != nil {
			_ = clientConn.Close()
			_ = walLog.Close()
			t.Fatalf("failed to send event: %v", err)
		}

		ackFrame, err := wire.ReadFrame(clientConn)
		if err != nil {
			_ = clientConn.Close()
			_ = walLog.Close()
			t.Fatalf("failed to read ACK: %v", err)
		}

		if ackFrame.Type != wire.FrameTypeAck {
			_ = clientConn.Close()
			_ = walLog.Close()
			t.Fatalf(
				"expected ACK frame type %d, got %d",
				wire.FrameTypeAck,
				ackFrame.Type,
			)
		}

		var ack wire.Ack
		if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
			_ = clientConn.Close()
			_ = walLog.Close()
			t.Fatalf("failed to decode ACK: %v", err)
		}

		if ack.Status != "accepted" {
			_ = clientConn.Close()
			_ = walLog.Close()
			t.Fatalf("expected accepted status, got %q", ack.Status)
		}

		if ack.ProducerId != event.ProducerId {
			_ = clientConn.Close()
			_ = walLog.Close()
			t.Fatalf(
				"expected producer ID %q, got %q",
				event.ProducerId,
				ack.ProducerId,
			)
		}

		if ack.Seq != event.Seq {
			_ = clientConn.Close()
			_ = walLog.Close()
			t.Fatalf(
				"expected sequence %d, got %d",
				event.Seq,
				ack.Seq,
			)
		}

		if ack.Idx != event.Idx {
			_ = clientConn.Close()
			_ = walLog.Close()
			t.Fatalf(
				"expected index %d, got %d",
				event.Idx,
				ack.Idx,
			)
		}
	}

	_ = clientConn.Close()
	<-serverDone

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

		if actual.ProducerEpoch != expected.ProducerEpoch {
			t.Errorf(
				"event %d: expected producer epoch %q, got %q",
				i,
				expected.ProducerEpoch,
				actual.ProducerEpoch,
			)
		}

		if !actual.EventTime.Equal(expected.EventTime) {
			t.Errorf(
				"event %d: expected event time %v, got %v",
				i,
				expected.EventTime,
				actual.EventTime,
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

	// Verify that all accepted events reached their respective queues.
	expectedByProducer := map[string][]wire.Event{
		"producer-001": {events[0], events[1]},
		"producer-002": {events[2]},
	}

	for producerID, expectedEvents := range expectedByProducer {
		queue := queues.Queue(producerID)

		for i, expected := range expectedEvents {
			select {
			case actual := <-queue:
				if actual.ProducerId != expected.ProducerId ||
					actual.ProducerEpoch != expected.ProducerEpoch ||
					actual.Seq != expected.Seq ||
					actual.Idx != expected.Idx ||
					actual.Payload != expected.Payload {
					t.Errorf(
						"producer %s, event %d: unexpected queued event: %+v",
						producerID,
						i,
						actual,
					)
				}
			default:
				t.Fatalf(
					"producer %s: expected %d queued events; queue became empty at event %d",
					producerID,
					len(expectedEvents),
					i,
				)
			}
		}

		select {
		case extra := <-queue:
			t.Errorf(
				"producer %s: unexpected extra queued event: %+v",
				producerID,
				extra,
			)
		default:
		}
	}
}
