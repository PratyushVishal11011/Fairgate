package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"strconv"
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

type protocolHarness struct {
	client  net.Conn
	metrics *observability.Metrics
	done    chan struct{}
}

// startProtocolHandler runs one connection handler with the given limits over an in-memory pipe.
func startProtocolHandler(t *testing.T, limits protocolLimits) *protocolHarness {
	t.Helper()
	walLog, err := wal.Open(t.TempDir() + "/test.wal")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = walLog.Close() })
	return startProtocolHandlerWithWAL(t, limits, walLog)
}

// startProtocolHandlerWithWAL runs one connection handler appending to walLog.
func startProtocolHandlerWithWAL(t *testing.T, limits protocolLimits, walLog *wal.WAL) *protocolHarness {
	t.Helper()
	queues, err := sched.NewQueues(100)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := admit.NewManager(1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	dedupManager, err := deduplication.NewManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	h := &protocolHarness{metrics: observability.New(), done: make(chan struct{})}
	serverConn, client := net.Pipe()
	h.client = client
	go func() {
		defer close(h.done)
		handleConn(context.Background(), serverConn, logger, admission, queues, walLog, h.metrics, dedupManager, nil, limits)
	}()
	t.Cleanup(func() {
		_ = client.Close()
		<-h.done
	})
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	return h
}

func (h *protocolHarness) writeRaw(t *testing.T, data []byte) {
	t.Helper()
	if _, err := h.client.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// frameHeader returns a length prefix followed by the type and flags bytes.
func frameHeader(length uint32, frameType byte) []byte {
	header := make([]byte, 6)
	binary.BigEndian.PutUint32(header, length)
	header[4] = frameType
	return header
}

func (h *protocolHarness) readAck(t *testing.T) wire.Ack {
	t.Helper()
	frame, err := wire.ReadFrame(h.client)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	var ack wire.Ack
	if err := json.Unmarshal(frame.Payload, &ack); err != nil {
		t.Fatal(err)
	}
	return ack
}

func (h *protocolHarness) sendEvent(t *testing.T, producer string, seq uint64) wire.Ack {
	t.Helper()
	payload, _ := json.Marshal(wire.Event{
		ProducerId:    producer,
		ProducerEpoch: "1",
		EventTime:     time.Now().UTC(),
		Seq:           seq,
		EventType:     "protocol_test",
	})
	if err := wire.WriteFrame(h.client, wire.Frame{Type: wire.FrameTypeEvent, Payload: payload}); err != nil {
		t.Fatalf("write event: %v", err)
	}
	return h.readAck(t)
}

// expectClosed checks that the handler exits and the client sees the connection end.
func (h *protocolHarness) expectClosed(t *testing.T) {
	t.Helper()
	select {
	case <-h.done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not close the connection")
	}
	if _, err := h.client.Read(make([]byte, 1)); !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("expected the connection to be closed, got %v", err)
	}
}

func (h *protocolHarness) expectProtocolErrors(t *testing.T, reason string, want int) {
	t.Helper()
	server := httptest.NewServer(h.metrics.Handler())
	defer server.Close()
	resp, err := server.Client().Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	line := `fairgate_protocol_errors_total{reason="` + reason + `"} `
	for _, l := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(l, line) {
			if got := strings.TrimPrefix(l, line); got != strconv.Itoa(want) {
				t.Fatalf("%s = %s, want %d", reason, got, want)
			}
			return
		}
	}
	t.Fatalf("metric for %s not found", reason)
}

func expectAck(t *testing.T, ack wire.Ack, status, message string) {
	t.Helper()
	if ack.Status != status || ack.Message != message {
		t.Fatalf("got ack %s/%s, want %s/%s", ack.Status, ack.Message, status, message)
	}
}

func TestUnsupportedFrameTypeIsAnsweredAndSkipped(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{})
	h.writeRaw(t, append(frameHeader(5, wire.FrameTypeAck), []byte("abc")...))
	expectAck(t, h.readAck(t), ackInvalid, ackUnsupportedFrameType)

	//the stream is still in step: the next event is handled normally.
	expectAck(t, h.sendEvent(t, "p", 1), ackAccepted, "Event successfully enqueued")
	h.expectProtocolErrors(t, observability.ProtocolUnsupportedFrameType, 1)
}

func TestOversizedFrameIsSkippedAndConnectionKept(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{maxFrameBytes: 1024})
	body := make([]byte, 3000)
	h.writeRaw(t, append(frameHeader(uint32(2+len(body)), wire.FrameTypeEvent), body...))
	expectAck(t, h.readAck(t), ackInvalid, ackFrameTooLarge)

	expectAck(t, h.sendEvent(t, "p", 1), ackAccepted, "Event successfully enqueued")
	h.expectProtocolErrors(t, observability.ProtocolFrameTooLarge, 1)
}

func TestFrameFarOverTheLimitClosesConnection(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{maxFrameBytes: 1024})
	//claims 1 GiB; the gateway neither allocates nor reads it.
	h.writeRaw(t, frameHeader(1<<30, wire.FrameTypeEvent))
	expectAck(t, h.readAck(t), ackInvalid, ackFrameTooLarge)
	h.expectClosed(t)
	h.expectProtocolErrors(t, observability.ProtocolFrameTooLarge, 1)
}

func TestMalformedLengthClosesConnection(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{})
	h.writeRaw(t, []byte{0, 0, 0, 1, 1})
	expectAck(t, h.readAck(t), ackInvalid, ackMalformedFrame)
	h.expectClosed(t)
	h.expectProtocolErrors(t, observability.ProtocolMalformedFrame, 1)
}

func TestStalledFrameTimesOut(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{frameTimeout: 100 * time.Millisecond})
	//announce 100 bytes and send only the header and a few of them.
	h.writeRaw(t, append(frameHeader(100, wire.FrameTypeEvent), []byte("{\"pro")...))
	h.expectClosed(t)
	h.expectProtocolErrors(t, observability.ProtocolFrameTimeout, 1)
}

func TestStalledLengthPrefixTimesOut(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{frameTimeout: 100 * time.Millisecond})
	h.writeRaw(t, []byte{0, 0})
	h.expectClosed(t)
	h.expectProtocolErrors(t, observability.ProtocolFrameTimeout, 1)
}

func TestIdleTimeout(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{idleTimeout: 100 * time.Millisecond})
	expectAck(t, h.sendEvent(t, "p", 1), ackAccepted, "Event successfully enqueued")
	h.expectClosed(t)
	h.expectProtocolErrors(t, observability.ProtocolIdleTimeout, 1)
}

func TestIdleConnectionsStayOpenWithoutIdleTimeout(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{frameTimeout: 50 * time.Millisecond})
	time.Sleep(200 * time.Millisecond)
	select {
	case <-h.done:
		t.Fatal("an idle connection was closed with idle_timeout 0")
	default:
	}
	expectAck(t, h.sendEvent(t, "p", 1), ackAccepted, "Event successfully enqueued")
}

func TestClientThatStopsReadingAcksIsDisconnected(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{writeTimeout: 100 * time.Millisecond})
	payload, _ := json.Marshal(wire.Event{
		ProducerId: "p", ProducerEpoch: "1", EventTime: time.Now().UTC(), Seq: 1, EventType: "protocol_test",
	})
	//net.Pipe has no buffer, so an ACK nobody reads blocks until the write deadline.
	if err := wire.WriteFrame(h.client, wire.Frame{Type: wire.FrameTypeEvent, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler stayed blocked on an unread ACK")
	}
	h.expectProtocolErrors(t, observability.ProtocolWriteTimeout, 1)
}

func TestConnectionEndingInsideAFrameIsCounted(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{})
	h.writeRaw(t, append(frameHeader(100, wire.FrameTypeEvent), []byte("{\"pro")...))
	_ = h.client.Close()
	<-h.done
	h.expectProtocolErrors(t, observability.ProtocolTruncatedFrame, 1)
}

func TestOverlongIdentifiersAreInvalid(t *testing.T) {
	h := startProtocolHandler(t, protocolLimits{})
	expectAck(t, h.sendEvent(t, strings.Repeat("p", wire.MaxIdentifierLength+1), 1), ackInvalid, "invalid_payload")
	expectAck(t, h.sendEvent(t, strings.Repeat("p", wire.MaxIdentifierLength), 1), ackAccepted, "Event successfully enqueued")
}
