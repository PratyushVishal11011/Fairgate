package loadgen

import (
	"FairGate/internal/wire"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type eventKey struct {
	producer string
	seq      uint64
}

// fakeGateway answers event frames like FairGate does, with scripted faults:
// it rejects every rejectEvery-th frame, and drops the connection without an ACK after
// persisting every dropEvery-th frame, so the producer's retry is answered as a duplicate.
type fakeGateway struct {
	listener    net.Listener
	rejectEvery int
	dropEvery   int

	mu     sync.Mutex
	frames int
	hellos int
	stored map[eventKey]wire.Event
}

func startFakeGateway(t *testing.T, rejectEvery, dropEvery int) *fakeGateway {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &fakeGateway{listener: listener, rejectEvery: rejectEvery, dropEvery: dropEvery, stored: map[eventKey]wire.Event{}}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go g.serve(conn)
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return g
}

func (g *fakeGateway) serve(conn net.Conn) {
	defer conn.Close()
	for {
		frame, err := wire.ReadFrame(conn)
		if err != nil {
			return
		}
		if frame.Type == wire.FrameTypeHello {
			g.mu.Lock()
			g.hellos++
			g.mu.Unlock()
			payload, _ := json.Marshal(wire.Welcome{Version: wire.ProtocolVersion})
			if wire.WriteFrame(conn, wire.Frame{Type: wire.FrameTypeWelcome, Payload: payload}) != nil {
				return
			}
			continue
		}
		event, err := wire.DecodeEvent(frame.Payload)
		if err != nil {
			if !g.ack(conn, wire.Ack{Status: "invalid", Message: "invalid_payload"}) {
				return
			}
			continue
		}

		g.mu.Lock()
		g.frames++
		n := g.frames
		key := eventKey{event.ProducerId, event.Seq}
		previous, duplicate := g.stored[key]
		reject := g.rejectEvery > 0 && n%g.rejectEvery == 0 && !duplicate
		drop := g.dropEvery > 0 && n%g.dropEvery == 0 && !duplicate
		if !reject && !duplicate {
			g.stored[key] = event
		}
		g.mu.Unlock()

		ack := wire.Ack{ProducerId: event.ProducerId, Seq: event.Seq, Idx: event.Idx, Status: "accepted", Message: "Event successfully enqueued"}
		switch {
		case duplicate && previous.Payload != event.Payload:
			ack.Status, ack.Message = "invalid", "event_identity_conflict"
		case duplicate:
			ack.Message = duplicateMessage
		case reject:
			ack.Status, ack.Message = "rejected", "rate_limited"
		case drop:
			return
		}
		if !g.ack(conn, ack) {
			return
		}
	}
}

func (g *fakeGateway) ack(conn net.Conn, ack wire.Ack) bool {
	payload, _ := json.Marshal(ack)
	return wire.WriteFrame(conn, wire.Frame{Type: wire.FrameTypeAck, Payload: payload}) == nil
}

func testConfig(addr string) Config {
	cfg := DefaultConfig()
	cfg.Addr = addr
	cfg.Producers = 8
	cfg.Events = 200
	cfg.Window = 4
	cfg.PayloadBytes = 64
	cfg.Epoch = "test-epoch"
	cfg.AckTimeout = 2 * time.Second
	cfg.MaxBackoff = 20 * time.Millisecond
	cfg.DrainTimeout = 10 * time.Second
	return cfg
}

func TestRunDeliversEveryEventThroughRejectionsAndDisconnects(t *testing.T) {
	gateway := startFakeGateway(t, 7, 50)
	cfg := testConfig(gateway.listener.Addr().String())
	cfg.MalformedRatio = 0.05

	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := result.Summary()
	want := cfg.Producers * cfg.Events
	if s.Totals.Events != want || s.Totals.Accepted != want || s.Totals.Unacked != 0 {
		t.Fatalf("expected all %d events accepted, got %+v", want, s.Totals)
	}
	if s.Totals.Rejected["rate_limited"] == 0 {
		t.Fatal("expected rejections to be retried")
	}
	if s.Totals.Disconnects == 0 || s.Totals.Duplicates == 0 {
		t.Fatalf("expected dropped connections to be retried as duplicates, got %+v", s.Totals)
	}
	//malformed frames are not resent, so one in flight on a dropped connection gets no answer.
	if s.Totals.Malformed == 0 || s.Totals.MalformedRejected == 0 || s.Totals.MalformedRejected > s.Totals.Malformed {
		t.Fatalf("expected malformed frames answered invalid, got %+v", s.Totals)
	}
	if len(s.Totals.Invalid) != 0 || s.Totals.UnmatchedAcks != 0 {
		t.Fatalf("retries must be byte-identical and ACKs must match, got %+v", s.Totals)
	}

	m := result.Manifest()
	if got := m.Count(OutcomeAccepted); got != uint64(want) {
		t.Fatalf("manifest has %d accepted events, want %d", got, want)
	}
	for _, p := range m.Producers {
		if len(p.Accepted) != 1 || p.Accepted[0] != (Range{1, uint64(cfg.Events)}) {
			t.Fatalf("producer %s accepted %v", p.ID, p.Accepted)
		}
	}

	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if len(gateway.stored) != want {
		t.Fatalf("gateway stored %d events, want %d", len(gateway.stored), want)
	}
	for key, event := range gateway.stored {
		if event.Payload != Payload(key.producer, cfg.Epoch, key.seq, cfg.PayloadBytes) {
			t.Fatalf("event %v has unexpected payload %q", key, event.Payload)
		}
	}
}

func TestRunRecordsUnackedEventsWhenGatewayIsDown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	cfg := testConfig(addr)
	cfg.Producers = 2
	cfg.Events = 3
	//without a duration, a count-bound run keeps redialling until it is canceled.
	cfg.Duration = 300 * time.Millisecond
	cfg.DrainTimeout = 200 * time.Millisecond

	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := result.Summary()
	if s.Totals.Accepted != 0 || s.Totals.DialFailures == 0 {
		t.Fatalf("expected only dial failures, got %+v", s.Totals)
	}
	//events are generated only when there is a connection to send them on.
	if s.Totals.Events != 0 || len(result.Manifest().Producers[0].Unacked) != 0 {
		t.Fatalf("expected no events generated without a connection, got %+v", s.Totals)
	}
}

func TestRunStopsAtDurationAndPacesRate(t *testing.T) {
	gateway := startFakeGateway(t, 0, 0)
	cfg := testConfig(gateway.listener.Addr().String())
	cfg.Producers = 2
	cfg.Events = 0
	cfg.Duration = 500 * time.Millisecond
	cfg.Rate = 40

	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range result.Producers {
		//40/s for 0.5 s is about 20 events; allow for timer slack.
		if p.Events < 10 || p.Events > 30 || p.Accepted != p.Events {
			t.Fatalf("producer %s sent %d events and got %d accepted", p.ID, p.Events, p.Accepted)
		}
	}
}

func TestHotProducersUseTheirOwnWindow(t *testing.T) {
	cfg := testConfig("127.0.0.1:1")
	cfg.Hot = 2
	cfg.HotWindow = 64
	cfg.HotRate = 1000
	if !cfg.isHot(1) || cfg.isHot(2) {
		t.Fatal("the first Hot producers are hot")
	}
	if cfg.windowFor(0) != 64 || cfg.windowFor(5) != cfg.Window || cfg.rateFor(0) != 1000 || cfg.rateFor(5) != 0 {
		t.Fatal("hot producers must use hot settings")
	}
}

func TestWriteFilesProducesReadableManifest(t *testing.T) {
	gateway := startFakeGateway(t, 0, 0)
	cfg := testConfig(gateway.listener.Addr().String())
	cfg.Producers = 3
	cfg.Events = 10

	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := result.WriteFiles(dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"summary.json", "manifest.json", "producers.csv", "timeseries.csv"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	m, err := ReadManifest(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch != cfg.Epoch || m.Count(OutcomeAccepted) != 30 || m.PayloadBytes != cfg.PayloadBytes {
		t.Fatalf("unexpected manifest %+v", m)
	}
}

func TestRangesRoundTripOutcomes(t *testing.T) {
	outcomes := []Outcome{OutcomeNotSent, OutcomeAccepted, OutcomeAccepted, OutcomeUnacked, OutcomeAccepted, OutcomeInvalid, OutcomeAccepted, OutcomeAccepted}
	p := ManifestProducer{
		ID:       "p",
		Accepted: RangesOf(outcomes, OutcomeAccepted),
		Unacked:  RangesOf(outcomes, OutcomeUnacked),
		Invalid:  RangesOf(outcomes, OutcomeInvalid),
	}
	if got := p.Accepted.String(); got != "1-2,4,6-7" {
		t.Fatalf("accepted ranges %q", got)
	}
	if p.Accepted.Count() != 5 {
		t.Fatalf("accepted count %d", p.Accepted.Count())
	}
	got := p.Outcomes()
	if len(got) != len(outcomes) {
		t.Fatalf("outcomes length %d, want %d", len(got), len(outcomes))
	}
	for seq := range outcomes {
		if got[seq] != outcomes[seq] {
			t.Fatalf("seq %d: got %v, want %v", seq, got[seq], outcomes[seq])
		}
	}
	if err := (Ranges{{3, 4}, {4, 6}}).validate(); err == nil {
		t.Fatal("overlapping ranges must be invalid")
	}
}

func TestPayloadIsDeterministicAndPadded(t *testing.T) {
	a := Payload("p-1", "e", 7, 100)
	if len(a) != 100 || a != Payload("p-1", "e", 7, 100) {
		t.Fatalf("payload %q", a)
	}
	if a == Payload("p-1", "e", 8, 100) {
		t.Fatal("payloads of different events must differ")
	}
	if short := Payload("p-1", "e", 7, 0); short != "p-1/e/7" {
		t.Fatalf("unpadded payload %q", short)
	}
}

func TestRunHandshakesUnlessLegacy(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		gateway := startFakeGateway(t, 0, 0)
		cfg := testConfig(gateway.listener.Addr().String())
		cfg.Producers = 3
		cfg.Events = 5
		cfg.Legacy = legacy

		result, err := Run(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if s := result.Summary(); s.Totals.Accepted != 15 {
			t.Fatalf("legacy=%v: expected all events accepted, got %+v", legacy, s.Totals)
		}
		gateway.mu.Lock()
		hellos := gateway.hellos
		gateway.mu.Unlock()
		if legacy && hellos != 0 || !legacy && hellos != cfg.Producers {
			t.Fatalf("legacy=%v: gateway saw %d hellos", legacy, hellos)
		}
	}
}
