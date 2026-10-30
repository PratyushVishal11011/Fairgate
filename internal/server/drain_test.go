package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"FairGate/internal/config"
	"FairGate/internal/sched"
	"FairGate/internal/store"
	"FairGate/internal/wire"
)

// lockedBuffer collects log output written from several goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type failingStore struct{}

func (failingStore) InsertBatch(context.Context, []wire.Event, string) error {
	return errors.New("store unavailable")
}

func (failingStore) Close() error { return nil }

// runDrainGateway starts the gateway with a shipper that polls only once an hour,
// so events reach the store before shutdown only if the drain wakes the shipper.
func runDrainGateway(t *testing.T, st store.Store, drainTimeout time.Duration) (string, *lockedBuffer, context.CancelFunc, <-chan error) {
	t.Helper()
	addr := testServerAddress(t)
	dir := t.TempDir()
	cfg := config.Default()
	cfg.Server.ListenAddr = addr
	cfg.Server.MetricsAddr = "127.0.0.1:0"
	cfg.Server.ShutdownGracePeriod = 200 * time.Millisecond
	cfg.Server.DrainTimeout = drainTimeout
	cfg.WAL.Path = filepath.Join(dir, "fairgate.wal")
	cfg.Shipper.CheckpointPath = filepath.Join(dir, "checkpoint.json")
	cfg.Shipper.PollInterval = time.Hour

	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runContext(ctx, cfg, logger, func() (store.Store, error) { return st, nil })
	}()
	t.Cleanup(cancel)

	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return addr, logs, cancel, done
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway did not become ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// sendAccepted sends n events for producer and requires an accepted ACK for each.
func sendAccepted(t *testing.T, addr, producer string, n int) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	for seq := 1; seq <= n; seq++ {
		payload, _ := json.Marshal(wire.Event{
			ProducerId:    producer,
			ProducerEpoch: "1",
			EventTime:     time.Now().UTC(),
			Seq:           uint64(seq),
			EventType:     "drain_test",
			Payload:       "payload",
		})
		if err := wire.WriteFrame(conn, wire.Frame{Type: wire.FrameTypeEvent, Payload: payload}); err != nil {
			t.Fatal(err)
		}
		frame, err := wire.ReadFrame(conn)
		if err != nil {
			t.Fatal(err)
		}
		var ack wire.Ack
		if err := json.Unmarshal(frame.Payload, &ack); err != nil || ack.Status != ackAccepted {
			t.Fatalf("event %d: ack %+v, %v", seq, ack, err)
		}
	}
}

func waitStopped(t *testing.T, done <-chan error, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("gateway stopped with an error: %v", err)
		}
	case <-time.After(within):
		t.Fatalf("gateway did not stop within %s", within)
	}
	return time.Since(start)
}

func TestShutdownDrainShipsAcceptedEventsBeforeExit(t *testing.T) {
	st := newToolingStore()
	addr, logs, cancel, done := runDrainGateway(t, st, 5*time.Second)
	sendAccepted(t, addr, "drain-producer", 25)

	cancel()
	waitStopped(t, done, 5*time.Second)

	stored, err := st.rows.Count(context.Background(), "1", []string{"drain-producer"})
	if err != nil {
		t.Fatal(err)
	}
	if stored != 25 {
		t.Fatalf("expected all 25 accepted events in the store at exit, got %d", stored)
	}
	if out := logs.String(); !strings.Contains(out, "Shutdown drain complete") || !strings.Contains(out, "queued_events_processed=") {
		t.Fatalf("expected a drain confirmation in the logs:\n%s", out)
	}
}

func TestShutdownDrainGivesUpAfterTimeout(t *testing.T) {
	addr, logs, cancel, done := runDrainGateway(t, failingStore{}, 300*time.Millisecond)
	sendAccepted(t, addr, "drain-producer", 5)

	cancel()
	elapsed := waitStopped(t, done, 5*time.Second)
	if elapsed > 3*time.Second {
		t.Fatalf("shutdown took %s; the drain timeout must bound it", elapsed)
	}
	out := logs.String()
	if !strings.Contains(out, "Shutdown drain incomplete") || !strings.Contains(out, "shipper_drain=timed_out") {
		t.Fatalf("expected an incomplete drain report:\n%s", out)
	}
	if strings.Contains(out, "unshipped_bytes=0 ") {
		t.Fatalf("expected unshipped bytes to be reported:\n%s", out)
	}
}

func TestShutdownDrainCanBeDisabled(t *testing.T) {
	st := newToolingStore()
	addr, logs, cancel, done := runDrainGateway(t, st, 0)
	sendAccepted(t, addr, "drain-producer", 5)

	cancel()
	waitStopped(t, done, 5*time.Second)

	//with no drain and an hourly poll, the events stay in the WAL for the next start.
	if stored, _ := st.rows.Count(context.Background(), "1", []string{"drain-producer"}); stored != 0 {
		t.Fatalf("expected nothing shipped without the drain, got %d", stored)
	}
	if out := logs.String(); !strings.Contains(out, "Shutdown drain skipped") {
		t.Fatalf("expected the skipped drain to be logged:\n%s", out)
	}
}

type countingWaker struct{ wakes atomic.Int64 }

func (w *countingWaker) Wake() { w.wakes.Add(1) }

// startSlowScheduler runs a DRR scheduler that takes perEvent to process each event.
func startSlowScheduler(queues *sched.Queues, perEvent time.Duration) (context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sched.NewDRRScheduler(1).Run(ctx, queues, func(wire.Event) error {
			time.Sleep(perEvent)
			return nil
		})
	}()
	return cancel, done
}

func fillQueues(t *testing.T, n int) *sched.Queues {
	t.Helper()
	queues, err := sched.NewQueues(n)
	if err != nil {
		t.Fatal(err)
	}
	for seq := 1; seq <= n; seq++ {
		queues.Enqueue(wire.Event{ProducerId: "p", ProducerEpoch: "1", Seq: uint64(seq)})
	}
	return queues
}

func TestDrainPipelineWaitsForQueuesThenShipper(t *testing.T) {
	queues := fillQueues(t, 20)
	cancelScheduler, schedulerDone := startSlowScheduler(queues, 5*time.Millisecond)
	defer cancelScheduler()

	waker := &countingWaker{}
	var lag atomic.Int64
	lag.Store(4096)
	shipperDone := make(chan struct{})
	//the store catches up after a few wakes.
	shipperLag := func() (int64, error) {
		if waker.wakes.Load() >= 3 {
			lag.Store(0)
		}
		return lag.Load(), nil
	}

	report := drainPipeline(5*time.Second, queues, schedulerDone, cancelScheduler, waker, shipperDone, shipperLag)
	if !report.complete() || report.queuedAtStart != 20 || report.queuedLeft != 0 {
		t.Fatalf("expected a complete drain of 20 queued events, got %+v", report)
	}
	if report.unshippedAtStart != 4096 || report.unshippedLeft != 0 || waker.wakes.Load() < 3 {
		t.Fatalf("expected the shipper to be woken until it caught up, got %+v (wakes %d)", report, waker.wakes.Load())
	}
	select {
	case <-schedulerDone:
	default:
		t.Fatal("the scheduler must be stopped after the drain")
	}
}

func TestDrainPipelineReportsWhatIsLeftAtTimeout(t *testing.T) {
	queues := fillQueues(t, 100)
	cancelScheduler, schedulerDone := startSlowScheduler(queues, 20*time.Millisecond)
	defer cancelScheduler()

	start := time.Now()
	report := drainPipeline(150*time.Millisecond, queues, schedulerDone, cancelScheduler, &countingWaker{}, make(chan struct{}),
		func() (int64, error) { return 512, nil })
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("drain took %s with a 150ms timeout", elapsed)
	}
	if report.queue != drainTimedOut || report.shipper != drainTimedOut {
		t.Fatalf("expected both stages to time out, got %+v", report)
	}
	if report.queuedLeft == 0 || report.queuedLeft >= 100 || report.unshippedLeft != 512 {
		t.Fatalf("expected a partial drain to be reported, got %+v", report)
	}
}

func TestWaitDrainedOutcomes(t *testing.T) {
	never := make(chan struct{})
	if got := waitDrained(time.Now().Add(time.Second), never, func() bool { return true }); got != drainComplete {
		t.Fatalf("got %s, want complete", got)
	}
	if got := waitDrained(time.Now().Add(50*time.Millisecond), never, func() bool { return false }); got != drainTimedOut {
		t.Fatalf("got %s, want timed_out", got)
	}
	stopped := make(chan struct{})
	close(stopped)
	if got := waitDrained(time.Now().Add(time.Second), stopped, func() bool { return false }); got != drainStopped {
		t.Fatalf("got %s, want stopped", got)
	}
}
