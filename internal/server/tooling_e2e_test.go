package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"FairGate/internal/chaos"
	"FairGate/internal/config"
	"FairGate/internal/loadgen"
	"FairGate/internal/reconcile"
	"FairGate/internal/sched"
	"FairGate/internal/store"
	"FairGate/internal/wire"
)

// toolingStore stores each event identity once, as the ClickHouse adapter does,
// and mirrors what it stores into a reconcile source.
type toolingStore struct {
	mu   sync.Mutex
	seen map[sched.EventKey]struct{}
	rows *reconcile.Memory
}

func newToolingStore() *toolingStore {
	return &toolingStore{seen: map[sched.EventKey]struct{}{}, rows: &reconcile.Memory{}}
}

func (s *toolingStore) InsertBatch(_ context.Context, events []wire.Event, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range events {
		key := sched.EventKey{ProducerId: event.ProducerId, ProducerEpoch: event.ProducerEpoch, Seq: event.Seq, Idx: event.Idx}
		if _, ok := s.seen[key]; ok {
			continue
		}
		s.seen[key] = struct{}{}
		s.rows.Add(event)
	}
	return nil
}

func (s *toolingStore) Close() error { return nil }

// startToolingGateway runs the full gateway on addr with its WAL and checkpoint in dir.
// The returned function shuts it down and waits for it to exit.
func startToolingGateway(t *testing.T, addr, dir string, st store.Store) func() {
	t.Helper()
	cfg := config.Default()
	cfg.Server.ListenAddr = addr
	cfg.Server.MetricsAddr = "127.0.0.1:0"
	cfg.Server.ShutdownGracePeriod = 200 * time.Millisecond
	cfg.WAL.Path = filepath.Join(dir, "fairgate.wal")
	cfg.Shipper.CheckpointPath = filepath.Join(dir, "checkpoint.json")
	cfg.Shipper.PollInterval = 50 * time.Millisecond
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runContext(ctx, cfg, logger, func() (store.Store, error) { return st, nil })
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		select {
		case err := <-done:
			cancel()
			t.Fatalf("gateway exited before becoming ready: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("gateway did not become ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("gateway stopped with an error: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("gateway did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestLoadgenReconcilesAcrossGatewayRestart(t *testing.T) {
	addr := testServerAddress(t)
	dir := t.TempDir()
	st := newToolingStore()
	stop := startToolingGateway(t, addr, dir, st)

	cfg := loadgen.DefaultConfig()
	cfg.Addr = addr
	cfg.Producers = 10
	cfg.Events = 300
	cfg.Rate = 200
	cfg.Window = 4
	cfg.PayloadBytes = 128
	cfg.Epoch = loadgen.NewEpoch()
	cfg.AckTimeout = 2 * time.Second
	cfg.MaxBackoff = 100 * time.Millisecond
	cfg.DrainTimeout = 20 * time.Second

	type outcome struct {
		result *loadgen.Result
		err    error
	}
	loadDone := make(chan outcome, 1)
	go func() {
		result, err := loadgen.Run(context.Background(), cfg)
		loadDone <- outcome{result, err}
	}()

	//restart the gateway on the same WAL and checkpoint while producers are mid-stream.
	time.Sleep(500 * time.Millisecond)
	stop()
	startToolingGateway(t, addr, dir, st)

	out := <-loadDone
	if out.err != nil {
		t.Fatal(out.err)
	}
	s := out.result.Summary()
	want := cfg.Producers * cfg.Events
	if s.Totals.Accepted != want || s.Totals.Unacked != 0 {
		t.Fatalf("expected all %d events accepted, got %+v", want, s.Totals)
	}
	if s.Totals.Disconnects == 0 {
		t.Fatal("expected the restart to disconnect producers")
	}

	report, err := reconcile.Run(context.Background(), st.rows, out.result.Manifest(), reconcile.Options{
		Timeout:       10 * time.Second,
		PollInterval:  50 * time.Millisecond,
		VerifyContent: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Passed || report.StoredIdentities != uint64(want) {
		t.Fatalf("reconcile failed: %+v", report)
	}
}

func TestChaosScenarioAgainstGateway(t *testing.T) {
	addr := testServerAddress(t)
	st := newToolingStore()
	startToolingGateway(t, addr, t.TempDir(), st)

	scenario := &chaos.Scenario{
		Name: "e2e",
		Load: chaos.Load{
			Addr:         addr,
			Producers:    8,
			Duration:     3 * time.Second,
			Rate:         100,
			Window:       8,
			PayloadBytes: 128,
			AckTimeout:   time.Second,
			MaxBackoff:   100 * time.Millisecond,
			DrainTimeout: 20 * time.Second,
		},
		Proxy: &chaos.ProxySpec{Faults: chaos.Faults{Latency: 5 * time.Millisecond, Jitter: 5 * time.Millisecond}},
		Steps: []chaos.Step{
			{Name: "fragment", At: 500 * time.Millisecond, Faults: &chaos.Faults{Fragment: 16}},
			{Name: "resets", At: time.Second, Faults: &chaos.Faults{ResetProbability: 0.02}},
			{Name: "partition", At: 1500 * time.Millisecond, Faults: &chaos.Faults{Blackhole: true}},
			{Name: "heal", At: 2 * time.Second, Faults: &chaos.Faults{}, Always: true},
			{Name: "disconnect", At: 2200 * time.Millisecond, Disconnect: true},
			{Name: "hook", At: 2400 * time.Millisecond, Exec: "true"},
			{Name: "flaky hook", At: 2500 * time.Millisecond, Exec: "false", IgnoreError: true},
		},
		Reconcile: chaos.ReconcileSpec{Timeout: 15 * time.Second, VerifyContent: true},
		Expect:    chaos.Expect{MinAcceptedRatio: 1},
	}
	if err := scenario.Validate(); err != nil {
		t.Fatal(err)
	}

	runner := &chaos.Runner{
		Out: t.TempDir(),
		OpenSource: func(chaos.ReconcileSpec) (reconcile.Source, io.Closer, error) {
			return st.rows, io.NopCloser(nil), nil
		},
	}
	report, err := runner.Run(context.Background(), scenario)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Passed {
		t.Fatalf("scenario failed: %v\nload: %+v\nreconcile: %+v", report.Failures, report.Load.Totals, report.Reconcile)
	}
	if report.Proxy == nil || report.Proxy.Resets == 0 {
		t.Fatalf("expected the proxy to reset connections, got %+v", report.Proxy)
	}
	if report.Load.Totals.Disconnects == 0 || report.Reconcile.StoredIdentities != uint64(report.Load.Totals.Accepted) {
		t.Fatalf("unexpected load %+v / reconcile %+v", report.Load.Totals, report.Reconcile)
	}
	if last := report.Steps[len(report.Steps)-1]; last.Error == "" || last.Skipped {
		t.Fatalf("the ignored failing step should have run and failed, got %+v", last)
	}
}
