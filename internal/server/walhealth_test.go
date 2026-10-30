package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"FairGate/internal/observability"
	"FairGate/internal/wal"
)

// brokenDisk fails every sync and every new segment, so the WAL degrades on its first append.
type brokenDisk struct{ opened atomic.Int32 }

func (d *brokenDisk) open(name string, flag int, perm os.FileMode) (wal.File, error) {
	//the first open is the active segment at startup; later ones are failover attempts.
	if d.opened.Add(1) > 1 {
		return nil, errors.New("disk unavailable")
	}
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return failingSync{file}, nil
}

type failingSync struct{ *os.File }

func (failingSync) Sync() error { return errors.New("fsync failed") }

func TestDegradedWALRefusesEventsAsRetryable(t *testing.T) {
	disk := &brokenDisk{}
	walLog, err := wal.OpenWithOptions(filepath.Join(t.TempDir(), "events.wal"), wal.Options{OpenFile: disk.open})
	if err != nil {
		t.Fatal(err)
	}
	defer walLog.Close()

	h := startProtocolHandlerWithWAL(t, protocolLimits{}, walLog)
	expectAck(t, h.sendEvent(t, "p", 1), ackRejected, ackStorageUnavailable)
	//the connection stays open, and the event can be retried.
	expectAck(t, h.sendEvent(t, "p", 1), ackRejected, ackStorageUnavailable)
	if !errors.Is(walLog.Health(), wal.ErrUnavailable) {
		t.Fatalf("expected a degraded WAL, got %v", walLog.Health())
	}
}

func TestReadyReportsWALHealth(t *testing.T) {
	metrics := observability.New()
	var health atomic.Pointer[error]
	metrics.SetReadiness(func() error {
		if err := health.Load(); err != nil {
			return *err
		}
		return nil
	})
	server := httptest.NewServer(metrics.Handler())
	defer server.Close()

	status := func(path string) int {
		resp, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if got := status("/ready"); got != http.StatusOK {
		t.Fatalf("/ready = %d while healthy", got)
	}
	degraded := wal.ErrUnavailable
	health.Store(&degraded)
	if got := status("/ready"); got != http.StatusServiceUnavailable {
		t.Fatalf("/ready = %d while degraded", got)
	}
	//liveness is unaffected.
	if got := status("/health"); got != http.StatusOK {
		t.Fatalf("/health = %d while degraded", got)
	}
}

func TestWatchWALExpiresOnlyAfterContinuousDegradation(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var mu sync.Mutex
	var current error
	health := func() error {
		mu.Lock()
		defer mu.Unlock()
		return current
	}
	set := func(err error) {
		mu.Lock()
		current = err
		mu.Unlock()
	}

	expired := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watchWAL(ctx, logger, health, 150*time.Millisecond, 10*time.Millisecond, func(err error) { expired <- err })

	//a short outage that heals does not stop the gateway.
	set(wal.ErrUnavailable)
	time.Sleep(60 * time.Millisecond)
	set(nil)
	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-expired:
		t.Fatalf("expired after a healed outage: %v", err)
	default:
	}

	set(wal.ErrUnavailable)
	select {
	case err := <-expired:
		if !errors.Is(err, wal.ErrUnavailable) {
			t.Fatalf("expired with %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a WAL degraded past the limit must stop the gateway")
	}
}
