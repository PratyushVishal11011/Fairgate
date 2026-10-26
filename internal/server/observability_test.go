package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"FairGate/internal/config"
	"FairGate/internal/store"
	"FairGate/internal/wire"
)

func httpGet(t *testing.T, url string) string {
	t.Helper()
	response, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %d", url, response.StatusCode)
	}
	return string(body)
}

func TestRunContextServesPipelineMetricsAndProfiling(t *testing.T) {
	dir := t.TempDir()

	cfg := config.Default()
	cfg.Server.ListenAddr = testServerAddress(t)
	cfg.Server.MetricsAddr = testServerAddress(t)
	cfg.Profiling.Enabled = true
	cfg.Profiling.Addr = testServerAddress(t)
	cfg.WAL.Path = filepath.Join(dir, "wal", "events.wal")
	cfg.Shipper.CheckpointPath = filepath.Join(dir, "checkpoint.json")
	cfg.Shipper.PollInterval = 10 * time.Millisecond
	cfg.Fairness.Producers = map[string]config.ProducerFairness{"vip": {Weight: 2}}

	storage := &countingStore{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runContext(ctx, cfg, logger, func() (store.Store, error) { return storage, nil })
	}()

	var conn net.Conn
	deadline := time.Now().Add(5 * time.Second)
	for conn == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not start listening")
		}
		conn, _ = net.DialTimeout("tcp", cfg.Server.ListenAddr, 100*time.Millisecond)
		if conn == nil {
			time.Sleep(20 * time.Millisecond)
		}
	}

	eventTime := time.Now().UTC().Truncate(time.Millisecond)
	send := func(producer string, seq uint64) {
		payload, err := json.Marshal(wire.Event{
			ProducerId: producer, ProducerEpoch: "1", EventTime: eventTime,
			Seq: seq, EventType: "test", Payload: "payload",
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := wire.WriteFrame(conn, wire.Frame{Type: wire.FrameTypeEvent, Payload: payload}); err != nil {
			t.Fatal(err)
		}
		if _, err := wire.ReadFrame(conn); err != nil {
			t.Fatal(err)
		}
	}
	send("vip", 1)
	send("vip", 1) // identical retry
	send("unlisted", 1)

	for storage.count() < 2 {
		if time.Now().After(deadline.Add(5 * time.Second)) {
			t.Fatalf("shipper delivered %d of 2 events", storage.count())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Metrics are updated just after the ACK and the insert, so poll until they settle.
	var body string
	want := []string{
		`fairgate_producer_events_total{outcome="accepted",producer="vip"} 1`,
		`fairgate_producer_events_total{outcome="duplicate",producer="vip"} 1`,
		`fairgate_producer_events_total{outcome="accepted",producer="other"} 1`,
		"fairgate_ingest_duration_seconds_count 3",
		"fairgate_wal_append_duration_seconds_count 2",
		"fairgate_shipper_events_total 2",
		"fairgate_shipper_lag_bytes 0",
		"fairgate_wal_segments 1",
		`fairgate_build_info{go_version="`,
		"go_goroutines ",
	}
	for {
		body = httpGet(t, "http://"+cfg.Server.MetricsAddr+"/metrics")
		missing := ""
		for _, line := range want {
			if !strings.Contains(body, line) {
				missing = line
				break
			}
		}
		if missing == "" {
			break
		}
		if time.Now().After(deadline.Add(5 * time.Second)) {
			t.Fatalf("metrics never reported %q", missing)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if index := httpGet(t, "http://"+cfg.Profiling.Addr+"/debug/pprof/"); !strings.Contains(index, "goroutine") {
		t.Fatal("pprof index does not list the goroutine profile")
	}

	_ = conn.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server returned an error: %v", err)
		}
	case <-time.After(shutdownGracePeriod() + 3*time.Second):
		t.Fatal("server did not stop")
	}
}
