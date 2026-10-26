package observability

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"FairGate/internal/sched"
	"FairGate/internal/shipper"
	"FairGate/internal/wal"
	"FairGate/internal/wire"
)

type nopStore struct{}

func (nopStore) InsertBatch(context.Context, []wire.Event, string) error { return nil }
func (nopStore) Close() error                                            { return nil }

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	server := httptest.NewServer(m.Handler())
	defer server.Close()

	response, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics returned %d", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func assertContains(t *testing.T, body string, lines ...string) {
	t.Helper()
	for _, line := range lines {
		if !strings.Contains(body, line) {
			t.Errorf("metrics output is missing %q", line)
		}
	}
}

// fullyWired returns metrics with every collector registered and every series touched once.
func fullyWired(t *testing.T) (*Metrics, *wal.WAL) {
	t.Helper()

	m := NewWithOptions(Options{Version: "test", Producers: []string{"vip"}})

	w, err := wal.Open(filepath.Join(t.TempDir(), "events.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	w.SetObserver(m)
	m.RegisterWAL(w)

	queues, err := sched.NewQueues(4)
	if err != nil {
		t.Fatal(err)
	}
	m.RegisterQueues(queues)

	s, err := shipper.New(nopStore{}, w, shipper.NewCheckpointStore(filepath.Join(t.TempDir(), "checkpoint.json")),
		shipper.NewBackoff(time.Millisecond, time.Millisecond, 1, 0, 1), shipper.Config{BatchSize: 1, PollInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	m.RegisterShipper(s, w)

	for _, producer := range []string{"vip", "anyone"} {
		m.EventAccepted(producer)
		m.EventDuplicate(producer)
		m.EventRejected(producer)
		m.EventConflict(producer)
		m.WALAppendError(producer)
		m.EventProcessed(producer)
	}
	m.EventInvalid()
	m.ObserveIngest(time.Millisecond)
	m.ObserveWALAppend(time.Millisecond)
	m.BatchInserted(3, 10*time.Millisecond)
	m.InsertFailed(1)
	m.CheckpointCommitted(wal.Position{})

	if err := w.Append(wire.Event{ProducerId: "vip", ProducerEpoch: "1", Seq: 1, EventType: "test"}); err != nil {
		t.Fatal(err)
	}
	queues.Enqueue(wire.Event{ProducerId: "vip", ProducerEpoch: "1", Seq: 1})

	return m, w
}

func TestProducerLabelsAreBounded(t *testing.T) {
	m := NewWithOptions(Options{Producers: []string{"vip"}})

	m.EventAccepted("vip")
	for i := 0; i < 50; i++ {
		m.EventAccepted("producer-" + string(rune('a'+i%26)) + string(rune('a'+i/26)))
	}
	m.EventProcessed("vip")
	m.EventProcessed("someone-else")

	body := scrape(t, m)
	assertContains(t, body,
		`fairgate_producer_events_total{outcome="accepted",producer="vip"} 1`,
		`fairgate_producer_events_total{outcome="accepted",producer="other"} 50`,
		`fairgate_producer_events_processed_total{producer="vip"} 1`,
		`fairgate_producer_events_processed_total{producer="other"} 1`,
		`fairgate_events_accepted_total 51`,
	)
	if strings.Contains(body, "producer-a") {
		t.Fatal("an unlisted producer ID leaked into a label")
	}
}

func TestDuplicatesAndConflictsKeepLegacyTotals(t *testing.T) {
	m := New()
	m.EventAccepted("p")
	m.EventDuplicate("p")
	m.EventConflict("p")
	m.EventInvalid()

	assertContains(t, scrape(t, m),
		"fairgate_events_accepted_total 2",
		"fairgate_events_duplicate_total 1",
		"fairgate_events_invalid_total 2",
		`fairgate_producer_events_total{outcome="duplicate",producer="other"} 1`,
		`fairgate_producer_events_total{outcome="conflict",producer="other"} 1`,
	)
}

func TestCollectorsReportPipelineState(t *testing.T) {
	m := NewWithOptions(Options{Producers: []string{"vip"}})

	w, err := wal.Open(filepath.Join(t.TempDir(), "events.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.SetObserver(m)
	m.RegisterWAL(w)

	for seq := uint64(1); seq <= 3; seq++ {
		if err := w.Append(wire.Event{ProducerId: "vip", ProducerEpoch: "1", Seq: seq, EventType: "test"}); err != nil {
			t.Fatal(err)
		}
	}

	queues, err := sched.NewQueuesWithCapacities(4, map[string]int{"vip": 2})
	if err != nil {
		t.Fatal(err)
	}
	m.RegisterQueues(queues)
	queues.Enqueue(wire.Event{ProducerId: "vip", ProducerEpoch: "1", Seq: 1})
	queues.Enqueue(wire.Event{ProducerId: "a", ProducerEpoch: "1", Seq: 1})
	queues.Enqueue(wire.Event{ProducerId: "b", ProducerEpoch: "1", Seq: 1})

	s, err := shipper.New(nopStore{}, w, shipper.NewCheckpointStore(filepath.Join(t.TempDir(), "checkpoint.json")),
		shipper.NewBackoff(time.Millisecond, time.Millisecond, 1, 0, 1), shipper.Config{BatchSize: 1, PollInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	m.RegisterShipper(s, w)

	stats, err := w.Stats()
	if err != nil {
		t.Fatal(err)
	}

	body := scrape(t, m)
	assertContains(t, body,
		"fairgate_wal_segments 1",
		"fairgate_wal_group_commit_records_count",
		"fairgate_queue_depth_events 3",
		"fairgate_queue_producers 3",
		"fairgate_queue_fill_ratio_max 0.5",
		`fairgate_producer_queue_depth_events{producer="vip"} 1`,
		`fairgate_producer_queue_depth_events{producer="other"} 2`,
		`fairgate_producer_queue_capacity_events{producer="vip"} 2`,
		`fairgate_producer_queue_capacity_events{producer="other"} 8`,
	)
	// Nothing has been shipped, so the shipper trails the WAL by everything durable.
	assertContains(t, body, "fairgate_shipper_lag_bytes "+strconv.FormatInt(stats.DurableEnd.Offset, 10))
	assertContains(t, body, "fairgate_wal_disk_bytes "+strconv.FormatInt(stats.Bytes, 10))
}

var metricName = regexp.MustCompile(`\bfairgate_[a-z_]+`)

// TestAlertsAndDashboardUseExportedMetrics keeps the alert rules and the Grafana dashboard in step
// with the metrics the gateway actually exports.
func TestAlertsAndDashboardUseExportedMetrics(t *testing.T) {
	m, _ := fullyWired(t)
	body := scrape(t, m)

	exported := make(map[string]bool)
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "fairgate_") {
			name := line
			if i := strings.IndexAny(name, "{ "); i >= 0 {
				name = name[:i]
			}
			exported[name] = true
			// A histogram may also be referred to by its base name.
			for _, suffix := range []string{"_bucket", "_sum", "_count"} {
				if base, ok := strings.CutSuffix(name, suffix); ok {
					exported[base] = true
				}
			}
		}
	}

	for _, file := range []string{
		filepath.Join("..", "..", "deploy", "prometheus-alerts.yml"),
		filepath.Join("..", "..", "deploy", "grafana", "dashboards", "fairgate-overview.json"),
	} {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}

		var missing []string
		seen := make(map[string]bool)
		for _, name := range metricName.FindAllString(string(data), -1) {
			if seen[name] {
				continue
			}
			seen[name] = true
			if !exported[name] {
				missing = append(missing, name)
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			t.Errorf("%s uses metrics the gateway does not export: %v", file, missing)
		}
	}
}

func TestProfilingHandlerServesPprof(t *testing.T) {
	server := httptest.NewServer(ProfilingHandler())
	defer server.Close()

	for _, path := range []string{"/debug/pprof/", "/debug/pprof/goroutine?debug=1", "/debug/pprof/heap?debug=1"} {
		response, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Errorf("GET %s returned %d", path, response.StatusCode)
		}
	}
}
