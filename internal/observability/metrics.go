package observability

import (
	"fmt"
	"net/http"
	"runtime"
	"time"

	"FairGate/internal/sched"
	"FairGate/internal/shipper"
	"FairGate/internal/wal"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// OtherProducers is the producer label for every producer that is not labelled individually.
const OtherProducers = "other"

// Options configures the metric set.
type Options struct {
	//Version is reported by fairgate_build_info.
	Version string
	//Producers are the producer IDs that get their own producer label.
	//Every other producer is reported as OtherProducers, which keeps label cardinality bounded
	//no matter how many producers connect.
	Producers []string
}

// Metrics is the gateway's Prometheus metric set. Producer IDs appear as labels only for the
// producers listed in Options.Producers, and other request data is never used as a label.
type Metrics struct {
	registry *prometheus.Registry
	labelled map[string]struct{}

	connections prometheus.Gauge
	accepted    prometheus.Counter
	rejected    prometheus.Counter
	invalid     prometheus.Counter
	duplicates  prometheus.Counter
	walErrors   prometheus.Counter
	processed   prometheus.Counter

	producerEvents    *prometheus.CounterVec
	producerProcessed *prometheus.CounterVec

	ingestDuration    prometheus.Histogram
	walAppendDuration prometheus.Histogram
	walSyncDuration   prometheus.Histogram
	walBatchRecords   prometheus.Histogram
	walWrittenBytes   prometheus.Counter
	walRotations      prometheus.Counter

	shipperBatches             prometheus.Counter
	shipperEvents              prometheus.Counter
	shipperFailures            prometheus.Counter
	shipperInsertDuration      prometheus.Histogram
	shipperConsecutiveFailures prometheus.Gauge
	shipperLastSuccess         prometheus.Gauge
	shipperLastCheckpoint      prometheus.Gauge
}

// Producer event outcomes for fairgate_producer_events_total.
const (
	outcomeAccepted    = "accepted"
	outcomeDuplicate   = "duplicate"
	outcomeRateLimited = "rate_limited"
	outcomeConflict    = "conflict"
	outcomeWALError    = "wal_error"
)

func New() *Metrics { return NewWithOptions(Options{}) }

func NewWithOptions(opts Options) *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		labelled: make(map[string]struct{}, len(opts.Producers)),
	}
	for _, producer := range opts.Producers {
		m.labelled[producer] = struct{}{}
	}

	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	}
	histogram := func(name, help string, buckets []float64) prometheus.Histogram {
		return prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets})
	}

	//0.5 ms to about 4 s, for ingestion, WAL append, and fsync latency.
	latency := prometheus.ExponentialBuckets(0.0005, 2, 14)

	m.connections = gauge("fairgate_tcp_connections_active", "Active TCP client connections.")
	m.accepted = counter("fairgate_events_accepted_total", "Events acknowledged as accepted, including identical retries.")
	m.rejected = counter("fairgate_events_rejected_total", "Events rejected by admission control.")
	m.invalid = counter("fairgate_events_invalid_total", "Events rejected as invalid payloads or identity conflicts.")
	m.duplicates = counter("fairgate_events_duplicate_total", "Identical retries acknowledged without a second WAL append.")
	m.walErrors = counter("fairgate_wal_append_errors_total", "WAL append failures.")
	m.processed = counter("fairgate_events_processed_total", "Events processed by the scheduler.")

	m.producerEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fairgate_producer_events_total",
		Help: "Ingestion outcomes per producer. Producers not listed in fairness.producers are reported as \"other\".",
	}, []string{"producer", "outcome"})
	m.producerProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fairgate_producer_events_processed_total",
		Help: "Events served by the DRR scheduler per producer. Producers not listed in fairness.producers are reported as \"other\".",
	}, []string{"producer"})

	m.ingestDuration = histogram("fairgate_ingest_duration_seconds",
		"Time from reading an event frame to sending its accepted ACK, including the WAL append.", latency)
	m.walAppendDuration = histogram("fairgate_wal_append_duration_seconds",
		"Time a handler waits for its WAL append, including the group commit.", latency)
	m.walSyncDuration = histogram("fairgate_wal_sync_duration_seconds",
		"Duration of each group commit fsync.", latency)
	m.walBatchRecords = histogram("fairgate_wal_group_commit_records",
		"Records made durable by one group commit.", prometheus.ExponentialBuckets(1, 2, 7))
	m.walWrittenBytes = counter("fairgate_wal_written_bytes_total", "Bytes made durable in the WAL.")
	m.walRotations = counter("fairgate_wal_segment_rotations_total", "WAL segment rotations.")

	m.shipperBatches = counter("fairgate_shipper_batches_total", "Batches inserted into the store.")
	m.shipperEvents = counter("fairgate_shipper_events_total", "Events submitted to the store in successful inserts.")
	m.shipperFailures = counter("fairgate_shipper_insert_failures_total", "Failed store insert attempts.")
	m.shipperInsertDuration = histogram("fairgate_shipper_insert_duration_seconds",
		"Duration of successful store inserts.", prometheus.ExponentialBuckets(0.005, 2, 12))
	m.shipperConsecutiveFailures = gauge("fairgate_shipper_consecutive_failures",
		"Failed insert attempts in a row for the current batch; 0 after a success.")
	m.shipperLastSuccess = gauge("fairgate_shipper_last_success_timestamp_seconds",
		"Unix time of the last successful store insert.")
	m.shipperLastCheckpoint = gauge("fairgate_shipper_last_checkpoint_timestamp_seconds",
		"Unix time of the last checkpoint commit.")

	buildInfo := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "fairgate_build_info",
		Help:        "FairGate build information; the value is always 1.",
		ConstLabels: prometheus.Labels{"version": opts.Version, "go_version": runtime.Version()},
	})
	buildInfo.Set(1)

	m.registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		buildInfo,
		m.connections, m.accepted, m.rejected, m.invalid, m.duplicates, m.walErrors, m.processed,
		m.producerEvents, m.producerProcessed,
		m.ingestDuration, m.walAppendDuration, m.walSyncDuration, m.walBatchRecords, m.walWrittenBytes, m.walRotations,
		m.shipperBatches, m.shipperEvents, m.shipperFailures, m.shipperInsertDuration,
		m.shipperConsecutiveFailures, m.shipperLastSuccess, m.shipperLastCheckpoint,
	)

	return m
}

func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	return mux
}

func (m *Metrics) producer(id string) string {
	if _, ok := m.labelled[id]; ok {
		return id
	}
	return OtherProducers
}

func (m *Metrics) ConnectionOpened() { m.connections.Inc() }
func (m *Metrics) ConnectionClosed() { m.connections.Dec() }
func (m *Metrics) EventInvalid()     { m.invalid.Inc() }

func (m *Metrics) EventAccepted(producer string) {
	m.accepted.Inc()
	m.producerEvents.WithLabelValues(m.producer(producer), outcomeAccepted).Inc()
}

func (m *Metrics) EventDuplicate(producer string) {
	m.accepted.Inc()
	m.duplicates.Inc()
	m.producerEvents.WithLabelValues(m.producer(producer), outcomeDuplicate).Inc()
}

func (m *Metrics) EventRejected(producer string) {
	m.rejected.Inc()
	m.producerEvents.WithLabelValues(m.producer(producer), outcomeRateLimited).Inc()
}

func (m *Metrics) EventConflict(producer string) {
	m.invalid.Inc()
	m.producerEvents.WithLabelValues(m.producer(producer), outcomeConflict).Inc()
}

func (m *Metrics) WALAppendError(producer string) {
	m.walErrors.Inc()
	m.producerEvents.WithLabelValues(m.producer(producer), outcomeWALError).Inc()
}

func (m *Metrics) EventProcessed(producer string) {
	m.processed.Inc()
	m.producerProcessed.WithLabelValues(m.producer(producer)).Inc()
}

func (m *Metrics) ObserveIngest(duration time.Duration) {
	m.ingestDuration.Observe(duration.Seconds())
}

func (m *Metrics) ObserveWALAppend(duration time.Duration) {
	m.walAppendDuration.Observe(duration.Seconds())
}

// BatchCommitted implements wal.Observer.
func (m *Metrics) BatchCommitted(records int, bytes int64, sync time.Duration) {
	m.walSyncDuration.Observe(sync.Seconds())
	m.walBatchRecords.Observe(float64(records))
	m.walWrittenBytes.Add(float64(bytes))
}

// SegmentRotated implements wal.Observer.
func (m *Metrics) SegmentRotated() { m.walRotations.Inc() }

// BatchInserted implements shipper.Observer.
func (m *Metrics) BatchInserted(events int, duration time.Duration) {
	m.shipperBatches.Inc()
	m.shipperEvents.Add(float64(events))
	m.shipperInsertDuration.Observe(duration.Seconds())
	m.shipperConsecutiveFailures.Set(0)
	m.shipperLastSuccess.SetToCurrentTime()
}

// InsertFailed implements shipper.Observer.
func (m *Metrics) InsertFailed(consecutiveFailures int) {
	m.shipperFailures.Inc()
	m.shipperConsecutiveFailures.Set(float64(consecutiveFailures))
}

// CheckpointCommitted implements shipper.Observer.
func (m *Metrics) CheckpointCommitted(wal.Position) { m.shipperLastCheckpoint.SetToCurrentTime() }

var (
	_ wal.Observer     = (*Metrics)(nil)
	_ shipper.Observer = (*Metrics)(nil)
)

// RegisterWAL reports the WAL's on-disk footprint at scrape time.
func (m *Metrics) RegisterWAL(w *wal.WAL) {
	m.registry.MustRegister(&walCollector{
		wal:      w,
		segments: prometheus.NewDesc("fairgate_wal_segments", "WAL segment files on disk.", nil, nil),
		bytes:    prometheus.NewDesc("fairgate_wal_disk_bytes", "Total size of the WAL segment files on disk.", nil, nil),
	})
}

// RegisterQueues reports producer queue depth and capacity at scrape time.
func (m *Metrics) RegisterQueues(queues *sched.Queues) {
	m.registry.MustRegister(&queueCollector{
		metrics: m,
		queues:  queues,
		depth: prometheus.NewDesc("fairgate_queue_depth_events",
			"Events waiting in all producer queues.", nil, nil),
		producers: prometheus.NewDesc("fairgate_queue_producers",
			"Producer queues that exist.", nil, nil),
		fill: prometheus.NewDesc("fairgate_queue_fill_ratio_max",
			"Fullest producer queue as a fraction of its capacity, from 0 to 1.", nil, nil),
		producerDepth: prometheus.NewDesc("fairgate_producer_queue_depth_events",
			"Events waiting per producer. \"other\" sums producers not listed in fairness.producers.", []string{"producer"}, nil),
		producerCapacity: prometheus.NewDesc("fairgate_producer_queue_capacity_events",
			"Queue capacity per producer. \"other\" sums producers not listed in fairness.producers.", []string{"producer"}, nil),
	})
}

// RegisterShipper reports how many durable WAL bytes the shipper's checkpoint trails by.
func (m *Metrics) RegisterShipper(s *shipper.Shipper, w *wal.WAL) {
	m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "fairgate_shipper_lag_bytes",
		Help: "Durable WAL bytes after the shipper's committed checkpoint, not yet delivered to the store.",
	}, func() float64 {
		lag, err := w.BytesAfter(s.Committed())
		if err != nil {
			return -1
		}
		return float64(lag)
	}))
}

type walCollector struct {
	wal      *wal.WAL
	segments *prometheus.Desc
	bytes    *prometheus.Desc
}

func (c *walCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.segments
	ch <- c.bytes
}

func (c *walCollector) Collect(ch chan<- prometheus.Metric) {
	stats, err := c.wal.Stats()
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.segments, err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.segments, prometheus.GaugeValue, float64(stats.Segments))
	ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(stats.Bytes))
}

type queueCollector struct {
	metrics          *Metrics
	queues           *sched.Queues
	depth            *prometheus.Desc
	producers        *prometheus.Desc
	fill             *prometheus.Desc
	producerDepth    *prometheus.Desc
	producerCapacity *prometheus.Desc
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.depth
	ch <- c.producers
	ch <- c.fill
	ch <- c.producerDepth
	ch <- c.producerCapacity
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.queues.Stats()

	total := 0
	maxFill := 0.0
	depth := make(map[string]int)
	capacity := make(map[string]int)

	for _, stat := range stats {
		total += stat.Depth
		if stat.Capacity > 0 {
			if fill := float64(stat.Depth) / float64(stat.Capacity); fill > maxFill {
				maxFill = fill
			}
		}
		label := c.metrics.producer(stat.ProducerId)
		depth[label] += stat.Depth
		capacity[label] += stat.Capacity
	}

	ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, float64(total))
	ch <- prometheus.MustNewConstMetric(c.producers, prometheus.GaugeValue, float64(len(stats)))
	ch <- prometheus.MustNewConstMetric(c.fill, prometheus.GaugeValue, maxFill)
	for label, value := range depth {
		ch <- prometheus.MustNewConstMetric(c.producerDepth, prometheus.GaugeValue, float64(value), label)
		ch <- prometheus.MustNewConstMetric(c.producerCapacity, prometheus.GaugeValue, float64(capacity[label]), label)
	}
}
