package loadgen

import (
	"encoding/csv"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"
)

// Summary is the machine-readable digest of a run, written as summary.json.
type Summary struct {
	Epoch           string        `json:"epoch"`
	Started         time.Time     `json:"started"`
	Finished        time.Time     `json:"finished"`
	DurationSeconds float64       `json:"duration_seconds"`
	Config          SummaryConfig `json:"config"`
	Totals          Totals        `json:"totals"`
	//accepted events per second over the whole run.
	Throughput float64 `json:"throughput_eps"`
	//latency from the last send of an event to its accepted ACK.
	AckLatency LatencyGroups `json:"ack_latency_ms"`
	//latency from the first send of an event to its accepted ACK, including retries and backoff.
	DeliveryLatency LatencyGroups `json:"delivery_latency_ms"`
	//Jain's fairness index over the normal producers' throughput: 1 is perfectly even.
	JainFairness float64 `json:"jain_fairness_normal"`
}

type SummaryConfig struct {
	Addr           string  `json:"addr"`
	Producers      int     `json:"producers"`
	Events         int     `json:"events_per_producer"`
	Duration       string  `json:"duration"`
	Rate           float64 `json:"rate"`
	Window         int     `json:"window"`
	PayloadBytes   int     `json:"payload_bytes"`
	EventType      string  `json:"event_type"`
	Prefix         string  `json:"prefix"`
	Ramp           string  `json:"ramp"`
	Hot            int     `json:"hot"`
	HotRate        float64 `json:"hot_rate"`
	HotWindow      int     `json:"hot_window"`
	MalformedRatio float64 `json:"malformed_ratio"`
	AckTimeout     string  `json:"ack_timeout"`
	DrainTimeout   string  `json:"drain_timeout"`
	Seed           int64   `json:"seed"`
}

type Totals struct {
	Events            int            `json:"events"`
	Sent              int            `json:"sent"`
	Accepted          int            `json:"accepted"`
	Duplicates        int            `json:"duplicates"`
	Unacked           int            `json:"unacked"`
	Rejected          map[string]int `json:"rejected"`
	Invalid           map[string]int `json:"invalid"`
	Errors            map[string]int `json:"errors"`
	Malformed         int            `json:"malformed"`
	MalformedRejected int            `json:"malformed_rejected"`
	UnmatchedAcks     int            `json:"unmatched_acks"`
	Disconnects       int            `json:"disconnects"`
	DialFailures      int            `json:"dial_failures"`
}

type LatencyGroups struct {
	All    Latency `json:"all"`
	Normal Latency `json:"normal"`
	Hot    Latency `json:"hot"`
}

// Latency holds percentiles in milliseconds.
type Latency struct {
	Count int     `json:"count"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	P999  float64 `json:"p999"`
	Max   float64 `json:"max"`
}

func (r *Result) Summary() Summary {
	cfg := r.Config
	s := Summary{
		Epoch:           cfg.Epoch,
		Started:         r.Started.UTC(),
		Finished:        r.Finished.UTC(),
		DurationSeconds: r.Finished.Sub(r.Started).Seconds(),
		Config: SummaryConfig{
			Addr:           cfg.Addr,
			Producers:      cfg.Producers,
			Events:         cfg.Events,
			Duration:       cfg.Duration.String(),
			Rate:           cfg.Rate,
			Window:         cfg.Window,
			PayloadBytes:   cfg.PayloadBytes,
			EventType:      cfg.EventType,
			Prefix:         cfg.Prefix,
			Ramp:           cfg.Ramp.String(),
			Hot:            cfg.Hot,
			HotRate:        cfg.HotRate,
			HotWindow:      cfg.HotWindow,
			MalformedRatio: cfg.MalformedRatio,
			AckTimeout:     cfg.AckTimeout.String(),
			DrainTimeout:   cfg.DrainTimeout.String(),
			Seed:           cfg.Seed,
		},
		Totals: Totals{
			Rejected: map[string]int{},
			Invalid:  map[string]int{},
			Errors:   map[string]int{},
		},
	}

	var ackAll, ackNormal, ackHot, delAll, delNormal, delHot []uint32
	var normalThroughput []float64
	t := &s.Totals
	for _, p := range r.Producers {
		t.Events += p.Events
		t.Sent += p.Sent
		t.Accepted += p.Accepted
		t.Duplicates += p.Duplicates
		t.Unacked += p.Unacked
		t.Malformed += p.Malformed
		t.MalformedRejected += p.MalformedRejected
		t.UnmatchedAcks += p.UnmatchedAcks
		t.Disconnects += p.Disconnects
		t.DialFailures += p.DialFailures
		addCounts(t.Rejected, p.Rejected)
		addCounts(t.Invalid, p.Invalid)
		addCounts(t.Errors, p.Errors)

		for _, smp := range p.samples {
			ackAll = append(ackAll, smp.ackUs)
			delAll = append(delAll, smp.deliveryUs)
			if p.Hot {
				ackHot = append(ackHot, smp.ackUs)
				delHot = append(delHot, smp.deliveryUs)
			} else {
				ackNormal = append(ackNormal, smp.ackUs)
				delNormal = append(delNormal, smp.deliveryUs)
			}
		}
		if !p.Hot {
			normalThroughput = append(normalThroughput, p.Throughput())
		}
	}

	if s.DurationSeconds > 0 {
		s.Throughput = float64(t.Accepted) / s.DurationSeconds
	}
	s.AckLatency = LatencyGroups{All: latencyOf(ackAll), Normal: latencyOf(ackNormal), Hot: latencyOf(ackHot)}
	s.DeliveryLatency = LatencyGroups{All: latencyOf(delAll), Normal: latencyOf(delNormal), Hot: latencyOf(delHot)}
	s.JainFairness = jain(normalThroughput)
	return s
}

func addCounts(dst, src map[string]int) {
	for key, n := range src {
		dst[key] += n
	}
}

// latencyOf sorts samples in place and returns their percentiles in milliseconds.
func latencyOf(us []uint32) Latency {
	if len(us) == 0 {
		return Latency{}
	}
	slices.Sort(us)
	var sum float64
	for _, v := range us {
		sum += float64(v)
	}
	pct := func(q float64) float64 {
		i := int(math.Ceil(q*float64(len(us)))) - 1
		return float64(us[min(max(i, 0), len(us)-1)]) / 1000
	}
	return Latency{
		Count: len(us),
		Mean:  sum / float64(len(us)) / 1000,
		P50:   pct(0.50),
		P90:   pct(0.90),
		P95:   pct(0.95),
		P99:   pct(0.99),
		P999:  pct(0.999),
		Max:   float64(us[len(us)-1]) / 1000,
	}
}

func jain(values []float64) float64 {
	var sum, squares float64
	for _, v := range values {
		sum += v
		squares += v * v
	}
	if squares == 0 {
		return 0
	}
	return sum * sum / (float64(len(values)) * squares)
}

// WriteFiles writes summary.json, manifest.json, producers.csv, and timeseries.csv into dir.
func (r *Result) WriteFiles(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "summary.json"), r.Summary()); err != nil {
		return err
	}
	if err := WriteManifest(filepath.Join(dir, "manifest.json"), r.Manifest()); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(dir, "producers.csv"), r.producerRows()); err != nil {
		return err
	}
	return writeCSV(filepath.Join(dir, "timeseries.csv"), r.timeseriesRows())
}

func (r *Result) producerRows() [][]string {
	rows := [][]string{{
		"producer_id", "role", "events", "sent", "accepted", "duplicates", "rejected", "invalid",
		"errors", "unacked", "disconnects", "throughput_eps", "p50_ms", "p95_ms", "p99_ms", "max_ms",
	}}
	for _, p := range r.Producers {
		role := "normal"
		if p.Hot {
			role = "hot"
		}
		us := make([]uint32, len(p.samples))
		for i, smp := range p.samples {
			us[i] = smp.ackUs
		}
		lat := latencyOf(us)
		rows = append(rows, []string{
			p.ID, role, itoa(p.Events), itoa(p.Sent), itoa(p.Accepted), itoa(p.Duplicates),
			itoa(sum(p.Rejected)), itoa(sum(p.Invalid)), itoa(sum(p.Errors)), itoa(p.Unacked),
			itoa(p.Disconnects), ftoa(p.Throughput()), ftoa(lat.P50), ftoa(lat.P95), ftoa(lat.P99), ftoa(lat.Max),
		})
	}
	return rows
}

func (r *Result) timeseriesRows() [][]string {
	var buckets []bucket
	latencies := map[int][]uint32{}
	for _, p := range r.Producers {
		for second, b := range p.buckets {
			for len(buckets) <= second {
				buckets = append(buckets, bucket{})
			}
			buckets[second].sent += b.sent
			buckets[second].accepted += b.accepted
			buckets[second].rejected += b.rejected
			buckets[second].invalid += b.invalid
			buckets[second].errors += b.errors
		}
		for _, smp := range p.samples {
			second := int(smp.atMs / 1000)
			latencies[second] = append(latencies[second], smp.ackUs)
		}
	}
	rows := [][]string{{"second", "sent", "accepted", "rejected", "invalid", "errors", "p50_ms", "p95_ms", "p99_ms", "max_ms"}}
	for second, b := range buckets {
		lat := latencyOf(latencies[second])
		rows = append(rows, []string{
			itoa(second), itoa(b.sent), itoa(b.accepted), itoa(b.rejected), itoa(b.invalid), itoa(b.errors),
			ftoa(lat.P50), ftoa(lat.P95), ftoa(lat.P99), ftoa(lat.Max),
		})
	}
	return rows
}

func writeCSV(path string, rows [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	if err := w.WriteAll(rows); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// WriteText prints a short human-readable report.
func (s Summary) WriteText(w io.Writer) {
	t := s.Totals
	fmt.Fprintf(w, "epoch %s: %d producers, %.1fs\n", s.Epoch, s.Config.Producers, s.DurationSeconds)
	fmt.Fprintf(w, "  events     %d generated, %d frames sent, %d accepted (%d duplicates), %d unacked\n",
		t.Events, t.Sent, t.Accepted, t.Duplicates, t.Unacked)
	fmt.Fprintf(w, "  rejected   %s\n", formatCounts(t.Rejected))
	fmt.Fprintf(w, "  invalid    %s\n", formatCounts(t.Invalid))
	fmt.Fprintf(w, "  errors     %s\n", formatCounts(t.Errors))
	if t.Malformed > 0 {
		fmt.Fprintf(w, "  malformed  %d sent, %d answered invalid\n", t.Malformed, t.MalformedRejected)
	}
	fmt.Fprintf(w, "  network    %d disconnects, %d dial failures, %d unmatched ACKs\n", t.Disconnects, t.DialFailures, t.UnmatchedAcks)
	fmt.Fprintf(w, "  throughput %.0f accepted/s, Jain fairness (normal) %.5f\n", s.Throughput, s.JainFairness)
	printLatency := func(name string, l Latency) {
		if l.Count == 0 {
			return
		}
		fmt.Fprintf(w, "  %-10s p50 %.1f  p95 %.1f  p99 %.1f  p99.9 %.1f  max %.1f ms (n=%d)\n",
			name, l.P50, l.P95, l.P99, l.P999, l.Max, l.Count)
	}
	printLatency("ack all", s.AckLatency.All)
	if s.Config.Hot > 0 {
		printLatency("ack normal", s.AckLatency.Normal)
		printLatency("ack hot", s.AckLatency.Hot)
	}
	printLatency("delivery", s.DeliveryLatency.All)
}

func formatCounts(counts map[string]int) string {
	if len(counts) == 0 {
		return "0"
	}
	out := ""
	for i, key := range slices.Sorted(maps.Keys(counts)) {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("%s=%d", key, counts[key])
	}
	return out
}

func sum(counts map[string]int) int {
	total := 0
	for _, n := range counts {
		total += n
	}
	return total
}

func itoa(n int) string { return strconv.Itoa(n) }

func ftoa(f float64) string { return strconv.FormatFloat(f, 'f', 3, 64) }
