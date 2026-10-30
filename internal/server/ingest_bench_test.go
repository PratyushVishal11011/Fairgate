package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"FairGate/internal/admit"
	"FairGate/internal/deduplication"
	"FairGate/internal/observability"
	"FairGate/internal/sched"
	"FairGate/internal/wal"
	"FairGate/internal/wire"
)

// BenchmarkIngest drives the TCP handler the way the EC2 benchmark does: every producer has one
// connection, sends one event, and waits for its ACK before sending the next. It reports throughput
// and ACK latency percentiles as the number of concurrent producers grows.
func BenchmarkIngest(b *testing.B) {
	for _, producers := range []int{1, 50, 250, 750} {
		b.Run(fmt.Sprintf("producers=%d", producers), func(b *testing.B) {
			benchmarkIngest(b, producers)
		})
	}
}

func benchmarkIngest(b *testing.B, producers int) {
	walLog, err := wal.Open(filepath.Join(b.TempDir(), "bench.wal"))
	if err != nil {
		b.Fatal(err)
	}
	defer walLog.Close()

	queues, err := sched.NewQueues(1024)
	if err != nil {
		b.Fatal(err)
	}
	admission, err := admit.NewManager(1e9, 1e9)
	if err != nil {
		b.Fatal(err)
	}
	dedupManager, err := deduplication.NewManager(nil)
	if err != nil {
		b.Fatal(err)
	}
	metrics := observability.New()
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleConn(ctx, conn, logger, admission, queues, walLog, metrics, dedupManager, nil, protocolLimits{})
		}
	}()

	conns := make([]net.Conn, producers)
	for i := range conns {
		producerId := fmt.Sprintf("producer-%03d", i)

		//stand in for the scheduler so producer queues never fill and block the handlers.
		queue := queues.Queue(producerId)
		go func() {
			for {
				select {
				case <-queue:
				case <-ctx.Done():
					return
				}
			}
		}()

		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			b.Fatal(err)
		}
		defer conn.Close()
		conns[i] = conn
	}

	var remaining atomic.Int64
	remaining.Store(int64(b.N))
	latencies := make([][]time.Duration, producers)

	var wg sync.WaitGroup
	b.ResetTimer()
	start := time.Now()
	for i, conn := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			producerId := fmt.Sprintf("producer-%03d", i)
			for seq := uint64(1); remaining.Add(-1) >= 0; seq++ {
				payload, err := json.Marshal(wire.Event{
					ProducerId:    producerId,
					ProducerEpoch: "bench",
					EventTime:     time.Now().UTC(),
					Seq:           seq,
					EventType:     "telemetry",
					Payload:       `{"measurement":"temperature","value":42.0}`,
				})
				if err != nil {
					b.Error(err)
					return
				}

				sent := time.Now()
				if err := wire.WriteFrame(conn, wire.Frame{Type: wire.FrameTypeEvent, Payload: payload}); err != nil {
					b.Error(err)
					return
				}
				frame, err := wire.ReadFrame(conn)
				if err != nil {
					b.Error(err)
					return
				}
				latencies[i] = append(latencies[i], time.Since(sent))

				var ack wire.Ack
				if err := json.Unmarshal(frame.Payload, &ack); err != nil || ack.Status != ackAccepted {
					b.Errorf("unexpected ACK %+v: %v", ack, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()

	var all []time.Duration
	for _, l := range latencies {
		all = append(all, l...)
	}
	if len(all) == 0 {
		return
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	quantile := func(q float64) float64 {
		return float64(all[int(q*float64(len(all)-1))]) / float64(time.Millisecond)
	}
	b.ReportMetric(float64(len(all))/elapsed.Seconds(), "events/s")
	b.ReportMetric(quantile(0.50), "p50-ms")
	b.ReportMetric(quantile(0.99), "p99-ms")
}
