// Command loadgen drives a FairGate gateway with simulated producers and records which events
// were accepted, so the reconcile command can check them against ClickHouse.
package main

import (
	"FairGate/internal/loadgen"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	cfg := loadgen.DefaultConfig()
	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "gateway ingestion address")
	flag.IntVar(&cfg.Producers, "producers", cfg.Producers, "number of producers, one connection each")
	flag.IntVar(&cfg.Events, "events", cfg.Events, "events per producer; 0 sends until -duration")
	flag.DurationVar(&cfg.Duration, "duration", cfg.Duration, "how long to send new events; 0 sends until -events")
	flag.Float64Var(&cfg.Rate, "rate", cfg.Rate, "events per second per producer; 0 is unpaced")
	flag.IntVar(&cfg.Window, "window", cfg.Window, "events in flight per connection")
	flag.IntVar(&cfg.PayloadBytes, "payload-bytes", cfg.PayloadBytes, "minimum payload size in bytes")
	flag.StringVar(&cfg.EventType, "event-type", cfg.EventType, "event_type of every event")
	flag.StringVar(&cfg.Prefix, "prefix", cfg.Prefix, "producer ID prefix")
	flag.StringVar(&cfg.Epoch, "epoch", "", "producer epoch for every producer (default: unique per run)")
	flag.DurationVar(&cfg.Ramp, "ramp", cfg.Ramp, "spread producer starts across this period")
	flag.IntVar(&cfg.Hot, "hot", cfg.Hot, "number of hot (noisy-neighbour) producers")
	flag.Float64Var(&cfg.HotRate, "hot-rate", cfg.HotRate, "events per second per hot producer; 0 is unpaced")
	flag.IntVar(&cfg.HotWindow, "hot-window", cfg.HotWindow, "events in flight per hot producer")
	flag.Float64Var(&cfg.MalformedRatio, "malformed-ratio", cfg.MalformedRatio, "probability of an extra malformed frame after each event")
	flag.DurationVar(&cfg.AckTimeout, "ack-timeout", cfg.AckTimeout, "reconnect when the oldest event waits this long for an ACK")
	flag.DurationVar(&cfg.DialTimeout, "dial-timeout", cfg.DialTimeout, "TCP connect timeout")
	flag.DurationVar(&cfg.MaxBackoff, "max-backoff", cfg.MaxBackoff, "cap on retry backoff")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", cfg.DrainTimeout, "time after sending stops to finish outstanding events")
	flag.Int64Var(&cfg.Seed, "seed", cfg.Seed, "random seed for jitter and malformed frames")
	flag.BoolVar(&cfg.Legacy, "legacy", cfg.Legacy, "skip the protocol handshake, like clients that predate protocol versioning")
	out := flag.String("out", "", "results directory (default: benchmarks/loadgen-<timestamp>)")
	flag.Parse()

	if cfg.Epoch == "" {
		cfg.Epoch = loadgen.NewEpoch()
	}
	if *out == "" {
		*out = filepath.Join("benchmarks", "loadgen-"+time.Now().Format("20060102-150405"))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	go func() {
		//the first signal stops new events and drains; restoring the default lets a second one exit at once.
		<-ctx.Done()
		stop()
	}()

	fmt.Fprintf(os.Stderr, "loadgen: %d producers -> %s, epoch %s\n", cfg.Producers, cfg.Addr, cfg.Epoch)
	result, err := loadgen.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(2)
	}
	if err := result.WriteFiles(*out); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen: write results:", err)
		os.Exit(2)
	}
	result.Summary().WriteText(os.Stdout)
	fmt.Printf("results in %s; reconcile with: go run ./cmd/reconcile -manifest %s\n",
		*out, filepath.Join(*out, "manifest.json"))
}
