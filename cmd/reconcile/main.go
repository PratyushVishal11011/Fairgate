// Command reconcile checks a loadgen manifest against ClickHouse: every accepted event must be
// stored exactly once, and nothing that was refused or never sent may be stored.
//
// Exit status: 0 when the run reconciles, 1 when it finds violations, 2 when it cannot check.
package main

import (
	"FairGate/internal/config"
	"FairGate/internal/loadgen"
	"FairGate/internal/reconcile"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	manifestPath := flag.String("manifest", "", "loadgen manifest.json to check (required)")
	configPath := flag.String("config", os.Getenv("FAIRGATE_CONFIG"), "gateway config file to read ClickHouse settings from")
	addr := flag.String("clickhouse-addr", "", "comma-separated ClickHouse addresses (overrides config and CLICKHOUSE_ADDR)")
	database := flag.String("clickhouse-database", "", "ClickHouse database (overrides config)")
	username := flag.String("clickhouse-user", "", "ClickHouse user (overrides config)")
	password := flag.String("clickhouse-password", "", "ClickHouse password (overrides config; prefer CLICKHOUSE_PASSWORD)")
	table := flag.String("table", reconcile.DefaultTable, "table the gateway writes events to")
	timeout := flag.Duration("timeout", 2*time.Minute, "how long to wait for accepted events to be stored")
	poll := flag.Duration("poll", 2*time.Second, "how often to check while waiting")
	verify := flag.Bool("verify-content", false, "also compare stored event_type and payload with what was sent")
	out := flag.String("out", "", "report path (default: reconcile.json next to the manifest)")
	flag.Parse()

	if *manifestPath == "" {
		fmt.Fprintln(os.Stderr, "reconcile: -manifest is required")
		flag.Usage()
		os.Exit(2)
	}
	manifest, err := loadgen.ReadManifest(*manifestPath)
	if err != nil {
		fail(err)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fail(err)
	}
	ch := cfg.ClickHouse
	if *addr != "" {
		ch.Addr = strings.Split(*addr, ",")
	}
	for _, override := range []struct{ dst, src *string }{
		{&ch.Database, database}, {&ch.Username, username}, {&ch.Password, password},
	} {
		if *override.src != "" {
			*override.dst = *override.src
		}
	}

	src, err := reconcile.OpenClickHouse(ch.Addr, ch.Database, ch.Username, ch.Password, *table)
	if err != nil {
		fail(err)
	}
	defer src.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	report, err := reconcile.Run(ctx, src, manifest, reconcile.Options{
		Timeout:       *timeout,
		PollInterval:  *poll,
		VerifyContent: *verify,
		Logf: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "reconcile: "+format+"\n", args...)
		},
	})
	if err != nil {
		fail(err)
	}

	if *out == "" {
		*out = filepath.Join(filepath.Dir(*manifestPath), "reconcile.json")
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(*out, append(data, '\n'), 0644); err != nil {
		fail(err)
	}

	report.WriteText(os.Stdout)
	fmt.Printf("report in %s\n", *out)
	if !report.Passed {
		os.Exit(1)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "reconcile:", err)
	os.Exit(2)
}
