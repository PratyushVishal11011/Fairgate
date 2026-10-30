// Command chaos injects faults around a FairGate deployment.
//
//	chaos proxy -listen 127.0.0.1:9101 -upstream 127.0.0.1:9001 [-admin 127.0.0.1:9102] [faults]
//	chaos run [-out dir] scenario.yaml
//
// proxy runs a fault-injecting TCP proxy whose faults can be changed through its admin API.
// run executes a scenario: load, timed fault steps, then reconciliation against ClickHouse.
// run exits 0 when the scenario passes, 1 when it fails, and 2 when it cannot run.
package main

import (
	"FairGate/internal/chaos"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "proxy":
		runProxy(os.Args[2:])
	case "run":
		runScenario(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: chaos proxy [flags] | chaos run [flags] scenario.yaml")
	os.Exit(2)
}

func runProxy(args []string) {
	flags := flag.NewFlagSet("proxy", flag.ExitOnError)
	listen := flags.String("listen", "127.0.0.1:9101", "address producers connect to")
	upstream := flags.String("upstream", "127.0.0.1:9001", "address traffic is forwarded to")
	admin := flags.String("admin", "127.0.0.1:9102", "admin API address; empty disables it")
	var faults chaos.Faults
	flags.DurationVar(&faults.Latency, "latency", 0, "delay per chunk in each direction")
	flags.DurationVar(&faults.Jitter, "jitter", 0, "random extra delay per chunk")
	flags.Float64Var(&faults.ResetProbability, "reset-prob", 0, "probability that a chunk resets its connection")
	flags.IntVar(&faults.Fragment, "fragment", 0, "split writes into pieces of at most this many bytes")
	flags.DurationVar(&faults.FragmentDelay, "fragment-delay", 0, "pause between fragments")
	flags.BoolVar(&faults.Blackhole, "blackhole", false, "hold all traffic until cleared")
	flags.BoolVar(&faults.Refuse, "refuse", false, "reset new connections")
	_ = flags.Parse(args)

	proxy, err := chaos.Listen(*listen, *upstream, faults)
	if err != nil {
		fmt.Fprintln(os.Stderr, "chaos proxy:", err)
		os.Exit(2)
	}
	fmt.Fprintf(os.Stderr, "chaos proxy: %s -> %s\n", proxy.Addr(), *upstream)

	if *admin != "" {
		listener, err := net.Listen("tcp", *admin)
		if err != nil {
			fmt.Fprintln(os.Stderr, "chaos proxy: admin:", err)
			os.Exit(2)
		}
		server := &http.Server{Handler: proxy.Handler(), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				fmt.Fprintln(os.Stderr, "chaos proxy: admin:", err)
			}
		}()
		defer server.Close()
		fmt.Fprintf(os.Stderr, "chaos proxy: admin API on http://%s (GET/PUT /faults, POST /disconnect, GET /stats)\n", listener.Addr())
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	_ = proxy.Close()
}

func runScenario(args []string) {
	flags := flag.NewFlagSet("run", flag.ExitOnError)
	out := flags.String("out", "", "results directory (default: benchmarks/chaos-<name>-<timestamp>)")
	_ = flags.Parse(args)
	if flags.NArg() != 1 {
		usage()
	}

	scenario, err := chaos.LoadScenario(flags.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, "chaos:", err)
		os.Exit(2)
	}
	if *out == "" {
		*out = filepath.Join("benchmarks", fmt.Sprintf("chaos-%s-%s", scenario.Name, time.Now().Format("20060102-150405")))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	go func() {
		//the first signal stops the load and skips to cleanup steps; a second one exits at once.
		<-ctx.Done()
		stop()
	}()

	runner := &chaos.Runner{
		Out: *out,
		Logf: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "chaos: "+format+"\n", args...)
		},
	}
	report, err := runner.Run(ctx, scenario)
	if err != nil {
		fmt.Fprintln(os.Stderr, "chaos:", err)
		os.Exit(2)
	}
	report.WriteText(os.Stdout)
	if !report.Passed {
		os.Exit(1)
	}
}
