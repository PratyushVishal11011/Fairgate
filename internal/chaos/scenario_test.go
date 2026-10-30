package chaos

import (
	"FairGate/internal/wire"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestShippedScenariosAreValid(t *testing.T) {
	paths, err := filepath.Glob("../../deploy/chaos/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no scenarios found in deploy/chaos")
	}
	for _, path := range paths {
		s, err := LoadScenario(path)
		if err != nil {
			t.Errorf("%s: %v", path, err)
			continue
		}
		for _, step := range s.Steps {
			if step.action() == "" {
				t.Errorf("%s: step %q has no action", path, step.Name)
			}
		}
	}
}

func writeScenario(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scenario.yaml")
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadScenarioParsesClearedFaults(t *testing.T) {
	s, err := LoadScenario(writeScenario(t, `
load: {producers: 2, duration: 1s}
proxy: {faults: {latency: 10ms}}
steps:
  - {at: 1s, faults: {}}
`))
	if err != nil {
		t.Fatal(err)
	}
	if s.Name != "scenario" || s.Steps[0].Faults == nil || *s.Steps[0].Faults != (Faults{}) {
		t.Fatalf("expected an empty faults step that clears faults, got %+v", s.Steps[0])
	}
	cfg, err := s.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Events != 0 || cfg.Duration != time.Second || cfg.Producers != 2 {
		t.Fatalf("a duration-only load must not stop at the default event count: %+v", cfg)
	}
}

func TestLoadScenarioRejectsBadScenarios(t *testing.T) {
	cases := map[string]string{
		"unknown key":      "load: {producers: 2}\nsteps:\n  - {at: 1s, exce: true}\n",
		"two actions":      "steps:\n  - {at: 1s, exec: 'true', disconnect: true}\nproxy: {}\n",
		"no action":        "steps:\n  - {at: 1s}\n",
		"faults no proxy":  "steps:\n  - {at: 1s, faults: {latency: 1ms}}\n",
		"steps out of ord": "steps:\n  - {at: 2s, exec: 'true'}\n  - {at: 1s, exec: 'true'}\n",
		"bad probability":  "proxy: {faults: {reset_probability: 1.5}}\n",
		"bad load":         "load: {producers: -1}\n",
	}
	for name, body := range cases {
		if _, err := LoadScenario(writeScenario(t, body)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

// startAcker accepts every event frame.
func startAcker(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				for {
					frame, err := wire.ReadFrame(conn)
					if err != nil {
						return
					}
					event, err := wire.DecodeEvent(frame.Payload)
					if err != nil {
						return
					}
					payload, _ := json.Marshal(wire.Ack{ProducerId: event.ProducerId, Seq: event.Seq, Status: "accepted"})
					if wire.WriteFrame(conn, wire.Frame{Type: wire.FrameTypeAck, Payload: payload}) != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().String()
}

func TestRunnerSkipsStepsAfterAFailureButRunsAlwaysSteps(t *testing.T) {
	out := t.TempDir()
	marker := filepath.Join(out, "cleanup-ran")
	s := &Scenario{
		Name: "abort",
		Load: Load{Addr: startAcker(t), Producers: 2, Events: 20, Rate: 50},
		Steps: []Step{
			{Name: "breaks", At: 50 * time.Millisecond, Exec: "exit 3"},
			{Name: "skipped", At: 60 * time.Millisecond, Exec: "true"},
			{Name: "cleanup", At: 70 * time.Millisecond, Exec: "touch " + marker, Always: true},
		},
		Reconcile: ReconcileSpec{Skip: true},
	}
	runner := &Runner{Out: out}
	report, err := runner.Run(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if report.Passed || len(report.Failures) != 1 || !strings.Contains(report.Failures[0], "breaks") {
		t.Fatalf("expected the failing step to fail the scenario, got %+v", report.Failures)
	}
	if !report.Steps[1].Skipped || report.Steps[2].Skipped || report.Steps[2].Error != "" {
		t.Fatalf("unexpected step results %+v", report.Steps)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("the always step did not run")
	}
	if report.Load.Totals.Accepted != 40 {
		t.Fatalf("the load should run to completion, got %+v", report.Load.Totals)
	}
	for _, name := range []string{"chaos-report.json", "manifest.json", "summary.json"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatalf("missing %s", name)
		}
	}
}

func TestRunnerFailsWhenTooFewEventsAreAccepted(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	s := &Scenario{
		Name:      "down",
		Load:      Load{Addr: addr, Producers: 1, Duration: 200 * time.Millisecond, DrainTimeout: 100 * time.Millisecond},
		Proxy:     &ProxySpec{},
		Reconcile: ReconcileSpec{Skip: true},
		Expect:    Expect{MinAcceptedRatio: 1},
	}
	report, err := (&Runner{Out: t.TempDir()}).Run(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	//the proxy accepts connections but cannot reach the gateway, so nothing is accepted.
	if report.Passed || report.Load.Totals.Accepted != 0 {
		t.Fatalf("expected a failure, got %+v", report)
	}
}
