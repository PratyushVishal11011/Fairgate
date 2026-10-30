package chaos

import (
	"FairGate/internal/config"
	"FairGate/internal/loadgen"
	"FairGate/internal/reconcile"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Scenario is a chaos experiment: a load run, timed fault steps, and a reconciliation.
type Scenario struct {
	Name        string        `yaml:"name"`
	Description string        `yaml:"description"`
	Load        Load          `yaml:"load"`
	Proxy       *ProxySpec    `yaml:"proxy"`
	Steps       []Step        `yaml:"steps"`
	Reconcile   ReconcileSpec `yaml:"reconcile"`
	Expect      Expect        `yaml:"expect"`
}

// Load overrides loadgen.DefaultConfig; zero fields keep the default.
type Load struct {
	Addr           string        `yaml:"addr"`
	Producers      int           `yaml:"producers"`
	Events         int           `yaml:"events"`
	Duration       time.Duration `yaml:"duration"`
	Rate           float64       `yaml:"rate"`
	Window         int           `yaml:"window"`
	PayloadBytes   int           `yaml:"payload_bytes"`
	Prefix         string        `yaml:"prefix"`
	Ramp           time.Duration `yaml:"ramp"`
	Hot            int           `yaml:"hot"`
	HotRate        float64       `yaml:"hot_rate"`
	HotWindow      int           `yaml:"hot_window"`
	MalformedRatio float64       `yaml:"malformed_ratio"`
	AckTimeout     time.Duration `yaml:"ack_timeout"`
	MaxBackoff     time.Duration `yaml:"max_backoff"`
	DrainTimeout   time.Duration `yaml:"drain_timeout"`
	Seed           int64         `yaml:"seed"`
}

// ProxySpec routes the load through a fault-injecting proxy in front of Upstream.
type ProxySpec struct {
	//proxy listen address; defaults to an ephemeral loopback port.
	Listen string `yaml:"listen"`
	//gateway address; defaults to load.addr.
	Upstream string `yaml:"upstream"`
	//faults active from the start.
	Faults Faults `yaml:"faults"`
}

// Step runs one action at offset At from the start of the load. Steps run in file order.
type Step struct {
	At   time.Duration `yaml:"at"`
	Name string        `yaml:"name"`

	//exactly one action:
	//shell command, run with sh -c.
	Exec string `yaml:"exec"`
	//URL polled until it answers 2xx.
	WaitHealthy string `yaml:"wait_healthy"`
	//replace the proxy's faults; {} clears them.
	Faults *Faults `yaml:"faults"`
	//reset every proxied connection.
	Disconnect bool `yaml:"disconnect"`

	//limit for exec and wait_healthy; defaults to 2m and 60s.
	Timeout time.Duration `yaml:"timeout"`
	//a failure of this step does not fail the scenario.
	IgnoreError bool `yaml:"ignore_error"`
	//run even after an earlier step failed or the run was interrupted, like a finally block.
	Always bool `yaml:"always"`
}

func (s Step) action() string {
	switch {
	case s.Exec != "":
		return "exec"
	case s.WaitHealthy != "":
		return "wait_healthy"
	case s.Faults != nil:
		return "faults"
	case s.Disconnect:
		return "disconnect"
	}
	return ""
}

// ReconcileSpec says where to check the run. ClickHouse settings come from Config (a gateway
// config file, plus the CLICKHOUSE_* environment overrides), then the fields set here.
type ReconcileSpec struct {
	Skip          bool          `yaml:"skip"`
	Config        string        `yaml:"config"`
	Addr          []string      `yaml:"addr"`
	Database      string        `yaml:"database"`
	Username      string        `yaml:"username"`
	Password      string        `yaml:"password"`
	Table         string        `yaml:"table"`
	Timeout       time.Duration `yaml:"timeout"`
	VerifyContent bool          `yaml:"verify_content"`
}

type Expect struct {
	//minimum fraction of generated events the gateway must accept by the end of the drain.
	MinAcceptedRatio float64 `yaml:"min_accepted_ratio"`
}

// LoadScenario reads and validates a scenario file. Unknown keys are rejected.
func LoadScenario(path string) (*Scenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var s Scenario
	if err := decoder.Decode(&s); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse scenario %s: %w", path, err)
	}
	if s.Name == "" {
		s.Name = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("scenario %s: %w", path, err)
	}
	return &s, nil
}

func (s *Scenario) Validate() error {
	var problems []string
	if _, err := s.LoadConfig(); err != nil {
		problems = append(problems, err.Error())
	}
	if s.Proxy != nil {
		if err := s.Proxy.Faults.Validate(); err != nil {
			problems = append(problems, "proxy.faults: "+err.Error())
		}
	}
	var last time.Duration
	for i, step := range s.Steps {
		label := fmt.Sprintf("steps[%d]", i)
		if step.Name != "" {
			label += " (" + step.Name + ")"
		}
		actions := 0
		for _, set := range []bool{step.Exec != "", step.WaitHealthy != "", step.Faults != nil, step.Disconnect} {
			if set {
				actions++
			}
		}
		if actions != 1 {
			problems = append(problems, label+": set exactly one of exec, wait_healthy, faults, disconnect")
		}
		if (step.Faults != nil || step.Disconnect) && s.Proxy == nil {
			problems = append(problems, label+": faults and disconnect need a proxy section")
		}
		if step.Faults != nil {
			if err := step.Faults.Validate(); err != nil {
				problems = append(problems, label+": "+err.Error())
			}
		}
		if step.At < last {
			problems = append(problems, label+": at must not be earlier than the previous step")
		}
		last = step.At
	}
	if s.Expect.MinAcceptedRatio < 0 || s.Expect.MinAcceptedRatio > 1 {
		problems = append(problems, "expect.min_accepted_ratio must be between 0 and 1")
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// LoadConfig returns the loadgen configuration for the scenario, with a fresh epoch.
func (s *Scenario) LoadConfig() (loadgen.Config, error) {
	cfg := loadgen.DefaultConfig()
	cfg.Prefix = "chaos"
	l := s.Load
	setString(&cfg.Addr, l.Addr)
	setString(&cfg.Prefix, l.Prefix)
	setNumber(&cfg.Producers, l.Producers)
	setNumber(&cfg.Events, l.Events)
	setNumber(&cfg.Duration, l.Duration)
	setNumber(&cfg.Rate, l.Rate)
	setNumber(&cfg.Window, l.Window)
	setNumber(&cfg.PayloadBytes, l.PayloadBytes)
	setNumber(&cfg.Ramp, l.Ramp)
	setNumber(&cfg.Hot, l.Hot)
	setNumber(&cfg.HotRate, l.HotRate)
	setNumber(&cfg.HotWindow, l.HotWindow)
	setNumber(&cfg.MalformedRatio, l.MalformedRatio)
	setNumber(&cfg.AckTimeout, l.AckTimeout)
	setNumber(&cfg.MaxBackoff, l.MaxBackoff)
	setNumber(&cfg.DrainTimeout, l.DrainTimeout)
	setNumber(&cfg.Seed, l.Seed)
	if l.Duration > 0 && l.Events == 0 {
		//a duration-only load sends until the duration ends.
		cfg.Events = 0
	}
	cfg.Epoch = loadgen.NewEpoch()
	return cfg, cfg.Validate()
}

func setString(dst *string, value string) {
	if value != "" {
		*dst = value
	}
}

func setNumber[T int | int64 | float64 | time.Duration](dst *T, value T) {
	if value != 0 {
		*dst = value
	}
}

// Report is the outcome of a scenario, written as chaos-report.json.
type Report struct {
	Name            string            `json:"name"`
	Passed          bool              `json:"passed"`
	Failures        []string          `json:"failures,omitempty"`
	Started         time.Time         `json:"started"`
	DurationSeconds float64           `json:"duration_seconds"`
	ResultsDir      string            `json:"results_dir"`
	Steps           []StepResult      `json:"steps"`
	Load            loadgen.Summary   `json:"load"`
	Proxy           *Stats            `json:"proxy,omitempty"`
	Reconcile       *reconcile.Report `json:"reconcile,omitempty"`
}

type StepResult struct {
	Name   string `json:"name"`
	Action string `json:"action"`
	At     string `json:"at"`
	//seconds from the start of the load.
	StartedAt       float64 `json:"started_at,omitempty"`
	DurationSeconds float64 `json:"duration_seconds,omitempty"`
	Skipped         bool    `json:"skipped,omitempty"`
	Error           string  `json:"error,omitempty"`
	Output          string  `json:"output,omitempty"`
}

// Runner runs scenarios.
type Runner struct {
	//results directory; loadgen files, reconcile.json, and chaos-report.json are written here.
	Out string
	//progress messages; nil discards them.
	Logf func(format string, args ...any)
	//opens the store to reconcile against; nil opens ClickHouse from the scenario's settings.
	OpenSource func(ReconcileSpec) (reconcile.Source, io.Closer, error)
}

func (r *Runner) logf(format string, args ...any) {
	if r.Logf != nil {
		r.Logf(format, args...)
	}
}

// Run executes the scenario. It returns an error only when the scenario could not be run;
// a scenario that ran and found problems returns a report with Passed false.
func (r *Runner) Run(ctx context.Context, s *Scenario) (*Report, error) {
	cfg, err := s.LoadConfig()
	if err != nil {
		return nil, err
	}

	var proxy *Proxy
	if s.Proxy != nil {
		listen := s.Proxy.Listen
		if listen == "" {
			listen = "127.0.0.1:0"
		}
		upstream := s.Proxy.Upstream
		if upstream == "" {
			upstream = cfg.Addr
		}
		proxy, err = Listen(listen, upstream, s.Proxy.Faults)
		if err != nil {
			return nil, fmt.Errorf("start proxy: %w", err)
		}
		defer proxy.Close()
		cfg.Addr = proxy.Addr()
		r.logf("proxy %s -> %s", proxy.Addr(), upstream)
	}

	report := &Report{Name: s.Name, Started: time.Now().UTC(), ResultsDir: r.Out}
	start := time.Now()
	r.logf("load: %d producers, epoch %s", cfg.Producers, cfg.Epoch)

	type loadOutcome struct {
		result *loadgen.Result
		err    error
	}
	loadDone := make(chan loadOutcome, 1)
	go func() {
		result, err := loadgen.Run(ctx, cfg)
		loadDone <- loadOutcome{result, err}
	}()

	report.Steps = r.runSteps(ctx, s, proxy, start, report)

	outcome := <-loadDone
	if outcome.err != nil {
		return nil, outcome.err
	}
	result := outcome.result
	report.Load = result.Summary()
	if proxy != nil {
		stats := proxy.Stats()
		report.Proxy = &stats
	}
	if err := result.WriteFiles(r.Out); err != nil {
		return nil, fmt.Errorf("write load results: %w", err)
	}
	r.logf("load finished: %d of %d events accepted", report.Load.Totals.Accepted, report.Load.Totals.Events)

	if events := report.Load.Totals.Events; s.Expect.MinAcceptedRatio > 0 {
		if events == 0 {
			report.Failures = append(report.Failures, "no events were sent")
		} else if ratio := float64(report.Load.Totals.Accepted) / float64(events); ratio < s.Expect.MinAcceptedRatio {
			report.Failures = append(report.Failures,
				fmt.Sprintf("accepted %.4f of events, expected at least %.4f", ratio, s.Expect.MinAcceptedRatio))
		}
	}

	if !s.Reconcile.Skip {
		rec, err := r.reconcile(ctx, s.Reconcile, result.Manifest())
		if err != nil {
			report.Failures = append(report.Failures, "reconcile: "+err.Error())
		} else {
			report.Reconcile = rec
			if !rec.Passed {
				report.Failures = append(report.Failures, fmt.Sprintf(
					"reconcile: %d missing, %d duplicated, %d unexpected, %d mismatched",
					rec.Missing, rec.Duplicated, rec.Unexpected, rec.Mismatched))
			}
		}
	}

	report.Passed = len(report.Failures) == 0
	report.DurationSeconds = time.Since(start).Seconds()
	if err := writeReport(filepath.Join(r.Out, "chaos-report.json"), report); err != nil {
		return nil, err
	}
	return report, nil
}

func (r *Runner) runSteps(ctx context.Context, s *Scenario, proxy *Proxy, start time.Time, report *Report) []StepResult {
	results := make([]StepResult, 0, len(s.Steps))
	aborted := false
	for i, step := range s.Steps {
		name := step.Name
		if name == "" {
			name = fmt.Sprintf("step %d", i+1)
		}
		res := StepResult{Name: name, Action: step.action(), At: step.At.String()}

		//an interrupted run skips the wait, so cleanup steps run right away.
		waitUntil(ctx, start.Add(step.At))
		if (aborted || ctx.Err() != nil) && !step.Always {
			res.Skipped = true
			results = append(results, res)
			r.logf("[%6.1fs] %s: skipped", time.Since(start).Seconds(), name)
			continue
		}

		stepCtx := ctx
		if step.Always {
			stepCtx = context.WithoutCancel(ctx)
		}
		began := time.Now()
		res.StartedAt = began.Sub(start).Seconds()
		r.logf("[%6.1fs] %s: %s", res.StartedAt, name, describe(step))
		output, err := r.runStep(stepCtx, step, proxy)
		res.DurationSeconds = time.Since(began).Seconds()
		res.Output = output
		if err != nil {
			res.Error = err.Error()
			r.logf("[%6.1fs] %s failed: %v", time.Since(start).Seconds(), name, err)
			if !step.IgnoreError {
				aborted = true
				report.Failures = append(report.Failures, fmt.Sprintf("step %q: %v", name, err))
			}
		}
		results = append(results, res)
	}
	return results
}

func describe(step Step) string {
	switch step.action() {
	case "exec":
		return "exec " + step.Exec
	case "wait_healthy":
		return "wait for " + step.WaitHealthy
	case "faults":
		data, _ := json.Marshal(step.Faults)
		return "set proxy faults " + string(data)
	default:
		return "disconnect all proxied connections"
	}
}

func (r *Runner) runStep(ctx context.Context, step Step, proxy *Proxy) (string, error) {
	switch step.action() {
	case "exec":
		timeout := step.Timeout
		if timeout <= 0 {
			timeout = 2 * time.Minute
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		output, err := exec.CommandContext(ctx, "sh", "-c", step.Exec).CombinedOutput()
		return tail(string(output), 2048), err
	case "wait_healthy":
		timeout := step.Timeout
		if timeout <= 0 {
			timeout = 60 * time.Second
		}
		return "", waitHealthy(ctx, step.WaitHealthy, timeout)
	case "faults":
		return "", proxy.SetFaults(*step.Faults)
	case "disconnect":
		return fmt.Sprintf("%d connections reset", proxy.DisconnectAll()), nil
	}
	return "", errors.New("step has no action")
}

func waitHealthy(ctx context.Context, url string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	var lastErr error
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			err = fmt.Errorf("status %s", resp.Status)
		}
		lastErr = err
		if !waitUntil(ctx, time.Now().Add(500*time.Millisecond)) {
			return fmt.Errorf("%s not healthy after %s: %v", url, timeout, lastErr)
		}
	}
}

// waitUntil sleeps until t and reports false if ctx ended first.
func waitUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *Runner) reconcile(ctx context.Context, spec ReconcileSpec, m *loadgen.Manifest) (*reconcile.Report, error) {
	open := r.OpenSource
	if open == nil {
		open = OpenClickHouse
	}
	src, closer, err := open(spec)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	timeout := spec.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	rec, err := reconcile.Run(ctx, src, m, reconcile.Options{
		Timeout:       timeout,
		VerifyContent: spec.VerifyContent,
		Logf:          func(format string, args ...any) { r.logf("reconcile: "+format, args...) },
	})
	if err != nil {
		return nil, err
	}
	if err := writeReport(filepath.Join(r.Out, "reconcile.json"), rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// OpenClickHouse opens the ClickHouse source a ReconcileSpec describes.
func OpenClickHouse(spec ReconcileSpec) (reconcile.Source, io.Closer, error) {
	cfg, err := config.Load(spec.Config)
	if err != nil {
		return nil, nil, err
	}
	ch := cfg.ClickHouse
	if len(spec.Addr) > 0 {
		ch.Addr = spec.Addr
	}
	setString(&ch.Database, spec.Database)
	setString(&ch.Username, spec.Username)
	setString(&ch.Password, spec.Password)
	table := spec.Table
	if table == "" {
		table = reconcile.DefaultTable
	}
	src, err := reconcile.OpenClickHouse(ch.Addr, ch.Database, ch.Username, ch.Password, table)
	if err != nil {
		return nil, nil, err
	}
	return src, src, nil
}

func writeReport(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}

// WriteText prints a short human-readable report.
func (r *Report) WriteText(w io.Writer) {
	verdict := "PASS"
	if !r.Passed {
		verdict = "FAIL"
	}
	fmt.Fprintf(w, "scenario %s: %s in %.1fs\n", r.Name, verdict, r.DurationSeconds)
	for _, step := range r.Steps {
		status := "ok"
		switch {
		case step.Skipped:
			status = "skipped"
		case step.Error != "":
			status = "error: " + step.Error
		}
		fmt.Fprintf(w, "  step %-24s at %-6s %s\n", step.Name, step.At, status)
	}
	if r.Proxy != nil {
		fmt.Fprintf(w, "  proxy: %d connections, %d refused, %d reset\n", r.Proxy.Accepted, r.Proxy.Refused, r.Proxy.Resets)
	}
	r.Load.WriteText(w)
	if r.Reconcile != nil {
		r.Reconcile.WriteText(w)
	}
	for _, failure := range r.Failures {
		fmt.Fprintf(w, "  FAILURE: %s\n", failure)
	}
	fmt.Fprintf(w, "results in %s\n", r.ResultsDir)
}
