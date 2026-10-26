package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

var envKeys = []string{
	"FAIRGATE_METRICS_ADDR",
	"FAIRGATE_CHECKPOINT_PATH",
	"CLICKHOUSE_ADDR",
	"CLICKHOUSE_DATABASE",
	"CLICKHOUSE_USER",
	"CLICKHOUSE_PASSWORD",
}

// clearEnv makes the test independent of the caller's environment.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, key := range envKeys {
		t.Setenv(key, "")
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fairgate.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaultIsValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadWithoutFileUsesDefaults(t *testing.T) {
	clearEnv(t)

	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg, Default()) {
		t.Fatalf("expected defaults, got %+v", cfg)
	}
}

func TestReferenceConfigMatchesDefaults(t *testing.T) {
	clearEnv(t)

	cfg, err := Load(filepath.Join("..", "..", "deploy", "fairgate.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg, Default()) {
		t.Fatalf("deploy/fairgate.yaml has drifted from Default():\nfile:     %+v\ndefaults: %+v", cfg, Default())
	}
}

func TestLoadOverridesOnlyGivenSettings(t *testing.T) {
	clearEnv(t)

	path := writeConfig(t, `
server:
  listen_addr: "127.0.0.1:7001"
  shutdown_grace_period: 1500ms
wal:
  segment_size: 1048576
shipper:
  retry:
    max_delay: 1m
clickhouse:
  addr: [ch-1:9000, ch-2:9000]
log:
  level: debug
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	want := Default()
	want.Server.ListenAddr = "127.0.0.1:7001"
	want.Server.ShutdownGracePeriod = 1500 * time.Millisecond
	want.WAL.SegmentSize = 1 << 20
	want.Shipper.Retry.MaxDelay = time.Minute
	want.ClickHouse.Addr = []string{"ch-1:9000", "ch-2:9000"}
	want.Log.Level = "debug"

	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("got %+v\nwant %+v", cfg, want)
	}
}

func TestLoadAcceptsEmptyFile(t *testing.T) {
	clearEnv(t)

	cfg, err := Load(writeConfig(t, "# nothing set\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg, Default()) {
		t.Fatalf("expected defaults, got %+v", cfg)
	}
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	clearEnv(t)

	_, err := Load(writeConfig(t, "shipper:\n  batchsize: 10\n"))
	if err == nil || !strings.Contains(err.Error(), "batchsize") {
		t.Fatalf("expected unknown key error, got %v", err)
	}
}

func TestLoadRejectsMissingFile(t *testing.T) {
	clearEnv(t)

	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("expected missing file error")
	}
}

func TestEnvironmentOverridesFile(t *testing.T) {
	clearEnv(t)
	t.Setenv("FAIRGATE_METRICS_ADDR", ":9200")
	t.Setenv("FAIRGATE_CHECKPOINT_PATH", "/var/lib/fairgate/checkpoint.json")
	t.Setenv("CLICKHOUSE_ADDR", "ch-a:9000, ch-b:9000")
	t.Setenv("CLICKHOUSE_DATABASE", "events_db")
	t.Setenv("CLICKHOUSE_USER", "writer")
	t.Setenv("CLICKHOUSE_PASSWORD", "secret")

	path := writeConfig(t, `
server:
  metrics_addr: ":9300"
clickhouse:
  addr: [file:9000]
  password: from-file
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Server.MetricsAddr != ":9200" {
		t.Errorf("metrics_addr: got %q", cfg.Server.MetricsAddr)
	}
	if cfg.Shipper.CheckpointPath != "/var/lib/fairgate/checkpoint.json" {
		t.Errorf("checkpoint_path: got %q", cfg.Shipper.CheckpointPath)
	}
	if !reflect.DeepEqual(cfg.ClickHouse.Addr, []string{"ch-a:9000", "ch-b:9000"}) {
		t.Errorf("clickhouse.addr: got %q", cfg.ClickHouse.Addr)
	}
	if cfg.ClickHouse.Database != "events_db" || cfg.ClickHouse.Username != "writer" || cfg.ClickHouse.Password != "secret" {
		t.Errorf("clickhouse credentials not overridden: %+v", cfg.ClickHouse)
	}
}

func TestValidateReportsEveryProblem(t *testing.T) {
	clearEnv(t)

	_, err := Load(writeConfig(t, `
admission:
  rate: 0
queues:
  capacity: -1
shipper:
  retry:
    initial_delay: 10s
    max_delay: 1s
    jitter: 1.5
log:
  level: loud
`))
	if err == nil {
		t.Fatal("expected validation error")
	}

	for _, want := range []string{
		"admission.rate",
		"queues.capacity",
		"shipper.retry.max_delay",
		"shipper.retry.jitter",
		"log.level",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("validation error does not mention %s:\n%v", want, err)
		}
	}
}
