// Package config loads FairGate's runtime settings.
//
// Settings are resolved in three layers, each overriding the one before:
// built-in defaults, an optional YAML file, and environment variables.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server     Server     `yaml:"server"`
	Admission  Admission  `yaml:"admission"`
	Queues     Queues     `yaml:"queues"`
	Scheduler  Scheduler  `yaml:"scheduler"`
	WAL        WAL        `yaml:"wal"`
	Shipper    Shipper    `yaml:"shipper"`
	ClickHouse ClickHouse `yaml:"clickhouse"`
	Log        Log        `yaml:"log"`
}

type Server struct {
	//TCP address for event ingestion.
	ListenAddr string `yaml:"listen_addr"`
	//HTTP address for /health and /metrics.
	MetricsAddr string `yaml:"metrics_addr"`
	//time active handlers get to finish before their connections are closed.
	ShutdownGracePeriod time.Duration `yaml:"shutdown_grace_period"`
}

type Admission struct {
	//token-bucket refill rate per producer, in events per second.
	Rate float64 `yaml:"rate"`
	//token-bucket capacity per producer.
	Burst int `yaml:"burst"`
}

type Queues struct {
	//events buffered per producer before enqueueing blocks.
	Capacity int `yaml:"capacity"`
}

type Scheduler struct {
	//maximum events served from one producer queue per DRR visit.
	Quantum int `yaml:"quantum"`
}

type WAL struct {
	//path of the first WAL segment; later segments are created next to it.
	Path string `yaml:"path"`
	//segment size in bytes before the writer rotates to a new segment.
	SegmentSize int64 `yaml:"segment_size"`
}

type Shipper struct {
	CheckpointPath string        `yaml:"checkpoint_path"`
	BatchSize      int           `yaml:"batch_size"`
	PollInterval   time.Duration `yaml:"poll_interval"`
	Retry          Retry         `yaml:"retry"`
}

type Retry struct {
	InitialDelay time.Duration `yaml:"initial_delay"`
	MaxDelay     time.Duration `yaml:"max_delay"`
	Multiplier   float64       `yaml:"multiplier"`
	//symmetric jitter as a fraction of the delay, between 0 and 1.
	Jitter float64 `yaml:"jitter"`
}

type ClickHouse struct {
	Addr     []string `yaml:"addr"`
	Database string   `yaml:"database"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
}

type Log struct {
	//one of debug, info, warn, error.
	Level string `yaml:"level"`
}

// Default returns the settings FairGate used before it was configurable.
func Default() Config {
	return Config{
		Server: Server{
			ListenAddr:          ":9001",
			MetricsAddr:         ":9100",
			ShutdownGracePeriod: 5 * time.Second,
		},
		Admission: Admission{
			Rate:  500,
			Burst: 1000,
		},
		Queues: Queues{
			Capacity: 512,
		},
		Scheduler: Scheduler{
			Quantum: 8,
		},
		WAL: WAL{
			Path:        "data/fairgate.wal",
			SegmentSize: 64 << 20,
		},
		Shipper: Shipper{
			CheckpointPath: "data/checkpoint.json",
			BatchSize:      200,
			PollInterval:   time.Second,
			Retry: Retry{
				InitialDelay: time.Second,
				MaxDelay:     30 * time.Second,
				Multiplier:   2,
				Jitter:       0.2,
			},
		},
		ClickHouse: ClickHouse{
			Addr:     []string{"localhost:9000"},
			Database: "fairgate",
			Username: "fairgate",
			Password: "fairgate_dev_password",
		},
		Log: Log{
			Level: "info",
		},
	}
}

// Load resolves the configuration from defaults, the YAML file at path (skipped when path is empty),
// and environment variables, then validates the result.
func Load(path string) (Config, error) {
	cfg := Default()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config file: %w", err)
		}

		if err := decode(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("parse config file %s: %w", path, err)
		}
	}

	cfg.applyEnv(os.Getenv)

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// FromEnv resolves the configuration from defaults and environment variables only.
func FromEnv() (Config, error) {
	return Load("")
}

func decode(data []byte, cfg *Config) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	//reject unknown keys so a misspelled setting fails instead of being ignored.
	decoder.KnownFields(true)

	if err := decoder.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return nil
}

// applyEnv overrides file settings with the environment variables FairGate already supported.
func (c *Config) applyEnv(getenv func(string) string) {
	if value := getenv("FAIRGATE_METRICS_ADDR"); value != "" {
		c.Server.MetricsAddr = value
	}
	if value := getenv("FAIRGATE_CHECKPOINT_PATH"); value != "" {
		c.Shipper.CheckpointPath = value
	}
	if value := getenv("CLICKHOUSE_ADDR"); value != "" {
		c.ClickHouse.Addr = splitList(value)
	}
	if value := getenv("CLICKHOUSE_DATABASE"); value != "" {
		c.ClickHouse.Database = value
	}
	if value := getenv("CLICKHOUSE_USER"); value != "" {
		c.ClickHouse.Username = value
	}
	if value := getenv("CLICKHOUSE_PASSWORD"); value != "" {
		c.ClickHouse.Password = value
	}
}

func splitList(value string) []string {
	var items []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}

// Validate reports every invalid setting at once.
func (c Config) Validate() error {
	var problems []string
	check := func(ok bool, format string, args ...any) {
		if !ok {
			problems = append(problems, fmt.Sprintf(format, args...))
		}
	}

	check(c.Server.ListenAddr != "", "server.listen_addr is required")
	check(c.Server.MetricsAddr != "", "server.metrics_addr is required")
	check(c.Server.ShutdownGracePeriod > 0, "server.shutdown_grace_period must be greater than zero")

	check(c.Admission.Rate > 0, "admission.rate must be greater than zero")
	check(c.Admission.Burst > 0, "admission.burst must be greater than zero")

	check(c.Queues.Capacity > 0, "queues.capacity must be greater than zero")
	check(c.Scheduler.Quantum > 0, "scheduler.quantum must be greater than zero")

	check(c.WAL.Path != "", "wal.path is required")
	check(c.WAL.SegmentSize > 0, "wal.segment_size must be greater than zero")

	check(c.Shipper.CheckpointPath != "", "shipper.checkpoint_path is required")
	check(c.Shipper.BatchSize > 0, "shipper.batch_size must be greater than zero")
	check(c.Shipper.PollInterval > 0, "shipper.poll_interval must be greater than zero")

	retry := c.Shipper.Retry
	check(retry.InitialDelay > 0, "shipper.retry.initial_delay must be greater than zero")
	check(retry.MaxDelay >= retry.InitialDelay, "shipper.retry.max_delay must be at least shipper.retry.initial_delay")
	check(retry.Multiplier >= 1, "shipper.retry.multiplier must be at least 1")
	check(retry.Jitter >= 0 && retry.Jitter <= 1, "shipper.retry.jitter must be between 0 and 1")

	check(len(c.ClickHouse.Addr) > 0, "clickhouse.addr needs at least one address")
	for _, addr := range c.ClickHouse.Addr {
		check(addr != "", "clickhouse.addr entries must not be empty")
	}
	check(c.ClickHouse.Database != "", "clickhouse.database is required")

	_, err := c.LogLevel()
	check(err == nil, "log.level must be one of debug, info, warn, error")

	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration:\n  - %s", strings.Join(problems, "\n  - "))
	}

	return nil
}

// LogLevel parses Log.Level.
func (c Config) LogLevel() (slog.Level, error) {
	var level slog.Level
	err := level.UnmarshalText([]byte(c.Log.Level))
	return level, err
}
