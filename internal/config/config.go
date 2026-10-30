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
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server     Server     `yaml:"server"`
	Admission  Admission  `yaml:"admission"`
	Queues     Queues     `yaml:"queues"`
	Scheduler  Scheduler  `yaml:"scheduler"`
	Fairness   Fairness   `yaml:"fairness"`
	Overload   Overload   `yaml:"overload"`
	WAL        WAL        `yaml:"wal"`
	Shipper    Shipper    `yaml:"shipper"`
	ClickHouse ClickHouse `yaml:"clickhouse"`
	Log        Log        `yaml:"log"`
	Profiling  Profiling  `yaml:"profiling"`
}

// Bounds on server.max_frame_bytes. Events larger than the WAL's 1 MiB record limit are refused
// anyway, so a much larger frame limit only costs memory.
const (
	MinMaxFrameBytes = 1 << 10
	MaxMaxFrameBytes = 64 << 20
)

type Server struct {
	//TCP address for event ingestion.
	ListenAddr string `yaml:"listen_addr"`
	//HTTP address for /health and /metrics.
	MetricsAddr string `yaml:"metrics_addr"`
	//time active handlers get to finish before their connections are closed.
	ShutdownGracePeriod time.Duration `yaml:"shutdown_grace_period"`
	//after handlers stop, time to empty the producer queues and ship the WAL to ClickHouse; 0 skips the drain.
	DrainTimeout time.Duration `yaml:"drain_timeout"`
	//largest frame accepted, counting the type and flags bytes; larger frames are answered and skipped.
	MaxFrameBytes int `yaml:"max_frame_bytes"`
	//close a connection that starts no new frame for this long; 0 keeps idle connections open.
	IdleTimeout time.Duration `yaml:"idle_timeout"`
	//time to receive the rest of a frame once its first byte has arrived.
	FrameTimeout time.Duration `yaml:"frame_timeout"`
	//time to write one ACK before the connection is closed.
	WriteTimeout time.Duration `yaml:"write_timeout"`
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

// Fairness controls how producers share each stage of the pipeline.
type Fairness struct {
	//DRR weight for producers that are not listed in Producers.
	DefaultWeight float64 `yaml:"default_weight"`
	//per-producer overrides keyed by producer_id; omitted or zero fields keep the defaults.
	Producers map[string]ProducerFairness `yaml:"producers"`
}

type ProducerFairness struct {
	//share of each DRR round relative to other producers: quantum * weight events per round.
	Weight float64 `yaml:"weight"`
	//admission token-bucket refill rate in events per second, replacing admission.rate.
	Rate float64 `yaml:"rate"`
	//admission token-bucket capacity, replacing admission.burst.
	Burst int `yaml:"burst"`
	//queue capacity in events, replacing queues.capacity.
	QueueCapacity int `yaml:"queue_capacity"`
}

// Weights returns the per-producer DRR weights that are set.
func (f Fairness) Weights() map[string]float64 {
	weights := make(map[string]float64)
	for producerId, producer := range f.Producers {
		if producer.Weight > 0 {
			weights[producerId] = producer.Weight
		}
	}
	return weights
}

// QueueCapacities returns the per-producer queue capacities that are set.
func (f Fairness) QueueCapacities() map[string]int {
	capacities := make(map[string]int)
	for producerId, producer := range f.Producers {
		if producer.QueueCapacity > 0 {
			capacities[producerId] = producer.QueueCapacity
		}
	}
	return capacities
}

// Overload sets the backlogs at which new events are refused with a retryable rejection
// before they are appended to the WAL. A zero limit disables that check.
type Overload struct {
	//refuse a producer's events once its queue holds this fraction of its capacity, from 0 to 1.
	QueueHighWater float64 `yaml:"queue_high_water"`
	//events queued across all producers before producers above their weighted share are refused.
	MaxQueuedEvents int `yaml:"max_queued_events"`
	//durable WAL bytes the shipper may trail by before every new event is refused.
	MaxShipperLagBytes int64 `yaml:"max_shipper_lag_bytes"`
	//a gateway-wide limit clears once its backlog falls below this fraction of it.
	ResumeRatio float64 `yaml:"resume_ratio"`
	//how often the gateway-wide backlogs are sampled.
	SampleInterval time.Duration `yaml:"sample_interval"`
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

// Profiling controls the net/http/pprof endpoints.
type Profiling struct {
	//serve /debug/pprof/ on Addr; off by default because profiles expose internals and cost CPU.
	Enabled bool `yaml:"enabled"`
	//listener for the pprof endpoints, separate from server.metrics_addr.
	Addr string `yaml:"addr"`
	//runtime.SetBlockProfileRate: record one blocking event per this many nanoseconds blocked; 0 disables.
	BlockProfileRate int `yaml:"block_profile_rate"`
	//runtime.SetMutexProfileFraction: sample one in this many mutex contention events; 0 disables.
	MutexProfileFraction int `yaml:"mutex_profile_fraction"`
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
			DrainTimeout:        10 * time.Second,
			MaxFrameBytes:       5 << 20,
			FrameTimeout:        30 * time.Second,
			WriteTimeout:        10 * time.Second,
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
		Fairness: Fairness{
			DefaultWeight: 1,
		},
		Overload: Overload{
			QueueHighWater:     0.9,
			MaxQueuedEvents:    100000,
			MaxShipperLagBytes: 1 << 30,
			ResumeRatio:        0.8,
			SampleInterval:     100 * time.Millisecond,
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
		Profiling: Profiling{
			Enabled: false,
			Addr:    "127.0.0.1:6060",
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
	check(c.Server.DrainTimeout >= 0, "server.drain_timeout cannot be negative")
	check(c.Server.MaxFrameBytes >= MinMaxFrameBytes && c.Server.MaxFrameBytes <= MaxMaxFrameBytes,
		"server.max_frame_bytes must be between %d and %d", MinMaxFrameBytes, MaxMaxFrameBytes)
	check(c.Server.IdleTimeout >= 0, "server.idle_timeout cannot be negative")
	check(c.Server.FrameTimeout > 0, "server.frame_timeout must be greater than zero")
	check(c.Server.WriteTimeout > 0, "server.write_timeout must be greater than zero")

	check(c.Admission.Rate > 0, "admission.rate must be greater than zero")
	check(c.Admission.Burst > 0, "admission.burst must be greater than zero")

	check(c.Queues.Capacity > 0, "queues.capacity must be greater than zero")
	check(c.Scheduler.Quantum > 0, "scheduler.quantum must be greater than zero")

	check(c.Fairness.DefaultWeight > 0, "fairness.default_weight must be greater than zero")
	for _, producerId := range sortedKeys(c.Fairness.Producers) {
		producer := c.Fairness.Producers[producerId]
		prefix := fmt.Sprintf("fairness.producers.%s", producerId)
		check(producerId != "", "fairness.producers keys must be non-empty producer IDs")
		check(producer.Weight >= 0, "%s.weight cannot be negative", prefix)
		check(producer.Rate >= 0, "%s.rate cannot be negative", prefix)
		check(producer.Burst >= 0, "%s.burst cannot be negative", prefix)
		check(producer.QueueCapacity >= 0, "%s.queue_capacity cannot be negative", prefix)
	}

	overload := c.Overload
	check(overload.QueueHighWater >= 0 && overload.QueueHighWater <= 1, "overload.queue_high_water must be between 0 and 1")
	check(overload.MaxQueuedEvents >= 0, "overload.max_queued_events cannot be negative")
	check(overload.MaxShipperLagBytes >= 0, "overload.max_shipper_lag_bytes cannot be negative")
	check(overload.ResumeRatio > 0 && overload.ResumeRatio <= 1, "overload.resume_ratio must be greater than 0 and at most 1")
	check(overload.SampleInterval > 0, "overload.sample_interval must be greater than zero")

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

	check(!c.Profiling.Enabled || c.Profiling.Addr != "", "profiling.addr is required when profiling is enabled")
	check(c.Profiling.BlockProfileRate >= 0, "profiling.block_profile_rate cannot be negative")
	check(c.Profiling.MutexProfileFraction >= 0, "profiling.mutex_profile_fraction cannot be negative")

	_, err := c.LogLevel()
	check(err == nil, "log.level must be one of debug, info, warn, error")

	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration:\n  - %s", strings.Join(problems, "\n  - "))
	}

	return nil
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// LogLevel parses Log.Level.
func (c Config) LogLevel() (slog.Level, error) {
	var level slog.Level
	err := level.UnmarshalText([]byte(c.Log.Level))
	return level, err
}
