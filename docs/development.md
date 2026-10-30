[FairGate](../README.md) › [Docs](README.md) › Development

# Development

## Project structure

```text
FairGate/
├── internal/
│   ├── admit/       # Admission control
│   ├── chaos/       # Fault-injecting proxy and chaos scenario runner
│   ├── config/      # Config file, environment overrides, validation
│   ├── loadgen/     # Simulated producers, run results, and the event manifest
│   ├── observability/ # Prometheus metrics, collectors, pprof handler
│   ├── overload/    # Backlog-aware overload control before the WAL append
│   ├── reconcile/   # Checks a load manifest against stored events
│   ├── sched/       # Producer queues and DRR scheduler
│   ├── server/      # TCP listener, connection handling, shipper lifecycle
│   ├── shipper/     # WAL reader loop, checkpoints, retry/backoff, reclamation trigger
│   ├── store/       # Storage interface and ClickHouse adapter
│   ├── wal/         # Segmented WAL, group commit, recovery, and reclamation
│   └── wire/        # Frame codec, event and ACK types
├── cmd/
│   ├── gateway/     # The FairGate server
│   ├── loadgen/     # Load generator
│   ├── reconcile/   # Exactly-once reconciliation against ClickHouse
│   └── chaos/       # Fault proxy and chaos scenario runner
├── docs/            # Detailed documentation
├── deploy/          # Reference config, ClickHouse schema, Prometheus rules, Alertmanager, Grafana, chaos scenarios
├── data/            # Local WAL data and checkpoint (runtime; do not commit)
├── Dockerfile
├── docker-compose.yml
├── install-docker-ubuntu.sh
├── go.mod
└── README.md
```

This reflects the current core packages. Additional packages and commands may be added as development continues.

## Getting started

### Requirements

- Go toolchain compatible with the version declared in `go.mod`
- Git
- Docker Engine and Docker Compose plugin for container deployment (the gateway always constructs a ClickHouse store, so running it outside tests needs a reachable ClickHouse; the Compose stack provides one)

### Installation

```bash
git clone https://github.com/PratyushVishal11011/Fairgate.git
cd Fairgate
go mod download
```

### Build and test

```bash
# Build all packages
go build ./...

# Run the test suite
go test ./...

# Run tests with the race detector
go test -race ./...
```

CI runs `go test -race -v ./...` on every push through GitHub Actions.

The graceful-shutdown tests use a no-op `Store` and a temporary checkpoint path, so the suite does not need a live ClickHouse service. The ClickHouse integration tests run only when `CLICKHOUSE_ADDR` is set:

```bash
CLICKHOUSE_ADDR=127.0.0.1:9000 go test -race ./internal/store/ ./internal/shipper/ ./internal/reconcile/
```

The load generator, reconciliation, and chaos tools are tested without Docker; `internal/server` runs them end to end against the real gateway with an in-memory store. To run them against a live stack, see [Load, reconcile, and chaos tools](tooling.md).

The repository is being developed as a gateway implementation. A stable command-line interface and client SDK are planned. Check the repository's current entry point and configuration before attempting to launch a server, or use the Compose stack described in [Docker deployment](deployment.md).
