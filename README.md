# FairGate

**A fair-share event ingestion gateway written in Go.**

FairGate accepts framed event streams over TCP and keeps producers isolated from one another. Each producer has its own admission limit and its own bounded queue, and a weighted Deficit Round Robin (DRR) scheduler gives each producer its share. When queues or delivery fall behind, new events are refused with a retryable rejection instead of piling up. Accepted events are persisted in a segmented Write-Ahead Log (WAL) before they are acknowledged. A checkpointed shipper then delivers them to ClickHouse exactly once per event identity.

> [!IMPORTANT]
> **Beta status.** FairGate is under active development. Interfaces, protocol details, configuration, and storage semantics may change. See [Limitations](docs/limitations.md) before relying on it.

## Highlights

- **Producer isolation and fairness.** Per-producer token-bucket admission and bounded queues, plus weighted DRR scheduling. Weights, rate limits, and queue sizes can be set per producer. [More](docs/scheduling.md)
- **Backlog-aware overload handling.** Before the WAL append, events get a retryable `rejected` ACK when the producer's queue is near full, when the gateway's total queue is over its limit (only producers above their weighted share), or when delivery to ClickHouse is too far behind. Identical retries are still accepted. [More](docs/scheduling.md#overload-handling)
- **Durable before acknowledged.** Events are written to the WAL and synced (with group commit) before the ACK is sent. Recovery replays them after a crash. [More](docs/wal.md)
- **Safe producer retries.** Each event has a stable identity. An identical retry is acknowledged without being stored twice, and reuse of an identity with different content is rejected. [More](docs/event-model.md)
- **Exactly-once delivery to ClickHouse.** Each event identity is stored once, through crashes, replays, and lost checkpoints. [More](docs/delivery-semantics.md)
- **Hardened wire protocol.** Every frame gets exactly one ACK, including malformed, oversized, and unsupported ones, so pipelining clients stay in step. Frame memory follows the bytes actually received, and stalled frames, unread ACKs, and optionally idle connections are timed out. [More](docs/wire-protocol.md#error-handling)
- **One config file.** Optional YAML config with strict validation. Environment variables override it for secrets. [More](docs/configuration.md)
- **Load, reconcile, and chaos tools.** `loadgen` drives the gateway with simulated producers, `reconcile` checks that every accepted event reached ClickHouse exactly once, and `chaos` injects network faults and runs crash and outage scenarios. [More](docs/tooling.md)
- **Observable.** Prometheus metrics for every pipeline stage, ready-made alert rules with Alertmanager, a provisioned Grafana dashboard, and optional `pprof` profiling. [More](docs/observability.md)

## Quick start

Run the full stack (FairGate, ClickHouse, Prometheus, Alertmanager, and Grafana) with Docker Compose:

```bash
git clone https://github.com/PratyushVishal11011/Fairgate.git
cd Fairgate
sudo docker compose up --build -d
```

| Endpoint | Address |
|---|---|
| Event ingestion (TCP, length-prefixed frames) | `localhost:9001` |
| Health and metrics | [http://localhost:9100/health](http://localhost:9100/health), [/metrics](http://localhost:9100/metrics) |
| Grafana (`admin` / `admin`) | [http://localhost:3001](http://localhost:3001) |
| Prometheus | [http://localhost:9090](http://localhost:9090) |
| Alertmanager | [http://localhost:9093](http://localhost:9093) |

The Compose file ships with development credentials. Replace them, and restrict the published ports, before any non-development use. See [Deployment](docs/deployment.md).

To build and test from source (Go version as declared in `go.mod`):

```bash
go build ./...
go test -race ./...
```

The ClickHouse integration tests run only when `CLICKHOUSE_ADDR` is set. See [Development](docs/development.md).

To load the running stack and check that every accepted event was stored exactly once:

```bash
go run ./cmd/loadgen -producers 100 -events 1000 -rate 100 -out benchmarks/run1
go run ./cmd/reconcile -manifest benchmarks/run1/manifest.json
```

To kill the gateway mid-load and check the same thing, run a chaos scenario. See [Load, reconcile, and chaos tools](docs/tooling.md).

```bash
go run ./cmd/chaos run deploy/chaos/gateway-crash.yaml
```

## Configuration

Pass a YAML file with `-config` or `FAIRGATE_CONFIG`. Without one, FairGate runs with built-in defaults. [`deploy/fairgate.yaml`](deploy/fairgate.yaml) lists every setting with its default:

```yaml
server:
  listen_addr: ":9001"
admission:
  rate: 500
  burst: 1000
fairness:
  default_weight: 1
  producers:
    payments-service:
      weight: 4
      rate: 2000
overload:
  queue_high_water: 0.9
  max_queued_events: 100000
  max_shipper_lag_bytes: 1073741824
```

See [Configuration](docs/configuration.md) for all settings and environment overrides.

## Documentation

| Document | Covers |
|---|---|
| [Documentation index](docs/README.md) | Every document, grouped by topic. |
| [Deployment](docs/deployment.md) | Docker Compose, ports, remote access, Apple Container. |
| [Configuration](docs/configuration.md) | Config file, settings, fairness overrides, overload limits, environment variables. |
| [Backpressure, scheduling, and fairness](docs/scheduling.md) | Queues, overload handling, weighted DRR, per-producer limits. |
| [Observability](docs/observability.md) | Metrics, dashboard, alerting, profiling. |
| [Delivery semantics](docs/delivery-semantics.md) | ACK meaning and the exactly-once guarantee. |
| [Event model and identity](docs/event-model.md) | Event fields, identity, idempotent retries. |
| [Wire protocol](docs/wire-protocol.md) | Frame format and acknowledgments. |
| [Load, reconcile, and chaos tools](docs/tooling.md) | Load generator, exactly-once reconciliation, fault proxy, chaos scenarios. |
| [Architecture](docs/architecture.md) | Design goals, components, event lifecycle. |
| [Write-Ahead Log](docs/wal.md) | Segments, group commit, reclamation, recovery. |
| [Storage integration](docs/storage.md) | Shipper, batch identity, ClickHouse schema. |
| [Server lifecycle and graceful shutdown](docs/server-lifecycle.md) | Contexts, shutdown sequence, embedding. |
| [Features](docs/features.md) | Everything currently implemented. |
| [Development](docs/development.md) | Project structure, building, testing. |
| [Limitations](docs/limitations.md) | Known limitations of the beta. |
| [Roadmap](docs/roadmap.md) | Implemented and planned work. |
| [Changelog](docs/changelog.md) | Dated updates. |

## Contributing

Issues, bug reports, design discussions, and pull requests are welcome. For substantial changes, open an issue first to discuss the proposed behavior or interface.

When contributing:

1. Keep core ingestion and scheduling independent of business-specific logic.
2. Prefer bounded memory and explicit backpressure.
3. Add tests for new behavior and failure paths.
4. Run `go test ./...` and `go test -race ./...` before submitting a pull request.

## License

FairGate is released under the [MIT License](LICENSE.txt).
