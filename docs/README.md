[FairGate](../README.md) › Docs

# FairGate documentation

Detailed documentation for FairGate. For a short overview and a quick start, see the [project README](../README.md).

## Using FairGate

| Document | Covers |
|---|---|
| [Deployment](deployment.md) | Docker Compose stack, published ports, remote access, and native Apple Container on macOS. |
| [Configuration](configuration.md) | The YAML config file, every setting and its default, fairness overrides, and environment overrides. |
| [Backpressure, scheduling, and fairness](scheduling.md) | Per-producer queues, weighted DRR scheduling, producer weights, and per-producer admission and queue limits. |
| [Observability](observability.md) | Metrics reference, Grafana dashboard, alert rules and Alertmanager, and profiling. |
| [Delivery semantics](delivery-semantics.md) | What an ACK means, the exactly-once guarantee, and the conditions it depends on. |
| [Event model and identity](event-model.md) | Event fields, validation, event identity, and idempotent retries. |
| [Wire protocol](wire-protocol.md) | Frame layout, frame types, and acknowledgments. |
| [Load, reconcile, and chaos tools](tooling.md) | The `loadgen`, `reconcile`, and `chaos` commands, the manifest format, the fault proxy, and chaos scenarios. |

## How it works

| Document | Covers |
|---|---|
| [Architecture](architecture.md) | Design goals, components and their responsibilities, and the event lifecycle. |
| [Write-Ahead Log](wal.md) | Segments, group commit, reclamation, recovery, and the durable reader. |
| [Storage integration](storage.md) | The shipper, batch and event identity, duplicate handling, and the ClickHouse schema. |
| [Server lifecycle and graceful shutdown](server-lifecycle.md) | Goroutines and contexts, the shutdown sequence, and embedding the server. |

## Project

| Document | Covers |
|---|---|
| [Features](features.md) | Everything currently implemented. |
| [Development](development.md) | Project structure, requirements, building, and running the tests. |
| [Limitations](limitations.md) | Known limitations. |
| [Security](SECURITY.md) | Trust model, what a connected client can do, safe deployment, and reporting a vulnerability. |
| [Roadmap](roadmap.md) | Implemented and planned work. |
| [Changelog](changelog.md) | Dated updates, newest first. |
