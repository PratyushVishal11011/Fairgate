[FairGate](../README.md) › [Docs](README.md) › Roadmap

# Roadmap

The roadmap lists planned work in dependency order. Items are subject to change.

```mermaid
flowchart TD
    P1["Phase 1: Core ingestion (implemented)<br/>TCP server, wire protocol, admission control<br/>Event identity and idempotent ingestion<br/>Per-producer queues, weighted DRR scheduling<br/>Segmented WAL with group commit and ordered recovery<br/>Context-aware lifecycle and graceful shutdown<br/>Store interface, ClickHouse adapter<br/>Health endpoint, Prometheus metrics, Grafana overview"]
    P2["Phase 2: Durable delivery (implemented)<br/>WAL shipping, checkpoints, retry and backoff<br/>Always-on runtime wiring<br/>Checkpoint-based WAL segment reclamation with durable-end guard<br/>End-to-end exactly-once delivery to ClickHouse"]
    P3["Phase 3: Fairness and overload control (in progress)<br/>Producer weights and per-producer limits (implemented)<br/>Backlog-aware overload handling (implemented)<br/>Load shedding (planned)"]
    P4["Phase 4: Operability and hardening (in progress)<br/>Profiling, pipeline metrics, alerting (implemented)<br/>Packaged load, reconcile, and chaos tools (planned)<br/>Protocol hardening (planned)"]

    P1 --> P2 --> P3 --> P4

    classDef done stroke-width:3px;
    classDef planned stroke-dasharray: 5 5,stroke-width:2px;
    class P1,P2 done;
    class P3,P4 planned;
```

A solid border marks implemented work. Dashed borders mark planned work.

| Area | Status |
|---|---|
| Ingestion, validation, admission control | Implemented |
| Producer epochs, event identity, idempotent ingestion | Implemented |
| Per-producer queues, DRR scheduling | Implemented |
| Configuration file with environment overrides | Implemented |
| WAL append and recovery | Implemented |
| WAL segmentation and group commit | Implemented |
| Cross-segment recovery validation | Implemented |
| Sparse segment sets (discovery, recovery, reader) | Implemented |
| Context-aware lifecycle, connection tracking, graceful shutdown | Implemented |
| Cancelable enqueueing and scheduler error propagation | Implemented |
| `Store` interface, ClickHouse adapter | Implemented |
| Optional ClickHouse integration test | Implemented |
| TCP integration tests, CI race detection | Implemented |
| Durable WAL reader and checkpoint-based batch shipping | Implemented |
| Durable checkpoints and retry with backoff | Implemented |
| Stable batch tokens and deterministic event IDs | Implemented |
| Pending-batch intent and exact-range replay | Implemented |
| Exactly-once ClickHouse writes (existence check, row-bound token) | Implemented; single writer per event identity |
| Shipper runtime wiring and lifecycle | Implemented; shipper always runs |
| WAL segment reclamation | Implemented for segments older than the committed checkpoint segment |
| Durable-end guard on reclamation | Implemented (`ErrCheckpointAhead`); same-segment offset check planned |
| Docker image, Compose stack | Implemented |
| Health endpoint, basic Prometheus metrics, Grafana overview dashboard | Implemented |
| Producer weights and per-producer admission and queue limits | Implemented |
| Backlog-aware overload handling | Implemented |
| Profiling, pipeline metrics, alerting | Implemented |
| Packaged load generator, reconciliation, chaos tooling | Planned |
| Explicit queue-drain confirmation on shutdown | Planned |
| Protocol hardening (malformed and oversized frame handling) | Planned |
