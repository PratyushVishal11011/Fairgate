[FairGate](../README.md) › [Docs](README.md) › Features

# Features

## Currently implemented

- **TCP event ingestion.** Accepts client connections and reads length-prefixed frames.
- **Framed wire protocol.** Identifies event and acknowledgment frames using frame types. The maximum frame size is 5 MiB.
- **JSON event decoding.** Decodes events and checks required fields, including `producer_epoch`.
- **Admission control.** Applies token-bucket limits to incoming events per producer (500 events per second with a burst of 1,000 by default), with optional per-producer rate and burst.
- **Event idempotency.** A deduplication manager identifies each event by producer ID, producer epoch, sequence number, and index. An identical retry is acknowledged without being appended to the WAL again. Reusing an identity with a different event time, type, or payload is rejected. A new identity is recorded only after the event has been persisted to the WAL.
- **Producer epochs.** The epoch is part of event identity, so identity stays unambiguous across producer restarts.
- **Per-producer queues.** Keeps producer events in separate bounded channels (512 events each by default, configurable per producer). Queues also track event identities so duplicates are not enqueued twice.
- **Weighted DRR scheduling.** Serves backlogged producers in rounds, giving each `quantum × weight` events per round. Fractional weights are supported, and idle producers don't bank credit.
- **Backlog-aware overload handling.** Refuses new events with a retryable `rejected` ACK before the WAL append when the producer's queue reaches a high-water mark, when the gateway's total queued events pass a limit (only producers above their weighted share), or when delivery to ClickHouse falls too far behind. Identical retries are still accepted. See [Overload handling](scheduling.md#overload-handling).
- **Producer fairness controls.** Per-producer overrides for DRR weight, admission rate and burst, and queue capacity, set under `fairness.producers` in the config file. See [Backpressure, scheduling, and fairness](scheduling.md).
- **Segmented Write-Ahead Log.** Appends events with length and CRC metadata across multiple segment files, rotating to a new segment when the configured segment size would be exceeded.
- **Group commit.** A dedicated writer goroutine batches concurrent append requests and issues a single `Sync()` per batch, bounded by a maximum request count and a short collection delay.
- **WAL recovery.** Lists the segment files that exist, sorts them by ID, scans them in order on startup, truncates an incomplete trailing record in the final segment only, rejects incomplete records in earlier segments, tolerates a sparse segment set, and replays recovered events into the producer queues.
- **WAL fatal error handling.** Write, rotation, and synchronization failures are recorded as fatal; affected requests receive an error and later batches are rejected.
- **Durable WAL reader.** Reads records only up to the WAL's synchronized durable end, verifies length, CRC32, and JSON, returns each event with its start and end positions, and advances across segment boundaries (to the next segment that exists) from any saved position.
- **WAL-backed batch shipper.** `Shipper.Run(ctx)` loads the checkpoint, reads durable records in order up to `BatchSize`, and sends each batch to the configured `Store`. At the durable WAL end it polls for new records and keeps running until its context is canceled.
- **Always-on gateway shipper.** `RunContext` always constructs the ClickHouse store and starts the shipper alongside the server. Shutdown always cancels and joins it before the store and WAL are closed.
- **Retry with backoff.** Retries failed batch inserts with capped exponential delay, configurable jitter, and context-aware waiting. The same batch is retried until it succeeds or the context is canceled.
- **Durable checkpoints.** Stores the last successfully inserted record position as a segment and offset. Checkpoints are written through a synced temporary file, an atomic rename, and a sync of the containing directory. A missing checkpoint starts reading at the beginning of the WAL, and invalid JSON or negative offsets are rejected.
- **Pending-batch intent.** Before each insert, the shipper durably records the batch's end position as `pending` in the checkpoint. After a crash, it first replays exactly that WAL range with the same token, even if more records have become durable or the batch size has changed. A pending end that is not a record boundary, or that lies past the durable end, is rejected before anything reaches the store.
- **Checkpoint-based WAL reclamation.** After each committed checkpoint, and once at startup against the checkpoint already on disk, the shipper calls `WAL.ReclaimBefore`. Only segments with IDs strictly less than the checkpoint segment are deleted; the checkpoint segment and the active segment are retained. A checkpoint beyond the durable end is rejected with `ErrCheckpointAhead`, and nothing is deleted. Reclamation is serialized with rotation, and the containing directory is synced after deletion.
- **Stable batch tokens.** Each shipped batch carries a deterministic SHA-256 token derived from its own WAL start and end positions. Retries and replays of the same batch reuse the same token.
- **Store interface.** `InsertBatch(ctx, token, events)` and `Close()` separate storage from the processing pipeline.
- **ClickHouse adapter.** Official Go client with configurable authentication, `Ping` health checks, batched inserts through `PrepareBatch` and `Send`, and graceful cleanup. Each row includes an `event_id` computed from `producer_id`, `producer_epoch`, `seq`, and `idx` using an unambiguous encoding, and stores the epoch in its own column. The adapter rejects an empty token for a non-empty batch.
- **Exactly-once ClickHouse writes.** Before inserting, the adapter removes duplicate identities within the batch and queries which `event_id` values are already stored, then inserts only the rest. The `insert_deduplication_token` it sends is derived from the batch token and the exact rows sent. A replay of the same rows reuses the token, so an insert still in flight when a replay starts is discarded by ClickHouse. A replay that sends fewer rows, because some already landed, gets a new token and is not wrongly discarded. Inserts run synchronously (`async_insert = 0`, `insert_deduplicate = 1`), so a committed insert is visible to the next existence check.
- **Optional ClickHouse integration tests.** Run only when `CLICKHOUSE_ADDR` is set. They cover connectivity, batch insertion, retrieval, and re-insertion of the same batch with the same token. They also cover replays under different tokens, duplicates within a batch, and partially landed batches, checking that each event appears exactly once. They use a 30-second timeout and quote the configured database name in their queries.
- **Crash-replay integration tests.** Unit tests crash the shipper after the store accepts a batch and check that the restart replays exactly the pending range with the same token. An integration test runs the WAL, shipper, and a Docker-hosted ClickHouse through the following and checks that every event is stored exactly once:
  - a crash before checkpoint commit;
  - a restart with more durable records and a larger batch size;
  - a lost checkpoint that re-ships the whole WAL.
- **Health endpoint.** FairGate responds with `200 OK` and `ok` at `/health` on port `9100`; Docker Compose uses this endpoint for its gateway healthcheck.
- **Pipeline metrics.** Prometheus metrics at `/metrics` on port `9100` cover every stage:
  - ingestion outcomes and latency;
  - WAL append and fsync latency, group-commit size, disk usage, and segments;
  - queue depth and fill;
  - scheduler throughput;
  - shipper lag, insert latency, failures, and progress timestamps;
  - Go runtime and process metrics.

  Producers listed in `fairness.producers` get their own `producer` label, and all others share `other`. See [Observability](observability.md#metrics).
- **Alerting.** Twelve Prometheus alert rules cover availability, durability, delivery, pipeline stalls, and producer behavior. An Alertmanager service with commented receiver examples is included. See [Observability](observability.md#alerting).
- **Profiling.** Optional `pprof` endpoints on their own listener, with configurable block and mutex sampling. Off by default. See [Observability](observability.md#profiling).
- **Docker deployment.** A `Dockerfile` and Compose stack run FairGate with ClickHouse, Prometheus, Alertmanager, and Grafana. Prometheus scrapes the gateway and evaluates the alert rules, and Grafana provisions its Prometheus datasource and the FairGate Overview dashboard, which has panels for every pipeline stage. `install-docker-ubuntu.sh` installs Docker Engine and the Compose plugin on Ubuntu.
- **Acknowledgments.** Sends an accepted acknowledgment after the event has been appended to the WAL and enqueued, and also for an identical retry of an event that was already accepted.
- **Concurrent connections.** Handles client connections in separate goroutines and tracks them for coordinated shutdown.
- **Context-aware lifecycle.** `RunContext` lets the caller control server lifetime through a `context.Context`.
- **Config file.** An optional YAML file sets addresses, admission limits, queue capacity, DRR quantum, producer fairness overrides, WAL path and segment size, shipper batching and retry, the ClickHouse connection, and the log level. Environment variables override it, and unknown keys or invalid values stop startup. See [Configuration](configuration.md).
- **Graceful shutdown.** Stops accepting clients, waits for active handlers within a grace period, forces closure of blocked connections afterward, drains the pipeline, stops the scheduler and shipper, and then closes the store and WAL.
- **Queue-drain confirmation.** After handlers stop, shutdown waits up to `server.drain_timeout` for the scheduler to empty every producer queue and for the shipper, woken instead of polling, to ship the WAL up to its durable end. It logs whether the drain completed or what was left; anything left stays in the WAL for the next start. See [Queue-drain confirmation](server-lifecycle.md#queue-drain-confirmation).
- **Cancelable enqueueing.** A handler blocked on a full producer queue can be interrupted during shutdown.
- **Scheduler error propagation.** An unexpected scheduler failure is reported to the caller instead of being silently lost.
- **Load generator.** `cmd/loadgen` simulates producers that pipeline events, pace themselves, retry rejections with backoff, and reconnect and resend after dropped connections, always with the same event identity. Hot producers and malformed frames can be mixed in. It writes a summary with latency percentiles and fairness, per-producer and per-second CSVs, and a manifest of every event's outcome. See [Load generator](tooling.md#load-generator).
- **Reconciliation.** `cmd/reconcile` waits for the shipper to catch up, then checks a manifest against ClickHouse: each accepted event stored exactly once, optionally with the content that was sent, and nothing stored that was refused or never sent. It exits non-zero on a violation. See [Reconciliation](tooling.md#reconciliation).
- **Chaos tooling.** `cmd/chaos proxy` is a TCP proxy that adds latency and jitter, fragments writes, resets connections, partitions traffic, and refuses connections, controlled through an HTTP admin API. `cmd/chaos run` runs YAML scenarios that combine load, timed fault steps (shell commands, health waits, proxy faults), and reconciliation. Scenarios for a gateway crash, a ClickHouse outage, and network faults are in `deploy/chaos/`. See [Chaos tooling](tooling.md#chaos-tooling).
- **Tests.** Includes unit and integration tests for core behavior, including idempotency, acknowledgments, recovery, queue deduplication, and TCP ingestion through the server. Graceful-shutdown tests use a no-op `Store` and a temporary checkpoint path, so they do not need a live ClickHouse service.
- **Continuous integration.** The GitHub Actions workflow runs the test suite with the race detector (`go test -race -v ./...`).

## Planned

Planned work is tracked in the [roadmap](roadmap.md). Planned capabilities are not implied to be available in this beta.
