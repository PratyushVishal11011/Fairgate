[FairGate](../README.md) › [Docs](README.md) › Changelog

# Changelog

## Recent updates

### 30th October, 2026: Protocol hardening

- **Every frame gets an answer.** A frame of an unsupported type used to be logged and dropped without an ACK, which left a pipelining client waiting and its later ACKs out of step. It is now answered `invalid` / `unsupported_frame_type` and the connection is kept.
- **Oversized frames.** A frame over the limit used to close the connection silently. It is now answered `invalid` / `frame_too_large`. If it is up to four times the limit, its body is discarded without being buffered and the connection kept; a larger one closes the connection without reading the body.
- **Malformed frames.** A length prefix of 0 or 1 is answered `invalid` / `malformed_frame` before the connection closes.
- **Memory follows the bytes received.** Frame bodies are read into a buffer that starts at 64 KiB and doubles, instead of allocating the claimed length up front. A client that announces 5 MiB and sends 10 bytes no longer costs 5 MiB.
- **Timeouts.** `server.frame_timeout` (30 s) closes a connection whose frame stops arriving partway, so a stalled client no longer holds its handler forever. `server.write_timeout` (10 s) disconnects a client that stops reading ACKs. `server.idle_timeout` optionally closes idle connections; it is off by default.
- **Configurable frame limit.** `server.max_frame_bytes` (5 MiB, from 1 KiB to 64 MiB) replaces the fixed limit.
- **Identifier limits.** `producer_id`, `producer_epoch`, and `event_type` longer than 256 bytes are refused as `invalid_payload`, because the gateway keeps them in memory for every event it tracks.
- **Metrics and alerting.** `fairgate_protocol_errors_total{reason}` counts each case, and the new `FairGateProtocolErrors` info alert fires when they pass one per second for 10 minutes.
- **Wire package.** `ReadFrameLimit`, `ReadFrameLength`, `CheckFrameLength`, and `ReadFrameBody` expose the steps of reading a frame, with `ErrMalformedFrame` and `*FrameTooLargeError` for the two kinds of bad length prefix. A stream that ends inside a frame returns `io.ErrUnexpectedEOF`.
- **Roadmap.** Load shedding has been removed from the roadmap. With protocol hardening, every planned phase is implemented.

See [Error handling](wire-protocol.md#error-handling).

### 30th October, 2026: Queue-drain confirmation on shutdown

- **Shutdown drains the pipeline before stopping it.** Once handlers have exited, the gateway waits until the scheduler has emptied every producer queue, then wakes the shipper until its checkpoint reaches the durable WAL end, so a clean stop leaves every acknowledged event in ClickHouse. Previously the scheduler and shipper were canceled as soon as the handlers exited, and events accepted since the shipper's last poll waited in the WAL for the next start.
- **Explicit result.** The outcome is logged once: `Shutdown drain complete` with the queued events processed and bytes shipped, or a `Shutdown drain incomplete` warning with each stage's outcome (`timed_out` or `stopped`) and the queued events and unshipped bytes left. Nothing is lost either way; leftovers are replayed from the WAL on the next start.
- **Configuration.** The new `server.drain_timeout` (10 s) bounds both stages together. `0` restores the previous behavior. See [Queue-drain confirmation](server-lifecycle.md#queue-drain-confirmation).
- **Shipper wake-up.** `Shipper.Wake` lets a shipper waiting at the WAL end check for new records at once, instead of at its next `shipper.poll_interval`.
- **Compose.** The gateway service gets `stop_grace_period: 30s`, so Docker does not kill it during the grace period and drain.

### 30th October, 2026: Load, reconcile, and chaos tools

The load generator, reconciliation, and chaos tooling now ship with FairGate as Go commands, so correctness under load and failure can be checked from a FairGate checkout alone.

- **`loadgen`.** Simulated producers, one connection each, pipeline up to `-window` events and optionally pace to `-rate`. Rejected events are retried after jittered exponential backoff, and a dropped connection or an ACK timeout resends every in-flight event. Retries are byte-identical, so the gateway acknowledges an already-persisted event as a duplicate. `-hot` adds noisy-neighbour producers with their own rate and window, and `-malformed-ratio` mixes in undecodable frames. Each run writes `summary.json` (ACK and delivery latency percentiles, throughput, Jain fairness), `producers.csv`, `timeseries.csv`, and `manifest.json`, which records every event as accepted, unacked, or invalid.
- **`reconcile`.** Waits for the shipper to catch up, then checks a manifest against `fairgate.events`. It reports accepted events that are missing, identities stored more than once, rows that were refused or never sent, and with `-verify-content`, rows whose content differs from what was sent. It exits `1` on any violation. ClickHouse settings come from the gateway config and the `CLICKHOUSE_*` variables.
- **`chaos proxy`.** A TCP proxy that adds latency and jitter, fragments writes, resets connections, holds traffic as a partition does, and refuses connections. Faults change at runtime through an HTTP admin API.
- **`chaos run`.** Runs a YAML scenario: load, timed steps (shell commands, health checks, proxy faults, mass disconnects), then reconciliation. `always` steps restore the system even after a failure or Ctrl+C. [`deploy/chaos/`](../deploy/chaos/) has scenarios for killing the gateway with `SIGKILL`, a 30-second ClickHouse outage, and a hostile network.
- **End-to-end tests.** `internal/server` runs the real gateway with an in-memory store, restarts it under load, and runs a proxy fault scenario against it, reconciling both runs. No Docker is needed.

See [Load, reconcile, and chaos tools](tooling.md).

### 30th October, 2026: Backlog-aware overload handling

- **Events are refused before the WAL when work backs up.** A new `overload` stage runs before the WAL append and sends a `rejected` ACK, which producers already treat as retry-after-backoff. Its message names the backlog: `queue_backlogged` when the producer's queue reaches `overload.queue_high_water` (90%) of its capacity, `gateway_backlogged` when all queues together hold `overload.max_queued_events` (100,000) and the producer holds at least its weighted share, and `storage_backlogged` when the shipper trails the WAL by `overload.max_shipper_lag_bytes` (1 GiB). Previously an event for a full queue was appended to the WAL and then held without an ACK until space freed up, and the WAL grew without limit during a ClickHouse outage.
- **Identical retries are still accepted.** The check runs inside the deduplication step, so an event that is already durable is acknowledged as a duplicate while the gateway is backlogged.
- **Hysteresis.** The total queued events and the shipper lag are sampled every `overload.sample_interval` (100 ms). Each signal clears only when its backlog falls below `overload.resume_ratio` (80%) of its limit.
- **Metrics and alerting.** `fairgate_events_overloaded_total{reason}` counts refusals, `fairgate_overload_active{signal}` shows the gateway-wide signals, and `fairgate_producer_events_total` gains the three reasons as outcomes. The new `FairGateOverloaded` warning fires when a signal stays active for 2 minutes.
- **Configuration.** The new `overload` section holds the settings above. Set a limit to `0` to disable that check. See [Configuration](configuration.md#overload).

### 26th October, 2026: Oversized events no longer reported as WAL failures

- **Oversized events are rejected as invalid.** An event larger than the WAL's 1 MiB record limit but within the 5 MiB frame limit used to fail inside the WAL. The handler counted that in `fairgate_wal_append_errors_total`, which fired the critical `FairGateWALAppendErrors` alert, sent `error` / `wal_append_failed`, and closed the connection, though the WAL was healthy. In the [EC2 benchmark](results/2026-10-26-ec2-benchmark.md) this happened 62 times, all from the oversized-event anomaly producers. These events now get `invalid` / `payload_too_large`, count in `fairgate_events_invalid_total`, and the connection stays open.
- **Records are encoded before they reach the WAL writer.** `WAL.Append` now encodes and size-checks the event on the caller's goroutine and returns `wal.ErrRecordTooLarge` for oversized records. The single writer goroutine only writes and syncs.

### 26th October, 2026: Ingestion latency fix

- **Concurrent WAL appends restored.** The deduplication manager held one global mutex across each WAL append, including its fsync, so every event waited for all earlier ones and group commit never batched more than one record. It now serializes only retries of the same event identity, and distinct events share group commits again. With 32 concurrent writers per CPU, the time per event in `BenchmarkPersistIfNewWAL` fell from about 11 ms to about 0.3 ms on an Apple M4.
- **Uncapped group commit.** The WAL writer made at most 64 records durable per `Sync()`, so with hundreds of producers connected each append waited through several batches. In the [EC2 benchmark](results/2026-10-26-ec2-benchmark.md) this capped the gateway at about 7,000 events/s, with a median latency of about 100 ms at 750 producers. The writer now takes every queued request into one batch, so an append waits about one `Sync()` under load.
- **No collection wait under load.** The writer waited up to 2 ms for more requests before every batch. It now waits only when a request arrives alone; requests already queued are written immediately.
- **Wider group commit histogram.** `fairgate_wal_group_commit_records` buckets now reach 4,096 records instead of 64, so larger batches stay visible.
- **Ingest benchmark.** `BenchmarkIngest` in `internal/server` drives the TCP handler with 1 to 750 producers, each sending one event at a time, and reports throughput and ACK latency percentiles.

### 26th October, 2026: Pipeline metrics, alerting, and profiling

Every pipeline stage is now measured, alert rules and an Alertmanager service ship with the Compose stack, and the gateway can serve Go profiles.

- **Pipeline metrics.** Metrics now cover:
  - ingestion latency and per-outcome counts;
  - WAL append and fsync latency, group-commit size, bytes written, rotations, segments, and disk usage;
  - queue depth, fill, and per-producer depth and capacity;
  - per-producer scheduler throughput;
  - shipper lag, insert latency, failures, consecutive failures, and progress timestamps;
  - build info, and the Go runtime and process metrics.
- **Bounded per-producer labels.** Producers listed in `fairness.producers` get their own `producer` label; all others share `other`.
- **Compatible metric names.** The existing metric names and their meanings are unchanged. The metrics are now produced by the Prometheus Go client.
- **Alerting.** [`deploy/prometheus-alerts.yml`](../deploy/prometheus-alerts.yml) adds twelve rules for availability, durability, delivery, pipeline stalls, and producer behavior. Compose runs Alertmanager on port `9093` with [`deploy/alertmanager.yml`](../deploy/alertmanager.yml), which has commented receiver examples.
- **Dashboard.** FairGate Overview gains summary stats, a firing-alerts table, and sections for ingestion, fairness, the WAL, the shipper, and the Go runtime.
- **Profiling.** The new `profiling` config section serves `pprof` on its own listener (`127.0.0.1:6060`), with optional block and mutex sampling. It is off by default.
- **Consistency test.** A test fails if the alert rules or the dashboard reference a `fairgate_*` metric the gateway does not export.

See [Observability](observability.md).

### 26th October, 2026: Producer weights and fairness controls

Producers no longer all get the same treatment. Each one can now get its own share of scheduling, admission, and buffering through the config file.

- **Weighted DRR.** Each backlogged producer is served `quantum × weight` events per round, so while producers compete, their shares are proportional to their weights. Fractional weights are supported: credit below one carries over while a producer stays backlogged. Idle producers don't bank credit.
- **Per-producer admission limits.** `rate` and `burst` replace the default token-bucket limit for one producer.
- **Per-producer queue capacity.** `queue_capacity` gives one producer a deeper or shallower buffer.
- **Configuration.** The new `fairness` section holds `default_weight` and the per-producer entries under `producers`, keyed by `producer_id`. Omitted fields keep the defaults, and negative values are rejected at startup.
- **Queue access fix.** `Queues.Queue` now takes the queue manager's mutex. It used to read and create map entries without it while `EnqueueContext` held it.
- **Docs.** The README is now a short overview, and the detailed documentation lives in [`docs/`](README.md).

See [Backpressure, scheduling, and fairness](scheduling.md) and [Configuration](configuration.md#fairness).

### 26th October, 2026: Configuration file

Runtime settings that were fixed in the code can now be set in an optional YAML config file, so a deployment can be tuned without rebuilding.

- **Config file.** Pass it with `-config` or `FAIRGATE_CONFIG`. It covers the ingestion and metrics addresses, the shutdown grace period, admission rate and burst, queue capacity, DRR quantum, WAL path and segment size, the shipper's checkpoint path, batch size, poll interval, and retry policy, the ClickHouse connection, and the log level.
- **Defaults.** Without a file, every setting keeps its previous value.
- **Strict parsing and validation.** Unknown keys and invalid values stop startup with a message that lists every problem.
- **Environment overrides.** The existing `FAIRGATE_*` and `CLICKHOUSE_*` variables still work and override the file, so secrets can stay out of it. `CLICKHOUSE_ADDR` now accepts a comma-separated list.
- **Reference file.** [`deploy/fairgate.yaml`](../deploy/fairgate.yaml) documents every setting. Docker Compose mounts it at `/etc/fairgate/fairgate.yaml`.
- **Embedding.** `server.RunConfig(ctx, cfg, logger)` runs the gateway with a resolved `config.Config`. `RunContext(ctx, addr, logger)` still works and uses the defaults plus environment overrides.

See [Configuration](configuration.md).

### 26th October, 2026: End-to-end exactly-once delivery

Delivery from producer to ClickHouse is now exactly-once per event identity. Earlier, duplicate suppression depended on ClickHouse's insert-deduplication window and on a replayed batch reproducing its original token. Two shipper bugs meant the token often did not match.

- **Batch tokens use each batch's own start.** The shipper never advanced its start position after the first batch, so every token used the run's first position. A batch replayed as the first batch after a restart therefore got a different token from the original, and ClickHouse did not recognize it. Each batch now derives its token from its own range.
- **Pending-batch intent.** After a restart, a replay could also cover a different range than the original insert, because more records had become durable. The shipper now writes the batch's end position to the checkpoint as `pending` before inserting, and on restart it replays exactly that range before reading further.
- **Existence check in the ClickHouse adapter.** Before each insert, the adapter drops events whose `event_id` is already stored and removes duplicates within the batch. This makes the outcome independent of the deduplication window. It also covers:
  - partially committed multi-partition inserts;
  - lost or rewound checkpoints;
  - events that a producer resent after their WAL segment was reclaimed.
- **Row-bound insert token.** The `insert_deduplication_token` is now derived from the batch token and the exact rows sent. Reusing the original token for a smaller replay would let ClickHouse discard the missing rows as a duplicate block. The integration tests show that this loses events.
- **Checkpoint directory sync.** Checkpoint commits now sync the containing directory after the rename, so an earlier checkpoint cannot reappear after a power loss.
- **Integration tests gated on `CLICKHOUSE_ADDR`.** `go test ./...` no longer fails without a live ClickHouse. Set `CLICKHOUSE_ADDR` (for example `127.0.0.1:9000`) to run them.

This supersedes the at-least-once caveats in the 25th October update and earlier entries. The scope of the guarantee is described in [Delivery semantics](delivery-semantics.md).

### 25th October, 2026: Crash-recoverable ingestion with idempotent batch retries

The write path is now covered end to end by integration tests against a real ClickHouse. Together, the WAL, stable batch tokens, and checkpointing let the gateway recover from a crash and retry a batch without inserting its rows twice, within ClickHouse's deduplication limits.

- **Crash recovery.** Accepted events are persisted in the WAL before acknowledgment, and the shipper resumes from its durable checkpoint after a restart.
- **Idempotent batch retries.** Failed inserts are retried with the same stable batch token, which the ClickHouse adapter passes as `insert_deduplication_token`.
- **Checkpointing.** Progress is committed atomically only after a batch has been inserted successfully.
- **Docker-hosted integration tests.** Tests run against ClickHouse in Docker and verify duplicate suppression when a batch is replayed after a checkpoint failure.
- **Scope of the guarantee.** Deduplication depends on ClickHouse's configured insert-deduplication window (`non_replicated_deduplication_window = 1000` in the schema) and on its storage behavior. A replay outside the window is not suppressed, and delivery remains at-least-once, not exactly-once.

See [Batch and event identity](storage.md#batch-and-event-identity) and [Duplicate handling](storage.md#duplicate-handling).

### 23rd October, 2026: Producer epochs, event idempotency, and TCP integration tests

Events now carry a stable identity that survives producer restarts, and the server uses it to make producer retries safe. This update also adds server and scheduler test coverage, race detection in CI, and a larger frame size limit.

- **Producer epoch.** The event format adds a required `producer_epoch` field. The decoder rejects events that omit it.
- **Stable event identity.** An event is identified by `producer_id`, `producer_epoch`, `seq`, and `idx`. The ClickHouse `event_id` hashes these four fields with an unambiguous encoding, so different field combinations cannot collide through concatenation.
- **Deduplication manager.** The manager checks both identity and content. An identical retry is accepted as a duplicate. Reusing an identity with a different event time, type, or payload is rejected as a conflict. A new identity is recorded only after WAL persistence succeeds.
- **Idempotent TCP handler.** The handler acknowledges identical retries without appending them to the WAL again.
- **Queue-level identity tracking.** Scheduler queues track event identities to avoid enqueuing duplicates.
- **Schema changes.** `deploy/clickhouse/schema.sql` adds a `producer_epoch` column and orders records by `event_id`. Existing tables need a migration; see the note below.
- **Frame size.** The maximum wire frame size is raised from 1 MiB to 5 MiB.
- **Tests.** New tests cover idempotency, acknowledgments, recovery, queue behavior, and server ingestion over TCP.
- **CI.** The GitHub Actions workflow now runs `go test -race -v ./...`.
- **Docs.** The README adds instructions for running the services with Apple's container runtime.

> [!WARNING]
> **Schema migration required.** `deploy/clickhouse/schema.sql` uses `CREATE TABLE IF NOT EXISTS`, so editing the schema file does not add `producer_epoch` or change the sorting key on a table that already exists. Deployments with an existing `fairgate.events` table need to migrate it (or recreate it) before running this version. Changing the sorting key of a MergeTree table generally means creating a new table and copying data into it.

Because a new identity is recorded only after the WAL write, and the WAL write happens before enqueueing, a stored event can wait for a restart before it is processed. If shutdown cancels a handler while it is enqueueing, the event is already in the WAL and already recorded as seen. A retry of that event during the same process lifetime is acknowledged as a duplicate without another enqueue, so the event may not reach the live processing queue until the next startup replays it from the WAL. It is not lost.

See [Event identity and idempotency](event-model.md#event-identity-and-idempotency) for the full rules.

### 17th October, 2026: Stable batch tokens and ClickHouse event identity

The shipper now gives each WAL batch a stable identity, and ClickHouse rows carry a deterministic event ID. Together these let a storage backend recognize data it has already received.

- **Deterministic batch token.** The shipper derives a SHA-256 token from the batch's WAL start and end positions and passes it to `Store.InsertBatch`. Retries of the same batch and replay after a restart reuse the same token.
- **Event identity.** The ClickHouse adapter writes an `event_id` computed from the event's identifying fields. The 23rd October update added `producer_epoch` to those fields.
- **Schema changes.** `deploy/clickhouse/schema.sql` adds the `event_id` column, changes `idx` from `UInt16` to `UInt64`, removes the `LowCardinality` type from `event_type`, and drops the `received_at` column. Tables created from the earlier schema need to be migrated before this version runs against them.
- **Tests.** Shipper tests check that batch tokens are non-empty and stay the same across retries. A crash-recovery test simulates a destination that accepts a batch before a crash, then checks that the batch is replayed and the checkpoint advances after restart. The optional ClickHouse integration test submits the same batch twice with the same token.

- **Empty-token guard.** The ClickHouse adapter rejects an empty token for a non-empty batch, so every batch that reaches ClickHouse carries one.
- **Insert deduplication token.** The adapter passes the batch token to ClickHouse as `insert_deduplication_token`.
- **Deduplication window.** `deploy/clickhouse/schema.sql` sets `non_replicated_deduplication_window = 1000`, intended to retain deduplication history for non-replicated MergeTree inserts.
- **Integration test.** The timeout is raised to 30 seconds. The test retries the same batch with the same token and checks that each event appears once. Its queries use the configured database name, quoted by a new helper.
- **Connection and batch cleanup.** The ClickHouse batch is closed with a deferred `batch.Close()`.

These changes make redelivered batches identifiable and give ClickHouse a token to deduplicate against. They do not by themselves make delivery exactly-once. See [Batch and event identity](storage.md#batch-and-event-identity).

### 10th October, 2026: Checkpoint guard for WAL reclamation

`WAL.ReclaimBefore` now rejects checkpoints that point past the WAL's durable end, and it holds the segment mutex for the whole reclamation so the active segment cannot change while files are being removed.

- **Durable-end guard.** A checkpoint whose segment is beyond the durable end returns `ErrCheckpointAhead` and deletes nothing. A caller that passes an unsynced position now gets an error instead of silently removing segments.
- **Active and checkpoint segments protected.** Reclamation never removes the checkpoint segment or the active segment, even if a caller passes an unexpected position.
- **Tests.** A new test covers reclamation across rotated segments, confirms that the checkpoint and active segments survive, and confirms that a checkpoint past the durable end is rejected.

Startup ordering is `Recover` (which truncates any torn tail in the final segment) followed by `wal.Open`, so `DurableEnd()` reflects only complete records.

### 09th October, 2026: Gateway health, metrics, and deployment monitoring

The gateway now exposes an HTTP health endpoint and Prometheus metrics alongside its TCP ingestion listener. The Compose deployment publishes the gateway's TCP and HTTP ports, checks gateway health, and provisions Prometheus and Grafana for monitoring.

- **Health endpoint.** `GET /health` on port `9100` returns `200 OK` with `ok`. Docker Compose uses it to report whether the FairGate container is healthy.
- **Prometheus metrics.** `GET /metrics` on port `9100` reports active TCP connections, accepted, rejected and invalid events, WAL append errors, and scheduler processing totals. Prometheus is configured to scrape the gateway every five seconds.
- **Grafana monitoring.** Compose provisions Prometheus as Grafana's data source and loads the FairGate Overview dashboard, including active connections, event rate, and WAL append errors.
- **Container image versions.** Prometheus and Grafana now use pinned image versions rather than `latest`.
- **Gateway capacity settings.** The configured shipper batch size is 200, admission control is set to 500 requests per second with a 1,000 request burst, and per-producer queues buffer up to 512 events.
- **Published service ports.** Compose publishes FairGate TCP port `9001`, its HTTP port `9100`, ClickHouse port `9000`, Prometheus port `9090`, and Grafana on host port `3001`.
- **Stress tests.** Ran multiple stress tests with varying producer counts and workload types, including bursty and misbehaving-producer workloads. A report is coming soon.

See [Docker deployment](deployment.md) for the Compose setup and monitoring URLs.

### 06th October, 2026: Checkpoint-based WAL reclamation and always-on shipper

Earlier versions kept every completed WAL segment on disk and started the shipper only when `FAIRGATE_SHIPPER_ENABLED=true`. The latest changes let the WAL reclaim segments the shipper has already delivered, and make shipping part of every gateway run.

- **Checkpoint-driven reclamation.** After each successful `Store.InsertBatch`, the shipper atomically commits the batch's end position and then calls `WAL.ReclaimBefore` with that position. At startup it also reclaims against the checkpoint already on disk.
- **Conservative deletion rule.** Reclamation deletes only segments whose IDs are strictly less than the checkpoint segment. The checkpoint segment is retained because its offset may still leave records to ship, and the active segment is always retained.
- **Rotation-safe deletion.** The WAL serializes reclamation with rotation using a segment mutex, and syncs the containing directory after deletion.
- **Sparse segment sets.** Segment discovery lists only files that exist and sorts them by ID. Recovery handles a sparse segment set, and the reader advances to the next extant segment instead of assuming contiguous IDs.
- **Shipper always on.** The gateway no longer reads or validates `FAIRGATE_SHIPPER_ENABLED`. `RunContext` always constructs the ClickHouse store and starts the shipper, and shutdown always cancels and joins it before storage and WAL cleanup. The redundant setting was removed from Docker Compose, and this README describes shipping as always enabled. Existing deployments can drop the variable; it has no effect.
- **Test seam.** Startup logic was split so the public entry point supplies the ClickHouse store while tests can supply a no-op `Store`. Graceful-shutdown tests also use a temporary checkpoint path, so they no longer depend on a live ClickHouse service.

Reclamation depends on the shipper. If ClickHouse is unavailable, the checkpoint does not advance and segments accumulate until delivery resumes. A crash after a successful insert but before the checkpoint commit can still replay that batch, so delivery remains at-least-once rather than exactly-once.

See [Write-Ahead Log](wal.md) for the reclamation rules and [Storage integration](storage.md) for the shipper flow.

### 05th October, 2026: Gateway shipper runtime and Docker deployment

The gateway could now start the WAL shipper as part of `RunContext` when `FAIRGATE_SHIPPER_ENABLED=true`. The shipper polls the durable WAL end for new records, retries ClickHouse insert failures, and persists its checkpoint after successful batches. Docker Compose passes the ClickHouse connection settings and waits for ClickHouse health before starting FairGate. The `install-docker-ubuntu.sh` script installs Docker Engine and the Compose plugin on Ubuntu.

The opt-in setting was removed in the 06th October update; the shipper now always runs.

For remote access, publish TCP port `9001` on a non-loopback interface and allow inbound TCP `9001` through the host firewall.

See [Docker deployment](deployment.md) and [Storage integration](storage.md).

### 05th October, 2026: WAL shipping, durable checkpoints, and retry backoff

The latest changes connect the storage building blocks to a WAL-reading shipper. They add record-position tracking across segments, persistent progress, and retry handling for failed storage writes.

- **Durable WAL reader.** Reads only records up to the WAL's synchronized durable end, verifies record length, CRC32, and JSON, and returns each event with its start and end positions. It advances across segment boundaries and can start from a saved position.
- **Checkpoint-based resume.** The checkpoint records a segment ID and byte offset. A missing checkpoint starts at the beginning of the WAL; invalid JSON and negative offsets are rejected.
- **Atomic checkpoint commit.** Each successful batch advances the checkpoint by writing and syncing a temporary JSON file, then renaming it over the checkpoint path.
- **WAL-backed batch shipping.** `Shipper.Run(ctx)` loads the checkpoint, reads events in WAL order up to `BatchSize`, calls `Store.InsertBatch`, and commits the batch end position only after insertion succeeds. It returns when it reaches the current durable WAL end.
- **Retry and backoff.** Failed inserts retry the same batch using exponential delays capped at a maximum delay, with optional symmetric jitter. Waiting respects context cancellation; retries otherwise continue until success.
- **Cross-segment recovery validation.** Recovery now rejects incomplete records in non-final segments instead of truncating them; only an incomplete tail in the final segment can be truncated.
- **Storage interface export.** `Store.InsertBatch` is exported so the shipper can call storage implementations across package boundaries.

At this stage the shipper was a standalone component: it was not started by the gateway command, `Run` returned at the current durable end instead of following later appends, and there was no automatic WAL segment reclamation. Gateway wiring and polling were added in the next update, and checkpoint-based reclamation in the 06th October update. A crash after a successful insert but before the checkpoint commit can replay that batch, so these changes do not provide exactly-once delivery.

See [Storage integration](storage.md) for the shipper flow and [Write-Ahead Log](wal.md) for recovery behavior.

### 03rd October, 2026: Asynchronous shipper, Store interface, and ClickHouse adapter

Earlier versions stopped at the scheduler, whose callback only logged events, and had no way to hand events to a database. The latest changes add an asynchronous shipping layer and a storage abstraction, with ClickHouse as the first backend, so event scheduling is decoupled from database insertion.

- **Asynchronous shipper.** A bounded in-memory channel buffers incoming events, so callers are not blocked on database inserts. `BatchSize`, `FlushInterval`, and `QueueSize` control batch formation, flushing frequency, and buffer capacity.
- **Context-aware submission.** `Submit(ctx, event)` hands an event to the shipper and respects cancellation, so a caller waiting on a full buffer can stop waiting.
- **`Store` interface.** Storage operations are separated from the event processing pipeline behind `InsertBatch(ctx, events)` and `Close()`. Different backends can be implemented without modifying the shipper.
- **ClickHouse adapter.** Built on the official Go client, with connection initialization, configurable authentication, connection health checks through `Ping`, and graceful connection cleanup.
- **Batched insertion.** The adapter uses `PrepareBatch` and `Send` to reduce per-event insertion overhead.
- **`fairgate.events` persistence.** Events are stored with their producer ID, event timestamp, sequence number, index, event type, and payload. The adapter follows the existing ClickHouse schema, including `ReplacingMergeTree`, monthly partitioning, and a 30-day TTL.
- **Optional integration test.** An environment-gated ClickHouse test validates connectivity, batch insertion, and retrieval of persisted events. It does not run unless ClickHouse is configured.

These changes provide the shipping and storage building blocks. At that point, durable checkpoints, retry and backoff, and WAL segment reclamation were still separate upcoming steps, so database delivery guarantees were not yet documented. They were added in the updates above. See [Storage integration](storage.md).

### 03rd October, 2026: Segmented WAL and group commit

Earlier versions of the WAL stored all events in a single file and performed a disk synchronization (`Sync()`) for every appended event. The latest changes introduce WAL segmentation and group commit to manage log growth and reduce the overhead of frequent disk synchronization.

- **Segmented WAL.** The WAL is now split into multiple files, with a configurable segment size. When the active segment reaches its size limit, the writer rotates to a new segment instead of continuing to grow a single file.
- **Ordered recovery.** Recovery discovers WAL segments and processes them in order, reconstructing events across multiple files while preserving the existing record format.
- **Incomplete record handling.** Recovery can truncate an incomplete trailing record in the final segment to the last valid record boundary, while treating corruption in completed records as an error.
- **Dedicated writer goroutine.** A separate `writerLoop` now owns WAL file operations. Instead of each `Append()` call writing directly to disk, requests are submitted to a shared channel and processed by the writer.
- **Group commit.** Multiple append requests are collected into batches, allowing the WAL to perform a single `Sync()` for the batch rather than synchronizing after every event.
- **Bounded batching.** Batches are limited by a maximum request count and a short collection delay, allowing the writer to balance synchronization overhead with append latency.
- **Asynchronous request submission.** Concurrent handlers can submit append requests to the writer goroutine, while each caller waits for confirmation that its batch has been written and synchronized.
- **Segment rotation during batching.** The writer checks each record's size before writing and rotates the active segment when the next record would exceed the configured segment limit.
- **Fatal error handling.** Write, rotation, and synchronization failures are recorded as fatal WAL errors. Affected requests receive an error, and subsequent batches are rejected rather than being reported as successfully persisted.
- **Graceful WAL closure.** Closing the WAL stops new submissions, allows queued requests to be processed, and then synchronizes and closes the active segment before the writer exits.

These changes establish a WAL that supports multiple segments and batched disk synchronization while retaining the existing record framing and recovery format. Queue-drain confirmation during server shutdown and ClickHouse persistence were still separate upcoming steps at that point.

See [Write-Ahead Log](wal.md) for the writer flow and recovery behavior.

### 03rd October, 2026: Graceful shutdown and context-aware event handling

Earlier versions accepted connections and processed events concurrently, but had no coordinated way to stop accepting clients, let in-flight handlers finish, and release resources safely. The latest changes add a controlled shutdown sequence built on `context`, `sync.WaitGroup`, `sync.Mutex`, and channels.

- **`RunContext`.** A new context-aware entry point gives the caller control over server lifetime. `Run` remains as a thin wrapper that uses a background context, so existing callers are unaffected.
- **Connection tracking.** Active connections are registered in a mutex-protected map and counted with a `WaitGroup`, so the server knows which handlers are running and can wait for them.
- **Listener watcher.** A watcher goroutine closes the TCP listener when the context is canceled, which unblocks `Accept` and begins shutdown. It exits cleanly if the accept loop ends first.
- **Cancelable enqueueing.** `EnqueueContext` replaces the blocking enqueue in the connection handler, so a handler stuck on a full queue can stop waiting when shutdown is forced.
- **Separate handler and scheduler contexts.** Handlers can be canceled independently, which lets the scheduler keep processing queued events while handlers drain.
- **Scheduler lifecycle.** The server waits for the scheduler goroutine to exit before returning, and an unexpected scheduler failure closes the listener and is returned to the caller.
- **Five-second grace period.** After the accept loop stops, handlers get a fixed grace period. If it expires, active connections are closed and the handler context is canceled to interrupt blocked reads and enqueues.
- **WAL safety.** The WAL is not closed until every tracked handler has exited, so a handler cannot append to a closed log.

See [Server lifecycle and graceful shutdown](server-lifecycle.md) for diagrams, behavior details, and limitations.
