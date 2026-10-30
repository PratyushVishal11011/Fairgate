[FairGate](../README.md) › [Docs](README.md) › Development status and limitations

# Development status and limitations

FairGate is an experimental beta. Interfaces, protocol details, configuration, and storage semantics may change. The project has tests for implemented components, but its end-to-end durability and fairness goals still require broader load, crash-recovery, and downstream-outage validation.

Known limitations of the current beta:

- The gateway always constructs the ClickHouse store and starts the shipper; there is no mode that runs without storage. `FAIRGATE_SHIPPER_ENABLED` is no longer read.
- Events must now include `producer_epoch`. Producers built against the earlier event format will have their events rejected until they send it.
- Exactly-once storage assumes a single writer per event identity. The adapter's existence check reads before it inserts, so two gateways shipping the same identity at the same moment could both insert it. Route each producer to one gateway.
- Each batch costs one extra checkpoint write (the pending intent) and one `event_id` lookup query in ClickHouse before the insert.
- Existing ClickHouse tables created from an earlier schema must be migrated to the current `deploy/clickhouse/schema.sql` before this version writes to them. The schema file uses `CREATE TABLE IF NOT EXISTS`, so it does not add `producer_epoch` or change the sorting key on an existing table.
- Reuse of an identity with different content is rejected only while the original event is still in the WAL. After its segment is reclaimed, a conflicting event is acknowledged and then dropped by the adapter (the first stored version wins).
- Rows removed by the table's 30-day TTL no longer count as stored, so a resend of an expired event is stored again.
- An event that was appended to the WAL but whose handler was canceled during enqueueing is not processed by the live scheduler, and a retry of it in the same process lifetime is acknowledged as a duplicate. It is replayed from the WAL on the next startup.
- WAL retention depends on the shipper. Reclamation advances only when a batch is inserted and its checkpoint committed, so while ClickHouse is unavailable, or a batch keeps failing and is retried, segments accumulate on disk until the shipper lag reaches `overload.max_shipper_lag_bytes` and new events are refused.
- Reclamation deletes whole segments older than the checkpoint segment. The checkpoint and active segments are retained, so some already shipped records remain on disk until a later checkpoint passes them, and reclaimed records are no longer available for recovery replay or inspection.
- The reclamation guard compares segment IDs only. A checkpoint in the same segment as the durable end but with an offset past it is not rejected yet.
- The ClickHouse integration tests are optional and run only when `CLICKHOUSE_ADDR` is set.
- The WAL record limit (1 MiB) and the WAL group commit collection delay are fixed in the code. The other runtime settings, including the maximum frame size, are set through the [config file](configuration.md).
- An oversized frame more than four times `server.max_frame_bytes` is answered and its connection closed without reading the body. If the client is still writing that body, the close can reset the connection before the client reads the `frame_too_large` ACK.
- `server.idle_timeout` is off by default, so idle connections are held until the client closes them. There is no limit on the number of connections.
- Every event costs the same in scheduling, whatever its size. Weights set each producer's share of events, not of bytes.
- Per-producer fairness overrides are read at startup. Changing a weight, limit, or queue capacity needs a restart.
- Overload handling refuses new events but never drops events that were already accepted. If a queue fills despite the high-water mark (several connections of one producer appending at once), enqueueing still blocks, cancelable during shutdown.
- The gateway-wide overload signals are sampled every `overload.sample_interval`, so a sudden burst can pass `max_queued_events` or `max_shipper_lag_bytes` by up to one interval's worth of events before refusals start.
- Overload limits, like the other settings, are read at startup.
- A WAL write, rotation, or sync failure is fatal: later appends are rejected until the WAL is reopened.
- Startup recovery fails on an incomplete record in any non-final segment, rather than repairing it.
- A handler stuck in an operation that ignores connection closure and context cancellation can delay shutdown beyond the grace period.
- The shutdown drain waits for the shipper only up to `server.drain_timeout`. During a ClickHouse outage it times out and the backlog stays in the WAL for the next start; it confirms queue processing by the scheduler, not by any downstream consumer of the queues.
- The Compose file ships with development credentials and publishes the FairGate, ClickHouse, Prometheus, Alertmanager, and Grafana ports; restrict inbound access to trusted addresses as described in [Docker deployment](deployment.md) and replace the credentials before any non-development use.
- The Apple Container launcher script is not tracked in the repository, so the macOS instructions do not work from a clean checkout.
- `reconcile` counts rows without `FINAL`. A background merge of the `ReplacingMergeTree` can collapse duplicate rows of one identity, so a duplicate merged away before reconciliation is not detected.
- `reconcile` scans the whole events table for the run's epoch and producers (the table is ordered by `event_id`), so it slows down as the table grows.
- `loadgen` always sends `idx` 0, and a count-bound run without `-duration` keeps redialling a gateway that never comes up until it is stopped.
- The fault proxy delays, fragments, and resets per chunk read from a socket, not per frame or per TCP segment, and it cannot drop or reorder bytes within a connection.
- Chaos scenario `exec` steps run shell commands with the caller's permissions, and the shipped scenarios assume the container names from `docker-compose.yml`.
- Per-producer metrics label only the producers listed in `fairness.producers`; all other producers share the `other` series.
- Alertmanager sends no notifications until a receiver is configured, and the alert thresholds are generic starting points.
- There is no metric for time spent waiting in the scheduler queue, or for events the ClickHouse adapter skipped as already stored.
