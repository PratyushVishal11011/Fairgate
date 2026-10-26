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
- WAL retention depends on the shipper. Reclamation advances only when a batch is inserted and its checkpoint committed, so while ClickHouse is unavailable, or a batch keeps failing and is retried, segments accumulate on disk.
- Reclamation deletes whole segments older than the checkpoint segment. The checkpoint and active segments are retained, so some already shipped records remain on disk until a later checkpoint passes them, and reclaimed records are no longer available for recovery replay or inspection.
- The reclamation guard compares segment IDs only. A checkpoint in the same segment as the durable end but with an offset past it is not rejected yet.
- The ClickHouse integration tests are optional and run only when `CLICKHOUSE_ADDR` is set.
- The maximum frame size and the WAL group commit limits are fixed in the code. The other runtime settings are set through the [config file](configuration.md).
- Every event costs the same in scheduling, whatever its size. Weights set each producer's share of events, not of bytes.
- Per-producer fairness overrides are read at startup. Changing a weight, limit, or queue capacity needs a restart.
- Enqueueing blocks on a full queue (cancelable during shutdown), and no load-shedding policy exists yet.
- A WAL write, rotation, or sync failure is fatal: later appends are rejected until the WAL is reopened.
- Startup recovery fails on an incomplete record in any non-final segment, rather than repairing it.
- Shutdown has no explicit queue-drain confirmation, and a handler stuck in an operation that ignores connection closure and context cancellation can delay it beyond the grace period.
- The Compose file ships with development credentials and publishes the FairGate, ClickHouse, Prometheus, and Grafana ports; restrict inbound access to trusted addresses as described in [Docker deployment](deployment.md) and replace the credentials before any non-development use.
- The Apple Container launcher script is not tracked in the repository, so the macOS instructions do not work from a clean checkout.
- FairGate currently exposes a basic Prometheus metrics set (active connections, accepted, rejected and invalid events, WAL append errors, scheduler totals), a `/health` endpoint, and a provisioned Grafana overview dashboard. Profiling, per-stage latency metrics, queue-depth metrics, and shipper lag metrics are not available yet.
