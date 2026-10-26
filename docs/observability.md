[FairGate](../README.md) › [Docs](README.md) › Observability

# Observability

FairGate exports Prometheus metrics for every pipeline stage, ships alert rules and an Alertmanager configuration, provisions a Grafana dashboard, and can serve Go's `pprof` profiles.

| Endpoint | Default address | Setting |
|---|---|---|
| `/health` | `:9100` | `server.metrics_addr` |
| `/metrics` (Prometheus) | `:9100` | `server.metrics_addr` |
| `/debug/pprof/` | `127.0.0.1:6060`, off by default | `profiling.enabled`, `profiling.addr` |

In the Docker Compose stack, Prometheus scrapes the gateway every 5 seconds and evaluates the alert rules. Alertmanager is at [http://localhost:9093](http://localhost:9093), and Grafana loads the FairGate Overview dashboard at [http://localhost:3001](http://localhost:3001). See [Deployment](deployment.md).

## Metrics

All FairGate metrics use the `fairgate_` prefix. The Go runtime (`go_*`) and process (`process_*`) metrics from the Prometheus client are exported as well: goroutines, heap, GC pauses, CPU, resident memory, and open file descriptors.

### Per-producer labels

Metrics with a `producer` label give each producer listed under [`fairness.producers`](configuration.md#fairness) its own series. Every other producer is counted under `producer="other"`. This keeps label cardinality bounded however many producers connect, and the producers you configure are the ones you would want to watch individually anyway. Producer IDs are never used as labels in any other way. Avoid naming a real producer `other`, because it would share that series.

### Ingestion

| Metric | Type | Description |
|---|---|---|
| `fairgate_tcp_connections_active` | gauge | Active TCP client connections. |
| `fairgate_events_accepted_total` | counter | Events acknowledged as accepted, including identical retries. |
| `fairgate_events_duplicate_total` | counter | Identical retries acknowledged without a second WAL append. |
| `fairgate_events_rejected_total` | counter | Events rejected by admission control. |
| `fairgate_events_invalid_total` | counter | Events rejected as malformed payloads or as identity conflicts. |
| `fairgate_producer_events_total{producer, outcome}` | counter | Ingestion outcome per producer. `outcome` is `accepted`, `duplicate`, `rate_limited`, `conflict`, or `wal_error`. Malformed payloads have no producer and appear only in `fairgate_events_invalid_total`. |
| `fairgate_ingest_duration_seconds` | histogram | Time from reading an event frame to sending its accepted ACK, including the WAL append. |

### Write-Ahead Log

| Metric | Type | Description |
|---|---|---|
| `fairgate_wal_append_duration_seconds` | histogram | Time a handler waits for its append, including the group commit. |
| `fairgate_wal_sync_duration_seconds` | histogram | Duration of each group-commit fsync. |
| `fairgate_wal_group_commit_records` | histogram | Records made durable per group commit, in buckets from 1 to 4,096. |
| `fairgate_wal_written_bytes_total` | counter | Bytes made durable. |
| `fairgate_wal_segment_rotations_total` | counter | Segment rotations. |
| `fairgate_wal_segments` | gauge | Segment files on disk. |
| `fairgate_wal_disk_bytes` | gauge | Total size of the segment files on disk. |
| `fairgate_wal_append_errors_total` | counter | Append failures. A write or sync failure is fatal, and later appends are rejected until restart. |

### Queues and scheduling

| Metric | Type | Description |
|---|---|---|
| `fairgate_queue_depth_events` | gauge | Events waiting in all producer queues. |
| `fairgate_queue_producers` | gauge | Producer queues that exist. |
| `fairgate_queue_fill_ratio_max` | gauge | The fullest producer queue as a fraction of its capacity, from 0 to 1. At 1, that producer is under backpressure. |
| `fairgate_producer_queue_depth_events{producer}` | gauge | Events waiting per producer. `other` is the sum over unlisted producers. |
| `fairgate_producer_queue_capacity_events{producer}` | gauge | Queue capacity per producer. `other` is the sum over unlisted producers. |
| `fairgate_events_processed_total` | counter | Events served by the DRR scheduler. |
| `fairgate_producer_events_processed_total{producer}` | counter | Events served per producer. Their rates show the weighted shares in action. |

### Shipper

| Metric | Type | Description |
|---|---|---|
| `fairgate_shipper_lag_bytes` | gauge | Durable WAL bytes after the committed checkpoint, not yet delivered to ClickHouse. |
| `fairgate_shipper_batches_total` | counter | Batches inserted. |
| `fairgate_shipper_events_total` | counter | Events submitted in successful inserts, including events the store skipped as already stored. |
| `fairgate_shipper_insert_duration_seconds` | histogram | Duration of successful inserts. |
| `fairgate_shipper_insert_failures_total` | counter | Failed insert attempts. |
| `fairgate_shipper_consecutive_failures` | gauge | Failed attempts in a row for the current batch; 0 after a success. |
| `fairgate_shipper_last_success_timestamp_seconds` | gauge | Unix time of the last successful insert. |
| `fairgate_shipper_last_checkpoint_timestamp_seconds` | gauge | Unix time of the last checkpoint commit. |

### Build

| Metric | Type | Description |
|---|---|---|
| `fairgate_build_info{version, go_version}` | gauge | Always 1; the labels identify the build. |

### Useful queries

```promql
# p99 ingestion latency
histogram_quantile(0.99, sum by (le) (rate(fairgate_ingest_duration_seconds_bucket[5m])))

# Share of scheduler output per producer
sum by (producer) (rate(fairgate_producer_events_processed_total[5m]))
  / ignoring(producer) group_left sum(rate(fairgate_producer_events_processed_total[5m]))

# Average records per fsync (group commit effectiveness)
rate(fairgate_wal_group_commit_records_sum[5m]) / rate(fairgate_wal_group_commit_records_count[5m])

# Seconds since delivery last made progress
time() - fairgate_shipper_last_checkpoint_timestamp_seconds
```

## Dashboard

[`deploy/grafana/dashboards/fairgate-overview.json`](../deploy/grafana/dashboards/fairgate-overview.json) is provisioned into Grafana as **FairGate Overview**. Its sections:

- **Summary.** Active connections, event rates, WAL append errors, shipper lag, fullest queue, and firing alerts.
- **Ingestion.** Latency percentiles, duplicate retries, and identity conflicts.
- **Fairness.** Processed events, queue depth, and rate-limited events per producer, plus overall queue fill.
- **Write-Ahead Log.** Append and fsync latency, group-commit size, disk usage, and segments.
- **Shipper.** Delivery lag, shipped events and insert failures, insert latency, and time since the last checkpoint.
- **Go runtime.** Goroutines, CPU, heap, and resident memory.

## Alerting

[`deploy/prometheus-alerts.yml`](../deploy/prometheus-alerts.yml) defines the alert rules. Prometheus loads them and sends firing alerts to Alertmanager, which is configured by [`deploy/alertmanager.yml`](../deploy/alertmanager.yml).

| Alert | Severity | Fires when |
|---|---|---|
| `FairGateDown` | critical | The gateway's metrics endpoint cannot be scraped for 1 minute. |
| `FairGateWALAppendErrors` | critical | Any WAL append error in the last 5 minutes. |
| `FairGateShipperStalled` | critical | There is undelivered WAL data and no checkpoint has been committed for 5 minutes, sustained for 5 minutes. |
| `FairGateSchedulerStalled` | critical | Events are queued but none were processed for 2 minutes, sustained for 2 minutes. |
| `FairGateWALSyncSlow` | warning | p99 fsync is over 100 ms for 10 minutes. |
| `FairGateWALDiskHigh` | warning | WAL segments use more than 1 GiB for 15 minutes. |
| `FairGateShipperInsertsFailing` | warning | The current batch has failed 5 or more times in a row, for 2 minutes. |
| `FairGateShipperLagHigh` | warning | Delivery lag is over 256 MiB for 10 minutes. |
| `FairGateQueueSaturated` | warning | The fullest producer queue is over 90% for 2 minutes. |
| `FairGateIngestLatencyHigh` | warning | p99 ingestion latency is over 250 ms for 10 minutes. |
| `FairGateHighRejectionRate` | warning | Over 20% of events, and more than 1 per second, are rate-limited for 10 minutes. |
| `FairGateInvalidEvents` | info | More than 1 invalid or conflicting event per second for 10 minutes. |

The thresholds are starting points. Tune them in the rules file to match your traffic, disk, and latency targets, then restart Prometheus (`sudo docker compose restart prometheus`).

### Notifications

Out of the box, Alertmanager has a single receiver with no integrations. Alerts show in the Alertmanager UI, in Prometheus, and on the dashboard, but no one is notified. To get notified, add a `webhook_configs`, `slack_configs`, `email_configs`, or other [Alertmanager integration](https://prometheus.io/docs/alerting/latest/configuration/) to the `default` receiver in `deploy/alertmanager.yml`. Commented examples are included. Then restart Alertmanager (`sudo docker compose restart alertmanager`). The file also shows how to route critical alerts to a separate receiver.

While `FairGateDown` is firing for an instance, its warning and info alerts are inhibited, since their metrics are stale.

### Validating changes

The repository's tests check that every `fairgate_*` metric referenced by the alert rules and the dashboard is actually exported. To also check rule syntax after editing, run `promtool` from the Prometheus image:

```bash
docker run --rm -v "$PWD/deploy:/deploy:ro" --entrypoint promtool prom/prometheus:v3.5.0 check rules /deploy/prometheus-alerts.yml
```

## Profiling

FairGate can serve Go's [`net/http/pprof`](https://pkg.go.dev/net/http/pprof) endpoints. Profiling is off by default because profiles expose internals and cost CPU while they are collected. The endpoints run on their own listener, never on the metrics port.

```yaml
profiling:
  enabled: true
  addr: "127.0.0.1:6060"
  # Optional: populate the block and mutex profiles (they are empty otherwise).
  block_profile_rate: 10000      # one sample per 10 µs spent blocked
  mutex_profile_fraction: 100    # sample 1 in 100 contention events
```

With profiling enabled:

```bash
# 30-second CPU profile, opened in the browser
go tool pprof -http=:8080 http://127.0.0.1:6060/debug/pprof/profile?seconds=30

# Heap in use, and goroutines with their stacks
go tool pprof http://127.0.0.1:6060/debug/pprof/heap
curl http://127.0.0.1:6060/debug/pprof/goroutine?debug=2

# Lock contention and blocking (needs the sampling settings above)
go tool pprof http://127.0.0.1:6060/debug/pprof/mutex
go tool pprof http://127.0.0.1:6060/debug/pprof/block

# Execution trace
curl -o trace.out http://127.0.0.1:6060/debug/pprof/trace?seconds=5 && go tool trace trace.out
```

The index at `/debug/pprof/` lists every available profile.

In Docker, the gateway's loopback is the container's own, so set `addr: ":6060"` and publish the port to the host's loopback only by adding `"127.0.0.1:6060:6060"` to the `fairgate` service's `ports` in `docker-compose.yml`. Don't publish the profiling port on a public interface.
