[FairGate](../README.md) › [Docs](README.md) › Configuration

# Configuration

FairGate reads an optional YAML config file. Settings are resolved in three layers, each overriding the one before:

1. Built-in defaults.
2. The config file, passed with `-config` or `FAIRGATE_CONFIG`. `-config` wins if both are set.
3. Environment variables (see [Environment overrides](#environment-overrides)).

Without a config file, the gateway runs with the defaults below, which match its behavior before the file existed. Every key in the file is optional, and a key you leave out keeps its default.

```bash
fairgate -config /etc/fairgate/fairgate.yaml
FAIRGATE_CONFIG=/etc/fairgate/fairgate.yaml fairgate
```

[`deploy/fairgate.yaml`](../deploy/fairgate.yaml) is the reference file. It lists every setting with its default and a comment, and Docker Compose mounts it into the gateway container. A test checks that its values match the built-in defaults. Copy it as a starting point and remove the settings you don't change.

Unknown keys are rejected, so a misspelled setting stops startup instead of being silently ignored. Invalid values are also rejected at startup, and every problem is reported at once.

## Settings

| Key | Default | Description |
|---|---|---|
| `server.listen_addr` | `:9001` | TCP address for event ingestion. |
| `server.metrics_addr` | `:9100` | HTTP address for `/health` and `/metrics`. If you change it, update the Compose port mapping, healthcheck, and Prometheus target to match. |
| `server.shutdown_grace_period` | `5s` | Time active handlers get to finish before their connections are closed. |
| `admission.rate` | `500` | Token-bucket refill rate per producer, in events per second. |
| `admission.burst` | `1000` | Token-bucket capacity per producer. |
| `queues.capacity` | `512` | Events buffered per producer before enqueueing blocks. |
| `scheduler.quantum` | `8` | Events served per DRR round to a producer of weight 1. |
| `fairness.default_weight` | `1` | DRR weight for producers without an entry under `fairness.producers`. |
| `fairness.producers.<id>.weight` | `default_weight` | The producer's share of each DRR round: `quantum × weight` events. Fractions are allowed. |
| `fairness.producers.<id>.rate` | `admission.rate` | Admission rate for the producer, in events per second. |
| `fairness.producers.<id>.burst` | `admission.burst` | Admission burst for the producer. |
| `fairness.producers.<id>.queue_capacity` | `queues.capacity` | Events buffered for the producer before enqueueing blocks. |
| `wal.path` | `data/fairgate.wal` | Path of the first WAL segment. Later segments are created next to it. |
| `wal.segment_size` | `67108864` (64 MiB) | Segment size in bytes before the writer rotates. Reclamation deletes whole segments, so smaller segments free disk space sooner. |
| `shipper.checkpoint_path` | `data/checkpoint.json` | Checkpoint file. Keep it on the same persistent volume as the WAL. |
| `shipper.batch_size` | `200` | Maximum events per ClickHouse insert. |
| `shipper.poll_interval` | `1s` | How often the shipper checks for new durable WAL records once it has caught up. |
| `shipper.retry.initial_delay` | `1s` | First retry delay after a failed insert. |
| `shipper.retry.max_delay` | `30s` | Cap on the retry delay. Must be at least `initial_delay`. |
| `shipper.retry.multiplier` | `2` | Growth factor between attempts. Must be at least 1. |
| `shipper.retry.jitter` | `0.2` | Symmetric jitter as a fraction of each delay, between 0 and 1. |
| `clickhouse.addr` | `[localhost:9000]` | One or more ClickHouse native-protocol addresses. |
| `clickhouse.database` | `fairgate` | Default database of the ClickHouse connection. The adapter always writes to the `fairgate.events` table. |
| `clickhouse.username` | `fairgate` | ClickHouse user. |
| `clickhouse.password` | `fairgate_dev_password` | ClickHouse password. Prefer `CLICKHOUSE_PASSWORD` to keep it out of the file. |
| `log.level` | `info` | One of `debug`, `info`, `warn`, `error`. |
| `profiling.enabled` | `false` | Serve the `pprof` endpoints under `/debug/pprof/`. |
| `profiling.addr` | `127.0.0.1:6060` | Listener for the `pprof` endpoints, separate from `server.metrics_addr`. |
| `profiling.block_profile_rate` | `0` | Sample one blocking event per this many nanoseconds blocked; `0` leaves the block profile empty. |
| `profiling.mutex_profile_fraction` | `0` | Sample one in this many mutex contention events; `0` leaves the mutex profile empty. |

## Fairness

The `fairness` section sets how producers share admission, buffering, and scheduling. `<id>` is a `producer_id`. Every field of an entry is optional: an omitted or zero field keeps the default named in the table above. Values must not be negative, and `default_weight` must be greater than zero.

```yaml
fairness:
  default_weight: 1
  producers:
    payments-service:
      weight: 4
      rate: 2000
      burst: 4000
      queue_capacity: 2048
    batch-importer:
      weight: 0.25
```

See [Backpressure, scheduling, and fairness](scheduling.md) for how weights translate into each producer's share.

Producers listed under `fairness.producers` also get their own `producer` label in the per-producer metrics. Every other producer is reported as `other`, so list a producer here (even with no overrides, as `my-producer: {}`) to monitor it individually. See [Observability](observability.md#per-producer-labels).

## Paths and durations

Durations use Go syntax, for example `500ms`, `5s`, or `1m30s`. Relative paths resolve against the gateway's working directory, which is `/app` in the Docker image.

These limits are not configurable:

- The maximum frame size (5 MiB) is part of the wire protocol.
- WAL group commit has no batch size limit, and its 2 ms collection delay for a lone request is fixed; see [Write-Ahead Log](wal.md#writer-and-group-commit).
- Segment reclamation has no separate switch: it is driven by the shipper's checkpoint.

## Environment overrides

These environment variables override the config file. They suit secrets and per-host values, and they are the same variables FairGate read before the file existed.

| Variable | Overrides |
|---|---|
| `FAIRGATE_CONFIG` | Config file path, used when `-config` is not given. |
| `FAIRGATE_METRICS_ADDR` | `server.metrics_addr` |
| `FAIRGATE_CHECKPOINT_PATH` | `shipper.checkpoint_path` |
| `CLICKHOUSE_ADDR` | `clickhouse.addr` (comma-separated for several addresses) |
| `CLICKHOUSE_DATABASE` | `clickhouse.database` |
| `CLICKHOUSE_USER` | `clickhouse.username` |
| `CLICKHOUSE_PASSWORD` | `clickhouse.password` |

An empty variable counts as unset. `FAIRGATE_SHIPPER_ENABLED` is no longer read. The shipper always runs, and the setting was removed from Docker Compose; it can be deleted from existing environments.
