[FairGate](../README.md) › [Docs](README.md) › Docker deployment

# Docker deployment

The Compose stack starts the FairGate TCP server and its health and metrics endpoint, ClickHouse, Prometheus, Alertmanager, and Grafana. Prometheus, Alertmanager, and Grafana use pinned image versions. FairGate waits for ClickHouse's health check before starting, Compose checks the gateway through `GET /health` on port `9100`, the shipper runs as part of the gateway (no enabling setting is needed), and the WAL and checkpoint persist in the `fairgate_wal` volume. As the shipper's checkpoint advances, older WAL segments in that volume are deleted automatically. Prometheus scrapes FairGate at `fairgate:9100`, evaluates the alert rules in [`deploy/prometheus-alerts.yml`](../deploy/prometheus-alerts.yml), and sends firing alerts to Alertmanager. Grafana automatically loads the Prometheus datasource and the FairGate Overview dashboard. Alertmanager sends no notifications until you add a receiver; see [Observability](observability.md#notifications).

Compose mounts [`deploy/fairgate.yaml`](../deploy/fairgate.yaml) read-only at `/etc/fairgate/fairgate.yaml` and points `FAIRGATE_CONFIG` at it. To tune the gateway, edit that file and restart the service with `sudo docker compose restart fairgate`. The ClickHouse address and credentials still come from the `CLICKHOUSE_*` variables in `docker-compose.yml`, which override the file. If you change `server.listen_addr` or `server.metrics_addr`, update the published ports, the healthcheck, and the Prometheus target to match. See [Configuration](configuration.md).

```bash
sudo docker compose up --build -d
sudo docker compose ps
sudo docker compose logs -f fairgate clickhouse
```

Open Grafana at [http://localhost:3001](http://localhost:3001) (default login `admin` / `admin`; change the password at first login). The standalone Next.js dashboard uses port `3000`. Prometheus is at [http://localhost:9090](http://localhost:9090); its host port is published for the remote dashboard, so restrict inbound TCP `9090` to trusted source addresses in the host firewall/security group. Alertmanager is at [http://localhost:9093](http://localhost:9093); restrict inbound TCP `9093` the same way. The gateway metrics endpoint is at [http://localhost:9100/metrics](http://localhost:9100/metrics), bound to loopback by default. `FAIRGATE_METRICS_ADDR` changes the gateway's metrics listener; if you change it, update the Compose port mapping and Prometheus target to match.

Published ports:

| Port | Service |
|---|---|
| `9001` | FairGate TCP ingestion (length-prefixed protocol, not HTTP) |
| `9100` | FairGate HTTP: `/health` and `/metrics` |
| `9000` | ClickHouse native protocol |
| `9090` | Prometheus |
| `9093` | Alertmanager |
| `3001` | Grafana |

Profiling is off by default and not published. To profile the containerized gateway, see [Observability](observability.md#profiling).

For access from outside the host, if your `fairgate` port mapping is still bound to loopback, edit it in `docker-compose.yml` from the loopback-only binding to:

```yaml
ports:
  - "9001:9001"
```

Recreate the service after changing the mapping:

```bash
sudo docker compose up --build -d --force-recreate fairgate
```

Allow inbound TCP traffic on port `9001` through the host firewall, restricted to the client IP range that needs access. Connect to the host's reachable IP or DNS name on port `9001`; this is FairGate's length-prefixed TCP protocol, not an HTTP endpoint.

The Compose file uses development ClickHouse credentials. Replace them before exposing a non-development deployment. Stopping the gateway waits up to `server.shutdown_grace_period` plus `server.drain_timeout` while it ships its backlog, so the Compose file gives it `stop_grace_period: 30s`; keep that above the sum if you raise either setting. `docker compose down` keeps the named volumes; `docker compose down -v` removes them, including the WAL, checkpoint, and database data.

If you already have a `fairgate.events` table from an earlier version, migrate it to the current schema before starting this version; see [Schema](storage.md#schema). Keeping the existing `clickhouse` volume does not apply schema changes.

## Native Apple Container on macOS

On an Apple silicon Mac running macOS 26 or later, the project can run with Apple's [`container` CLI](https://github.com/apple/container) without Docker Desktop. Apple's CLI runs OCI Linux images in lightweight VMs and does not provide Docker Compose, so the project uses a launcher script, `scripts/apple-container.sh`, to build and run the same four services with equivalent ports, networking, and persistent data volumes.

> [!NOTE]
> The launcher script is not currently tracked in the repository (`scripts/` is listed in `.gitignore`), so these instructions will not work from a clean checkout until the script is committed.

Install Apple's signed `container` package from its [release page](https://github.com/apple/container/releases), then start its system service once:

```bash
container system start
./scripts/apple-container.sh up
./scripts/apple-container.sh status
```

The local endpoints match the Compose setup: FairGate TCP at `localhost:9001`, health and metrics at `localhost:9100`, Prometheus at `localhost:9090`, and Grafana at `localhost:3001` (default login `admin` / `admin`). ClickHouse's native port is `localhost:9000`. Named volumes persist data across `stop` and `down`; remove them explicitly with `./scripts/apple-container.sh down -v`. Follow logs with `./scripts/apple-container.sh logs [fairgate|clickhouse|prometheus|grafana]`.

This launcher is for local development on Apple silicon. It does not change the Linux EC2 deployment, and the Apple Container CLI requirements and available flags are controlled by Apple's runtime release.
