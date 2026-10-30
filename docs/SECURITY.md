[FairGate](../README.md) › [Docs](README.md) › Security

# Security

## Trust model

FairGate is designed to run **inside** a larger system that owns authentication and network access. It does not authenticate or encrypt anything itself:

- **Ingestion (`server.listen_addr`, port 9001)** accepts plain TCP from any client that can connect. There is no TLS and no authentication.
- **`producer_id` is not verified.** The gateway trusts the producer ID in each event. A client that can connect can send events as any producer.
- **Health and metrics (`server.metrics_addr`, port 9100)** serve `/health`, `/ready`, and `/metrics` without authentication. Metrics include the producer IDs listed in `fairness.producers`.
- **Profiling (`profiling.addr`)** exposes Go internals when enabled. It is off by default and listens on `127.0.0.1`.

Treat every client that can reach port 9001 as fully trusted.

## What a client that can connect can do

Because producer identity is not checked, any client on the network path can:

- use up another producer's admission rate, queue share, or the gateway-wide overload limits, so that producer's events are refused;
- send an event under another producer's identity first, so the real event is refused as `event_identity_conflict`. After the WAL segment holding the first event is reclaimed, a later event with the same identity is acknowledged and then dropped by the ClickHouse adapter, which keeps the first stored version;
- fill the WAL and ClickHouse with arbitrary events;
- hold connections open (bounded by `server.frame_timeout`, `server.write_timeout`, and optionally `server.idle_timeout`; there is no connection limit).

These are acceptable only when every client that can connect is trusted, for example the host system's own services.

## Deploying FairGate safely

- **Keep port 9001 off untrusted networks.** Run FairGate on a private network, in the same pod or host as its producers, or behind a proxy that terminates TLS and authenticates clients.
- **If producers of different trust levels share a gateway,** put an authenticating layer in front that only lets each client send events for its own producer IDs.
- **Restrict port 9100** to your monitoring systems. `/ready` and `/metrics` reveal operational details.
- **Leave profiling off** in production, or keep `profiling.addr` on a loopback address.
- **Replace the development credentials:** the ClickHouse and Grafana passwords in `docker-compose.yml`, and the built-in default ClickHouse password (`fairgate_dev_password`). Pass the ClickHouse password through `CLICKHOUSE_PASSWORD` rather than the config file. The Compose file publishes FairGate, ClickHouse, Prometheus, Alertmanager, and Grafana ports on every interface; restrict them as described in [Docker deployment](deployment.md).
- **Protect the data directory.** The WAL, checkpoint, and `quarantine/` copies under `wal.path` hold raw event payloads. FairGate creates them with mode `0600` and the directory with `0700`.

## Reporting a vulnerability

Please report security problems privately rather than in a public issue. Use [GitHub's private vulnerability reporting](https://github.com/PratyushVishal11011/Fairgate/security/advisories/new) for this repository. Include the FairGate version, how to reproduce the problem, and its impact. You will get an acknowledgement as soon as the report has been read.

Security fixes are made on `main` and released in the latest 1.x version. Older versions do not receive fixes.
