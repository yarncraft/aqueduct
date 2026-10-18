# Aqueduct

Postgres change-data-capture (CDC) over logical replication (`pgoutput`), written in Rust and
built to run **only in a container**. Aqueduct streams row-level changes to a sink with
at-least-once delivery: the LSN is checkpointed and acknowledged to Postgres only after the sink succeeds.

## Quick start

```sh
docker compose up --build
docker compose exec postgres psql -U aqueduct app -c "INSERT INTO users(name) VALUES ('ada')"
docker compose logs -f aqueduct
```

The compose file starts Postgres 16 with `wal_level=logical`, creates a `users` table and an
`aqueduct` publication (`config/init.sql`), and runs Aqueduct against it.

## Configuration

TOML at `/etc/aqueduct/aqueduct.toml` (override the path with `AQUEDUCT_CONFIG`; override the
Postgres URL with `AQUEDUCT_PG_URL`). See `config/aqueduct.example.toml`. Mount your own file over it.

| Sink | Config | Behaviour |
|------|--------|-----------|
| stdout | `type = "stdout"` | JSON lines to the container log |
| file | `type = "file"`, `path` | JSON lines, fsynced on flush |
| webhook | `type = "webhook"`, `url` | `POST` JSON; retries network errors, 5xx and 429 with backoff |
| kafka | `type = "kafka"`, `brokers = ["host:9092"]`, `topic`, optional `partitions` (default 1, used only if the topic must be created) | JSON value, key `schema.table`, headers `op` and `lsn`; partition = hash(key), so a table's changes stay ordered. Acked by the broker before checkpointing |

Each event is one JSON object: `lsn`, `xid`, `commit_ts_micros`, `schema`, `table`, `op`
(`insert`/`update`/`delete`/`truncate`), `before`, `after`. Delivery is at-least-once, so consumers should tolerate duplicates.

## Operations

- Health: `GET :8080/` returns `200 ok`; the image `HEALTHCHECK` runs `aqueduct --healthcheck`.
- Persist `/var/lib/aqueduct` (checkpoint, file sink) on a volume.
- The image is distroless and runs as non-root.

## Layout

`aqueduct-core` (types, traits, pipeline) · `aqueduct-sinks` (stdout/file/webhook/kafka) · `aqueduct` (binary).
The Postgres source lives in `aqueduct-source-postgres`.

## Status

Not yet compiled or run end to end: CI is the first build.

## Admin cockpit

The HTTP port (`health_addr`, default `:8080`) serves a live dashboard at `/`: throughput chart, total events, replication lag, sink latency, errors, per-operation and per-table breakdown, last error. Data streams over Server-Sent Events (`/api/stream`, 1 snapshot/s); `/api/metrics` returns one JSON snapshot and `/healthz` is the container health probe.

Set `admin_token` in the config (or `AQUEDUCT_ADMIN_TOKEN`) to require it for `/api/*`; open the dashboard as `http://host:8080/?token=<token>`. The dashboard page itself is public, so do not expose the port to untrusted networks without a token.
