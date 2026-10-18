# Deployment guide and configuration reference

Aqueduct runs only as a container. This guide covers preparing Postgres, running the image, every
configuration key, and operating it safely.

## How it works

```
Postgres (logical slot, pgoutput) --> Aqueduct --> sink (stdout | file | webhook | Kafka)
                                          |
                                   checkpoint file  +  admin cockpit (:8080)
```

1. Aqueduct opens a plain SQL connection and creates a logical replication slot (`pgoutput`) if it does not exist.
2. It polls the slot (every 500 ms when idle) for whole committed transactions and decodes them into row-level events.
3. Each event is sent to the sink. Only after the sink succeeds does Aqueduct write the checkpoint and advance the slot to that transaction's commit LSN.
4. On restart it resumes from the slot and the checkpoint. A crash between "sink succeeded" and "checkpoint written" replays at most that event, so **delivery is at-least-once**: consumers must tolerate duplicates (use `lsn` plus the row key to deduplicate).

Limits to know about:

- Only changes made after the slot is created are captured. There is **no initial table snapshot**.
- One Aqueduct instance per slot. Run a second instance with a different `slot` name, never two on one slot.
- Postgres connections are currently unencrypted (no TLS). Keep Aqueduct and Postgres on a private network.
- DDL is not streamed. Add a new table to the publication, and expect `before` images only if the table's `REPLICA IDENTITY` allows it.

## 1. Prepare Postgres

Postgres 10 or newer (tested on 16).

```conf
# postgresql.conf (restart required)
wal_level = logical
max_replication_slots = 4      # at least one per Aqueduct instance
max_wal_senders = 4
```

Create a role, a publication and set the replica identity:

```sql
CREATE ROLE aqueduct LOGIN PASSWORD '...' REPLICATION;
GRANT USAGE ON SCHEMA public TO aqueduct;

CREATE PUBLICATION aqueduct FOR TABLE public.users, public.orders;
-- or: CREATE PUBLICATION aqueduct FOR ALL TABLES;

-- Updates and deletes carry the old row only if the table has a replica identity:
ALTER TABLE public.orders REPLICA IDENTITY FULL;   -- full "before" image
-- default identity (primary key) gives only the key columns in "before"
```

Tables without a primary key and with the default identity reject `UPDATE`/`DELETE` once they are published; set `REPLICA IDENTITY FULL` or add a key.

The publication must exist before Aqueduct starts. The slot is created automatically.

## 2. Run the container

### Docker Compose (reference)

`docker-compose.yml` in the repository starts Postgres 16 with logical replication and Aqueduct:

```sh
docker compose up --build              # stdout sink
docker compose --profile kafka up --build kafka aqueduct-kafka   # Kafka sink demo
```

Podman works too (`podman-compose up -d --build`); build with `--format docker` if you want the image `HEALTHCHECK` kept.

### Plain `docker run`

```sh
docker build -t aqueduct .
docker run -d --name aqueduct --restart unless-stopped \
  -e AQUEDUCT_PG_URL='postgres://aqueduct:SECRET@db.internal:5432/app' \
  -e AQUEDUCT_ADMIN_TOKEN='long-random-string' \
  -v aqueduct-state:/var/lib/aqueduct \
  -v ./my-aqueduct.toml:/etc/aqueduct/aqueduct.toml:ro \
  -p 127.0.0.1:8080:8080 \
  aqueduct
```

### Kubernetes (sketch)

Run a single replica (`strategy: Recreate`, never HPA) with a `PersistentVolumeClaim` on `/var/lib/aqueduct`,
the TOML in a `ConfigMap` mounted at `/etc/aqueduct/aqueduct.toml`, `AQUEDUCT_PG_URL` and `AQUEDUCT_ADMIN_TOKEN`
from a `Secret`, and an HTTP probe on `/healthz`. Use `fsGroup: 65532` if your storage class ignores image ownership.

### Image facts

- Distroless `cc-debian12`, runs as non-root uid 65532, no shell.
- State directory `/var/lib/aqueduct` (checkpoint, and the file sink if you use it) must be on a volume.
- Port 8080: cockpit, API and health probe. `HEALTHCHECK` runs `aqueduct --healthcheck`, which calls `/healthz`.
- Logs are JSON lines on stdout/stderr (`RUST_LOG=debug` for more detail).

## 3. Configuration reference

Configuration is one TOML file, read from `/etc/aqueduct/aqueduct.toml` or the path in `AQUEDUCT_CONFIG`.
Environment overrides: `AQUEDUCT_PG_URL`, `AQUEDUCT_ADMIN_TOKEN`. See `config/aqueduct.example.toml`.

```toml
health_addr = "0.0.0.0:8080"
checkpoint_path = "/var/lib/aqueduct/checkpoint"
admin_token = "change-me"
inspector_buffer = 500

[postgres]
url = "postgres://aqueduct:SECRET@db.internal:5432/app"
slot = "aqueduct"
publication = "aqueduct"

[sink]
type = "stdout"
```

### Top level

| Key | Default | Description |
|-----|---------|-------------|
| `health_addr` | `0.0.0.0:8080` | Listen address of the cockpit, API and `/healthz` |
| `checkpoint_path` | `/var/lib/aqueduct/checkpoint` | Last acknowledged LSN, written atomically. Keep on the state volume |
| `admin_token` | unset | If set, `/api/*` requires it (`Authorization: Bearer ...` or `?token=`). Env: `AQUEDUCT_ADMIN_TOKEN` |
| `inspector_buffer` | `500` | Recent events kept in memory for the cockpit inspector; `0` turns the inspector off |

### `[postgres]`

| Key | Default | Description |
|-----|---------|-------------|
| `url` | required | `postgres://user:pass@host:5432/db`. A normal connection: **do not** add `replication=database`. Env: `AQUEDUCT_PG_URL` |
| `slot` | `aqueduct` | Logical slot name, created if missing. Use a distinct name per instance |
| `publication` | `aqueduct` | Existing publication to stream |

### `[sink]`

`type` selects the sink. Every event is one JSON object: `lsn`, `xid`, `commit_ts_micros`, `schema`, `table`,
`op` (`insert`/`update`/`delete`/`truncate`), `before`, `after`.

| `type` | Keys | Notes |
|--------|------|-------|
| `stdout` (default) | none | JSON lines to the container log |
| `file` | `path` | JSON lines appended and fsynced; put it on the state volume, for example `/var/lib/aqueduct/events.jsonl` |
| `webhook` | `url` | `POST` JSON per event. Retries network errors, 5xx and 429 with exponential backoff (5 attempts); other 4xx stop the pipeline |
| `kafka` | `brokers`, `topic`, `partitions`, `[[sink.routes]]` | See below |

#### Kafka sink

| Key | Default | Description |
|-----|---------|-------------|
| `brokers` | required | Bootstrap list, for example `["kafka-1:9092", "kafka-2:9092"]`. Plaintext connections |
| `topic` | unset | Fallback topic for events no route matches. May use `{schema}` and `{table}` |
| `partitions` | `1` | Partition count used when Aqueduct has to create a topic |
| `[[sink.routes]]` | none | `match` (`schema.table`, `*` wildcard) and `topic`. First match wins |

Records: key `schema.table`, value the JSON event, headers `op` and `lsn`, timestamp = commit time. The partition is a
stable hash of the key, so one table's changes stay ordered. A produce is acknowledged by the broker before the
checkpoint moves. At least one of `topic` or a route is required; an event that matches nothing while `topic` is unset
fails the pipeline instead of being silently dropped. Topics are created on first use, so pre-create them (with the
replication factor and retention you want) in production.

```toml
[sink]
type = "kafka"
brokers = ["kafka:9092"]
topic = "cdc.{schema}.{table}"

[[sink.routes]]
match = "public.orders"
topic = "orders-events"

[[sink.routes]]
match = "*.audit_*"
topic = "audit"
```

## 4. Admin cockpit

Open `http://host:8080/` (or `/?token=<admin_token>` when a token is set). It shows throughput, totals, lag,
sink latency, errors, per-operation and per-table breakdowns and an event inspector.
Routes: `/` and `/admin.css` (page), `/healthz`, `/api/metrics`, `/api/stream` (SSE), `/api/events?after=<seq>&limit=<n>`.

The inspector shows row data. Set `admin_token`, publish the port only on a trusted network or behind a reverse proxy
with TLS and authentication, or set `inspector_buffer = 0`.

## 5. Operating Aqueduct

- **WAL retention.** An unacknowledged slot makes Postgres keep WAL. If Aqueduct is down for long, or the sink is stuck,
  disk usage grows. Monitor `pg_replication_slots` (`confirmed_flush_lsn`, `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`)
  and alert on `max_slot_wal_keep_size` pressure. Set `max_slot_wal_keep_size` on Postgres 13+ as a safety valve.
- **Decommissioning.** Drop the slot or it will keep WAL forever: `SELECT pg_drop_replication_slot('aqueduct');`
- **Restarts and upgrades.** Stop the container, deploy the new image with the same volume, start. It resumes from the slot.
  Do not delete the state volume; losing the checkpoint only causes replay (the slot still decides what is pending).
- **Sink outages.** The pipeline stops on a sink failure and the container exits non-zero; `restart: unless-stopped`
  (or your orchestrator) brings it back and delivery resumes from the last acknowledged transaction.
- **Changing the publication or sink.** Edit the config and restart. Changing `slot` starts a new stream from "now".
- **Health.** `GET /healthz` returns `200 ok` while the process is up. Use the cockpit's state and error count (`/api/metrics`) for deeper checks.

## 6. Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| `invalid connection string` | `replication=database` in `postgres.url`; remove it |
| `publication "x" does not exist` | Create the publication before starting |
| `logical decoding requires wal_level >= logical` | Set `wal_level = logical` and restart Postgres |
| `permission denied to use replication slots` | The role needs `REPLICATION` (or superuser) |
| `checkpoint error: Permission denied` | State volume not writable by uid 65532; use the provided Dockerfile or `chown 65532` the volume |
| `cannot update table ... does not have a replica identity` | Set a primary key or `REPLICA IDENTITY FULL` |
| Kafka: `InvalidTopicException` | A topic name with illegal characters; check route `topic` values and templates |
| Same transaction delivered twice after a crash | Expected with at-least-once delivery; deduplicate downstream |
