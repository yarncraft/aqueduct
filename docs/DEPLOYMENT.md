# Deployment guide and configuration reference

Aqueduct runs only as a container. This guide covers preparing Postgres, running the image, every
configuration key, and operating it safely.

## How it works

```
Postgres (logical slot, pgoutput) --> Aqueduct --> Kafka
                                          |
                                   checkpoint file  +  admin cockpit (:8080)
```

1. Aqueduct opens a plain SQL connection and creates a logical replication slot (`pgoutput`) if it does not exist.
2. It polls the slot (every 500 ms when idle) for whole committed transactions and decodes them into row-level events.
3. Each event is produced to Kafka. Only after the sink succeeds does Aqueduct write the checkpoint and advance the slot to that transaction's commit LSN.
4. On restart it resumes from the slot and the checkpoint. A crash between "sink succeeded" and "checkpoint written" replays at most that event, so **delivery is at-least-once**: consumers must tolerate duplicates (use `lsn` plus the row key to deduplicate).

Limits to know about:

- Only changes made after the slot is created are captured. There is **no initial table snapshot**.
- One Aqueduct instance per slot. Run a second instance with a different `slot` name, never two on one slot.
- Postgres connections use TLS with certificate and hostname verification (`ssl_mode = "verify-full"`) by default, so a server without TLS fails at startup, **including on localhost**. Plaintext needs an explicit `ssl_mode = "disable"` (non-loopback plaintext then logs a warning).
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
docker compose up --build              # postgres + kafka + aqueduct
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
- State directory `/var/lib/aqueduct` (checkpoint) must be on a volume.
- Port 8080: cockpit, API and health probe. `HEALTHCHECK` runs `aqueduct --healthcheck`, which calls `/healthz`.
- Logs are JSON lines on stdout/stderr (`RUST_LOG=debug` for more detail).

## 3. Configuration reference

Configuration is one TOML file, read from `/etc/aqueduct/aqueduct.toml` or the path in `AQUEDUCT_CONFIG`.
Environment overrides: `AQUEDUCT_PG_URL`, `AQUEDUCT_ADMIN_TOKEN`. See `config/aqueduct.example.toml`.

```toml
health_addr = "0.0.0.0:8080"
checkpoint_path = "/var/lib/aqueduct/checkpoint"
admin_token = "change-me"
inspector = true

[postgres]
url = "postgres://aqueduct:SECRET@db.internal:5432/app"
slot = "aqueduct"
publication = "aqueduct"

[sink]
brokers = ["kafka:9092"]
topic = "cdc.{schema}.{table}"
```

### Top level

| Key | Default | Description |
|-----|---------|-------------|
| `health_addr` | `0.0.0.0:8080` | Listen address of the cockpit, API and `/healthz` |
| `checkpoint_path` | `/var/lib/aqueduct/checkpoint` | Last acknowledged LSN, written atomically. Keep on the state volume |
| `admin_insecure_no_auth` | `false` | Allow a non-loopback `health_addr` without `admin_token`. Without this, startup refuses that combination; with it, a warning is logged. Loopback binds never need a token |
| `admin_token` | unset | Required unless `health_addr` is loopback. If set, `/metrics` and `/api/*` require it (`Authorization: Bearer ...` only; `?token=` is rejected). Env: `AQUEDUCT_ADMIN_TOKEN` |
| `inspector` | `true` | Cockpit event inspector, reading recent events back from Kafka. Needs `admin_token`; `false` turns it off |

### `[postgres]`

| Key | Default | Description |
|-----|---------|-------------|
| `url` | required | `postgres://user:pass@host:5432/db`. A normal connection: **do not** add `replication=database`. Env: `AQUEDUCT_PG_URL` |
| `slot` | `aqueduct` | Logical slot name, created if missing. Use a distinct name per instance |
| `publication` | `aqueduct` | Existing publication to stream |
| `ssl_mode` | from URL, else `verify-full` | `disable`, `prefer`, `require`, `verify-ca`, `verify-full`. Overrides `sslmode=` in the URL. Weaker modes must be set explicitly; `verify-full` is the default and the production setting |
| `ssl_root_cert` | unset | PEM CA bundle to verify the server against (also accepted as `sslrootcert=` in the URL). Unset means the bundled public roots |

TLS uses rustls (no OpenSSL). Semantics follow libpq: `prefer` and `require` encrypt but do **not** verify the
certificate (`require` with `ssl_root_cert` set does verify the chain); `verify-ca` checks the chain; `verify-full` also
checks the hostname. After connecting, Aqueduct asks the server (`pg_stat_ssl`) whether the session is really
encrypted. A plaintext session to a non-loopback host **aborts startup** unless `ssl_mode = "disable"` is set explicitly (then it only logs a warning); an encrypted but unverified session (`prefer`/`require` without a CA) logs a warning.
Server side you need `ssl = on` with a certificate, and `hostssl` lines in `pg_hba.conf` if you want to forbid plaintext.

### `[source]` (alternative sources)

`[postgres]` is shorthand for `[source] type = "postgres"` with the same keys. Set exactly one of the two.
`type` can also be `kafka` or `mongodb` (both below).

#### Kafka source

Reads topics through the pipeline, e.g. to re-key or fan out events. Offsets are kept in Aqueduct's own
checkpoint (no consumer group, nothing committed to the broker) and saved only after the sink has the events.

| Key | Default | Meaning |
|-----|---------|---------|
| `brokers`, `topics` | required | Topic names may use `*` wildcards. Partitions are discovered at start; restart to pick up new ones. |
| `start_at` | `earliest` | Where partitions without a checkpoint begin: `earliest` or `latest`. |
| `format` | `auto` | `auto`: Aqueduct event JSON passes through, other values are wrapped. `raw`: always wrap as `schema=kafka`, `table=<topic>`, `op=insert`, `after=<value>`. |
| `tls`, `tls_ca_file`, `sasl_*` | as for the sink | SASL secrets: `AQUEDUCT_KAFKA_SOURCE_SASL_USERNAME` / `_PASSWORD`. |

Do not point the source and sink at the same topics: the pipeline would feed itself. A checkpoint ahead of the
log (topic recreated) stops startup; one behind retention logs a warning and restarts at the earliest offset.

#### MongoDB source

Reads a change stream, so MongoDB must run as a **replica set** (a single node started with `--replSet` and
`rs.initiate()` is enough; standalone servers have no change streams). Atlas works as is.

```toml
[source]
type = "mongodb"
uri = "mongodb://aqueduct:SECRET@mongo.internal:27017/?replicaSet=rs0&tls=true"
database = "app"
collections = ["users", "orders"]   # empty = the whole database
pre_images = true
```

| Key | Default | Meaning |
|-----|---------|---------|
| `uri` | required | `mongodb://` or `mongodb+srv://`. Credentials and `tls=true` go in the URI. |
| `database` | required | Database to watch. It becomes `schema`; the collection becomes `table`. |
| `collections` | all | Collections to include. |
| `pre_images` | `true` | Ask for `fullDocumentBeforeChange`. It only has an effect on collections with pre-images enabled (`collMod` with `changeStreamPreAndPostImages: {enabled: true}`, MongoDB 6.0+); otherwise `before` stays empty for updates. |

- The role needs the `find` and `changeStream` actions on the watched database (the built-in `read` role covers both).
- The position is the change-stream resume token, saved in Aqueduct's checkpoint only after the sink has the event. A restart resumes just after the last checkpointed event. Nothing is stored on the server.
- The resume token must still be in the oplog. If Aqueduct is down longer than the oplog window, the stream cannot resume and startup fails; size the oplog for your longest expected outage.
- Updates use `updateLookup`, so `after` is the document as it is when read: it can be newer than the change, or only the `_id` if the document was deleted meanwhile. Deletes carry the pre-image when available, otherwise just the `_id`.
- There is no initial snapshot, same as the other sources.
- While the stream is idle, or only filtered-out collections change, the checkpoint still follows the stream's resume token (checked every 30 s, saved only when it moved). Nothing is written to Kafka for this, so the checkpoint can advance with no sink output; it keeps the token inside the oplog window.
- Only insert, update, replace and delete events are forwarded. Drop, rename, dropDatabase and invalidate events are skipped, and `truncate` is never emitted.
- `pre_images = true` asks for `fullDocumentBeforeChange` on every stream. MongoDB older than 6.0 may reject that option at startup; set `pre_images = false` there (not verified against a server).

### `[sink]`

Kafka is the only sink. Every event is one JSON object: `lsn`, `xid`, `commit_ts_micros`, `schema`, `table`,
`op` (`insert`/`update`/`delete`/`truncate`), `before`, `after`.

#### Kafka sink

| Key | Default | Description |
|-----|---------|-------------|
| `brokers` | required | Bootstrap list, for example `["kafka-1:9093", "kafka-2:9093"]` |
| `topic` | unset | Fallback topic for events no route matches. May use `{schema}` and `{table}` |
| `partitions` | `1` | Partition count used when Aqueduct has to create a topic |
| `tls` | `false` | Encrypt with TLS and verify the broker certificate and hostname (rustls). Set it for any non-local cluster |
| `tls_ca_file` | unset | PEM CA bundle for brokers with a private CA |
| `sasl_mechanism` | unset | `plain`, `scram-sha-256` or `scram-sha-512`. Needs username and password; `plain` should only be used with `tls` |
| `sasl_username` | unset | Env: `AQUEDUCT_KAFKA_SASL_USERNAME` |
| `sasl_password` | unset | Env: `AQUEDUCT_KAFKA_SASL_PASSWORD`. Never logged |
| `[[sink.routes]]` | none | `match` (`schema.table`, `*` wildcard) and `topic`. First match wins |

Records: key `schema.table`, value the JSON event, headers `op` and `lsn`, timestamp = commit time. The partition is a
stable hash of the key, so one table's changes stay ordered. A produce is acknowledged by the broker before the
checkpoint moves. At least one of `topic` or a route is required; an event that matches nothing while `topic` is unset
fails the pipeline instead of being silently dropped. Topics are created on first use, so pre-create them (with the
replication factor and retention you want) in production.

```toml
[sink]
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

Open `http://host:8080/` (or `/#token=<admin_token>` when a token is set; the fragment is never sent to the server). It shows throughput, totals, lag,
sink latency, errors, per-operation and per-table breakdowns and an event inspector.
Routes: `/` and `/admin.css` (page), `/healthz` (liveness), `/readyz` (`200` only while streaming, else `503`), `/metrics` (Prometheus), `/api/metrics`, `/api/stream` (SSE), `/api/events?topic=<t>&limit=<n>` (read from Kafka).

The inspector shows row data. `admin_token` is required on a non-loopback bind (see `admin_insecure_no_auth`); also publish the port only on a trusted network or behind a reverse proxy
with TLS and authentication, or set `inspector = false`.

The inspector adds no work to the pipeline: nothing is buffered in memory. Each refresh (every 3 s while a tab is open,
shared across tabs for 2 s) makes a separate Kafka read of up to `limit` records per partition from the topics the sink
routes to, using its own connection (created on first use). A closed tab costs nothing.

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

## Securing connections (checklist)

1. Postgres: `ssl_mode = "verify-full"` and `ssl_root_cert` pointing at your CA; create the Aqueduct role with a strong password and only `REPLICATION` + `SELECT` on the published tables.
2. Kafka: `tls = true`, SASL `scram-sha-512`, and ACLs limiting the principal to `Write`/`Describe`/`Create` on the target topics.
3. Cockpit: set `admin_token` (startup refuses a non-loopback `health_addr` without one); never set `admin_insecure_no_auth` in production.
4. Pass credentials through the environment (`AQUEDUCT_PG_URL`, `AQUEDUCT_KAFKA_SASL_PASSWORD`, `AQUEDUCT_ADMIN_TOKEN`) or orchestrator secrets instead of committing them to the TOML file. Secrets in the config are redacted from debug output.

## Always-on simulation

`docker-compose.sim.yml` runs Postgres, Kafka, Aqueduct and a workload generator (`sim/simulate.sh`) that
keeps inserting, updating and deleting rows in `users`, `orders` and `payments` forever (tables are capped
at roughly 2000 recent rows; Kafka retention is capped at 256 MiB / 24 h).

```sh
export AQUEDUCT_ADMIN_TOKEN=$(openssl rand -hex 32)   # required, no default
podman-compose -f docker-compose.sim.yml -p aqsim up -d --build   # or: docker compose -f ... -p aqsim up -d --build
# UI: http://localhost:8088/#token=$AQUEDUCT_ADMIN_TOKEN
SIM_RATE=40 podman-compose -f docker-compose.sim.yml -p aqsim up -d simulator   # change load (tx/s)
podman-compose -f docker-compose.sim.yml -p aqsim down      # add -v to wipe data
```

All services use `restart: unless-stopped`. It publishes only ports 8088 and 8089, so it does not clash with the main compose file (8080).

The `aqueduct-kafka` service (port 8089, `sim/aqueduct.kafka.toml`) runs the Kafka source over `cdc.public.*` and writes `mirror.{schema}.{table}`; compare offsets of `cdc.public.<t>` and `mirror.public.<t>` to check nothing is lost or duplicated, including across `podman restart aqsim_aqueduct-kafka_1`.
