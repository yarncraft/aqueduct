# Aqueduct

Change-data-capture (CDC) written in Rust and built to run **only in a container**. Aqueduct reads
changes from a source (**Postgres** logical replication via `pgoutput`, **Kafka** topics, or **MongoDB**
change streams) and streams them to a sink with at-least-once delivery: the source position (Postgres LSN,
Kafka offsets, Mongo resume token) is checkpointed and acknowledged only after the sink succeeds.

- Sources: **Postgres** (default), **Kafka**, **MongoDB**, chosen with `[source] type`; a top-level `[postgres]` table still works
- Sink: **Kafka** (the only sink), with per-table topic routing
- **Admin cockpit**: live throughput, lag, latency, per-table stats and an event inspector
- Distroless, non-root image; state in one volume

## Quick start

Docker or Podman (with `podman-compose`; use `podman build --format docker` if you want the image `HEALTHCHECK` kept):

```sh
docker compose up --build
docker compose exec postgres psql -U aqueduct app -c "INSERT INTO users(name) VALUES ('ada')"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic cdc.public.users --from-beginning
```

Then open the cockpit at <http://localhost:8080/>.

The compose file starts Postgres 16 with `wal_level=logical`, creates a `users` table and an
`aqueduct` publication (`config/init.sql`), a single-node Kafka (KRaft), and runs Aqueduct against both.

## Run the simulation

`docker-compose.sim.yml` starts a self-contained demo: Postgres, Kafka, Aqueduct and a `simulator` container that
endlessly inserts, updates and deletes rows in `users`, `orders` and `payments`, so the cockpit always has live traffic.
You only need Docker or Podman; nothing else is installed on the host.

```sh
# start (builds the Aqueduct image on first run)
podman-compose -f docker-compose.sim.yml -p aqsim up -d --build     # or: docker compose -f docker-compose.sim.yml -p aqsim up -d --build

# open the cockpit
#   http://localhost:8088/?token=dev-token-change-me

# change the load (transactions per second, default 15; each is ~2-3 events)
SIM_RATE=40 podman-compose -f docker-compose.sim.yml -p aqsim up -d simulator

# stop (keeps data); add -v to also wipe the Postgres, Aqueduct checkpoint volumes
podman-compose -f docker-compose.sim.yml -p aqsim down
```

Set `AQUEDUCT_ADMIN_TOKEN` to use a different admin token. The simulation publishes only port 8088, so it can run
alongside the regular `docker-compose.yml` (port 8080). Tables are capped at about 2000 recent rows and Kafka retention at
256 MiB / 24 h, so it can run indefinitely. See `docs/DEPLOYMENT.md` for details.

## Configuration

Full deployment guide (Postgres setup, Docker/Kubernetes, every config key, operations, troubleshooting): [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).

TOML at `/etc/aqueduct/aqueduct.toml` (override the path with `AQUEDUCT_CONFIG`). See
`config/aqueduct.example.toml`. Mount your own file over it.

| Key | Default | Meaning |
|-----|---------|---------|
| `postgres.url` | – | Connection string, plain SQL connection (no `replication=` parameter). Override with `AQUEDUCT_PG_URL` |
| `postgres.slot` / `postgres.publication` | `aqueduct` | Logical slot (created if missing) and an existing publication |
| `source.type` | `postgres` | `postgres`, `kafka` or `mongodb`. Without a `[source]` table, `[postgres]` is used |
| `checkpoint_path` | `/var/lib/aqueduct/checkpoint` | Last acknowledged source position (LSN, offsets or resume token), written atomically |
| `health_addr` | `0.0.0.0:8080` | HTTP listener for the cockpit and health probe |
| `admin_token` | unset | Protects `/api/*` when set. Override with `AQUEDUCT_ADMIN_TOKEN` |
| `inspector` | `true` | Event inspector in the cockpit; reads recent events back from Kafka. Also needs `admin_token` |

### Kafka sink

Kafka is the only sink. Keys under `[sink]`: `brokers`, `topic` (fallback), `[[sink.routes]]`, optional `partitions` (default 1, used when a topic has to be created). Records: JSON value, key `schema.table`, headers `op` and `lsn`; partition = hash(key), so a table's changes stay ordered. Acked by the broker before checkpointing.

Each event is one JSON object: `lsn`, `xid`, `commit_ts_micros`, `schema`, `table`, `op`
(`insert`/`update`/`delete`/`truncate`), `before`, `after`. Delivery is at-least-once, so consumers should tolerate duplicates.

Each event goes to the first `[[sink.routes]]` entry whose `match` fits its `schema.table` (`*` is a wildcard), otherwise to `topic`. Topic names may use `{schema}` and `{table}`. Topics are created on first use. An event that matches no route while `topic` is unset fails the pipeline rather than being dropped.

```toml
[sink]
type = "kafka"
brokers = ["kafka:9092"]
topic = "cdc.{schema}.{table}"        # fallback: one topic per table

[[sink.routes]]
match = "public.orders"               # exact table
topic = "orders-events"

[[sink.routes]]
match = "*.audit_*"                   # any schema, tables starting with audit_
topic = "audit"
```

### Kafka source

`[source] type = "kafka"` with `brokers`, `topics` (`*` wildcard), optional `start_at` (`earliest` default, or `latest`), `format` (`auto` or `raw`), `tls`, `tls_ca_file` and SASL keys. The position is the per-partition offsets, kept in Aqueduct's checkpoint (no consumer group). Details in `docs/DEPLOYMENT.md`; integration test: `crates/aqueduct-source-kafka/tests/broker.rs`.

### MongoDB source

```toml
[source]
type = "mongodb"
uri = "mongodb://user:pass@host:27017/?replicaSet=rs0&tls=true"
database = "app"
collections = ["users", "orders"]   # empty = whole database
pre_images = true                   # fullDocumentBeforeChange when the collection has it enabled
```

Needs a replica set (change streams). `schema` is the database, `table` the collection; the position is the
change-stream resume token, so a restart resumes just after the last checkpointed event. Updates use
`updateLookup`, so `after` is the document as it is when the source reads it (it can be newer than the change,
or only the `_id` if the document was deleted meanwhile). Deletes carry the pre-image, or just the `_id`.
Integration test: `crates/aqueduct-source-mongodb/tests/replica_set.rs` (see its header).

## Admin cockpit

The HTTP port serves a dark operations console at `/`: throughput chart, total events, end-to-end lag,
sink latency, errors, operation and per-table breakdown, last fault, and an **event inspector**
(reads the newest events straight from the Kafka topics, nothing is buffered in the pipeline; pick a topic, filter by op/table, search payloads, pause, click a row for a
before/after diff and Copy JSON).

| Route | Purpose |
|-------|---------|
| `GET /` , `GET /admin.css` | Dashboard (embedded HTML + Tailwind CSS) |
| `GET /healthz` | `200 ok`, used by the image `HEALTHCHECK` (`aqueduct --healthcheck`) |
| `GET /api/metrics` | One JSON snapshot |
| `GET /api/stream` | Server-Sent Events, one snapshot per second |
| `GET /api/events?topic=<t>&limit=<n>` | Newest events read from Kafka (default 100, max 500; all sink topics unless `topic` is given). Cached for 2 s |

With `admin_token` set, `/api/*` needs `Authorization: Bearer <token>` or `?token=<token>`; open the
dashboard as `http://host:8080/?token=<token>`. The page itself is public and **events contain row data**,
so always set a token and keep the port off untrusted networks.

## Operations

- Persist `/var/lib/aqueduct` on a volume (the image pre-creates it writable for the non-root user).
- The slot retains WAL until acknowledged; drop it (`pg_drop_replication_slot`) if you decommission Aqueduct.

## Development

```sh
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
```

Layout: `aqueduct-core` (types, traits, pipeline, metrics) · `aqueduct-source-postgres` (pgoutput decoder + source), `aqueduct-source-kafka`, `aqueduct-source-mongodb` ·
`aqueduct-sinks` (Kafka sink + inspector reader) · `aqueduct` (binary: config, checkpoint, admin server and UI).
See `CLAUDE.md` for contributor and agent notes.

The cockpit CSS is generated by the Tailwind v4 standalone CLI and committed:
`TAILWIND=/path/to/tailwindcss crates/aqueduct/ui/build.sh` (rerun after changing `crates/aqueduct/src/admin.html`).

## Status

Built and run end to end under Podman: Postgres 16 to Kafka with insert/update/delete, restart
without duplicates or gaps. The Kafka sink and topic routing were verified against a real broker (Apache Kafka 4, KRaft): two routes plus a templated fallback, topics created on first use, and a restart with no duplicates or gaps.
