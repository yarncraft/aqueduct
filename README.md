# Aqueduct

Change-data-capture (CDC) written in Rust and built to run **only in a container**. Aqueduct reads
changes from a source (**Postgres** logical replication via `pgoutput`, or **MongoDB**
change streams) and streams them to a sink with at-least-once delivery: the source position (Postgres LSN,
Mongo resume token) is checkpointed and acknowledged only after the sink succeeds.

- Sources: **Postgres** (default), **MongoDB**, chosen with `[source] type`; a top-level `[postgres]` table still works
- Sink: **Kafka** (the only sink), with per-table topic routing
- **Admin cockpit**: live throughput, lag, latency, per-table stats and an event inspector
- Distroless, non-root image; state in one volume

## Architecture

One process runs one pipeline: a **source**, the **Kafka sink**, a **checkpoint file** and the **admin server**,
all inside one container.

```mermaid
flowchart LR
    subgraph db["Source database"]
        PG[("Postgres<br/>logical slot + publication")]
        MG[("MongoDB<br/>change stream")]
    end

    subgraph aq["Aqueduct container (distroless, uid 65532)"]
        SRC["Source<br/>pgoutput decoder / change-stream mapper<br/>snapshot engine (Postgres, MongoDB)"]
        PIPE["run_pipeline<br/>batches of up to 1000 events"]
        SINK["Kafka sink<br/>Debezium-style envelope · topic router<br/>key: table + primary key (JSON) · partition = FNV-1a(key)"]
        CK[("checkpoint file<br/>/var/lib/aqueduct")]
        MET["Metrics"]
        ADM["Admin server :8080<br/>/ · /api/* · /metrics · /healthz · /readyz<br/>/api/snapshot requests"]
        INS["Event inspector"]
    end

    K[("Kafka topics")]
    U(["Browser / Prometheus / probes"])

    PG -- "peek changes + watermarks (poll)<br/>or replication stream (stream)" --> SRC
    SRC -- "snapshot chunks (keyset SELECT)" --> PG
    MG -- "watch (changes + watermarks)" --> SRC
    SRC -- "snapshot chunks (_id index scan) + watermark inserts" --> MG
    SRC --> PIPE --> SINK --> K
    PIPE -- "save position" --> CK
    PIPE -. "ack(position)" .-> SRC
    SRC -. "slot advance (Postgres)" .-> PG
    PIPE --> MET --> ADM
    ADM -. "snapshot start / cancel" .-> SRC
    K -- "read back recent events" --> INS --> ADM
    U -- "Bearer for /api/* and /metrics<br/>/, /healthz, /readyz are open" --> ADM
```

### Delivery: at-least-once

The checkpoint is written and the source acknowledged only after Kafka has acknowledged the whole batch. A crash
before that point replays the batch, so consumers must tolerate duplicates.

```mermaid
sequenceDiagram
    autonumber
    participant S as Source
    participant P as run_pipeline
    participant K as Kafka sink
    participant C as Checkpoint file
    Note over P,C: startup: load checkpoint, source.start(position)
    loop until shutdown
        P->>S: next() + try_next() (up to 1000 events)
        S-->>P: batch of ChangeEvents
        P->>K: send_batch(batch) + flush() (60 s timeout)
        K-->>P: broker ack
        P->>C: save(last position) (tmp file, fsync, rename)
        P->>S: ack(last position)
        Note over S: Postgres: slot advances only to a commit-end LSN
    end
```

Heartbeat events (MongoDB, idle or filtered streams) never reach the sink. They only move the checkpoint.

### Crates

```mermaid
flowchart BT
    core["aqueduct-core<br/>events, traits, pipeline, metrics"]
    tls["aqueduct-tls<br/>rustls config"]
    pg["aqueduct-source-postgres"]
    mg["aqueduct-source-mongodb"]
    sinks["aqueduct-sinks<br/>Kafka sink + inspector"]
    bin["aqueduct<br/>binary: config, checkpoint, admin"]
    bench["aqueduct-bench"]

    pg --> core
    pg --> tls
    mg --> core
    sinks --> core
    sinks --> tls
    bin --> core
    bin --> pg
    bin --> mg
    bin --> sinks
    bench --> core
    bench --> sinks
    bench --> pg
```

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
endlessly inserts, updates and deletes rows in an industrial asset-management schema (`organizations`, `sites`, `assets`, `asset_readings`, `work_orders`, `maintenance_events`), so the cockpit always has live traffic.
You only need Docker or Podman; nothing else is installed on the host.

**Upgrading from the old users/orders/payments sim:** run `podman-compose -f docker-compose.sim.yml -p aqsim down -v` (or `docker compose -f docker-compose.sim.yml -p aqsim down -v`) once first; a bare `down -v` from the repo root targets the regular compose file and deletes your dev Postgres volume. Postgres only runs `sim/init.sql` on an empty volume, so an old volume keeps the old schema, and the simulator then fails every transaction silently (the cockpit just looks idle).

```sh
# choose an admin token (required, no default), then start (builds the Aqueduct image on first run)
export AQUEDUCT_ADMIN_TOKEN=$(openssl rand -hex 32)
podman-compose -f docker-compose.sim.yml -p aqsim up -d --build     # or: docker compose -f docker-compose.sim.yml -p aqsim up -d --build

# open the cockpit
#   http://localhost:8088/#token=$AQUEDUCT_ADMIN_TOKEN

# change the load (transactions per second, default 15; each is ~2-3 events)
SIM_RATE=40 podman-compose -f docker-compose.sim.yml -p aqsim up -d simulator

# stop (keeps data); add -v to also wipe the Postgres, Aqueduct checkpoint volumes
podman-compose -f docker-compose.sim.yml -p aqsim down
```

`AQUEDUCT_ADMIN_TOKEN` must be set (compose refuses to start without it). The simulation publishes only port 8088, so it can run
alongside the regular `docker-compose.yml` (port 8080). Tables are capped at about 2000 recent rows and Kafka retention at
256 MiB / 24 h, so it can run indefinitely. See `docs/DEPLOYMENT.md` for details.

## Configuration

Full deployment guide (Postgres setup, Docker/Kubernetes, every config key, operations, troubleshooting): [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).

Upgrading? Read [`CHANGELOG.md`](CHANGELOG.md) first: the next release changes the TLS and admin-token defaults.

TOML at `/etc/aqueduct/aqueduct.toml` (override the path with `AQUEDUCT_CONFIG`). See
`config/aqueduct.example.toml`. Mount your own file over it.

| Key | Default | Meaning |
|-----|---------|---------|
| `postgres.url` | – | Connection string, plain SQL connection (no `replication=` parameter). Override with `AQUEDUCT_PG_URL` |
| `postgres.slot` / `postgres.publication` | `aqueduct` | Logical slot (created if missing) and an existing publication |
| `postgres.ssl_mode` / `postgres.ssl_root_cert` | `verify-full` / unset | TLS with certificate and hostname checks by default, so a server without TLS fails startup, even on localhost. For a non-TLS dev Postgres set `ssl_mode = "disable"` explicitly |
| `postgres.transport` | `poll` | `poll` peeks the slot over SQL every 500 ms; `stream` receives changes over a replication connection as they commit (same events and keys). See DEPLOYMENT.md, "Streaming transport" |
| `postgres.snapshot` | `never` | `initial` copies every published table on a start without a checkpoint, `on_demand` only on request; rows arrive as `"op": "r"`. Postgres 14+, or 12 and 13 with `snapshot_watermark_table`. `snapshot_chunk_size`, `snapshot_max_rows_per_sec` and `snapshot_key_columns` tune it; see DEPLOYMENT.md, "Snapshots" |
| `postgres.checkpoint_skip_system_check` | `false` | Skip comparing the checkpoint's cluster and timeline with the server's at startup (the database is still compared). Only for servers that refuse `pg_control_system()`; see DEPLOYMENT.md, "Checkpoint origin check" |
| `postgres.heartbeat_interval_secs` / `postgres.heartbeat_action_query` | `10` / unset | While idle, the slot is advanced to the server's WAL position this often (never past a checkpointed event), so writes to unpublished tables do not pin WAL. `0` disables it. The optional query (e.g. an upsert into a small published table) runs before each heartbeat; see DEPLOYMENT.md |
| `source.snapshot` (MongoDB) | `never` | `initial` copies every watched collection on a start without a checkpoint, `on_demand` only on request; documents arrive as `"op": "r"`. Writes watermarks into `<database>.aqueduct_watermarks`. `snapshot_chunk_size` (1024) and `snapshot_max_rows_per_sec` tune it; see DEPLOYMENT.md, "MongoDB snapshots" |
| `source.type` | `postgres` | `postgres` or `mongodb`. (`kafka` was removed; see `CHANGELOG.md`.) Without a `[source]` table, `[postgres]` is used |
| `checkpoint_path` | `/var/lib/aqueduct/checkpoint` | Last acknowledged source position (LSN or resume token), written atomically |
| `health_addr` | `0.0.0.0:8080` | HTTP listener for the cockpit and health probe |
| `admin_insecure_no_auth` | `false` | Opt out of the token requirement on a non-loopback `health_addr` (logs a warning) |
| `admin_token` | unset | Protects `/metrics` and `/api/*` (Bearer header only). **Required** when `health_addr` is not loopback (startup refuses otherwise; an empty value counts as unset). A set token must be 16+ characters, not whitespace-only and not contain `${`. Override with `AQUEDUCT_ADMIN_TOKEN` |
| `inspector` | `true` | Event inspector in the cockpit; reads recent events back from Kafka. Also needs `admin_token` |

### Kafka sink

Kafka is the only sink. Keys under `[sink]`: `brokers`, `topic` (fallback), `[[sink.routes]]`, optional `partitions` (default 1) and `replication_factor` (default 3, or 1 with a warning on a smaller cluster), both used when a topic has to be created, and `partition_by`. The partition is an FNV-1a hash of the record key, so by default each row's changes stay
ordered and a table spreads over partitions (Debezium's behaviour). `partition_by = "table"` hashes only the table
instead, for total per-table order. A row's changes stay on one partition while its primary key does not change;
rows of a table without a primary key, and its truncates, share the table's partition. Acked by the broker before
checkpointing.

Each event is one JSON object in the shape of Debezium's change event (without the schema part, as with
`schemas.enable=false`):

```json
{
  "before": {"id": 6320, "status": "running", "updated_at": "2026-10-20T13:23:46.322279Z"},
  "after":  {"id": 6320, "status": "idle",    "updated_at": "2026-10-20T13:23:46.992107Z"},
  "source": {"version": "0.1.0", "connector": "postgresql", "ts_ms": 1792502627001, "ts_us": 1792502627001627,
             "snapshot": "false", "db": "app", "schema": "public", "table": "assets", "txId": 215319, "lsn": 286771792},
  "op": "u", "ts_ms": 1792502627093, "ts_us": 1792502627093140
}
```

- `op`: `c` insert, `u` update, `d` delete, `t` truncate, `r` a row a snapshot read (`source.snapshot` is then
  `incremental`, otherwise `false`).
- `source.ts_ms`/`ts_us`: the commit time; the top-level `ts_ms`/`ts_us`: when Aqueduct produced the record.
  `source.lsn` is the change's WAL position, `txId` the transaction. MongoDB events carry `db` and `collection` instead
  of `schema`, `table`, `txId` and `lsn`.
- Values as Debezium encodes them by default: `timestamptz` an ISO-8601 string in UTC
  (`"2026-10-20T13:23:46.992107Z"`), `timestamp` microseconds since the epoch, `date` days since the epoch. Numbers
  and booleans are JSON numbers and booleans, `json`/`jsonb` are embedded, everything else is Postgres's text output
  (including `numeric`, which stays exact, and `time`). An unchanged TOASTed column is left out of `after`.
- Record key: the primary key as a JSON object plus where the row lives,
  `{"db": "app", "schema": "public", "table": "assets", "key": {"id": 6320}}` (MongoDB:
  `{"db": "shop", "collection": "orders", "key": {"_id": ...}}`, the `documentKey`). `key` is `null` for a table
  without a primary key and for a truncate. Headers: `op` (the letter) and `lsn`. Record timestamp: the commit time.

Delivery is at-least-once, so consumers should tolerate duplicates.

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

### MongoDB source

```toml
[source]
type = "mongodb"
uri = "mongodb://user:pass@host:27017/?replicaSet=rs0&tls=true"
database = "app"
collections = ["users", "orders"]   # empty = whole database
pre_images = true                   # fullDocumentBeforeChange when the collection has it enabled
snapshot = "never"                  # or "on_demand" / "initial": copy existing documents
```

Needs a replica set (change streams). `schema` is the database, `table` the collection; the position is the
change-stream resume token, so a restart resumes just after the last checkpointed event. Updates use
`updateLookup`, so `after` is the document as it is when the source reads it (it can be newer than the change,
or only the `_id` if the document was deleted meanwhile). Deletes carry the pre-image, or just the `_id`.
Snapshots copy existing documents in `_id` order between watermark inserts into `aqueduct_watermarks` (needs
`insert`, `remove` and `createIndex` on that one collection); see DEPLOYMENT.md, "MongoDB snapshots".
Integration tests: `crates/aqueduct-source-mongodb/tests/replica_set.rs` and `snapshot_it.rs` (see their headers).

## Admin cockpit

The HTTP port serves a dark operations console at `/`: throughput chart, total events, end-to-end lag,
sink latency, errors, operation and per-table breakdown, last fault, and an **event inspector**
(reads the newest events straight from the Kafka topics, nothing is buffered in the pipeline; pick a topic, filter by op/table, search payloads, pause, click a row for a
before/after diff and Copy JSON).

| Route | Purpose |
|-------|---------|
| `GET /` , `GET /admin.css` | Dashboard (embedded HTML + Tailwind CSS) |
| `GET /healthz` | `200 ok`, used by the image `HEALTHCHECK` (`aqueduct --healthcheck`) |
| `GET /readyz` | `200` while the pipeline is streaming, else `503` (readiness probe) |
| `GET /metrics` | Prometheus text exposition (`aqueduct_*` series). Needs the token like `/api/*` |
| `GET /api/metrics` | One JSON snapshot |
| `GET /api/stream` | Server-Sent Events, one snapshot per second |
| `GET /api/events?topic=<t>&limit=<n>` | Newest events read from Kafka (default 100, max 500; all sink topics unless `topic` is given). Cached for 2 s |
| `GET` / `POST` / `DELETE /api/snapshot` | State of the running or last table snapshot; start one with `{"tables": ["schema.table", ...]}` (Postgres tables, or `database.collection` for MongoDB; `snapshot = "on_demand"` or `"initial"`); cancel after the chunk in flight. See DEPLOYMENT.md, "Snapshots" |

Aqueduct fails closed: on a non-loopback `health_addr` it refuses to start without `admin_token`, unless
`admin_insecure_no_auth = true` (logged as a warning). With a token, `/metrics` and `/api/*` need `Authorization: Bearer <token>`
(a `?token=` query parameter is rejected; URLs leak into logs, history and Referer). Open the dashboard once as
`http://host:8080/#token=<token>` (the fragment is never sent to the server; the page moves it into session storage and strips it from the URL; a `?token=` in the page URL is ignored). The page itself is public and
**events contain row data**, so keep the port off untrusted networks.

The cockpit smooths throughput, peak and recent sink latency as trailing averages over the last 15 one-second snapshots (`SMOOTH_WINDOW` in `admin.html`). Counters, totals, lag and max latency are exact.

When the source reports a table snapshot, a **Snapshot** card shows it: state, tables done, the table being copied, rows
copied against the planner estimate, skipped tables, and a Cancel button (`DELETE /api/snapshot`). Snapshot rows are
counted as `READ` in the operations breakdown, the event-rate chart and the inspector's op filter.

A second row shows replication health: slot lag in bytes and the confirmed flush LSN, the age of the last heartbeat against its interval (amber past 2x, red past 5x), and how many heartbeats were skipped and why. These come from `slot_lag_bytes`, `confirmed_flush_lsn` and `heartbeat` in the `/api/metrics` and `/api/stream` snapshots. A field the server does not send shows `–`, and `off` means heartbeats are disabled. The age is measured against the browser clock, so a large clock skew between the browser and the container shifts it.

## Operations

- Persist `/var/lib/aqueduct` on a volume (the image pre-creates it writable for the non-root user).
- The slot retains WAL until acknowledged; drop it (`pg_drop_replication_slot`) if you decommission Aqueduct.

## Development

```sh
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
```

Layout: `aqueduct-core` (types, traits, pipeline, metrics) · `aqueduct-source-postgres` (pgoutput decoder + source), `aqueduct-source-mongodb` ·
`aqueduct-sinks` (Kafka sink + inspector reader) · `aqueduct-tls` (shared rustls config) · `aqueduct` (binary: config, checkpoint, admin server and UI) · `aqueduct-bench` (throughput benchmark).
See `CLAUDE.md` for contributor and agent notes.

Benchmark (no Postgres needed): `cargo run --release -p aqueduct-bench -- --events 200000 --profile industrial` measures the pipeline with a null sink; add `--sink kafka --brokers localhost:9092` to include a real broker. `--source postgres --pg-url <url> --transport poll|stream` instead measures the Postgres source against a real server (`wal_level=logical`): throughput, idle single-row latency (p50/p99) and peak RSS for a 1M-row transaction, the numbers ADR 0003 §8 uses to decide the default transport. The Kafka sink end-to-end test in `crates/aqueduct-sinks/tests/kafka_it.rs` runs when `AQUEDUCT_KAFKA_BROKERS` is set and is skipped otherwise.

The cockpit CSS is generated by the Tailwind v4 standalone CLI and committed:
`TAILWIND=/path/to/tailwindcss crates/aqueduct/ui/build.sh` (rerun after changing `crates/aqueduct/src/admin.html`).

## Status

Built and run end to end under Podman: Postgres 16 to Kafka with insert/update/delete, restart
without duplicates or gaps. The Kafka sink and topic routing were verified against a real broker (Apache Kafka 4, KRaft): two routes plus a templated fallback, topics created on first use, and a restart with no duplicates or gaps.
