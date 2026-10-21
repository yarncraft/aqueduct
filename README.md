# Aqueduct

Change-data-capture (CDC) written in Rust and built to run **only in a container**. Aqueduct reads
changes from a source (**Postgres** logical replication via `pgoutput`, or **MongoDB**
change streams) and streams them to a sink with at-least-once delivery: the source position (Postgres LSN,
Mongo resume token) is checkpointed and acknowledged only after the sink succeeds.

- Sources: **Postgres** (default), **MongoDB**, chosen with `[source] type`; a top-level `[postgres]` table still works
- Sink: **Kafka** (the only sink), with per-table topic routing
- **Admin cockpit**: whether Aqueduct is behind its source and when it catches up, live throughput, lag, latency, per-table high-water marks and an event inspector
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
        SINK["Kafka sink<br/>route plan: match · ops/where filter · topic (template, row fields → fallback) · field ops<br/>Debezium-style envelope (+ tombstones), the row (format = row), or outbox payload (inserts)<br/>key: table + primary key or route key columns (JSON), outbox aggregate id · partition = FNV-1a(key)"]
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
    SINK -. "type errors: dead-letter topic" .-> K
    SINK -. "route plan + counts (/api/routes)" .-> ADM
    PIPE -- "save position" --> CK
    PIPE -. "ack(position)" .-> SRC
    SRC -. "slot advance (Postgres)" .-> PG
    PIPE --> MET --> ADM
    ADM -. "snapshot start / cancel" .-> SRC
    K -- "read back recent events<br/>and topic offsets" --> INS --> ADM
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
| `postgres.auth` / `postgres.azure_client_id` | `password` / unset | `azure-workload-identity` (AKS) or `azure-container-apps` signs in to Azure Database for PostgreSQL with the managed identity's Microsoft Entra token instead of a password; only with `ssl_mode = "verify-full"`. See DEPLOYMENT.md, "Azure managed identity" |
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

A route can also filter what it claims with `ops` and a `where` expression (SQL-style three-valued logic; a field
that is null or missing makes a comparison unknown, and unknown is filtered). Rows the expression cannot evaluate
(a type error) go to `dead_letter_topic` with an `aqueduct-error` header instead of stopping the pipeline. The first
matching route owns the event: nothing falls through. See DEPLOYMENT.md, "Filtering events per route".

```toml
[sink]
dead_letter_topic = "aqueduct.dead-letter"  # required once a route uses `where`

[[sink.routes]]
match = "public.orders"
ops = ["insert", "update", "delete"]
where = "row.status == 'paid' or op == 'delete'"
topic = "orders-paid"
```

`key = ["col", ...]` on a route keys its records by those row columns instead of the primary key (deletes by their
before-image), and partitions by that key whatever `partition_by` says. See DEPLOYMENT.md, "Keying a route".

A route's `topic` can read the row, such as `topic = "orders.{row.region}"`. It then needs a `topic_fallback`, and it
takes an optional `topic_values` allow-list. Values must be one segment (`[A-Za-z0-9_-]{1,64}`). Anything else goes to
the fallback: a null value, a value that is not allowed, or a value past `max_dynamic_topics` (default 100, counted from
the broker). See DEPLOYMENT.md, "Routing by row content".

A route can protect personal data: `include` or `exclude` columns, `mask` them (strings become `"***"`, other values
`null`), `hash` them (HMAC-SHA256 hex, the same digest everywhere so consumers can still join) and `rename` them. The
operations apply to `before`, `after`, dead-letter records and the primary-key object in the record key. The hash key
comes only from `AQUEDUCT_HASH_KEY` or `hash_key_file`, and `hash_key_id` is sent as the `aqueduct-hash-key-id`
header. See DEPLOYMENT.md, "Protecting fields per route".

```toml
[sink]
hash_key_file = "/run/secrets/aqueduct-hash-key"  # or AQUEDUCT_HASH_KEY
hash_key_id = "2026-10"

[[sink.routes]]
match = "public.customers"
topic = "customers"
exclude = ["internal_note"]
mask = ["card_last4"]
hash = ["email"]
```

`outbox = { ... }` on a route publishes an outbox table's inserts as their own events, much like Debezium's outbox
router: topic from a template such as `outbox.{aggregate_type}`, key = the raw `aggregate_id` value (not the JSON
table key), value = the `payload` column as is, headers `id` and `type` when configured. Updates and deletes of the
outbox are dropped, and snapshot rows too unless the route's `ops` lists `read`; a row without an `aggregate_id` or
`payload` goes to the dead-letter topic. Unlike Debezium, a `jsonb` payload that is a bare string loses its quotes, a
`text` payload holding JSON reads as JSON, and `bytea` arrives as hex text: use a `jsonb` object or array, or `text`,
as the payload. See DEPLOYMENT.md, "Outbox".

```toml
[[sink.routes]]
match = "public.outbox"
outbox = { topic = "outbox.{aggregate_type}", id = "id", type = "type" }   # Debezium's column names by default
topic_fallback = "outbox.unknown"
```

`format = "row"` on a route makes the value the row itself (`after`, after field operations) instead of the
envelope; `op` and `lsn` stay in the headers. A delete then follows `deletes`: `tombstone` (default, a null value
under the row's key), `drop`, or `row` (the before-image plus `"__deleted": true`). Truncates produce nothing. On an
envelope route, `tombstones = true` adds a null-value record after every delete, on the same partition; it needs a
per-row key (`key = [...]` or `partition_by = "key"`), and a delete of a row without a primary key gets none. TOAST
caveat: Postgres does not send an unchanged large column on update, so a row-format update can lack it and a compacted
consumer that replaces whole rows loses it; `unchanged = "dead_letter"` sends such rows to the dead-letter topic
instead. See DEPLOYMENT.md, "Shaping the output".

```toml
[[sink.routes]]
match = "public.customers"
topic = "customers.compacted"
format = "row"
deletes = "tombstone"         # or "drop", "row"
unchanged = "dead_letter"     # needs sink.dead_letter_topic

[[sink.routes]]
match = "public.*"
topic = "cdc.{table}"
tombstones = true
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

The HTTP port serves a dark operations console at `/`: whether Aqueduct is **behind its source**, throughput chart, total events, end-to-end lag,
sink latency, errors, operation breakdown, a **routing graph**, per-table high-water marks, last fault, and an **event inspector**
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
| `GET /api/routes` | The compiled route plan (match, ops, where, key, topic, topic_fallback, topic_values, field operations, outbox, format, deletes, tombstones, unchanged per route, plus `default`) with live per-route counts and unseen fields; no row data. Drawn by the Routing card |
| `GET /api/routes/history` | The last 120 s at 1 s resolution: per route, events produced, filtered, sent to the fallback and dead-lettered per second; per topic, records Kafka acknowledged per second. The Routing card's step lines; the stream then adds the newest second |
| `GET /api/events?topic=<t>&limit=<n>` | Newest events read from Kafka (default 100, max 500; all sink topics unless `topic` is given), with their headers. Values that are not an envelope (outbox payloads, row-format rows, tombstones as `null`) come back raw. Cached for 2 s |
| `GET /api/tables` | Per table high-water marks: topic, counts per op, newest delivered LSN, commit and delivery time, and the topic's Kafka offsets per partition (needs the inspector; cached for 5 s, 3 s timeout), plus the last checkpoint. See DEPLOYMENT.md, "Tables · high-water marks" |
| `GET` / `POST` / `DELETE /api/snapshot` | State of the running or last table snapshot; start one with `{"tables": ["schema.table", ...]}` (Postgres tables, or `database.collection` for MongoDB; `snapshot = "on_demand"` or `"initial"`); cancel after the chunk in flight. See DEPLOYMENT.md, "Snapshots" |

Aqueduct fails closed: on a non-loopback `health_addr` it refuses to start without `admin_token`, unless
`admin_insecure_no_auth = true` (logged as a warning). With a token, `/metrics` and `/api/*` need `Authorization: Bearer <token>`
(a `?token=` query parameter is rejected; URLs leak into logs, history and Referer). Open the dashboard once as
`http://host:8080/#token=<token>` (the fragment is never sent to the server; the page moves it into session storage and strips it from the URL; a `?token=` in the page URL is ignored). The page itself is public and
**events contain row data**, so keep the port off untrusted networks.

The cockpit smooths throughput, peak and recent sink latency as trailing averages over the last 15 one-second snapshots (`SMOOTH_WINDOW` in `admin.html`). Counters, totals, lag and max latency are exact.

The **Behind source** card compares the source's head (Postgres WAL head, from the slot stats query and stream
keepalives; MongoDB's newest majority-committed cluster time, from `hello` every second; both read independently of the
pipeline loop) with the position Aqueduct
delivered. It shows a status (`caught up` within 1 MiB of WAL or 2 s of cluster time, `catching up`, `falling behind`,
`stalled` after 30 s over the threshold without progress, or `unknown` while the head is more than 3 s old), the backlog in bytes and time with a one-minute sparkline, the source and
delivered rates, the time to catch up, and while a snapshot runs its rows/s, rows left, ETA and whether
`snapshot_max_rows_per_sec` sets the pace. The End-to-end lag tile reads `idle` while caught up with nothing new.
`/api/metrics` and `/api/stream` carry it as `source_lag`; Prometheus as `aqueduct_source_backlog_bytes`,
`aqueduct_source_behind_seconds`, `aqueduct_source_catch_up_eta_seconds`, `aqueduct_snapshot_rows_per_second` and
`aqueduct_snapshot_eta_seconds`. See DEPLOYMENT.md, "Behind source".

In the routing graph every route and topic carries a small step line of its rate over the last 120 s (a route's
produced rate, with filtered, fallback and errors as thin lines; a topic's records acknowledged by Kafka), and each
"+N more" group one for the group as a whole. Click a route or a topic for a larger chart with the current, average and
maximum rate; a topic's panel also lists the routes that write to it and opens it in the event inspector.

When the source reports a table snapshot, a **Snapshot** card shows it: state, tables done, the table being copied, rows
copied against the planner estimate, skipped tables, and a Cancel button (`DELETE /api/snapshot`). Snapshot rows are
counted as `READ` in the operations breakdown, the event-rate chart and the inspector's op filter.

The **Tables · high-water marks** card has one row per table that delivered events: its topic, event count, a bar
of its share of all events split by operation, the newest delivered change's LSN, how long ago that change committed
and was delivered, and the topic's Kafka high-water mark (click a row for the offset per partition). The last-commit
age turns amber when a table is more than 1 minute behind the freshest table and red past 10 minutes, only while the
pipeline is streaming. Click a column header to sort. The checkpoint shown in the card header is global.

The **Routing** card draws the sink's routing as a graph, left to right: every table seen, the
`[[sink.routes]]` in match order (numbered, first match wins, `sink.topic` last as `default`), and the topics they
write, plus the dead-letter topic in red. A table connects to the first route whose `match` fits it; a route
connects to the topics it rendered and, when it has a `where` or errors, with a dashed line to the dead-letter topic.
Each route shows its `match`, `ops` and `where`, its produced / filtered / fallback / errors counts with a bar, and an
amber **never seen** warning for fields its `where` reads that no row has had (usually a typo). Line width follows the
counts and lines with traffic in the last 3 s animate. Hover a table to light up its path, click a route for its
details and full `where`, click a topic to open it in the event inspector. A route with more than 6 tables or topics
shows the busiest and a "+N more" node that expands. Without `[[sink.routes]]` the graph is simply tables →
`default` → topics.

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

Benchmark (no Postgres needed): `cargo run --release -p aqueduct-bench -- --events 200000 --profile industrial` measures the pipeline with a null sink; `--sink render` runs the route plan (topic, key, JSON value, headers) without a broker; add `--sink kafka --brokers localhost:9092` to include a real broker. `--source postgres --pg-url <url> --transport poll|stream` instead measures the Postgres source against a real server (`wal_level=logical`): throughput, idle single-row latency (p50/p99) and peak RSS for a 1M-row transaction, the numbers ADR 0003 §8 uses to decide the default transport. The Kafka sink end-to-end test in `crates/aqueduct-sinks/tests/kafka_it.rs` runs when `AQUEDUCT_KAFKA_BROKERS` is set and is skipped otherwise.

The cockpit CSS is generated by the Tailwind v4 standalone CLI and committed:
`TAILWIND=/path/to/tailwindcss crates/aqueduct/ui/build.sh` (rerun after changing `crates/aqueduct/src/admin.html`).

## Status

Built and run end to end under Podman: Postgres 16 to Kafka with insert/update/delete, restart
without duplicates or gaps. The Kafka sink and topic routing were verified against a real broker (Apache Kafka 4, KRaft): two routes plus a templated fallback, topics created on first use, and a restart with no duplicates or gaps.
