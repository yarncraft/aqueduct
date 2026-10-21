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
   With `transport = "stream"` it instead receives them over a replication connection as they commit; see
   [Streaming transport](#streaming-transport).
3. Each event is produced to Kafka. Only after the sink succeeds does Aqueduct write the checkpoint and advance the slot to that transaction's commit LSN.
4. On restart it resumes from the slot and the checkpoint. A crash between "sink succeeded" and "checkpoint written" replays at most that event, so **delivery is at-least-once**: consumers must tolerate duplicates (use `lsn` plus the row key to deduplicate).

Limits to know about:

- Only changes made after the slot is created are streamed. Rows that already exist are copied only by a
  [snapshot](#snapshots) (off by default).
- One Aqueduct instance per slot. Run a second instance with a different `slot` name, never two on one slot.
- Postgres connections use TLS with certificate and hostname verification (`ssl_mode = "verify-full"`) by default, so a server without TLS fails at startup, **including on localhost**. Plaintext needs an explicit `ssl_mode = "disable"` (non-loopback plaintext then logs a warning).
- DDL is not streamed. Add a new table to the publication, and expect `before` images only if the table's `REPLICA IDENTITY` allows it.

## 1. Prepare Postgres

Postgres 12 or newer (tested on 12, 13, 14, 16 and 17). Postgres 10 has no `pg_replication_slot_advance`, and on 11
repeated slot peeks over one connection fail with `out of relcache_callback_list slots`, so neither works. Snapshots
use logical messages on 14 or newer, or a [watermark table](#watermark-table-postgres-12-and-13) on 12 and 13.

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

Tables without a primary key and with the default identity reject `UPDATE`/`DELETE` once they are published; set `REPLICA IDENTITY FULL` or add a key. Records are keyed by the primary key (see [Kafka sink](#kafka-sink)), so give tables one: a table without a
primary key has a null key and all its rows share one partition.

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
admin_token = "<output of: openssl rand -hex 32>"
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
| `admin_token` | unset | Required unless `health_addr` is loopback. If set, `/metrics` and `/api/*` require it (`Authorization: Bearer ...` only; `?token=` is rejected). Must be at least 16 characters, not whitespace-only and not contain `${`; startup refuses it otherwise. Env: `AQUEDUCT_ADMIN_TOKEN` |
| `inspector` | `true` | Cockpit event inspector, reading recent events back from Kafka. Needs `admin_token`; `false` turns it off |

### `[postgres]`

| Key | Default | Description |
|-----|---------|-------------|
| `url` | required | `postgres://user:pass@host:5432/db`. A normal connection: **do not** add `replication=database`. Env: `AQUEDUCT_PG_URL` |
| `slot` | `aqueduct` | Logical slot name, created if missing. Use a distinct name per instance |
| `publication` | `aqueduct` | Existing publication to stream |
| `ssl_mode` | from URL, else `verify-full` | `disable`, `prefer`, `require`, `verify-ca`, `verify-full`. Overrides `sslmode=` in the URL. Weaker modes must be set explicitly; `verify-full` is the default and the production setting |
| `ssl_root_cert` | unset | PEM CA bundle to verify the server against (also accepted as `sslrootcert=` in the URL). Unset means the bundled public roots |
| `heartbeat_interval_secs` | `10` | While idle, advance the slot to the server's WAL position at most this often, so writes to tables outside the publication do not pin WAL. `0` turns it off. See [Idle heartbeat](#idle-heartbeat) |
| `heartbeat_action_query` | unset | Optional SQL run before each heartbeat, e.g. an upsert into a small published table. See [Idle heartbeat](#idle-heartbeat) |
| `checkpoint_skip_system_check` | `false` | Turn off the [checkpoint origin check](#checkpoint-origin-check). Only for servers that refuse `pg_control_system()` to the Aqueduct role; without it such a server fails at startup |
| `transport` | `poll` | `poll` reads the slot with SQL peeks every `poll_interval`; `stream` receives changes over a replication connection as they commit. See [Streaming transport](#streaming-transport) |
| `stream_buffer_bytes` | `67108864` (64 MiB) | Stream transport: received messages buffered ahead of the sink before reading pauses |
| `snapshot` | `never` | `never`, `on_demand` or `initial`. Copies rows that already exist; see [Snapshots](#snapshots). Anything but `never` needs Postgres 14+, or `snapshot_watermark_table` |
| `snapshot_chunk_size` | `1024` | Rows per snapshot chunk |
| `snapshot_max_rows_per_sec` | unset | Upper bound on snapshot rows read per second. Unset is unthrottled |
| `snapshot_watermark_table` | unset | `schema.table` of a published table that carries snapshot watermarks as rows, for Postgres 12 and 13 (or where `pg_logical_emit_message` is not allowed). See [Watermark table](#watermark-table-postgres-12-and-13) |
| `snapshot_key_columns` | empty | Key columns for tables without a primary key, e.g. `{ "public.events" = ["day", "seq"] }`. They must be published, `NOT NULL` and unique together |

TLS uses rustls (no OpenSSL). Semantics follow libpq: `prefer` and `require` encrypt but do **not** verify the
certificate (`require` with `ssl_root_cert` set does verify the chain); `verify-ca` checks the chain; `verify-full` also
checks the hostname. After connecting, Aqueduct asks the server (`pg_stat_ssl`) whether the session is really
encrypted. A plaintext session to a non-loopback host **aborts startup** unless `ssl_mode = "disable"` is set explicitly (then it only logs a warning); an encrypted but unverified session (`prefer`/`require` without a CA) logs a warning.
Server side you need `ssl = on` with a certificate, and `hostssl` lines in `pg_hba.conf` if you want to forbid plaintext.

### `[source]` (alternative sources)

`[postgres]` is shorthand for `[source] type = "postgres"` with the same keys. Set exactly one of the two.
`type` can also be `mongodb` (below). There is no Kafka source; Kafka is sink-only.

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
snapshot = "never"                  # or "on_demand" / "initial", see "Snapshots"
```

| Key | Default | Meaning |
|-----|---------|---------|
| `uri` | required | `mongodb://` or `mongodb+srv://`. Credentials and `tls=true` go in the URI. |
| `database` | required | Database to watch. It becomes `schema`; the collection becomes `table`. |
| `collections` | all | Collections to include. |
| `snapshot` | `never` | `on_demand` or `initial` copy existing documents; see "MongoDB snapshots" in section 5. |
| `snapshot_chunk_size` | `1024` | Documents per snapshot chunk. |
| `snapshot_max_rows_per_sec` | unset | Upper bound on snapshot documents read per second; unset or `0` is unthrottled. |
| `pre_images` | `true` | Ask for `fullDocumentBeforeChange`. It only has an effect on collections with pre-images enabled (`collMod` with `changeStreamPreAndPostImages: {enabled: true}`, MongoDB 6.0+); otherwise `before` stays empty for updates. |

- The role needs the `find` and `changeStream` actions on the watched database (the built-in `read` role covers both).
  With `snapshot` other than `never` it also needs `insert`, `remove` and `createIndex` on the one collection
  `<database>.aqueduct_watermarks`; startup checks this and fails with the missing privilege otherwise.
- The position is the change-stream resume token, saved in Aqueduct's checkpoint only after the sink has the event. A restart resumes just after the last checkpointed event. Nothing is stored on the server.
- The resume token must still be in the oplog. If Aqueduct is down longer than the oplog window, the stream cannot resume and startup fails; size the oplog for your longest expected outage.
- Updates use `updateLookup`, so `after` is the document as it is when read: it can be newer than the change, or only the `_id` if the document was deleted meanwhile. Deletes carry the pre-image when available, otherwise just the `_id`.
- Existing documents are not copied unless `snapshot` is set; see "MongoDB snapshots" in section 5.
- On a first start (no checkpoint) the stream opens at "now", and its opening position is checkpointed before
  anything else is read, so a restart soon after the first start does not skip changes made in between.
- While the stream is idle, or only filtered-out collections change, the checkpoint still follows the stream's resume token (checked every 30 s, saved only when it moved). Nothing is written to Kafka for this, so the checkpoint can advance with no sink output; it keeps the token inside the oplog window.
- Only insert, update, replace and delete events are forwarded. Drop, rename, dropDatabase and invalidate events are skipped, and `truncate` is never emitted. Changes of `aqueduct_watermarks` are never forwarded, and that name cannot be listed in `collections`.
- `pre_images = true` asks for `fullDocumentBeforeChange` on every stream. MongoDB older than 6.0 may reject that option at startup; set `pre_images = false` there (not verified against a server).

### `[sink]`

Kafka is the only sink. Each event is one JSON object in the shape of Debezium's change event (without the schema part, as with
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

#### Kafka sink

| Key | Default | Description |
|-----|---------|-------------|
| `brokers` | required | Bootstrap list, for example `["kafka-1:9093", "kafka-2:9093"]` |
| `topic` | unset | Fallback topic for events no route matches. May use `{schema}`, `{table}` and `{op}`, not row fields |
| `partitions` | `1` | Partition count used when Aqueduct has to create a topic |
| `replication_factor` | `3`, or `1` if the cluster has fewer than 3 brokers (logged as a warning) | Replicas used when Aqueduct has to create a topic. Existing topics are not changed. Must be at least 1. An explicit value the broker rejects (more replicas than brokers) is an error, at startup for the default topic and on first send for routed or templated topics; only the unset default falls back, and it falls back to 1, not to the broker count, so on a 2-broker cluster set `replication_factor = 2` yourself. Set it explicitly in production |
| `partition_by` | `key` | What the partition is hashed from. `key`: the whole record key (table and primary key), so each row's changes stay ordered and a table spreads over partitions, as with Debezium. `table`: only the table, for total order per table. The record key itself is the same either way |
| `tls` | `false` | Encrypt with TLS and verify the broker certificate and hostname (rustls). Set it for any non-local cluster |
| `tls_ca_file` | unset | PEM CA bundle for brokers with a private CA |
| `sasl_mechanism` | unset | `plain`, `scram-sha-256` or `scram-sha-512`. Needs username and password; `plain` should only be used with `tls` |
| `sasl_username` | unset | Env: `AQUEDUCT_KAFKA_SASL_USERNAME` |
| `sasl_password` | unset | Env: `AQUEDUCT_KAFKA_SASL_PASSWORD`. Never logged |
| `max_dynamic_topics` | `100` | How many topics content routing may create, counted from the broker (existing topics a template could produce count). Beyond it, events go to the route's `topic_fallback`. `0` means no new topics: only topics that already exist are used. See [Routing by row content](#routing-by-row-content) |
| `dead_letter_topic` | unset | Where events go that a route's `where` cannot evaluate (a type error). Required as soon as a route uses `where`; opened (created if missing) at startup. See [Filtering events per route](#filtering-events-per-route) |
| `[[sink.routes]]` | none | `match` (`schema.table`, `*` wildcard) and `topic`. First match wins. Optional `name` (unique) labels the route in per-route stats; it defaults to `match`. Optional `ops` and `where` filter the events the route claims, see [Filtering events per route](#filtering-events-per-route). Optional `key` sets the record key from row columns, see [Keying a route](#keying-a-route). A `topic` can read row fields with `topic_fallback` and optional `topic_values`, see [Routing by row content](#routing-by-row-content). Topic names are checked at startup |

Records: key and value as above, headers `op` and `lsn`, timestamp = commit time. The partition is a stable FNV-1a
hash. With the default `partition_by = "key"`, a row's changes stay ordered **while its primary key does not change**:
an update of a primary-key column moves the row to the key's new partition. The primary key comes from the catalog,
so tables with `REPLICA IDENTITY FULL` are keyed by it like any other. Rows of a table without a primary key, and
every `TRUNCATE`, have `"key": null`, so they share the table's partition and a truncate is not ordered against the
rows on other partitions; do not use it as a barrier. With `partition_by = "table"`, all of a table's changes stay
ordered. A produce is acknowledged by the broker before the
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

#### Filtering events per route

A route can drop events before they reach Kafka with `ops` and `where`. Both are checked after `match`, on the
original event:

```toml
[sink]
brokers = ["kafka:9092"]
topic = "cdc.{schema}.{table}"
dead_letter_topic = "aqueduct.dead-letter"   # required once any route uses `where`

[[sink.routes]]
name = "paid-orders"
match = "public.orders"
ops = ["insert", "update", "delete"]
where = "row.status == 'paid' or op == 'delete'"
topic = "orders-paid"
```

- **The first matching route owns the event.** If its `ops` or `where` drops the event, or `where` fails on it, the
  event does not fall through to a later route or to `topic`.
- **`ops`** lists the operations that pass: `insert`, `update`, `delete`, `truncate`, `read` (snapshot rows). Unset,
  every op passes. An unknown name or an empty list fails startup.
- **`where`** is a predicate. Only events it is definitely true for pass; false and unknown are filtered. A syntax
  error fails startup with a message naming the route.
  - Fields: `op` (`insert`, `update`, `delete`, `truncate`, `read`), `schema`, `table`, `before.<column>`,
    `after.<column>` and `row.<column>` (`after` if the event has it, else `before`, so a delete reads its
    before-image). A dotted path reads a nested MongoDB field: `after.address.city`.
  - Operators: `==` `!=` `<` `<=` `>` `>=`, `in [..]`, `is null`, `is not null`, `and`, `or`, `not` and parentheses.
    Literals: `'strings'` (a quote inside is written `''`), numbers, `true` and `false`. Test for null with `is null`;
    `== null` is rejected at startup. No functions, arithmetic or regular expressions. At most 4 KiB and 32 levels of
    nesting.
  - Comparisons are typed by the literal. A number literal compares exactly (decimal, not floating point) against a
    JSON number or a string holding a decimal (`numeric` columns arrive as strings). A string literal allows only
    `==`, `!=` and `in`; ordering a string (`row.name < 'm'`) is rejected at startup. A boolean literal matches a JSON
    boolean only.
  - Times: `timestamp` columns are microseconds since the epoch and `date` columns days since the epoch, both JSON
    numbers, so they compare with number literals (`row.created_at >= 1767225600000000`). `timestamptz` is an ISO-8601
    string in UTC, so it allows only `==`, `!=` and `in`. Values that have no number form (`infinity`, `-infinity`,
    dates outside the representable range) stay strings, so a number literal compared with one of them is a type
    error and the event goes to the dead-letter topic.
  - **Three-valued logic, as in SQL.** A missing field is null, and any comparison with null is *unknown*. Unknown
    propagates: `not unknown` is unknown, `false and unknown` is false, `true or unknown` is true. An event whose
    `where` ends unknown is filtered. So `not (row.region == 'eu')` drops rows whose `region` is null or missing;
    write `row.region is null or not (row.region == 'eu')` to keep them. Unchanged TOAST columns are left out of
    Postgres updates and read as null too. A `TRUNCATE` has no row, so only a `where` that tests `op`, `schema` or
    `table` can pass it.
- **Rows leaving the filter.** `where` sees only the event's own row. An update `status: paid -> refunded` under
  `where = "row.status == 'paid'"` is dropped, so the topic keeps the row as `paid` and consumers never learn that it
  left the filter. Include such transitions in the predicate (for example `op == 'delete'`) or route them to another
  topic if consumers need them.
- **Type errors go to the dead-letter topic.** A comparison of a mistyped value (`row.amount > 10` on a row where
  `amount` is `'abc'`, `NaN` or a JSON object) is a type error. It wins over true, false and unknown with no
  short-circuit, so `not (...)` cannot let a mistyped row through. The event is produced to
  `sink.dead_letter_topic` instead: value = the event (the same envelope), key = the record key with `"key": null`
  (the table only, no row values), partitioned by that key, headers `op`, `lsn` and `aqueduct-error`. The
  `aqueduct-error` header names the route, the expression and the field with the type it should have, never the value,
  for example ``route `paid-orders`: where `row.amount > 10`: type error: `row.amount` is not a finite number``. The
  first one per route is logged as a warning. The batch is acknowledged as usual once the dead-letter record is
  written too, so one bad row never stops the pipeline or pins the replication slot.
- **`dead_letter_topic`** is required as soon as any route has `where`. It must be a valid topic name, must not start
  with `_`, and must not be a topic events are routed to (a route's or `topic`'s name, or a name a `{schema}`/`{table}`
  template could produce). It is opened at startup and created if missing (with `partitions` and
  `replication_factor`), so a dead-letter topic that cannot be created or read fails startup. Pre-create it with the
  retention you want. The event inspector also reads the dead-letter topic.
- **Check the dead-letter topic's write ACL before you enable `where`.** Startup can only check that the topic exists;
  write permission is first used by the first dead-letter record. If the broker refuses that write, the batch fails,
  nothing is acknowledged and the process exits. After a restart it reads the same row again and fails again, so it
  crash-loops, and the Postgres replication slot (or the MongoDB resume token) stays where it was, pinning WAL, until
  the topic is writable.
- **Unseen fields.** A misspelled field (`row.stauts`) is not an error: it reads as null and the route filters
  everything. Aqueduct counts, per route and field path in `where`, `key` or a topic template, the rows that had the row the path reads (an
  insert has no `before`) but not the field. Once 1000 such rows have arrived and no row has had the field, it logs a
  warning once and reports the field in `unseen_fields` and `aqueduct_route_field_unseen`. A field seen once is never
  reported, so a column that only some rows have (a delete's before-image holds only the key) does not trigger it.

#### Keying a route

`key = ["col", ...]` on a route sets the record key from columns of the row, instead of the primary key:

```toml
[[sink.routes]]
match = "public.orders"
topic = "orders"
key = ["tenant_id", "order_id"]
```

- **Format.** The record key keeps its JSON shape, with `"key"` built from the listed columns in the order given:
  `{"db": "app", "schema": "public", "table": "orders", "key": {"tenant_id": "acme", "order_id": 42}}` (MongoDB:
  `{"db", "collection", "key"}`). Each value is copied from the row as is (its JSON), and a column the row lacks is
  `null`.
- **Which row.** The columns are read from `row`: `after`, else `before`. So an update is keyed by its new values and a
  delete by its before-image. Under the default `REPLICA IDENTITY`, a Postgres delete's before-image holds only the
  replica-identity (primary-key) columns, so key by those columns, or set `REPLICA IDENTITY FULL`, or deletes get
  `null` for the other columns and land on another partition than the row's other changes. MongoDB deletes carry the
  `documentKey` (`_id`) unless pre-images are enabled. A truncate has no row and gets `"key": null`.
- **Partition.** The partition is the FNV-1a hash of that whole key, whatever `partition_by` says: the route's
  `key` overrides `partition_by` for the events it produces.
- **Ordering.** Changes keep their order per key value within one topic. If a key column changes, the row's later
  events hash to another partition, and order across the change is lost, as with an update of a primary-key column
  under `partition_by = "key"`. Key by columns that never change (the primary key, a tenant id) to keep a row's
  events in order. This is also the fix for keying by an immutable column when the replica identity does not match it.
- **Validation.** `key` must be a non-empty list of distinct, top-level column names (no dots: nested MongoDB fields
  are not supported); anything else fails startup naming the route. A misspelled column is reported as an unseen
  field (above), as `row.<column>`.
- Dead-letter records keep the table-only key.

#### Routing by row content

A route's `topic` can take values from the row, so one table fans out over several topics:

```toml
[sink]
max_dynamic_topics = 100              # default

[[sink.routes]]
match = "public.orders"
topic = "orders.{row.region}"         # orders.eu, orders.us, ...
topic_fallback = "orders.unknown"     # required: where an event goes when the value cannot be used
topic_values = ["eu", "us", "apac"]   # optional allow-list
```

- **Placeholders.** `{schema}`, `{table}`, `{op}` (`insert`, `update`, ...), and row fields: `{row.<col>}` (`after`,
  else `before`, so a delete routes by its before-image), `{after.<col>}`, `{before.<col>}`, dotted for nested MongoDB
  fields (`{row.address.city}`). Strings substitute their text, numbers and booleans their JSON text. A topic with only
  `{schema}`, `{table}` and `{op}` works as before and needs no fallback.
- **Rules, checked at startup** for a `topic` that reads a row field, so row data cannot pick an arbitrary topic:
  - The template starts with fixed text: `orders.{row.region}` is allowed, `{row.tenant}` and `{schema}.{row.x}` are not.
    Once any route reads a row field, no topic anywhere in the config (route topics, `sink.topic`, fallbacks, the
    dead-letter topic) may start with `_` (Kafka's internal topics such as `__consumer_offsets`). Without content
    routing, such topics are accepted as before.
  - `topic_fallback` is required. It is a fixed topic name (no placeholders), opened (created if missing) at startup.
  - No fixed topic in the config may match the template read as a glob (`orders.{row.region}` is `orders.*`): not
    another route's `topic`, `sink.topic`, another route's `topic_fallback` or `dead_letter_topic`. Only the route's
    own fallback may (`orders.unknown` above).
  - The template read as a glob may not overlap another template's glob that has no row field (a route `topic` or
    `sink.topic` with `{schema}`, `{table}` or `{op}`). For example, `cdc.{row.kind}` next to `cdc.{table}` is
    refused: a row with `kind = "customers"` would write into the `cdc.customers` topic of the other route. Give the
    content-routed template fixed text that no other template produces.
  - `topic_values`, if set, is a non-empty list of values that are each a valid segment (below).
  - `topic_fallback` and `topic_values` on a topic that reads no row field fail startup.
- **At runtime** each substituted row value must be one segment, `[A-Za-z0-9_-]{1,64}`: no dots (`x.payments` is
  refused), no spaces, nothing empty. The event goes to `topic_fallback`, and counts under `fallback`, when a field is
  null or missing (a truncate has no row), is an object or array, breaks the segment rule, is not in `topic_values`, the
  topic name would be invalid, or the topic would be new beyond `max_dynamic_topics`. Its key and value are unchanged.
  The first fallback per route is logged once with the reason (the field path, never the value). Content routing
  never sends events to the dead-letter topic.
- **Topics Kafka refuses go to the fallback too.** Kafka refuses a new topic whose name equals an existing one once
  `.` and `_` are treated as the same (`orders.eu` while `orders_eu` exists, or `events.a_b.c` then `events.a.b_c`).
  Aqueduct keeps the broker's topic names from startup, plus every topic it opens or creates. It sends a value that
  would hit such a name to the fallback without trying to create it. If the broker still refuses the create with
  `InvalidTopicException`, `PolicyViolation`, `TopicAuthorizationFailed` or `ClusterAuthorizationFailed`, the record
  goes to the fallback and counts under `fallback`, and Aqueduct logs a warning once per route (with the template, not
  the value). These refusals cannot change on a retry, so Aqueduct remembers the refused name (up to 10 000 names,
  oldest forgotten first): later rows with that value go to the fallback without asking the broker again. The memory
  lasts until a restart, so restart Aqueduct after pre-creating such a topic or granting the ACL. Any other create error (broker down, timeout) fails the batch,
  which is retried, as for every other topic: delivery stays at-least-once. A failed or refused create holds no
  `max_dynamic_topics` slot.
- **Without create rights**, pre-create the topics and set `max_dynamic_topics = 0` or `topic_values`, so Aqueduct
  only uses topics that exist. Otherwise every new value goes to the fallback after a refused create.
- **Row values appear in topic names.** A content-routed topic's name holds the row value (`orders.eu`). Kafka shows
  it to everyone who can list topics, and it appears in broker logs and in Aqueduct's error messages for that topic
  (a failed produce or create). Do not route by a column you would not show to that audience.
- **`max_dynamic_topics`** counts the topics on the broker that a content-routed template could produce when the sink
  starts: each placeholder part is one valid segment (and in `topic_values`), and the topic is not one of the fixed
  topics above. Every new topic since then counts too. So `orders.a.b` or an over-long name does not count against
  `orders.{row.region}`. So the cap holds across restarts, and a crash loop
  cannot create more topics on each start. Topics that exist already are always used, even above the cap. A topic
  someone else created that matches the glob counts too. When the cap is reached, Aqueduct logs a warning once.
- **Ordering.** Order holds per key within one topic. If a row's routing field changes, its later events go to another
  topic, and order across the two topics is lost (ADR 0002 §6).
- **Topics are created on first use** with `partitions` and `replication_factor`. Pre-create them in production, or
  set `topic_values` so you know the set.
- The event inspector reads the content-routed topics (the template as a glob) and the fallback.

Per-route counters: every event a route claims counts as `matched`, then as exactly one of `produced`, `filtered`,
`fallback` (sent to `topic_fallback` by content routing) or `errors`. They appear in `/api/metrics` (`routes`),
`/api/routes` and Prometheus:

| Series | Meaning |
|--------|---------|
| `aqueduct_route_records_total{route, outcome}` | Counter. `outcome` is `produced`, `filtered`, `fallback` or `errors`. `matched` is not exported; it is the sum over the outcomes |
| `aqueduct_route_field_unseen{route, field}` | Gauge, `1` for each field path reported as unseen (absent otherwise) |
| `aqueduct_topic_records_total{topic}` | Counter. Records Kafka acknowledged per topic, counted after each produce succeeds (a failed produce counts when its batch is retried). The first 500 topics are counted by name, any further ones under `topic="(other)"` (logged once) |

`route` is the route's `name`, else its `match`; `default` is reserved and cannot be used as a name. Events no route
claims are counted under `default` when `sink.topic` is set; without `sink.topic` such an event is an error that stops
the pipeline. Route labels must be unique: two routes with the same `name`, a `name` equal to another route's `match`,
or two unnamed routes with the same `match` (the second could never match) fail startup.

## 4. Admin cockpit

Open `http://host:8080/` (or `/#token=<admin_token>` when a token is set; the fragment is never sent to the server). It shows throughput, totals, lag,
sink latency, errors, a per-operation breakdown, the routing graph, per-table high-water marks and an event inspector.
Routes: `/` and `/admin.css` (page), `/healthz` (liveness), `/readyz` (`200` only while streaming, else `503`), `/metrics` (Prometheus), `/api/metrics`, `/api/stream` (SSE), `/api/events?topic=<t>&limit=<n>` (read from Kafka), `/api/tables` (per table high-water marks), `/api/routes` (the route plan with per-route counts), `/api/routes/history` (per-route and per-topic rates over the last 120 s), `/api/snapshot` (`GET`/`POST`/`DELETE`, see [Snapshots](#snapshots)). Only `/api/snapshot` accepts methods other than `GET`.

The inspector shows row data. `admin_token` is required on a non-loopback bind (see `admin_insecure_no_auth`); also publish the port only on a trusted network or behind a reverse proxy
with TLS and authentication, or set `inspector = false`.

The inspector adds no work to the pipeline: nothing is buffered in memory. Each refresh (every 3 s while a tab is open,
shared across tabs for 2 s) makes a separate Kafka read of up to `limit` records per partition from the topics the sink
routes to, using its own connection (created on first use). A closed tab costs nothing.

### Tables · high-water marks

The card shows, per table (or collection) that has delivered events since the process started, how far it has got:

| Column | Source |
|--------|--------|
| Topic | The sink route the table matches |
| Events, Ops | Events delivered by this process, and a bar of the table's share of all events split by `c`/`u`/`d`/`t`/`r` |
| Last LSN | LSN of the newest delivered change (`–` for MongoDB, whose resume tokens are not shown per table) |
| Last commit | How long ago that change committed. Amber when the table is more than 1 minute behind the freshest table, red past 10 minutes; only while the pipeline is streaming. Measured against the freshest table rather than the clock, so a database that is idle as a whole is not flagged; a table that is simply written rarely is |
| Delivered | How long ago the sink acknowledged it |
| Kafka HWM | Sum over the topic's partitions of the next offset Kafka will assign; click the row for each partition. A topic several tables share shows `×N` and the whole topic's offsets |

The header shows the last checkpointed position, which is global, not per table. While a snapshot runs it is the
stream position followed by `(snapshot: <table> <i>/<n>)`; the snapshot cursor (the last chunk's primary-key values)
stays in the checkpoint file and is never shown. Pipeline numbers refresh every second
with the stream. Kafka offsets come from `GET /api/tables`, polled every 5 s: one offsets request per partition of
the routed topics (no records are read), with the inspector's connection and TLS/SASL settings, shared across tabs
for 5 s and abandoned after 3 s, so a Kafka outage leaves the column empty (the reason is in the card header and the
cell's tooltip) but never blocks the cockpit. Offsets need the inspector (`admin_token` and `inspector = true`);
without it the column shows `–`.

Events and Delivered count every event the source delivered for the table, including events a route filtered
(`ops`, `where`) or sent to the dead-letter topic. A table whose events are all filtered therefore shows events and
recent deliveries while its topic's Kafka HWM does not move; the per-route counts in `/api/routes` show why.
A table whose route picks the topic per event (`{op}` or a row field in `topic`) has no single topic to read
offsets from: its Topic column shows the template, and Kafka HWM says `content-routed (see Routing)`.

`GET /api/tables` (Bearer token) returns:

```json
{"state": "streaming", "checkpoint": "0/1A2B3C8", "inspector": true, "kafka_read_ms": 1760965426000,
 "tables": [{"table": "public.orders", "topic": "cdc.public.orders", "events": 1200,
             "ops": {"c": 800, "u": 350, "d": 40, "t": 0, "r": 10},
             "last_lsn": "0/1A2B3C0", "last_commit_ms": 1760965425210, "last_delivered_ms": 1760965425250,
             "kafka": {"hwm_total": 5310, "partitions": [{"partition": 0, "hwm": 2650}, {"partition": 1, "hwm": 2660}],
                       "tables_on_topic": 1}}]}
```

`kafka` is `null` with an `error` string when the offsets could not be read (inspector off, Kafka unreachable or slow,
topic missing). Counts and marks start from zero when the process restarts. Prometheus has the commit time per table
as `aqueduct_table_last_commit_timestamp_seconds{table="schema.table"}`; Kafka offsets are not exported there.

### Routing graph

The **Routing** card shows where each table's events go:

- **Tables** (left): every table that delivered events, with its count, grouped by the route that claims it. A route
  with more than 6 tables shows its 5 busiest and a "+N more" node; click it to expand. A table no route matches
  while `sink.topic` is unset is listed in red under "no route" (its events fail the pipeline).
- **Routes** (middle): `[[sink.routes]]` in match order, numbered, then `default` (`sink.topic`, marked `else`). A
  table belongs to the first route whose `match` fits `schema.table` (`*` matches any run of characters, dots
  included), the same rule the sink uses. Each route shows `match`, its `ops` (or "all ops"), its `where` (hover for
  the full text, click the route for details), a badge per routing option it sets, and its produced, filtered,
  fallback and errors counts with a bar. An amber **never seen** box lists fields its `where` reads that no row has
  had after enough rows (`aqueduct_route_field_unseen`); that is usually a misspelt column, and the route then
  filters everything.
- **Topics** (right): per topic template, the topics it rendered for the tables seen (a template such as
  `cdc.{schema}.{table}` is shown dashed above them), and the dead-letter topic in red with the error count. A
  template that reads the event (`{op}`, `{row.x}`) lists the topics the sink has produced to since start, and its
  `topic_fallback`.

A line's width follows its count (table events in, the route's produced count out, errors to the dead-letter
topic); lines with traffic in the last 3 s carry moving dots, which stand still when the browser asks for reduced
motion. The line to the dead-letter topic is dashed and appears for routes with a `where` or with errors. Hover a
table to highlight its path. The plan is read once from `GET /api/routes` when the page loads; counts update every
second from the stream. Without any `[[sink.routes]]` the graph is all tables → `default` → their topics.

**Rates.** Each route and topic shows a step line of its rate over the last 120 s, one sample per second, on one
shared time axis (nothing animates):

- a route: events produced per second, with filtered (grey), fallback (violet) and errors (red) as thin lines;
- a topic: records Kafka acknowledged per second, counted by the sink after the broker acknowledged the produce, so
  a failed produce never counts and a retried batch counts when it succeeds;
- a "+N more" group: the sum over its tables (events per second) or topics.

Click a route for its details and a larger chart split by outcome. Click a topic for a panel with a larger chart, its
current, average and maximum rate over the window, the routes that write to it (by template, as their
`topic_fallback`, or as the dead-letter topic), and an **Open in inspector** button (shown when the inspector is on).

The history is kept by the server, so the charts are full when the page opens: `GET /api/routes/history` returns
the window, and each stream frame carries only the newest second (`rates`: `seq`, every route's outcomes in plan
order, the topics with records in that second). It needs the admin token like every `/api/*` route. Memory is bounded:
routes are fixed at startup, and the sink counts at most 500 topics by name and the rest as `(other)`. Topic names can
contain row values under content routing; they are shown only behind the token, as in the graph. Rates start from
zero when the process restarts.

```json
{"seq": 431, "interval_ms": 1000, "len": 120,
 "routes": [{"route": "paid-orders", "produced": [3, 0, 5], "filtered": [1, 0, 2], "fallback": [0, 0, 0], "errors": [0, 0, 0]}],
 "topics": {"orders-paid": [3, 0, 5], "aqueduct.dead-letter": [0, 0, 0]}}
```

### Routing plan

`GET /api/routes` describes the compiled routing for the cockpit's routing graph: each route in match order, then
the `sink.topic` fallback as a route named `default` with `"match": null`, with live counts. It never contains row data.
Later routing options add keys to each route, so readers should ignore keys they do not know.

```json
{"default_topic": "cdc.{schema}.{table}", "dead_letter_topic": "aqueduct.dead-letter", "partition_by": "key",
 "routes": [
   {"route": "paid-orders", "match": "public.orders", "ops": ["insert", "update", "delete"],
    "where": "row.status == 'paid' or op == 'delete'", "key": ["order_id"], "topic": "orders-paid",
    "topic_fallback": null, "topic_values": null,
    "counts": {"matched": 120, "produced": 80, "filtered": 39, "fallback": 0, "errors": 1}, "unseen_fields": []},
   {"route": "regional", "match": "public.customers", "ops": null, "where": null, "key": null,
    "topic": "customers.{row.region}", "topic_fallback": "customers.unknown", "topic_values": ["eu", "us"],
    "counts": {"matched": 300, "produced": 290, "filtered": 0, "fallback": 10, "errors": 0}, "unseen_fields": []},
   {"route": "default", "match": null, "ops": null, "where": null, "key": null, "topic": "cdc.{schema}.{table}",
    "topic_fallback": null, "topic_values": null,
    "counts": {"matched": 5000, "produced": 5000, "filtered": 0, "fallback": 0, "errors": 0}, "unseen_fields": []}]}
```

`/api/metrics` and `/api/stream` carry the same counts flat, as
`routes: [{"route", "matched", "produced", "filtered", "fallback", "errors", "unseen_fields"}]`.

## 5. Operating Aqueduct

- **WAL retention.** An unacknowledged slot makes Postgres keep WAL. If Aqueduct is down for long, or the sink is stuck,
  disk usage grows. Monitor `pg_replication_slots` (`confirmed_flush_lsn`, `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`)
  and alert on `max_slot_wal_keep_size` pressure. Set `max_slot_wal_keep_size` on Postgres 13+ as a safety valve.
- **Idle databases.** Writes to tables outside the publication also produce WAL. The slot is moved forward for them by the
  [idle heartbeat](#idle-heartbeat), on by default.
- **Decommissioning.** Drop the slot or it will keep WAL forever: `SELECT pg_drop_replication_slot('aqueduct');`
- **Restarts and upgrades.** Stop the container, deploy the new image with the same volume, start. It resumes from the slot.
  Do not delete the state volume; losing the checkpoint only causes replay (the slot still decides what is pending).
- **Sink outages.** The pipeline stops on a sink failure and the container exits non-zero; `restart: unless-stopped`
  (or your orchestrator) brings it back and delivery resumes from the last acknowledged transaction.
- **Changing the publication or sink.** Edit the config and restart. Changing `slot` starts a new stream from "now".
- **Health.** `GET /healthz` returns `200 ok` while the process is up. Use the cockpit's state and error count (`/api/metrics`) for deeper checks.

### Idle heartbeat

A logical slot only moves when Aqueduct confirms a position. If the database is busy but nothing in the publication
changes, nothing is confirmed, and Postgres keeps all that WAL. Every `heartbeat_interval_secs` (default 10) the source
checks whether it is idle and, if so, confirms the server's current WAL position, so the slot follows the database.

It is idle only when no event is queued and every delivered event is already checkpointed. An event that has been read
but not yet checkpointed always holds the slot back, so delivery stays at-least-once: after a crash the slot is
never past an event that was not delivered, and a restart replays at most the unacknowledged tail, as before.
The heartbeat position is not written to the checkpoint file, which only holds the last delivered event; the
slot itself carries the heartbeat position. Set `heartbeat_interval_secs = 0` to disable.

WAL position alone is enough on a normal server. If your Postgres produces no WAL while idle, or the slot still lags
(for example a low-traffic managed database), give the heartbeat something to write with `heartbeat_action_query`.
Run it once before each heartbeat. A small published table works:

With a published action table the heartbeat row is itself delivered as an event, so the slot moves when that event is
acknowledged; the jump to the server's WAL position then rarely fires. Both paths are safe. If the action query or the
WAL position query fails, the source logs a warning and skips that heartbeat instead of stopping. A standby cannot run
an `UPDATE` action query, so leave it unset there. A checkpoint ahead of the server's WAL position (a state volume
copied from another cluster, or a restore of this one) is rejected at startup; see [Checkpoint origin check](#checkpoint-origin-check).

### Checkpoint origin check

Every Postgres checkpoint records the cluster's `system_identifier`, the database OID and the cluster's timeline
(`<commit>@<change>#<ord>;sys=<id>;db=<oid>;tli=<timeline>`). At startup, before the slot is looked up or created,
Aqueduct reads them from the server (`pg_control_system()` and `pg_control_checkpoint()`; the integration suite calls
both as a plain `LOGIN REPLICATION` role) and compares. `allow_slot_recreate` does not bypass this.

| The checkpoint was written ... | Result |
|---|---|
| on another cluster (another `initdb`: a re-pointed URL, a primary rebuilt from scratch, a major-version `pg_upgrade`) | refused: point the URL back, or remove the checkpoint |
| on another database of the same cluster | refused |
| on an earlier timeline of the same cluster (a failover to a replica, or a PITR, since) | refused with "restored, recovered to a point in time (PITR) or failed over" |
| ahead of the server's WAL on the same cluster (a restore that went back before the checkpoint) | refused with the same message |

A failover or restore can lose changes: everything between the new server's WAL end and the checkpoint never reaches
Kafka, and before Postgres 17 the logical slot does not follow a failover at all. The safe recovery is a resync:
remove the checkpoint and take a [snapshot](#snapshots). If you have verified that nothing was lost (a planned
switchover with no replication lag), delete only the `;tli=` field from the checkpoint file; the next checkpoint
records the new timeline.

- A checkpoint written by an older version lacks some fields. It is accepted with a warning, and the next checkpoint
  records them.
- If the role cannot call these functions (some managed services), startup fails with a message naming
  `checkpoint_skip_system_check`. Set it to `true` to skip the cluster and timeline comparison; the database is still
  compared.

```sql
CREATE TABLE aqueduct_heartbeat (id int PRIMARY KEY, ts timestamptz NOT NULL);
INSERT INTO aqueduct_heartbeat VALUES (1, now());
ALTER PUBLICATION aqueduct ADD TABLE aqueduct_heartbeat;
-- the Aqueduct role needs SELECT (already required) and UPDATE on it
GRANT UPDATE ON aqueduct_heartbeat TO aqueduct;
```

```toml
[postgres]
heartbeat_action_query = "UPDATE aqueduct_heartbeat SET ts = now() WHERE id = 1"
```

The table is in the publication, so its changes are delivered as normal events (topic `cdc.public.aqueduct_heartbeat`
by default, one per interval); route or ignore that topic downstream. Monitor the effect with
`pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)` from `pg_replication_slots`, or read it from Aqueduct:

| Field on `/api/metrics` and `/api/stream` | Meaning |
|-------|---------|
| `slot_lag_bytes` | WAL between the server head and the slot's `confirmed_flush_lsn`, read every 5 s. `null` when unknown (no slot yet, query failed, or a MongoDB source). |
| `confirmed_flush_lsn` | The slot's confirmed position as text, `null` when unknown. |
| `heartbeat.interval_ms` | The heartbeat interval, `null` when `heartbeat_interval_secs = 0`. |
| `heartbeat.last_ms` | Unix ms of the last completed heartbeat, `null` before the first. |
| `heartbeat.skipped_total` | Heartbeats skipped because the action query or the WAL position query failed. |
| `heartbeat.last_skip_reason` | Why the latest one was skipped, `null` until one is. |

The `heartbeat` object is absent for sources without heartbeats (MongoDB). Heartbeats are not events: they are not counted in `events_total`
and never reach the sink. `/metrics` has `aqueduct_slot_lag_bytes`, `aqueduct_heartbeat_skipped_total` and
`aqueduct_heartbeat_last_timestamp_seconds`; each is left out while its value is unknown.

### Streaming transport

`transport = "stream"` reads the slot over a replication connection (`START_REPLICATION`, the protocol
`pg_recvlogical` uses) instead of polling it with SQL. Changes arrive as they commit, nothing is decoded twice, and
the idle 500 ms poll goes away. The events, their keys and the checkpoint are the same as with `poll`: both
transports feed the same decoder, and the integration suite runs in both modes, including a test that compares them
event by event. `poll` stays the default.

- **Connection:** the same URL, TLS settings and password rules as the SQL connection (`verify-full` by default;
  cleartext and md5 refused over plaintext to a remote host; SCRAM channel binding is never given up once offered).
  The SQL connection stays open for slot management, heartbeats and snapshots. Startup refuses a replication
  connection that reaches another cluster than the SQL connection.
- **Server:** each instance uses one more walsender, so `max_wal_senders` must exceed the number of logical
  replication connections. `pg_hba.conf` needs a line that allows the role on **the database** (logical replication
  connections match the database name, not the `replication` keyword), e.g.
  `host app aqueduct 10.0.0.0/8 scram-sha-256`. The role already has `REPLICATION`.
- **Poolers:** a replication connection cannot go through PgBouncer or other transaction poolers; point the URL at
  Postgres directly.
- **Progress:** the slot is confirmed by status updates, sent within 200 ms of an acknowledgement and at least every
  `wal_sender_timeout / 3` (at most 10 s), also while the sink holds up reading, so a slow sink does not drop the
  connection.
- **Memory:** at most `stream_buffer_bytes` of received messages plus one batch of events are held; a one-million-row
  transaction is streamed with a few MiB.
- **Failures:** a broken connection is a source error, and the restart resumes from the checkpoint (duplicates only).
  If a previous connection still holds the slot, startup retries for up to 15 s.

### Snapshots

A snapshot copies rows that already exist into the same topics as live changes, while streaming continues. It is
off by default. Postgres is described first; MongoDB works the same way, with the differences under
"MongoDB snapshots" below.

| `snapshot =` | What happens |
|---|---|
| `never` (default) | Nothing is copied, and the slot is read exactly as without this feature |
| `on_demand` | Snapshots run only when requested through the admin API |
| `initial` | On a start **without a checkpoint**, every published table is copied; later starts do not repeat it |

Each table is copied in chunks of `snapshot_chunk_size` rows, in primary-key order. Around each chunk Aqueduct writes
two watermarks into the WAL with `pg_logical_emit_message` and reads the chunk between them. A live insert, update or
delete of a row while its chunk is in flight replaces the chunk's copy of that row, so a snapshot row never
overwrites a newer change. A `TRUNCATE` in that window drops the chunk. Rows that already exist arrive as events with
`"op": "r"` and `"source": {"snapshot": "incremental"}` (Kafka header `op: r`), `before` null and `after` the full row, encoded byte for byte like an
`insert` of the same row and keyed the same way, so they land on the row's partition. Consumers that apply events
"last write wins" end up with the table.

- **Requirements:** Postgres 14 or newer, and `EXECUTE` on `pg_logical_emit_message` for the Aqueduct role (startup
  checks both). On 12 and 13 use a [watermark table](#watermark-table-postgres-12-and-13) instead. The role already
  needs `SELECT` on the published tables.
- **Which columns and rows:** the publication's column list and row filter (Postgres 15+) apply to snapshot rows too.
- **Tables without a primary key** are skipped with a warning, unless `snapshot_key_columns` names a published,
  `NOT NULL`, unique key for them. A key column outside the publication's column list skips the table too.
- **Crash safety:** progress (table and last key of the last chunk fully delivered) is saved in the same checkpoint
  write as the stream position. After a crash the current chunk is read again: duplicates, never a missing row. The
  checkpoint file holds JSON (`{"pos": ..., "snap": {...}}`) while a snapshot runs and returns to the plain form when
  it finishes. Setting `snapshot = "never"` while one is unfinished abandons it with a warning.
- **WAL and load:** no long transaction is held open; each chunk is one short `SELECT` and two tiny WAL records, and
  the slot keeps advancing as events are delivered. `snapshot_max_rows_per_sec` throttles the reads.
- **Ordering:** a row's snapshot copy can arrive after a live change of another row, but never after a newer change
  of the same row.
- **`REPLICA IDENTITY USING INDEX`** on a key that is not the primary key: a live delete then does not tell which row
  went, so a chunk with such a delete in its window is read again (with a growing pause, up to 10 s).

#### Watermark table (Postgres 12 and 13)

Logical messages exist from Postgres 14 on. On 12 and 13, or where the role may not call `pg_logical_emit_message`,
the watermarks can be rows of a small table instead. Create it, add it to the publication and let the Aqueduct role
write it:

```sql
CREATE TABLE aqueduct_watermarks (slot text NOT NULL, snapshot_id text NOT NULL,
    chunk bigint NOT NULL, mark text NOT NULL, PRIMARY KEY (slot, chunk, mark));
ALTER PUBLICATION aqueduct ADD TABLE aqueduct_watermarks;
GRANT INSERT, DELETE ON aqueduct_watermarks TO aqueduct;
```

```toml
[postgres]
snapshot = "initial"
snapshot_watermark_table = "public.aqueduct_watermarks"
```

Each chunk inserts two rows (low and high, each its own transaction) and deletes the previous chunk's rows. The
table's changes are never delivered as events, and the table is never snapshotted itself. Its rows carry the slot
name, so several Aqueduct instances can share one table. Startup refuses the setting when the table is not in the
publication or the role cannot insert into and delete from it.

#### Starting a snapshot on request

With `snapshot = "on_demand"` (or `"initial"`), the admin API starts, inspects and cancels snapshots. It needs the admin
token like every `/api/*` route.

```sh
curl -H "Authorization: Bearer $TOKEN" -X POST http://host:8080/api/snapshot \
     -d '{"tables": ["public.orders", "public.customers"]}'
# 202 {"id":"s1792…","state":"running","durable":false,"note":"…"}
curl -H "Authorization: Bearer $TOKEN" http://host:8080/api/snapshot           # progress
curl -H "Authorization: Bearer $TOKEN" -X DELETE http://host:8080/api/snapshot # cancel
```

| Request | Answer |
|---|---|
| `POST` with tables while none runs | `202`, a new snapshot of those tables |
| `POST` while one runs | `202` with the running snapshot's id; the tables are appended to it |
| `POST` naming a table outside the publication, or with `snapshot = "never"` | `409` with the reason |
| `POST` with a body that is not `{"tables": ["schema.table", ...]}` | `400` |
| `DELETE` | `202`; the snapshot stops after the chunk in flight, its rows are still delivered. `409` when none runs |
| `GET` | the running or last snapshot: `id`, `state` (`running`, `finished`, `cancelled`), `tables`, `tables_done`, `table`, `rows`, `rows_estimated`, `skipped`; `{"state":"idle"}` before the first |

**A request is not durable until the first chunk is checkpointed.** If Aqueduct restarts before then, the request is
forgotten; send it again. After that, the snapshot resumes from its checkpoint like an initial one. The source answers
requests between batches, so a reply can take as long as one Kafka round trip (`504` after 30 s).

The same progress is in `/api/metrics` and `/api/stream` (`snapshot`), and in Prometheus as
`aqueduct_snapshot_running`, `aqueduct_snapshot_rows_total`, `aqueduct_snapshot_tables_total`,
`aqueduct_snapshot_tables_done` and `aqueduct_snapshot_rows_estimated` (each absent before the first snapshot). `rows`
counts this process only: after a restart it starts again from 0.

#### MongoDB snapshots

`snapshot`, `snapshot_chunk_size` and `snapshot_max_rows_per_sec` go in the MongoDB `[source]` table and mean the same
as for Postgres. `initial` copies every collection in `collections`, or every collection of the database when that
list is empty (views and time-series collections are skipped with a warning). Admin requests name collections as
`database.collection`, for example `{"tables": ["app.users"]}`; with an empty `collections` list the collection must
exist. `rows_estimated` comes from `estimatedDocumentCount`.

- **Watermarks:** around each chunk Aqueduct inserts two small documents into `<database>.aqueduct_watermarks`
  (`_id` `"<snapshot id>:<chunk>:low|high"`, field `at`), which the change stream sees like any write. The source
  recognises them and never forwards them, deletes each pair when its chunk closes and the rest when the snapshot
  ends, and a TTL index `aqueduct_ttl` (24 h on `at`) removes any a crash leaves behind (an existing TTL index on
  `{at: 1}` under another name is used instead; any other index on `{at: 1}` fails startup as a conflict). The role
  needs `insert`, `remove` and `createIndex` on that collection, and startup fails with a clear error without them.
  Chunk numbers start at a random value, so documents another client inserts there cannot close a chunk early.
- **Consistency:** the watermarks are written with `w: majority` and the chunk is read on the primary with
  `readConcern: majority`, in one causally consistent session, so the read sees everything up to the low watermark
  and finishes before the high one is written. Any change the read may have missed is then a live event between the
  two, and it replaces the chunk's copy of that document. Startup runs such a read once, so a deployment that
  refuses it (for example a primary-secondary-arbiter set on MongoDB 4.4 or older with majority reads off) fails
  there.
- **Failures:** a watermark write waits at most 30 s for a majority and a chunk read at most 60 s. A chunk the server
  refuses or that times out is read again after 2 s, then 4 s; on the third failure in a row the collection is
  skipped (`skipped` in the status, reason `chunk read failed: ...`, and a WARN) and the snapshot goes on with the
  next one, while live changes keep flowing. Connection errors still stop the source, as for live reads.
- **Chunks** follow the `_id` index (`hint({_id: 1})` with `min` set to the last `_id` read, skipping that document),
  so collections whose `_id`s mix BSON types (numbers, strings, ObjectIds, ...) are copied whole, in MongoDB's
  cross-type sort order. A `{_id: {$gt: ...}}` query would stop at the first type.
- **Rows** are encoded exactly like a live `insert` of the same document (same `after`, same Kafka key from the
  `documentKey`). Through `mongos`, a sharded collection's `documentKey` holds the shard key fields and then `_id`;
  Aqueduct reads the shard key from `config.collections` (needs `find` on it) and builds the same key. If it cannot
  read it, it logs a warning, and `read` rows of sharded collections are keyed by `_id` alone, so they may land on
  another partition than the document's later changes. Sharded clusters are
  not covered by Aqueduct's tests.
- **In the window:** an insert, update, replace or delete of a document in the chunk drops the chunk's copy of it; a
  drop or rename of the collection, a rename of another collection onto it, or a dropped database drops the whole
  chunk; a change without a `documentKey._id` makes the chunk be read again. With a `collections` list, the change
  stream also asks for renames into a watched collection and `dropDatabase` for this (neither is forwarded).
- **Admin requests** while a cancelled snapshot finishes its last chunk get `409` ("try again in a moment").
- **Checkpoint:** while a snapshot runs, the checkpoint file holds `{"pos": <resume token>, "snap": {...}}`; the last
  `_id` is kept as canonical extended JSON, so its BSON type survives a restart.

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
keeps inserting, updating and deleting rows in the industrial schema (`organizations`, `sites`, `assets`, `asset_readings`, `work_orders`, `maintenance_events`) forever (tables are capped
at roughly 2000 recent rows; Kafka retention is capped at 256 MiB / 24 h).

**Upgrading from the old users/orders/payments sim:** run `podman-compose -f docker-compose.sim.yml -p aqsim down -v` (or `docker compose -f docker-compose.sim.yml -p aqsim down -v`) once first; a bare `down -v` from the repo root targets the regular compose file and deletes your dev Postgres volume. Postgres only runs `sim/init.sql` on an empty volume, so an old volume keeps the old schema, and the simulator then fails every transaction silently (the cockpit just looks idle).

```sh
export AQUEDUCT_ADMIN_TOKEN=$(openssl rand -hex 32)   # required, no default
podman-compose -f docker-compose.sim.yml -p aqsim up -d --build   # or: docker compose -f ... -p aqsim up -d --build
# UI: http://localhost:8088/#token=$AQUEDUCT_ADMIN_TOKEN
SIM_RATE=40 podman-compose -f docker-compose.sim.yml -p aqsim up -d simulator   # change load (tx/s)
podman-compose -f docker-compose.sim.yml -p aqsim down      # add -v to wipe data
```

All services use `restart: unless-stopped`. It publishes only port 8088, so it does not clash with the main compose file (8080).
