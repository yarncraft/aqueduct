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
Environment overrides: `AQUEDUCT_PG_URL`, `AQUEDUCT_ADMIN_TOKEN`, `AQUEDUCT_HASH_KEY`. See `config/aqueduct.example.toml`.

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
| `url` | required | `postgres://user:pass@host:5432/db`. A normal connection: **do not** add `replication=database`. Env: `AQUEDUCT_PG_URL`. Unless the URL sets them, connections use `connect_timeout=30` and TCP keepalives `keepalives_idle=30`, `keepalives_interval=10`, `keepalives_retries=3`, so a vanished server (failover, dropped NAT flow) fails the source in about a minute and the container restarts, instead of hanging for the OS default of over 2 h |
| `slot` | `aqueduct` | Logical slot name, created if missing. Use a distinct name per instance |
| `publication` | `aqueduct` | Existing publication to stream |
| `ssl_mode` | from URL, else `verify-full` | `disable`, `prefer`, `require`, `verify-ca`, `verify-full`. Overrides `sslmode=` in the URL. Weaker modes must be set explicitly; `verify-full` is the default and the production setting |
| `ssl_root_cert` | unset | PEM CA bundle to verify the server against (also accepted as `sslrootcert=` in the URL). Unset means the bundled public roots |
| `auth` | `password` | `password` uses the password in `url`. `azure-workload-identity` (AKS) or `azure-container-apps` signs in with a Microsoft Entra token from the pod's or app's managed identity instead; see [Azure managed identity](#azure-managed-identity) |
| `azure_client_id` | unset | With an `azure-*` `auth`: the managed identity's client ID. Overrides `AZURE_CLIENT_ID` on AKS; on Container Apps it picks a user-assigned identity (unset: the system-assigned one) |
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
encrypted. A plaintext session to a non-loopback host **aborts startup** unless `ssl_mode = "disable"` is set explicitly (then it only logs a warning). A `hostaddr` in the URL is the address actually dialled, so it must be loopback too for the host to count as local, and `prefer` to a non-loopback host dials as `require`, so a server without TLS is refused before any password is sent; an encrypted but unverified session (`prefer`/`require` without a CA) logs a warning.
Server side you need `ssl = on` with a certificate, and `hostssl` lines in `pg_hba.conf` if you want to forbid plaintext.
With an `azure-*` `auth` only `verify-full` is accepted: startup refuses every other `ssl_mode`, also `verify-ca`
and `require` with `ssl_root_cert`.

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
| `dead_letter_topic` | unset | Global override: the dead-letter topic of every route that does not set its own `dead_letter_topic`. Unset, each route uses its own topic plus `.dlq`. See [Dead-letter topics](#dead-letter-topics) |
| `hash_key_file` | unset | File holding the key for route `hash` (at least 32 bytes; one trailing newline is ignored). Or env `AQUEDUCT_HASH_KEY` (also without one trailing newline), not both. A `hash_key` in this file is rejected. Never logged. See [Protecting fields per route](#protecting-fields-per-route) |
| `hash_key_id` | unset | Short label of the hash key (1 to 64 of `A-Z a-z 0-9 . _ : -`), sent as the `aqueduct-hash-key-id` header on every record of a hashing route. Required with a key, and an error without one |
| `[[sink.routes]]` | none | `match` (`schema.table`, `*` wildcard) and `topic` (or `outbox`). First match wins. Optional `name` (unique) labels the route in per-route stats; it defaults to `match`. Optional `where` filters the events the route claims, operations included (`op in ['insert', 'update']`), see [Filtering events per route](#filtering-events-per-route). Optional `key` sets the record key from row columns, see [Keying a route](#keying-a-route). A `topic` can read row fields with `topic_fallback` and optional `topic_values`, see [Routing by row content](#routing-by-row-content). `outbox` publishes an outbox table's rows as their own events, see [Outbox](#outbox). Topic names are checked at startup. Optional `include`, `exclude`, `mask`, `hash` and `rename` protect personal data, see [Protecting fields per route](#protecting-fields-per-route). Optional `format`, `deletes`, `tombstones` and `unchanged` reshape the records, see [Shaping the output](#shaping-the-output). Optional `dead_letter_topic` (a fixed name) is where the route's rows that cannot be evaluated go; default `sink.dead_letter_topic`, else the route's topic plus `.dlq`, see [Dead-letter topics](#dead-letter-topics) |

Records: key and value as above (a route with `format = "row"` or `tombstones = true` changes the value, see
[Shaping the output](#shaping-the-output)), headers `op` and `lsn`, timestamp = commit time. The partition is a stable FNV-1a
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

A route can drop events before they reach Kafka with `where`. It is checked after `match`, on the original event,
and filters operations as well as row values:

```toml
[sink]
brokers = ["kafka:9092"]
topic = "cdc.{schema}.{table}"

[[sink.routes]]
name = "paid-orders"
match = "public.orders"
where = "op in ['insert', 'update'] and row.status == 'paid' or op == 'delete'"
topic = "orders-paid"

[[sink.routes]]
name = "audit"
match = "audit.*"
where = "op != 'truncate'"                   # no row data: never dead-letters
topic = "audit"
```

- **The first matching route owns the event.** If its `where` drops the event, or fails on it, the event does not
  fall through to a later route or to `topic`.
- **Operations** are filtered in `where` through `op`: `op in ['insert', 'update']` keeps inserts and updates,
  `op != 'read'` drops snapshot rows. `op` is one of `insert`, `update`, `delete`, `truncate` and `read` (snapshot
  rows), in lower case; any other value fails startup. There is no separate `ops` key: a route that sets one fails
  startup with the `where` that does the same, such as `where = "op in ['insert', 'update']"`, or
  `where = "(<your where>) and op in [...]"` when the route already has a `where`.
- **`where`** is a predicate. Only events it is definitely true for pass; false and unknown are filtered. A syntax
  error fails startup with a message naming the route.
  - Fields: `op` (`insert`, `update`, `delete`, `truncate`, `read`), `schema`, `table` (all three strings, so they
    compare only with string literals; `op == 1` fails startup), `before.<column>`,
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
  short-circuit, so `not (...)` cannot let a mistyped row through. That includes an `op` test next to it: under
  `op == 'insert' and row.amount > 10`, a delete whose `amount` is `'abc'` goes to the dead-letter topic, not to
  `filtered`. The event is produced to the route's
  [dead-letter topic](#dead-letter-topics) instead (`orders-paid.dlq` above): value = the event (the same envelope), key = the record key with `"key": null`
  (the table only, no row values), partitioned by that key, headers `op`, `lsn` and `aqueduct-error`. The
  `aqueduct-error` header names the route, the expression and the field with the type it should have, never the value,
  for example ``route `paid-orders`: where `row.amount > 10`: type error: `row.amount` is not a finite number``. The
  first one per route is logged as a warning. The batch is acknowledged as usual once the dead-letter record is
  written too, so one bad row never stops the pipeline or pins the replication slot.
- A `where` on `op`, `schema` and `table` alone cannot hit a type error, so it never dead-letters.
- **Check the dead-letter topic's write ACL before you enable a `where` on row data.** Startup can only check that the topic exists;
  write permission is first used by the first dead-letter record. If the broker refuses that write, the batch fails,
  nothing is acknowledged and the process exits. After a restart it reads the same row again and fails again, so it
  crash-loops, and the Postgres replication slot (or the MongoDB resume token) stays where it was, pinning WAL, until
  the topic is writable.
- **Unseen fields.** A misspelled field (`row.stauts`) is not an error: it reads as null and the route filters
  everything. Aqueduct counts, per route and row field path in `where` (`op`, `schema` and `table` are never
  missing), `key`, a topic template, a field operation or `outbox`, the rows that had the row the path reads (an
  insert has no `before`) but not the field. Once 1000 such rows have arrived and no row has had the field, it logs a
  warning once and reports the field in `unseen_fields` and `aqueduct_route_field_unseen`. A field seen once is never
  reported, so a column that only some rows have (a delete's before-image holds only the key) does not trigger it.

#### Dead-letter topics

Every route has a dead-letter topic: where its rows go that it cannot evaluate. That is a type error in `where`, an
outbox row without an `aggregate_id` or `payload`, a row lacking unchanged TOAST columns under
`unchanged = "dead_letter"`, a `rename` that collides with an existing column, or a value `hash` cannot hash. No
setting is required. By convention the dead-letter topic is the route's own topic plus `.dlq`:

| The route's topic | Its dead-letter topic |
|-------------------|-----------------------|
| static, `sim.assets.active` | `sim.assets.active.dlq` |
| `{schema}`/`{table}` template, `cdc.{schema}.{table}` | `cdc.{schema}.{table}.dlq`, rendered for the event (`cdc.public.orders.dlq`) and created on first use, like the topics of the template |
| a template with `{op}`, `cdc.{table}.{op}` | `cdc.{table}.dlq`: `{op}` is dropped with the separator next to it, so there is one dead-letter topic per table, not one per operation (a topic that is only `{op}` keeps it) |
| content-routed, `orders.{row.region}` with `topic_fallback = "orders.unknown"` | `orders.unknown.dlq`: the row that failed cannot render the template, so the fallback's name is used |
| `outbox` | `<outbox topic_fallback>.dlq`, or `<topic>.dlq` for a static outbox topic |
| the `default` route (`sink.topic`) | the same rules; it never dead-letters a row, as it has no `where` or field operations |

Two settings override it, a route's own first:

```toml
[sink]
brokers = ["kafka:9092"]
dead_letter_topic = "aqueduct.dead-letter"   # optional: every route without its own

[[sink.routes]]
match = "public.payments"
where = "row.amount > 0"
topic = "payments"
dead_letter_topic = "payments.errors"        # optional: this route only
```

Precedence: the route's `dead_letter_topic`, then `sink.dead_letter_topic`, then `<topic>.dlq`. Both overrides are
fixed names (no placeholders).

- **Startup checks.** A dead-letter topic must be a valid topic name (at most 249 characters, so a 246-character topic
  has no room for `.dlq`) and must not start with `_`. It must not be a route's `topic_fallback`, and no route's topic
  may be able to produce it: not a static topic or `sink.topic`, not a name `{schema}`/`{table}`/`{op}` could fill
  (`cdc.dlq` next to `cdc.{table}`; a schema or table name is taken as one name without dots), and not a name a row
  value could render. A `{table}` dead-letter template also may not overlap a content-routed template read as a glob.
  A default `<topic>.dlq` that breaks a rule fails startup naming the route, and you set `dead_letter_topic`; on a route
  that cannot dead-letter a row it is dropped instead (`null` in `/api/routes`).
- **`dlq` is a reserved row value.** `orders.{row.region}` read as a glob (`orders.*`) matches `orders.dlq` and
  `orders.unknown.dlq`, but a row value is one segment without dots and never the exact value `dlq` (that row goes to
  `topic_fallback`, and `topic_values` may not list it). So a content-routed template can never render a name ending in
  `.dlq` from a placeholder, also not with several placeholders (`a.{row.x}.{row.y}` with `y = "dlq"`), and a route's
  own `<topic_fallback>.dlq`, or the `.dlq` of a static topic beside it, is accepted. A template with a fixed `.dlq`
  suffix (`orders.{row.r}.dlq`) can render one, and is refused next to such a name.
- **Created when.** The fixed dead-letter topics of routes that can dead-letter a row (a `where` on row data, `outbox`,
  `unchanged = "dead_letter"`, `hash`, or `rename` without `include`) are opened at startup and created if missing
  (with `partitions` and `replication_factor`), so one that cannot be created or read fails startup; pre-create them
  with the retention you want. A rendered one (`cdc.public.orders.dlq`) is created on its first record, like any
  topic; if that fails, the batch fails and is retried, nothing is acknowledged. A rendered name Kafka would refuse
  (a schema starting with `_`) fails the batch the same way. `sink.dead_letter_topic` is only opened when a route uses
  it.
- **Visibility.** `/api/routes` gives each route's `dead_letter_topic` (the template for a `{table}` one; the
  top-level `dead_letter_topic` is the global override). The event inspector reads every dead-letter topic, and each
  route counts its rows under `errors`.

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
- **Tombstones.** With `tombstones = true`, or `format = "row"` and `deletes = "tombstone"` (see
  [Shaping the output](#shaping-the-output)), a delete's null-value record carries this key, and a compacted topic
  then forgets every record with it. The `key` columns must therefore be unique per row and present in the delete's
  before-image (the replica identity's columns, or all columns under `REPLICA IDENTITY FULL`). A delete with any
  `key` column null or missing gets no null-value record. Uniqueness cannot be checked: keying by a column several
  rows share makes their tombstones erase each other.

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
    Once any route reads a row field, no topic anywhere in the config (route topics, `sink.topic`, fallbacks,
    dead-letter topics) may start with `_` (Kafka's internal topics such as `__consumer_offsets`). Without content
    routing, such topics are accepted as before.
  - `topic_fallback` is required. It is a fixed topic name (no placeholders), opened (created if missing) at startup.
  - No fixed topic in the config may match the template read as a glob (`orders.{row.region}` is `orders.*`): not
    another route's `topic`, `sink.topic` or another route's `topic_fallback`. Only the route's own fallback may
    (`orders.unknown` above). Dead-letter topics are checked by what a row can render instead, see
    [Dead-letter topics](#dead-letter-topics).
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
  The value `dlq` is reserved, so it goes to the fallback too (see [Dead-letter topics](#dead-letter-topics)).
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

#### Protecting fields per route

A route can remove, mask, hash and rename top-level columns before its records leave Aqueduct:

```toml
[sink]
hash_key_file = "/run/secrets/aqueduct-hash-key"   # or AQUEDUCT_HASH_KEY; never in this file
hash_key_id = "2026-10"                            # required with a key

[[sink.routes]]
match = "public.customers"
topic = "customers"
exclude = ["internal_note"]
mask = ["card_last4"]                    # "4242" -> "***"
hash = ["email"]                         # HMAC-SHA256, lowercase hex
rename = { cust_id = "customer_id" }
```

- **Where they apply.** `before` and `after` of every record the route produces, and of its dead-letter records.
  `source` and the headers are never rewritten. They run after `where`, `key` and `topic`, which read the
  original row.
- **`include`** keeps only the listed columns, **`exclude`** drops the listed ones; not both on one route. A listed
  column a row does not have is simply absent.
- **`mask`**: a string becomes `"***"`. Any other non-null value (number, bool, object, array) becomes `null`, so a
  column keeps its JSON type or goes null, and a number never turns into a string. `null` stays `null`. Since #138
  `timestamp` and `date` columns are numbers, so they mask to `null`; `timestamptz`, `numeric` and `uuid` are strings
  and mask to `"***"`.
- **`hash`**: HMAC-SHA256 with the hash key, as 64 lowercase hex characters (a JSON string). The input is the value's
  canonical text: the content of a string (unescaped), a number or bool exactly as the source wrote it (`42`,
  `true`), or the compact JSON of an object or array (whitespace removed, key order kept; a MongoDB `ObjectId` is
  `{"$oid":"..."}`). `null` stays `null`. So the same value hashes to the same digest in every table, topic and
  source, and consumers can join on it: a Postgres `int4 42`, a MongoDB `Int32`/`Int64` `42` and the string `"42"`
  all give one digest. **A MongoDB double is written `42.0` and hashes differently from the integer `42`**, so
  hash identifiers stored as integers or strings in MongoDB, not as doubles. Edge cases of the canonical text:
  - Only the top-level string is unescaped. Inside an object or array, escapes stay as the source wrote them, so
    `{"n":"\u00e9"}` and `{"n":"é"}` hash differently (each source writes its own values consistently).
  - Numbers are not normalised: `-0` and `0`, `1e2` and `100`, `42.0` and `42` all differ.
  - The text carries no type: `true` and the string `"true"` give the same digest, as do the object `{"a":1}` and
    the string `"{\"a\":1}"`, and the number `42` and the string `"42"`.
  - A string that is not valid Unicode (a lone surrogate escape such as `"\ud800"`) cannot be hashed: the event goes
    to the route's [dead-letter topic](#dead-letter-topics) with `before` and `after` set to `null`, and the header
    names the column.
- **`rename`** maps old column names to new ones. A target may not be another renamed column, another target, or a
  column listed in `include`. The other operations name the **original** columns: a target listed in `mask`, `hash`
  or `exclude` fails startup (`rename = { email = "x" }` with `hash = ["x"]` would hash nothing and pass the email
  through as `x`), so list the source column (`hash = ["email"]`) instead. Without `include` the columns are only known when rows arrive, so a row that already
  has a column named like a target goes to the route's dead-letter topic (value: the operations applied without the
  renames; header `aqueduct-error`: ``route `customers`: rename `cust_id` to `customer_id`: the row already has a
  column `customer_id` ``). With `include`, collisions are ruled out at startup.
- **Names** are top-level columns only and **case-sensitive**: `Email` does not match a column `email`, which matters
  for MongoDB, where field names are not folded. A misspelt or wrongly cased name is reported as an unseen field
  (below). A dotted path (`address.city`) fails startup: nested MongoDB fields cannot be
  addressed, but a whole nested value can be dropped, masked, hashed or renamed through its top-level column. Other
  startup errors: `include` together with a non-empty `exclude` (an empty `exclude` is no exclusion), an empty `include`, a column both masked and hashed, a column
  masked, hashed or renamed that `exclude` drops or `include` leaves out.
- **The record key.** Without `key = [...]`, the record key's `"key"` object holds the primary-key columns, so the
  route's operations are applied to it too: excluded or not-included columns are removed (an object left empty
  becomes `"key": null`), masked ones masked, hashed ones hashed with the same key as the value, renamed ones
  renamed. Under `partition_by = "key"` the partition is hashed from that rewritten key, so a hashed primary key
  still keeps each row on one partition. If every key column is removed, the table's rows share one partition.
  With `key = [...]`, a key column may not be excluded, masked, hashed or left out of `include` (startup error), so
  no protected value reaches the key; a renamed column may key the route and keeps its original name there.
- **The topic.** A content-routed `topic` (see [Routing by row content](#routing-by-row-content)) reads the original
  row, so it may not read a column the route excludes, masks, hashes or leaves out of `include` (any path into it:
  `row.region`, `after.region`, `row.address.city` for `address`); that fails startup. `topic_values` limits those
  same fields, and `topic_fallback` is a fixed name, so they are covered too. A record sent to the fallback, at render
  time or after the broker refused to create a topic, has the same rewritten value and key and the same
  `aqueduct-hash-key-id` header as any other.
- **Hash key.** Read from the `AQUEDUCT_HASH_KEY` environment variable or the file `sink.hash_key_file`, never from
  the TOML. One trailing newline (`\n` or `\r\n`) is ignored in both, so the same secret gives the same digests from
  either: a `hash_key` key in the config fails startup. Setting both also fails.
  The key must be at least 32 bytes (`openssl rand -hex 32` gives 64); it is required as soon as a route uses `hash`,
  redacted from debug output and never logged. **`hash_key_id`** (1 to 64 of `A-Z a-z 0-9 . _ : -`) is required with
  a key, fails startup without one, and is sent as the header `aqueduct-hash-key-id` on every record of a route that hashes, dead-letter records
  included. It is shown in `/api/routes`; the key never is.
- **Rotating the key.** A new key changes every digest. Generate a new key, give it a new `hash_key_id`, and restart.
  Consumers join or deduplicate per `aqueduct-hash-key-id`: digests under different ids are not comparable. Records
  replayed after a restart (at-least-once) are hashed with the key in use when they are produced.
- **Unseen fields.** Every column the operations name is watched like the paths of `where` and `key` (as
  `row.<column>`), on routes with or without `where`, so a misspelled `mask = ["card_last_4"]` shows up as an unseen
  field instead of silently letting `card_last4` through.
- **Cost.** A route without field operations takes the unchanged path. With them, each row is rewritten in one pass
  over its top-level columns; hashing costs one HMAC per hashed non-null value.

#### Outbox

`outbox` on a route publishes the rows of an outbox table as their own events, much like Debezium's outbox event
router, with the differences listed under "Differences from Debezium" below. The application writes an event into the outbox table in the same transaction as its business change. Aqueduct turns
each **insert** into one Kafka record whose key is the aggregate id and whose value is the payload:

```toml
[[sink.routes]]
match = "public.outbox"
outbox = { aggregate_type = "aggregatetype", aggregate_id = "aggregateid", payload = "payload", topic = "outbox.{aggregate_type}", id = "id", type = "type" }
topic_fallback = "outbox.unknown"            # required: the topic reads a row value
topic_values = ["order", "customer"]         # optional allow-list, as for any content-routed topic
```

| Key | Default | Description |
|-----|---------|-------------|
| `aggregate_type` | `aggregatetype` | Column that `{aggregate_type}` in `topic` reads |
| `aggregate_id` | `aggregateid` | Column whose value is the record key |
| `payload` | `payload` | Column whose value is the record value |
| `topic` | `outbox.event.{aggregate_type}` | Topic template over the outbox columns |
| `id` | unset | Column sent as the `id` header |
| `type` | unset | Column sent as the `type` header |

The defaults are Debezium's column names and topic, so `outbox = {}` reads a Debezium-style outbox table. The `id`
and `type` headers are opt-in: set `id` and `type` to the columns. Unknown keys inside `outbox` fail startup, so a
misspelled key never falls back to a default column.

- **Inserts, and snapshot rows only on request.** Updates, deletes and truncates of the outbox table never produce a
  record (whatever `where` says) and count as `filtered`, so deleting outbox rows once written is safe. Without a
  `where`, a route publishes inserts only. Snapshot rows (`read`) are published only when the route has a `where`
  that is true for them, for example `where = "op in ['insert', 'read']"`; they are then published like inserts,
  with the `op` header `r`. A `where` on the row applies to inserts and snapshot rows alike, so
  `where = "row.aggregatetype == 'order'"` publishes the matching snapshot rows too; add `and op == 'insert'` to
  keep them out. Publish snapshot rows to send rows that were already in the outbox table before Aqueduct streamed
  it (a first deployment with `snapshot = "initial"`). The trade-off: every snapshot of the table publishes every
  row still in it, including rows already published when they were inserted, so consumers see those events twice.
  Delete outbox rows once written, or snapshot the table only once, to keep that small.
- **Startup refusals.** A `where` that reads `before.<column>` (inserts and snapshot rows have no before-image, so
  it could never be true) fails startup naming the route.
- **Key: the raw `aggregate_id` value**, not the JSON record key other routes use. A string is sent as its content
  (unescaped UTF-8: `A-1`, not `"A-1"`), a number or boolean as its JSON text (`42`), an object (a MongoDB `ObjectId`)
  as its JSON (`{"$oid":"..."}`). The partition is the FNV-1a hash of that key, whatever `partition_by` says, so the
  events of one aggregate stay in order on one partition. Consumers of an outbox expect this key; code that parses
  the JSON keys of other topics cannot read outbox topics unchanged.
- **Value: the `payload` value, passed through.** A `json` or `jsonb` payload is its JSON text as stored. A text
  payload is sent as its content, not as a JSON string literal: `say "hi"`, not `"say \"hi\""`. The value is not the
  change-event envelope: it has no `source`, `op` or timestamps. **Use a `jsonb` object or array, or `text`, as the
  payload**; the other cases differ from Debezium (below).
- **Headers:** `op` (`c`, or `r` for a snapshot row) and `lsn`, as on every record, plus `id` and `type` from the configured columns, with the
  same text rules as the key. A null or missing `id` or `type` leaves that header out. These two headers carry row
  values; no other header does.
- **Topic.** The template may use `{aggregate_type}`, `{schema}`, `{table}`, `{after.<column>}` and `{row.<column>}`
  (both read the inserted row). `{before.<column>}` fails startup, since an insert has no before-image; a syntax
  error names its byte position in the template as written. A template that
  reads a row value follows every rule of [Routing by row content](#routing-by-row-content): fixed text first,
  `topic_fallback` required, one segment per value, `topic_values`, no collision with fixed topics,
  `max_dynamic_topics`. A null or invalid `aggregate_type` sends the record (same key, value and headers) to the
  fallback. A fixed `topic` such as `outbox.events` needs no fallback.
- **Rows that cannot be published go to the dead-letter topic** (`<topic_fallback>.dlq` by default, see
  [Dead-letter topics](#dead-letter-topics)): a null or missing `aggregate_id` or `payload`, or a
  string in the key, payload or a header column that is not valid Unicode (a lone `\ud800` escape). The dead-letter
  record is the full change-event envelope of the row, the payload included (an outbox route has no field
  operations to apply), keyed by the table only, with an `aqueduct-error` header that names
  the column, never its value, for example ``route `outbox`: outbox `aggregate_id` column `aggregateid` is missing or
  null``. It counts under `errors`, and the pipeline goes on.
- **Exclusive keys.** `outbox` sets the topic, the key and the value, so a route with `outbox` may not also set `topic`,
  `key` or any field operation (`include`, `exclude`, `mask`, `hash`, `rename`, see
  [Protecting fields per route](#protecting-fields-per-route)); startup fails naming the route and the keys. Shape the
  payload in the application instead. Column names must be top-level (no dots).
- **Unseen fields.** The outbox columns are watched like any other field path, as `after.<column>`: a misspelled
  column is reported after 1000 inserts that lack it.
- The cockpit's route card shows the `payload` column under `SELECT` (tagged `outbox`) and the `aggregate_id` column
  under `PARTITIONED-BY`. The event inspector shows outbox records as raw values with their
  headers and their topic in place of the table, since they are not envelopes.
- **Differences from Debezium's outbox event router.** Aqueduct reads the payload from the row's JSON, which does not
  record the column type, so:
  - A `jsonb` payload that is a bare JSON string (`'"hello"'::jsonb`) is sent without its quotes (`hello`); Debezium
    keeps the JSON text (`"hello"`).
  - A `text` payload that holds JSON text (`{"a":1}`) is sent as that text, so a JSON consumer reads an object;
    Debezium (with the JSON converter) delivers a JSON string.
  - A `bytea` payload arrives as Postgres hex text (`\x4869`), not as the raw bytes or base64.
  - Debezium publishes snapshot rows like inserts and only warns on updates; Aqueduct publishes snapshot rows only
    when the route's `where` is true for them and always drops updates.
  - The `id` and `type` headers are opt-in.

  Use a `jsonb` object or array as the payload (the same JSON Debezium sends), or `text` (sent as plain text). Avoid
  `jsonb` scalars and `bytea`.

#### Shaping the output

By default a record's value is the change-event envelope (`before`, `after`, `source`, `op`, ...). Four route keys
change that, after the route's filter, key, topic and field operations:

```toml
[[sink.routes]]
match = "public.customers"
topic = "customers"
format = "row"                # value = the row itself
deletes = "tombstone"         # row format: tombstone (default) | drop | row
unchanged = "dead_letter"     # omit (default) | dead_letter

[[sink.routes]]
match = "public.*"
topic = "cdc.{table}"
tombstones = true             # envelope format: a null-value record after every delete
```

| Key | Default | Meaning |
|-----|---------|---------|
| `format` | `envelope` | `row`: the value is `after` (inserts, updates and snapshot reads) after field operations, with no envelope. Truncates have no row: they produce nothing and count as `filtered`. The `op` and `lsn` headers stay, so consumers that need them can still read them |
| `deletes` | `tombstone` | Only with `format = "row"`. `tombstone`: the delete is a record with a null value and the row's key, which a compacted topic reads as "remove this key". `drop`: no record, counted as `filtered`. `row`: the before-image (after field operations) with `"__deleted": true` added as its last member; a column of the row named `__deleted` is replaced. Under the default replica identity the before-image holds only the primary key |
| `tombstones` | `false` | Only with the envelope format (with `format = "row"`, `deletes = "tombstone"` already is the tombstone). After every delete, also a null-value record with the same key, on the same partition, right after the delete. The key is the delete's record key after field operations, so a hashed key column stays hashed |
| `unchanged` | `omit` | What happens to a row the source sent without some columns (see TOAST below). `omit`: produce it as it is. `dead_letter`: send it to the route's [dead-letter topic](#dead-letter-topics) instead (the envelope, with the route's field operations; the `aqueduct-error` header names the columns, never values, with control characters and backticks shown as `?`), counted under `errors`. Columns the route excludes or leaves out of `include` do not count, since the route would not send them anyway. Allowed on any route |

Rules, checked at startup:

- `deletes` without `format = "row"`, and `tombstones` with it, fail startup; so do unknown values (case does not
  matter).
- **Null values need a per-row key.** A record with a null value (an envelope tombstone, or a row-format delete
  under `deletes = "tombstone"`) tells a compacted topic to forget every record with that key. So a route that
  produces one needs `key = [...]`, or `partition_by = "key"` (the default): with `partition_by = "table"` and no
  route `key`, a route with `tombstones = true`, or with `format = "row"` and `deletes = "tombstone"` (also when
  `deletes` is unset), fails startup. Use `deletes = "drop"` or `"row"` there instead.
- **Each delete is checked too: no null value under a key other rows share.** A delete's key does not name one row
  when:
  - the table has no primary key (no `REPLICA IDENTITY` key, or `FULL` without a primary key): the record key's
    `key` is null and would stand for the whole table;
  - a route `key = [...]` column is null or missing in the before-image (under the default replica identity the
    before-image holds only the primary key);
  - the route's field operations exclude, leave out of `include` or mask any primary-key column (a masked key is
    `"***"` for every row; a composite key missing a column is shared). Hashing (HMAC) and renaming keep the key
    unique.

  Then an envelope delete is produced without its tombstone, and a row-format `deletes = "tombstone"` delete produces
  nothing (counted as `filtered`). Either way a warning is logged once per route and `tombstones_skipped` counts it
  (see the counters below). Give tables behind a compacted topic a primary key, and key routes by columns that are
  unique and present in the delete's before-image. Uniqueness of `key = [...]` columns cannot be checked: listing a
  column that several rows share makes their tombstones erase each other.
- `outbox` is exclusive with all four keys: an outbox record is the payload as is.
- A MongoDB update whose document was deleted before the change stream looked it up has no `fullDocument`. The
  envelope keeps its `documentKey` in `after`, as before; a row-format route does not take that key for the document:
  it produces nothing (counted as `filtered`, logged once per route), and the delete that follows does.

**TOAST and compaction.** Postgres does not send a large (TOASTed) column that an update left unchanged, so `after`
lacks that column (the decoder leaves it out rather than guess). In the envelope a consumer can see the column is
missing; in `format = "row"` an update can lack a large column, and **a compacted consumer that replaces whole rows
loses it**. A `where` on that column reads `null`. Either set `unchanged = "dead_letter"` to keep such rows out of the
topic (and handle them from the dead-letter topic), or make the consumer merge updates into the row it has. MongoDB
sends whole documents and never has this case. `REPLICA IDENTITY FULL` does not help yet: filling the column from the
before-image is a follow-up.

The cockpit's route card shows `format = "row"` as `SELECT ROW ...` and `deletes`, `tombstones` and `unchanged` as
tags on that row, and the event inspector shows
row-format values raw and tombstones as `tombstone`, with their `op` and `lsn` headers.

Per-route counters: every event a route claims counts as `matched`, then as exactly one of `produced`, `filtered`,
`fallback` (sent to `topic_fallback` by content routing) or `errors`. They appear in `/api/metrics` (`routes`),
`/api/routes` and Prometheus:

| Series | Meaning |
|--------|---------|
| `aqueduct_route_records_total{route, outcome}` | Counter. `outcome` is `produced`, `filtered`, `fallback` or `errors`. `matched` is not exported; it is the sum over the outcomes |
| `aqueduct_route_field_unseen{route, field}` | Gauge, `1` for each field path reported as unseen (absent otherwise) |
| `aqueduct_route_tombstones_skipped_total{route}` | Counter. Deletes a `tombstones = true` route produced without a tombstone because the row has no key. Not an outcome: the delete itself counts as `produced` |
| `aqueduct_topic_records_total{topic}` | Counter. Records Kafka acknowledged per topic, counted after each produce succeeds (a failed produce counts when its batch is retried). Tombstones (null-value records) count like any other record. The first 500 topics are counted by name, any further ones under `topic="(other)"` (logged once) |

`route` is the route's `name`, else its `match`; `default` is reserved and cannot be used as a name. Events no route
claims are counted under `default` when `sink.topic` is set; without `sink.topic` such an event is an error that stops
the pipeline. Route labels must be unique: two routes with the same `name`, a `name` equal to another route's `match`,
or two unnamed routes with the same `match` (the second could never match) fail startup.

## 4. Admin cockpit

Open `http://host:8080/` (or `/#token=<admin_token>` when a token is set; the fragment is never sent to the server). It shows whether Aqueduct is behind its source
(backlog, rates and time to catch up), throughput, totals, lag, sink latency, errors, the
routing graph, per-table high-water marks and an event inspector. The inspector is a pop-up window that opens only from
a topic's detail panel in the routing graph (**Open in inspector**), filtered to that topic; it reads nothing from
Kafka while closed, and Close, Esc or a click outside closes it.
Routes: `/` and `/admin.css` (page), `/healthz` (liveness), `/readyz` (`200` only while streaming, else `503`), `/metrics` (Prometheus), `/api/metrics`, `/api/stream` (SSE), `/api/events?topic=<t>&limit=<n>` (read from Kafka), `/api/tables` (per table high-water marks), `/api/routes` (the route plan with per-route counts), `/api/snapshot` (`GET`/`POST`/`DELETE`, see [Snapshots](#snapshots)). Only `/api/snapshot` accepts methods other than `GET`.

Open `http://host:8080/` (or `/#token=<admin_token>` when a token is set; the fragment is never sent to the server). It shows throughput, totals, lag,
sink latency, errors, the routing graph, per-table high-water marks and an event inspector.
Routes: `/` and `/admin.css` (page), `/healthz` (liveness), `/readyz` (`200` only while streaming, else `503`), `/metrics` (Prometheus), `/api/metrics`, `/api/stream` (SSE), `/api/events?topic=<t>&limit=<n>` (read from Kafka), `/api/tables` (per table high-water marks), `/api/routes` (the route plan with per-route counts), `/api/routes/history` (per-route and per-topic rates, also per operation, over the last 120 s), `/api/snapshot` (`GET`/`POST`/`DELETE`, see [Snapshots](#snapshots)). Only `/api/snapshot` accepts methods other than `GET`.

The inspector shows row data. `admin_token` is required on a non-loopback bind (see `admin_insecure_no_auth`); also publish the port only on a trusted network or behind a reverse proxy
with TLS and authentication, or set `inspector = false`.

The inspector adds no work to the pipeline: nothing is buffered in memory. It shows a sample, not a feed: the newest
100 events, read once when it opens, when its topic filter changes and on **Resample**. Each sample (shared across tabs
for 2 s) makes a separate Kafka read of up to `limit` records per partition from the topics the sink
routes to, using its own connection (created on first use). A closed tab costs nothing.

### Behind source

The **Behind source** card, below the top tiles, answers "is Aqueduct behind its source, and when does it catch up?".
It compares two positions the source reports:

| | Postgres | MongoDB |
|---|---|---|
| Head | The server's WAL head: `pg_current_wal_lsn()` (`pg_last_wal_replay_lsn()` on a standby), read once a second with the slot stats by a task of its own on the SQL connection; with `transport = "stream"` also the WAL end of each keepalive, whichever is newer | The newest majority-committed write, `lastWrite.majorityOpTime` from `hello`, run once a second by a task of its own (falls back to `lastWrite.opTime`, then the reply's `operationTime`) |
| Delivered | The change LSN of the newest acked event, the commit end the slot was advanced to (also by idle heartbeats), and the WAL flush position (`pg_current_wal_flush_lsn()`, read with the slot stats) as of before a peek that drained the slot, or the WAL end of a keepalive that ended a stream fetch, once everything was handed on | The cluster time of the newest acked event's resume token, and of the postBatchResumeToken of an empty change-stream batch once everything was handed on |

The head is read independently of the pipeline loop, so it keeps moving while the sink holds the pipeline up (a Kafka
outage, up to the 60 s sink timeout). On Postgres the stats query shares the SQL connection, so it waits behind a peek
in flight.

Once a second the admin server takes a sample of both; the card is derived from the last 60 samples (one minute):

- **Status.** `caught up` (green) when the backlog is within the threshold: **1 MiB of WAL** for Postgres, **2 s of
  cluster time** for MongoDB. It is never exactly 0: heartbeat writes, `pg_current_wal_lsn()` running ahead of the
  delivered commit end, and the batch in flight leave a little, and MongoDB times have one-second resolution with an
  idle replica set moving its clock only with the 10 s no-op writer. `catching up` (blue) when the delivered position
  moves faster than the head over the window; `falling behind` (amber) when it does not; `stalled` (red) when there is
  a backlog over the threshold and nothing was delivered for **30 s** of it while the pipeline is streaming (quiet
  time before the backlog appeared does not count). `unknown` (grey) until the source has reported both positions
  (Postgres: the head within a second of start, delivered at the first ack or drained poll; MongoDB: the head within a
  second of start, delivered at the first ack or empty change-stream batch), while the head was last read more than
  **3 s** ago (the card then says how long), and while a backlog has less than 2 s of samples to tell its direction.
- **Backlog.** Postgres: WAL bytes between head and delivered, and the time behind: while there is a backlog, now
  minus the later of the newest delivered change's commit time and the moment the backlog crossed the threshold (when
  that happened inside the window), so a burst after a quiet hour reads seconds, not an hour; 0 when caught up.
  MongoDB: the head's cluster time minus the newest delivered one. A sparkline shows the backlog over the minute (at
  most 60 values).
- **Source vs delivered.** How fast the head and the delivered position move over the window: WAL bytes per second,
  or for MongoDB seconds of cluster time per second (about 1.0 for the source; above 1.0 delivered means catching
  up). Rates need 2 s of samples.
- **Time to catch up.** Backlog divided by (delivered rate minus source rate), only while the backlog shrinks; 0 when
  caught up.
- **Snapshot row**, while a snapshot runs: rows per second over the minute, rows left (the planner estimate of all its
  tables minus the rows this process copied, so after a restart it overstates), the ETA (*estimate exceeded* when
  rows left reaches 0 while the snapshot still runs), and a *throttled by
  snapshot_max_rows_per_sec* chip when the rate is at 90 % or more of that limit. The live stream's status sits next
  to it, so a snapshot that makes the stream fall behind shows.

The **End-to-end lag** tile reads `idle` when the status is caught up and nothing was delivered for 10 s, instead of
counting the idle time up. The Kafka HWM in a table's detail panel is sink-side (what reached Kafka); it does not say whether
Aqueduct is behind the source.

`/api/metrics` and `/api/stream` carry the card as `source_lag`, plus `source_head_lsn` (Postgres, `X/Y`) and
`source_head_time_ms` (MongoDB):

```json
"source_lag": {"state": "catching_up", "backlog_bytes": 10485760, "behind_secs": 30.0,
  "ingest_rate": 1048576.0, "process_rate": 3145728.0, "rate_unit": "bytes/s", "eta_secs": 5.0,
  "window_secs": 60.0, "head_age_secs": 0.4, "threshold": {"backlog_bytes": 1048576, "behind_secs": null, "stalled_after_secs": 30},
  "history": [10485760, ...],
  "snapshot": {"rows_per_sec": 480.0, "rows_left": 95200, "rows_left_scope": "all_tables", "eta_secs": 198.3,
               "throttled": true, "max_rows_per_sec": 500}}
```

`state` is `unknown`, `caught_up`, `catching_up`, `falling_behind` or `stalled`. `head_age_secs` is how long ago the
head was read; past 3 s the state is `unknown` and the numbers are `null`. `backlog_bytes` is `null` for
MongoDB; the rates are `null` for the first 2 s; `eta_secs` is `null` unless the backlog shrinks; `snapshot` is
`null` unless one runs. Prometheus has `aqueduct_source_backlog_bytes`, `aqueduct_source_behind_seconds`,
`aqueduct_source_catch_up_eta_seconds`, `aqueduct_snapshot_rows_per_second` and `aqueduct_snapshot_eta_seconds`, each
left out while unknown. Like the rest of `/api/*` and `/metrics` they need the admin token; none of them carry row
data. Samples, rates and the window start over when the process restarts.

`/api/events` returns each record's `topic`, `partition`, `offset`, `timestamp_ms`, `key`, `headers` (as text) and
`event`: the value parsed as JSON, or its text when it is not JSON, and `null` for a tombstone. A value that is not a
change-event envelope (an outbox payload, a `format = "row"` row, a tombstone) is shown raw, with its op and lsn taken
from the headers; the table of a row or tombstone comes from its JSON record key.

### Per-table high-water marks

Click a table (or collection) in the routing graph: its detail panel shows how far it has got on the source side
since the process started (nothing Kafka-side):

| Field | Source |
|--------|--------|
| Events, ops | Events delivered by this process, per operation, and the table's share of all events |
| Last LSN | LSN of the newest delivered change (`–` for MongoDB, whose resume tokens are not shown per table) |
| Last commit | How long ago that change committed. Amber when the table is more than 1 minute behind the freshest table, red past 10 minutes; only while the pipeline is streaming. Measured against the freshest table rather than the clock, so a database that is idle as a whole is not flagged; a table that is simply written rarely is |
| Delivered | How long ago the sink acknowledged it |

The panel also shows the last checkpointed position, which is global, not per table. While a snapshot runs it is the
stream position followed by `(snapshot: <table> <i>/<n>)`; the snapshot cursor (the last chunk's primary-key values)
stays in the checkpoint file and is never shown. Pipeline numbers refresh every second
with the stream.

The Kafka side is in a topic's detail panel (click the topic): its **Kafka HWM**, the sum over the topic's partitions
of the next offset Kafka will assign, with each partition's offset, and when the offsets were read. Kafka offsets come
from `GET /api/tables`, polled every 5 s: one offsets request per partition of
the routed topics (no records are read), with the inspector's connection and TLS/SASL settings, shared across tabs
for 5 s and abandoned after 3 s, so a Kafka outage leaves the offsets empty (the topic panel shows the reason) but never blocks the cockpit. Offsets need
the inspector (`admin_token` and `inspector = true`); without it the panel says so. The routing graph also takes each
table's topic from `/api/tables`.

Events and Delivered count every event the source delivered for the table, including events a route filtered
(`where`) or sent to the dead-letter topic. A table whose events are all filtered therefore shows events and
recent deliveries while its topic's Kafka HWM does not move; the per-route counts in `/api/routes` show why.
A table whose route picks the topic per event (`{op}` or a row field in `topic`) has no single topic to read
offsets from: in `/api/tables` its topic is the template and `kafka` is null with `content-routed (see Routing)`.

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
  included), the same rule the sink uses. Each route card has the same five rows, keyword on the left, in this order;
  a clause the route leaves unset shows its effective default, dimmed:

  | Row | Shows | Default |
  |-----|-------|---------|
  | `SELECT` | What the record value holds: `include` as a column list, `exclude` as `* EXCEPT (a, b)`, then `mask(col)`, `hash(col)` and `col AS new`. `format = "row"` adds `ROW` (`SELECT ROW *`); the delete mode, `tombstones` and `unchanged` are small tags after it. An outbox route shows its `payload` column | `*`, the change-event envelope |
  | `WHERE` | The `where` text, operation filter included. Long text is cut; hover for it all, or click the route | `true` |
  | `FROM` | The `match` glob, then the tables it claimed, ` / `-separated, the first three and `+N` | `* (unmatched)` on `default` |
  | `INTO` | The topic template; with content routing, `ELSE` the `topic_fallback` and a tag with the number of `topic_values` (hover for them). The dead-letter topic is in the route's panel, not on the card | `sink.topic` on `default` |
  | `PARTITIONED-BY` | `key (a, b)` for a route `key`; the `aggregate_id` column for an outbox | `primary key` (`partition_by = "key"`) or `table` |

  The step number and route name are on top, above the rows. Below them, four metrics, each in the colour of its line
  in the card's step chart: **processed** (blue, every event the route took), **matched** (green, processed minus
  filtered: the `where` held; a row the `where` could not evaluate also counts here, and under errors), **fallback**
  (violet, sent to `topic_fallback`) and **errors** (red, sent to the dead-letter topic). Produced is in the panel. An amber **never seen** box lists fields its `where` reads that no row has had after enough rows
  (`aqueduct_route_field_unseen`); that is usually a misspelt column, and the route then filters everything.
- **Topics** (right): only real topics, never a template: the topics a template rendered for the tables seen, and
  under **dead letter**, in red, one card per distinct dead-letter topic that has received events (a
  `{schema}`/`{table}` dead-letter template shows the dead-letter topics it rendered), with the errors of the routes
  that use it; a dead-letter topic with no events is not drawn. A route whose template has not rendered a topic yet has no topic edge. The `default` route
  (`sink.topic`) is left out until an event reaches it. Beyond
  six, the quietest ones are behind "+N more". A template that reads the event (`{op}`, `{row.x}`) lists the topics the
  sink has produced to since start (not its dead-letter topics), and its `topic_fallback`.

A line's width follows its count (table events in, the route's produced count out, errors to the dead-letter
topic); lines with traffic in the last 3 s carry moving dots, which stand still when the browser asks for reduced
motion. The line from a route to its dead-letter topic is solid red (only the moving dots are striped) and appears once
that topic has events. A route that
picks its topic per row (content routing) animates its lines to the topics it writes whenever it produces. Hover a
table to highlight its path. The plan is read once from `GET /api/routes` when the page loads; counts update every
second from the stream. Without any `[[sink.routes]]` the graph is all tables → `default` → their topics.

**Operation shares and rate.** Each table and topic card has a thin stacked bar of its counts per operation since
start, as shares (insert green, update blue, delete red, truncate violet, read amber; hover it for the counts and
percentages); every card, routes included, has a step line of its rate over the last 120 s (a route: one line, processed;
its detail chart has four, processed, matched, fallback and errors). The bar counts:

- a table: events the source delivered for it (`table_ops` in `/api/metrics` and every stream frame);
- a topic (dead-letter topics have no bar, only their step line): records Kafka acknowledged, counted by the sink after the broker acknowledged
  the produce, so a failed produce never counts and a retried batch counts when it succeeds. A tombstone carries its
  delete's `op` header and counts as a delete. Topic cards also show the acknowledged count;
- a "+N more" group: the sum over its tables or topics.

Table, route and topic cards keep their counts (events, the route's produced/filtered/fallback/errors, acknowledged
records, a dead-letter topic's errors).

**Rates.** Click a table, a route or a topic for a panel with one step line over the last 120 s, one sample per
second (nothing animates), with its current, average and maximum rate: a table's events, a route's produced events or
a topic's acknowledged records per second. A table's and a topic's chart draw one line per operation, in the op colours, for the operations toggled on with the
buttons above it; only operations that have happened for that table or topic get a button (insert, update, delete,
truncate, read); the selection is shared by the two. A route's chart has no
buttons: four lines, processed, matched, fallback and errors, from the route's own counters. A table's panel names the route that claims it. A route's panel shows only what its card does not: the matched count, its produced events per
operation, the full `topic_fallback` and `topic_values`, the topics a template wrote, its dead-letter topic (click to
select it), the full table list when the card's `FROM` is cut short, and any other config keys.  A topic's panel lists its acknowledged records per
operation, the routes that write to it (by template or as their `topic_fallback`), **dead letter**: the dead-letter
topic of each of those routes that can dead-letter a row (a `{table}` template filled in for this topic,
`cdc.public.orders.dlq`; click one to select it in the graph), and an **Open in inspector** button (shown when the
inspector is on). A dead-letter topic's panel works the same (its line and per-op counts sum every topic a `{table}`
template rendered) and shows **dead letter for**, the routes it serves and their topics; a `{table}` template's panel
lists the dead-letter topics it has rendered, each opening the inspector.

The history is kept by the server, so the charts are full when the page opens: `GET /api/routes/history` returns
the window, and each stream frame carries only the newest second (`rates`: `seq`, every route's outcomes in plan
order as `routes` and its produced records per op as `route_ops`, the topics with records in that second as `topics`
and per op as `topic_ops`). Per-op rows are `[insert, update, delete, truncate, read]`. The window adds `ops` per route
(produced per op and second), `topic_ops` (acknowledged per op and second) and `topic_totals` (acknowledged per op
since start, up to `seq`). It needs the admin token like every `/api/*` route. Memory is bounded:
routes are fixed at startup, and the sink counts at most 500 topics by name and the rest as `(other)`. Topic names can
contain row values under content routing; they are shown only behind the token, as in the graph. Rates start from
zero when the process restarts.

```json
{"seq": 431, "interval_ms": 1000, "len": 120,
 "routes": [{"route": "paid-orders", "produced": [3, 0, 5], "filtered": [1, 0, 2], "fallback": [0, 0, 0], "errors": [0, 0, 0],
             "ops": [[2, 1, 0, 0, 0], [0, 0, 0, 0, 0], [3, 0, 2, 0, 0]]}],
 "topics": {"orders-paid": [3, 0, 7], "orders-paid.dlq": [0, 0, 0]},
 "topic_ops": {"orders-paid": [[2, 1, 0, 0, 0], [0, 0, 0, 0, 0], [3, 0, 4, 0, 0]],
               "orders-paid.dlq": [[0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]]},
 "topic_totals": {"orders-paid": {"insert": 610, "update": 95, "delete": 40, "truncate": 0, "read": 0},
                  "orders-paid.dlq": {"insert": 1, "update": 0, "delete": 0, "truncate": 0, "read": 0}}}
```

### Routing plan

`GET /api/routes` describes the compiled routing for the cockpit's routing graph: each route in match order, then
the `sink.topic` fallback as a route named `default` with `"match": null`, with live counts. It never contains row data.
Later routing options add keys to each route, so readers should ignore keys they do not know. A route with field
operations also has `include`, `exclude`, `mask` and `hash` (column lists) and `rename` (the map), each left out when
unset; with a hash key the top level has `hash_key_id`. The hash key itself is never shown. An outbox route has an
`outbox` object with its column names and topic template (`aggregate_type`, `aggregate_id`, `payload`, `topic`, and
`id` and `type` when set); other routes leave the key out. A route with `format = "row"` has `"format": "row"` and
its `deletes` (`tombstone`, `drop` or `row`, the default included), a route with `tombstones = true` has
`"tombstones": true`, and a route with `unchanged = "dead_letter"` has `"unchanged": "dead_letter"`; each is left out
otherwise. `counts` also has `tombstones_skipped`, which is not an outcome, and `ops`: the produced events per
operation (`insert`, `update`, `delete`, `truncate`, `read`; they sum to `produced`, and a delete's tombstone is not
counted again). Every route has its resolved
`dead_letter_topic` (a `{schema}`/`{table}` template for one rendered per event, `null` only if the default name is
unusable on a route that cannot dead-letter); the top-level `dead_letter_topic` is `sink.dead_letter_topic`, `null`
when unset.

```json
{"default_topic": "cdc.{schema}.{table}", "dead_letter_topic": null, "partition_by": "key",
 "routes": [
   {"route": "paid-orders", "match": "public.orders",
    "where": "op in ['insert', 'update'] and row.status == 'paid' or op == 'delete'", "key": ["order_id"], "topic": "orders-paid",
    "topic_fallback": null, "topic_values": null, "dead_letter_topic": "orders-paid.dlq",
    "counts": {"matched": 120, "produced": 80, "filtered": 39, "fallback": 0, "errors": 1, "tombstones_skipped": 0,
               "ops": {"insert": 50, "update": 20, "delete": 10, "truncate": 0, "read": 0}},
    "unseen_fields": []},
   {"route": "regional", "match": "public.customers", "where": null, "key": null,
    "topic": "customers.{row.region}", "topic_fallback": "customers.unknown", "topic_values": ["eu", "us"],
    "format": "row", "deletes": "tombstone", "dead_letter_topic": "customers.unknown.dlq",
    "counts": {"matched": 300, "produced": 290, "filtered": 0, "fallback": 10, "errors": 0, "tombstones_skipped": 0,
               "ops": {"insert": 200, "update": 85, "delete": 5, "truncate": 0, "read": 0}},
    "unseen_fields": []},
   {"route": "default", "match": null, "where": null, "key": null, "topic": "cdc.{schema}.{table}",
    "topic_fallback": null, "topic_values": null, "dead_letter_topic": "cdc.{schema}.{table}.dlq",
    "counts": {"matched": 5000, "produced": 5000, "filtered": 0, "fallback": 0, "errors": 0, "tombstones_skipped": 0,
               "ops": {"insert": 3000, "update": 1500, "delete": 500, "truncate": 0, "read": 0}},
    "unseen_fields": []}]}
```

`/api/metrics` and `/api/stream` carry the same counts flat, as
`routes: [{"route", "matched", "produced", "filtered", "fallback", "errors", "tombstones_skipped", "ops", "unseen_fields"}]`.

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
| `slot_lag_bytes` | WAL between the server head and the slot's `confirmed_flush_lsn`, read every second. `null` when unknown (no slot yet, query failed, or a MongoDB source). |
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

### Azure managed identity

On Azure Database for PostgreSQL (Flexible Server) Aqueduct can sign in with a managed identity instead of a stored
password: the server accepts a Microsoft Entra access token as the password, so no static database secret is stored
anywhere. Aqueduct fetches a token, caches it until 5 minutes before the
expiry the identity endpoint reports (at most 24 hours), and uses a valid one as the password of **every** connection
it opens: the SQL session and, with `transport = "stream"`, the replication connection. Each start of the source,
including a restart after an error, uses a cached or fresh token. A failed token fetch fails that connection like a
wrong password would (a source error; the restart applies).

```toml
[postgres]
# The user is the Postgres role of the managed identity (its name). No password.
url = "postgres://aqueduct-mi@myserver.postgres.database.azure.com:5432/app"
auth = "azure-workload-identity"   # or "azure-container-apps"
# azure_client_id = "00000000-0000-0000-0000-000000000000"
```

Fail-closed rules, checked at startup:

- The token only travels over TLS with the certificate chain **and hostname** verified: `ssl_mode = "verify-full"`
  (the default), with the bundled public roots or an explicit `ssl_root_cert`. Every other mode is refused, including
  `verify-ca` and `require` with `ssl_root_cert`: without the hostname check, anyone holding a certificate from a
  trusted CA could intercept the bearer token. The `*.postgres.database.azure.com` certificates chain to public roots
  (DigiCert Global Root G2, Microsoft RSA Root Certificate Authority 2017) that the bundled set should contain, so the
  default `verify-full` should work without `ssl_root_cert`; this was not verified against a real server. If the
  handshake reports an unknown issuer, download Azure's root certificates and point `ssl_root_cert` at them.
- A URL that also carries a password is refused, and so is a URL without a user.
- A missing platform variable is refused with its name. Tokens, the federated assertion and `IDENTITY_HEADER` are
  never logged and are redacted from debug output; a token endpoint error shows the HTTP status and the OAuth `error`
  and `error_description` only.
- The token endpoint is reached over HTTPS with certificate and hostname checks. Plain `http://` is accepted only for
  a loopback host (the Container Apps identity endpoint): a name must resolve to loopback addresses only, and
  Aqueduct connects to those checked addresses.

The server asks for the token as a cleartext password. That is the same rule as for any password: Aqueduct sends it
only inside TLS, and the replication connection keeps the [ADR 0003 §6](adr/0003-streaming-replication.md) rules
(cleartext is never sent over plaintext to a remote host).

Nothing in the image changes: the token exchange is a small built-in HTTPS client, so the distroless image needs no
shell, `az` CLI or extra files.

#### AKS (Workload Identity)

`auth = "azure-workload-identity"`. The workload identity webhook injects `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
`AZURE_FEDERATED_TOKEN_FILE` and `AZURE_AUTHORITY_HOST`. Aqueduct re-reads the federated token file on every fetch
(the kubelet rotates it) and exchanges it at `{AZURE_AUTHORITY_HOST}{AZURE_TENANT_ID}/oauth2/v2.0/token` for a token
with scope `https://ossrdbms-aad.database.windows.net/.default`. `azure_client_id` overrides `AZURE_CLIENT_ID`.

1. Enable the OIDC issuer and workload identity on the cluster
   (`az aks update -g <rg> -n <cluster> --enable-oidc-issuer --enable-workload-identity`).
2. Create a user-assigned managed identity and a federated credential that trusts the service account:

   ```sh
   az identity create -g <rg> -n aqueduct-mi
   az identity federated-credential create -g <rg> --identity-name aqueduct-mi --name aqueduct \
     --issuer "$(az aks show -g <rg> -n <cluster> --query oidcIssuerProfile.issuerUrl -o tsv)" \
     --subject system:serviceaccount:<namespace>:aqueduct --audience api://AzureADTokenExchange
   ```

3. Annotate the service account with the identity's client ID and label the pod:

   ```yaml
   apiVersion: v1
   kind: ServiceAccount
   metadata:
     name: aqueduct
     namespace: <namespace>
     annotations:
       azure.workload.identity/client-id: "<client ID of aqueduct-mi>"
   ---
   # in the Deployment
   spec:
     template:
       metadata:
         labels:
           azure.workload.identity/use: "true"
       spec:
         serviceAccountName: aqueduct
   ```

#### Azure Container Apps

`auth = "azure-container-apps"`. Give the app a system-assigned identity (`az containerapp identity assign -g <rg>
-n <app> --system-assigned`) or a user-assigned one (`--user-assigned <identity resource ID>`, then set
`azure_client_id` to its client ID). Container Apps sets `IDENTITY_ENDPOINT` and `IDENTITY_HEADER`; Aqueduct calls
`$IDENTITY_ENDPOINT?api-version=2019-08-01&resource=https://ossrdbms-aad.database.windows.net` (plus `&client_id=`)
with the `X-IDENTITY-HEADER` header.

#### Postgres side

1. Turn on Microsoft Entra authentication on the server and set an Entra admin.
2. As the Entra admin (signed in with an Entra token, connected to the `postgres` database), create the role for the
   identity. The role name is the identity's name: for a user-assigned identity its resource name, for a
   system-assigned one the name of the app. It is also the user in Aqueduct's `url`.

   ```sql
   select * from pgaadauth_create_principal('aqueduct-mi', false, false);
   ```

3. Grant what Aqueduct needs. **`REPLICATION` must be granted to the Entra role itself**, as for a password role.
   (Granting `REPLICATION` to a role made by `pgaadauth_create_principal` with `ALTER ROLE` has not been checked
   against Microsoft's documentation or a real server; if it is refused, see Azure's logical replication docs for
   the supported way to grant it.)

   ```sql
   ALTER ROLE "aqueduct-mi" WITH REPLICATION;
   -- in the replicated database:
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO "aqueduct-mi";
   CREATE PUBLICATION aqueduct FOR TABLE ...;   -- or let its owner create it
   ```

   The role creates and advances its slot (`REPLICATION`) and reads the published tables (`SELECT`, also needed for
   snapshots); snapshots on Postgres 14+ also need `EXECUTE` on `pg_logical_emit_message`. Set the server parameter
   `wal_level = logical` as in [Prepare Postgres](#1-prepare-postgres).

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
| `permission denied to use replication slots` | The role needs `REPLICATION` (or superuser). With a managed identity: `ALTER ROLE "<identity name>" WITH REPLICATION` |
| `getting a Microsoft Entra token (...): ... HTTP 400; error: invalid_client` | AKS: often `AADSTS70021`, the federated credential's issuer or subject does not match the cluster's OIDC issuer and `system:serviceaccount:<namespace>:<name>` |
| `postgres auth = "azure-...": ... is not set` | AKS: the pod lacks the `azure.workload.identity/use: "true"` label or the service account annotation. Container Apps: the app has no managed identity |
| `password authentication failed` with an `azure-*` `auth` | The URL user is not the identity's role name, or the role was not created with `pgaadauth_create_principal` |
| `checkpoint error: Permission denied` | State volume not writable by uid 65532; use the provided Dockerfile or `chown 65532` the volume |
| `cannot update table ... does not have a replica identity` | Set a primary key or `REPLICA IDENTITY FULL` |
| Kafka: `InvalidTopicException` | A topic name with illegal characters; check route `topic` values and templates |
| Same transaction delivered twice after a crash | Expected with at-least-once delivery; deduplicate downstream |

## Securing connections (checklist)

1. Postgres: `ssl_mode = "verify-full"` and `ssl_root_cert` pointing at your CA; create the Aqueduct role with a strong password and only `REPLICATION` + `SELECT` on the published tables. On Azure, a managed identity (`auth`, see [Azure managed identity](#azure-managed-identity)) needs no stored password at all.
2. Kafka: `tls = true`, SASL `scram-sha-512`, and ACLs limiting the principal to `Write`/`Describe`/`Create` on the target topics.
3. Cockpit: set `admin_token` (startup refuses a non-loopback `health_addr` without one); never set `admin_insecure_no_auth` in production.
4. Pass credentials through the environment (`AQUEDUCT_PG_URL`, `AQUEDUCT_KAFKA_SASL_PASSWORD`, `AQUEDUCT_ADMIN_TOKEN`, `AQUEDUCT_HASH_KEY`) or orchestrator secrets instead of committing them to the TOML file. Secrets in the config are redacted from debug output.

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
