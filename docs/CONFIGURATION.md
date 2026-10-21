# Configuration reference

Every setting, source and sink. To run Aqueduct see [DEPLOYMENT.md](DEPLOYMENT.md); for the cockpit see [COCKPIT.md](COCKPIT.md).

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

## Top level

| Key | Default | Description |
|-----|---------|-------------|
| `health_addr` | `0.0.0.0:8080` | Listen address of the cockpit, API and `/healthz` |
| `checkpoint_path` | `/var/lib/aqueduct/checkpoint` | Last acknowledged LSN, written atomically. Keep on the state volume |
| `admin_insecure_no_auth` | `false` | Allow a non-loopback `health_addr` without `admin_token`. Without this, startup refuses that combination; with it, a warning is logged. Loopback binds never need a token |
| `admin_token` | unset | Required unless `health_addr` is loopback. If set, `/metrics` and `/api/*` require it (`Authorization: Bearer ...` only; a `?token=` query parameter is ignored, so such a request gets `401`). Must be at least 16 characters, not whitespace-only and not contain `${`; startup refuses it otherwise. Env: `AQUEDUCT_ADMIN_TOKEN` |
| `inspector` | `true` | Cockpit event inspector, reading recent events back from Kafka. Needs `admin_token`; `false` turns it off |

## `[postgres]`

| Key | Default | Description |
|-----|---------|-------------|
| `url` | required | `postgres://user:pass@host:5432/db`. A normal connection: **do not** add `replication=database`. Env: `AQUEDUCT_PG_URL`. Unless the URL sets them, connections use `connect_timeout=30` and TCP keepalives `keepalives_idle=30`, `keepalives_interval=10`, `keepalives_retries=3`, so a vanished server (failover, dropped NAT flow) fails the source in about a minute and the container restarts, instead of hanging for the OS default of over 2 h |
| `slot` | `aqueduct` | Logical slot name, created if missing. Use a distinct name per instance |
| `publication` | `aqueduct` | Existing publication to stream |
| `ssl_mode` | from URL, else `verify-full` | `disable`, `prefer`, `require`, `verify-ca`, `verify-full`. Overrides `sslmode=` in the URL. Weaker modes must be set explicitly; `verify-full` is the default and the production setting |
| `ssl_root_cert` | unset | PEM CA bundle to verify the server against (also accepted as `sslrootcert=` in the URL). Unset means the bundled public roots |
| `auth` | `password` | `password` uses the password in `url`. `azure-workload-identity` (AKS) or `azure-container-apps` signs in with a Microsoft Entra token from the pod's or app's managed identity instead; see [Azure managed identity](DEPLOYMENT.md#azure-managed-identity) |
| `azure_client_id` | unset | With an `azure-*` `auth`: the managed identity's client ID. Overrides `AZURE_CLIENT_ID` on AKS; on Container Apps it picks a user-assigned identity (unset: the system-assigned one) |
| `heartbeat_interval_secs` | `10` | While idle, advance the slot to the server's WAL position at most this often, so writes to tables outside the publication do not pin WAL. `0` turns it off. See [Idle heartbeat](DEPLOYMENT.md#idle-heartbeat) |
| `heartbeat_action_query` | unset | Optional SQL run before each heartbeat, e.g. an upsert into a small published table. See [Idle heartbeat](DEPLOYMENT.md#idle-heartbeat) |
| `checkpoint_skip_system_check` | `false` | Turn off the [checkpoint origin check](DEPLOYMENT.md#checkpoint-origin-check). Only for servers that refuse `pg_control_system()` to the Aqueduct role; without it such a server fails at startup |
| `transport` | `poll` | `poll` reads the slot with SQL peeks every `poll_interval`; `stream` receives changes over a replication connection as they commit. See [Streaming transport](DEPLOYMENT.md#streaming-transport) |
| `stream_buffer_bytes` | `67108864` (64 MiB) | Stream transport: received messages buffered ahead of the sink before reading pauses |
| `snapshot` | `never` | `never`, `on_demand` or `initial`. Copies rows that already exist; see [Snapshots](DEPLOYMENT.md#snapshots). Anything but `never` needs Postgres 14+, or `snapshot_watermark_table` |
| `snapshot_chunk_size` | `1024` | Rows per snapshot chunk |
| `snapshot_max_rows_per_sec` | unset | Upper bound on snapshot rows read per second. Unset is unthrottled |
| `snapshot_watermark_table` | unset | `schema.table` of a published table that carries snapshot watermarks as rows, for Postgres 12 and 13 (or where `pg_logical_emit_message` is not allowed). See [Watermark table](DEPLOYMENT.md#watermark-table-postgres-12-and-13) |
| `snapshot_key_columns` | empty | Key columns for tables without a primary key, e.g. `{ "public.events" = ["day", "seq"] }`. They must be published, `NOT NULL` and unique together |

TLS uses rustls (no OpenSSL). Semantics follow libpq: `prefer` and `require` encrypt but do **not** verify the
certificate (`require` with `ssl_root_cert` set does verify the chain); `verify-ca` checks the chain; `verify-full` also
checks the hostname. After connecting, Aqueduct asks the server (`pg_stat_ssl`) whether the session is really
encrypted. A plaintext session to a non-loopback host **aborts startup** unless `ssl_mode = "disable"` is set explicitly (then it only logs a warning). A `hostaddr` in the URL is the address actually dialled, so it must be loopback too for the host to count as local, and `prefer` to a non-loopback host dials as `require`, so a server without TLS is refused before any password is sent; an encrypted but unverified session (`prefer`/`require` without a CA) logs a warning.
Server side you need `ssl = on` with a certificate, and `hostssl` lines in `pg_hba.conf` if you want to forbid plaintext.
With an `azure-*` `auth` only `verify-full` is accepted: startup refuses every other `ssl_mode`, also `verify-ca`
and `require` with `ssl_root_cert`.

## `[source]` (alternative sources)

`[postgres]` is shorthand for `[source] type = "postgres"` with the same keys. Set exactly one of the two.
`type` can also be `mongodb` (below). There is no Kafka source; Kafka is sink-only.

### MongoDB source

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
| `snapshot` | `never` | `on_demand` or `initial` copy existing documents; see [MongoDB snapshots](DEPLOYMENT.md#mongodb-snapshots). |
| `snapshot_chunk_size` | `1024` | Documents per snapshot chunk. |
| `snapshot_max_rows_per_sec` | unset | Upper bound on snapshot documents read per second; unset or `0` is unthrottled. |
| `pre_images` | `true` | Ask for `fullDocumentBeforeChange`. It only has an effect on collections with pre-images enabled (`collMod` with `changeStreamPreAndPostImages: {enabled: true}`, MongoDB 6.0+); otherwise `before` stays empty for updates. |

- The role needs the `find` and `changeStream` actions on the watched database (the built-in `read` role covers both).
  With `snapshot` other than `never` it also needs `insert`, `remove` and `createIndex` on the one collection
  `<database>.aqueduct_watermarks`; startup checks this and fails with the missing privilege otherwise.
- The position is the change-stream resume token, saved in Aqueduct's checkpoint only after the sink has the event. A restart resumes just after the last checkpointed event. Nothing is stored on the server.
- The resume token must still be in the oplog. If Aqueduct is down longer than the oplog window, the stream cannot resume and startup fails; size the oplog for your longest expected outage.
- Updates use `updateLookup`, so `after` is the document as it is when read: it can be newer than the change, or only the `_id` if the document was deleted meanwhile. Deletes carry the pre-image when available, otherwise just the `_id`.
- Existing documents are not copied unless `snapshot` is set; see [MongoDB snapshots](DEPLOYMENT.md#mongodb-snapshots).
- On a first start (no checkpoint) the stream opens at "now", and its opening position is checkpointed before
  anything else is read, so a restart soon after the first start does not skip changes made in between.
- While the stream is idle, or only filtered-out collections change, the checkpoint still follows the stream's resume token (checked every 30 s, saved only when it moved). Nothing is written to Kafka for this, so the checkpoint can advance with no sink output; it keeps the token inside the oplog window.
- Only insert, update, replace and delete events are forwarded. Drop, rename and dropDatabase events are skipped, and `truncate` is never emitted. Changes of `aqueduct_watermarks` are never forwarded, and that name cannot be listed in `collections`.
- `pre_images = true` asks for `fullDocumentBeforeChange` on every stream. MongoDB older than 6.0 may reject that option at startup; set `pre_images = false` there (not verified against a server).
- Dropping the watched database invalidates the change stream. The source then **fails** (`MongoDB invalidated the change
  stream`) instead of ending or skipping past it, and every restart fails the same way until the checkpoint is reset: see
  "MongoDB change stream invalidated" in `docs/RUNBOOK.md`.
- A change event over 16 MB (the document from `updateLookup` plus, with `pre_images`, its pre-image) cannot be returned
  by the server (`BSONObjectTooLarge`), and the source fails at it on every restart with a message saying so. Keep
  documents well below 16 MB, or set `pre_images = false` for collections with large documents; Aqueduct does not use
  `$changeStreamSplitLargeEvent` (MongoDB 7.0+). See "MongoDB change event larger than 16 MB" in `docs/RUNBOOK.md`.

## `[sink]`

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

### Kafka sink

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
checkpoint moves. A batch's records for one partition go out in requests of at most about 900 KiB each, one after
another so their order holds, which keeps each request under the broker's default `message.max.bytes` (1 MiB); a
single record larger than that still goes alone, and the broker (or the topic's `max.message.bytes`) decides. At least one of `topic` or a route is required; an event that matches nothing while `topic` is unset
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

### Filtering events per route

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

### Dead-letter topics

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

### Keying a route

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

### Routing by row content

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

### Protecting fields per route

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

### Outbox

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

### Shaping the output

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
`fallback` (sent to `topic_fallback` by content routing) or `errors`. They count a batch once Kafka acknowledged it, so
a failed batch the pipeline retries is counted once, when it is delivered. They appear in `/api/metrics` (`routes`),
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
