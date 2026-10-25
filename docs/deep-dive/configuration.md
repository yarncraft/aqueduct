# Configuration deep dive

Every key with its full semantics. The short version is [CONFIGURATION.md](../CONFIGURATION.md).

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
| `uri` | required | `mongodb://` or `mongodb+srv://`. Credentials and `tls=true` go in the URI. Plaintext to a non-loopback host fails startup unless the URI says `tls=false` explicitly, which then logs a warning; so do `tlsInsecure` and `tlsAllowInvalidCertificates`. `mongodb+srv://` uses TLS by default. |
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
  without a primary key and for a truncate. Headers: `op` (the letter) and `lsn`. Record timestamp: the produce time (`[sink] record_timestamp = "commit"` uses the commit time); the commit time is always in `source.ts_ms`.

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
| `max_dynamic_topics` | `100` | How many topics content routing may create, counted from the broker (existing topics a template could produce count). Beyond it, events go to the route's `topic_fallback`. `0` means no new topics: only topics that already exist are used. See [Routing by row content](routing.md#routing-by-row-content) |
| `dead_letter_topic` | unset | Global override: the dead-letter topic of every route that does not set its own `dead_letter_topic`. Unset, each route uses its own topic plus `.dlq`. See [Dead-letter topics](routing.md#dead-letter-topics) |
| `hash_key_file` | unset | File holding the key for route `hash` (at least 32 bytes; one trailing newline is ignored). Or env `AQUEDUCT_HASH_KEY` (also without one trailing newline), not both. A `hash_key` in this file is rejected. Never logged. See [Protecting fields per route](routing.md#protecting-fields-per-route) |
| `hash_key_id` | unset | Short label of the hash key (1 to 64 of `A-Z a-z 0-9 . _ : -`), sent as the `aqueduct-hash-key-id` header on every record of a hashing route. Required with a key, and an error without one |
| `[[sink.routes]]` | none | `match` (`schema.table`, `*` wildcard) and `topic` (or `outbox`). First match wins. Optional `otherwise` names a later route without `match` that takes the rows `where` rejects, see [Chaining routes with `otherwise`](routing.md#chaining-routes-with-otherwise). Optional `name` (unique) labels the route in per-route stats; it defaults to `match`. Optional `where` filters the events the route claims, operations included (`op in ['insert', 'update']`), see [Filtering events per route](routing.md#filtering-events-per-route). Optional `key` sets the record key from row columns, see [Keying a route](routing.md#keying-a-route). A `topic` can read row fields with `topic_fallback` and optional `topic_values`, see [Routing by row content](routing.md#routing-by-row-content). `outbox` publishes an outbox table's rows as their own events, see [Outbox](routing.md#outbox). Topic names are checked at startup. Optional `include`, `exclude`, `mask`, `hash` and `rename` protect personal data, see [Protecting fields per route](routing.md#protecting-fields-per-route). Optional `format`, `deletes`, `tombstones` and `unchanged` reshape the records, see [Shaping the output](routing.md#shaping-the-output). Optional `dead_letter_topic` (a fixed name) is where the route's rows that cannot be evaluated go; default `sink.dead_letter_topic`, else the route's topic plus `.dlq`, see [Dead-letter topics](routing.md#dead-letter-topics) |

Records: key and value as above (a route with `format = "row"` or `tombstones = true` changes the value, see
[Shaping the output](routing.md#shaping-the-output)), headers `op` and `lsn`, timestamp = the produce time (`record_timestamp = "commit"`: the commit time, which is always in `source.ts_ms`). The partition is a stable FNV-1a
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
