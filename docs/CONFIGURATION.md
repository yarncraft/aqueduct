# Configuration

One TOML file, read from `/etc/aqueduct/aqueduct.toml` (override with `AQUEDUCT_CONFIG`). Full example:
[`config/aqueduct.example.toml`](../config/aqueduct.example.toml). Environment overrides: `AQUEDUCT_PG_URL`,
`AQUEDUCT_ADMIN_TOKEN`, `AQUEDUCT_HASH_KEY`, `AQUEDUCT_KAFKA_SASL_USERNAME`, `AQUEDUCT_KAFKA_SASL_PASSWORD`.

Every key with its full semantics: [deep dive: configuration](deep-dive/configuration.md).

```toml
health_addr = "0.0.0.0:8080"
admin_token = "<output of: openssl rand -hex 32>"

[postgres]
url = "postgres://aqueduct:SECRET@db.internal:5432/app"
slot = "aqueduct"
publication = "aqueduct"

[sink]
brokers = ["kafka:9092"]
topic = "cdc.{schema}.{table}"
```

## Top level

| Key | Default | |
|-----|---------|-|
| `health_addr` | `0.0.0.0:8080` | Cockpit, API and health probes. Plain HTTP; a non-loopback bind logs a startup warning |
| `checkpoint_path` | `/var/lib/aqueduct/checkpoint` | Last acknowledged position. Keep on the state volume |
| `admin_token` | unset | Bearer token for `/api/*` and `/metrics`. Required unless `health_addr` is loopback; 16+ characters. Sent over plain HTTP: [put TLS in front](DEPLOYMENT.md#cockpit-over-tls) |
| `admin_insecure_no_auth` | `false` | Allow a non-loopback bind without a token (logs a warning) |
| `inspector` | `true` | Event inspector in the cockpit; needs `admin_token` |

## `[postgres]`

| Key | Default | |
|-----|---------|-|
| `url` | required | Plain connection string. Do not add `replication=database` |
| `slot`, `publication` | `aqueduct` | Logical slot (created if missing) and an existing publication |
| `ssl_mode` | `verify-full` | `disable`, `prefer`, `require`, `verify-ca`, `verify-full`. A server without TLS fails startup unless `disable` |
| `ssl_root_cert` | unset | PEM CA bundle |
| `transport` | `poll` | `stream` receives changes over a replication connection as they commit |
| `snapshot` | `never` | `initial` or `on_demand` copies existing rows |
| `auth`, `azure_client_id` | `password` | Azure managed identity instead of a password |
| `heartbeat_interval_secs` | `10` | Moves the slot while idle so unpublished writes don't pin WAL; `0` disables |

More keys (`stream_buffer_bytes`, `snapshot_*`, `heartbeat_action_query`, `checkpoint_skip_system_check`):
[deep dive](deep-dive/configuration.md#postgres).

## `[source]` for MongoDB

```toml
[source]
type = "mongodb"
uri = "mongodb://aqueduct:SECRET@mongo.internal:27017/?replicaSet=rs0&tls=true"
database = "app"
collections = ["users", "orders"]   # empty = whole database
```

Needs a replica set. Plaintext to a non-loopback host is refused unless the URI says `tls=false`. `database` becomes `schema`, the collection `table`. Other keys: `pre_images`, `snapshot`,
`snapshot_chunk_size`, `snapshot_max_rows_per_sec`. Privileges, resume-token and oplog behaviour:
[deep dive](deep-dive/configuration.md#source-alternative-sources).

## `[sink]` (Kafka)

| Key | Default | |
|-----|---------|-|
| `brokers` | required | Bootstrap list |
| `topic` | unset | Fallback topic for events no route matches. Supports `{schema}`, `{table}`, `{op}` |
| `partitions`, `replication_factor` | `1`, `3` | Used when Aqueduct creates a topic |
| `partition_by` | `key` | `key` keeps a row's changes ordered; `table` gives total order per table |
| `record_timestamp` | `produce` | Kafka record timestamp: `produce` (when Aqueduct sent it, as Debezium does) or `commit` (the source commit time). With `commit`, a backlog older than the topic's `retention.ms` can be deleted before consumers read it unless the topic uses `message.timestamp.type=LogAppendTime`. The commit time is always in `source.ts_ms` |
| `tls`, `tls_ca_file` | `false` | Encrypt and verify the brokers. Set `tls` for any non-local cluster |
| `sasl_mechanism`, `sasl_username`, `sasl_password` | unset | `plain`, `scram-sha-256`, `scram-sha-512` |
| `max_dynamic_topics` | `100` | Cap on topics created by content routing |
| `dead_letter_topic` | unset | Global dead-letter topic override |
| `hash_key_file`, `hash_key_id` | unset | Key and label for the `hash` field operation |

Records are Debezium-style change events keyed by table and primary key, with `op` and `lsn` headers. Delivery is
at-least-once. Format and encoding: [deep dive](deep-dive/configuration.md#sink).

## Routes

`[[sink.routes]]` entries are tried in order; the first whose `match` fits claims the event. No match goes to `topic`.

```toml
[[sink.routes]]
match = "public.orders"                       # schema.table, * is a wildcard
where = "op in ['insert', 'update'] and row.status == 'paid'"
topic = "orders-paid"                         # bad rows go to orders-paid.dlq

[[sink.routes]]
match = "public.customers"
topic = "customers"
exclude = ["internal_note"]
mask = ["card_last4"]
hash = ["email"]
format = "row"                                # value is the row, deletes become tombstones
```

| Option | Does | Details |
|--------|------|---------|
| `match`, `name` | Pick events by `schema.table`; label the route | [deep dive](deep-dive/routing.md) |
| `where` | Filter by operation and row values (SQL-style expressions) | [Filtering](deep-dive/routing.md#filtering-events-per-route) |
| `topic`, `topic_fallback`, `topic_values` | Static, templated or row-content topics (`orders.{row.region}`) | [Routing by row content](deep-dive/routing.md#routing-by-row-content) |
| `dead_letter_topic` | Where unevaluable rows go (default: topic + `.dlq`) | [Dead-letter topics](deep-dive/routing.md#dead-letter-topics) |
| `key` | Key records by chosen columns | [Keying a route](deep-dive/routing.md#keying-a-route) |
| `include`, `exclude`, `mask`, `hash`, `rename` | Protect or reshape columns | [Protecting fields](deep-dive/routing.md#protecting-fields-per-route) |
| `outbox` | Publish an outbox table's inserts as their own events | [Outbox](deep-dive/routing.md#outbox) |
| `format`, `deletes`, `tombstones`, `unchanged` | Row-only values, tombstones, TOAST handling | [Shaping the output](deep-dive/routing.md#shaping-the-output) |
