# Changelog

Notable changes to Aqueduct. Newest first.

## Unreleased

### Breaking changes and upgrade notes

- **Events use Debezium's envelope, keys and time encodings** (Postgres and MongoDB). Every consumer must be updated:
  - Value: `{"before", "after", "source": {"version", "connector", "ts_ms", "ts_us", "snapshot", "db", "schema",
    "table", "txId", "lsn"}, "op", "ts_ms", "ts_us"}` (MongoDB: `db` and `collection` in `source`). The old top-level
    `lsn`, `xid`, `commit_ts_micros`, `schema` and `table` moved into `source` (`lsn`, `txId`, `ts_us`, `schema`,
    `table`).
  - `op` is a letter: `c`, `u`, `d`, `t`, `r` (was `insert`, `update`, `delete`, `truncate`, `read`); the Kafka header
    `op` too.
  - `timestamptz` values are ISO-8601 in UTC (`2026-10-20T13:23:46.992107Z`, was Postgres text with an offset),
    `timestamp` values microseconds since the epoch, `date` values days since the epoch (both were text).
  - Record key: `{"db": "app", "schema": "public", "table": "orders", "key": {"id": 42}}` (was `public.orders` or
    `public.orders:42`); `key` is the primary key, also for `REPLICA IDENTITY FULL` tables.
  - Partitioning: `partition_by` now defaults to `key` (per-row order, a table spreads over partitions). Set
    `partition_by = "table"` under `[sink]` to keep total per-table order. Records of a table can land on other
    partitions than before, so let consumers drain the topics before upgrading if they rely on order across the
    switch.
  - Upgrade: deploy consumers that read the new format first (or a new topic per route), then upgrade Aqueduct.
- **The supported Postgres versions are 12 and newer**, not 10 as the docs said. Postgres 10 has no
  `pg_replication_slot_advance`, and on 11 repeated slot peeks over one connection fail with
  `out of relcache_callback_list slots`; neither ever worked. Nothing changes on 12 and newer.
- **Auto-created Kafka topics use replication factor 3** (#56). They used to be created with factor 1, so one broker
  loss meant data loss. New `replication_factor` under `[sink]` sets it explicitly. Unset, Aqueduct tries 3 and, if the
  cluster has fewer than 3 brokers, creates the topic with 1 and logs a warning. Topics that already exist are never
  changed; raise their factor with Kafka's partition-reassignment tooling.
- **Postgres: rows were lost on overlapping transactions, multi-row `COPY` and multi-table `TRUNCATE` (#94).**
  Change LSNs are neither commit-ordered nor unique, but the source deduplicated on them. The source now resumes from
  a key `(commit LSN, change LSN, ord)`, checkpointed as text `<commit>@<change>#<ord>`. The event JSON is unchanged.
  - Upgrade: a plain-LSN checkpoint from an older version still starts. The slot replays from its
    `confirmed_flush_lsn`, so some events are delivered again, never lost.
  - **Rollback is not supported.** An older binary refuses the new checkpoint (`not a Postgres LSN`) and will not
    start. To go back, delete the checkpoint file; the slot's `confirmed_flush_lsn` then decides where it resumes.
  - Rows already lost by earlier versions are not recovered by this fix; re-snapshot or backfill the affected tables.
- **Postgres: a checkpoint from another cluster, database or timeline is refused at startup (#63).** Checkpoints now
  record the cluster's `system_identifier`, the database OID and the timeline (`;sys=<id>;db=<oid>;tli=<n>` after the
  key). Startup fails if the cluster or database differs (a re-pointed URL, a rebuilt primary, a major-version
  `pg_upgrade`), and also after a failover or PITR of the same cluster (a new timeline), because changes may have been
  lost; the error says "restored, recovered to a point in time (PITR) or failed over" and how to resync. See
  DEPLOYMENT.md, "Checkpoint origin check".
  - Upgrade: existing checkpoints lack these fields; they are accepted with a warning and the next checkpoint records
    them.
  - The Aqueduct role must be able to call `pg_control_system()` and `pg_control_checkpoint()`; a plain
    `LOGIN REPLICATION` role can. Where a managed service refuses them, startup fails; set
    `checkpoint_skip_system_check = true` under `[postgres]` to skip the cluster and timeline comparison.
  - After a planned switchover you have verified, delete only the `;tli=` field from the checkpoint file.
  - A slot with the configured name that belongs to another database (or is a physical slot) is now refused instead
    of used.
  - Rollback: a version before this one refuses the new checkpoint (`not a Postgres checkpoint`) and will not start.
    To go back, remove the `;sys=...` suffix from the checkpoint file, or delete the file.
- **Postgres TLS defaults to `verify-full`** (#12, PR #14). With no `ssl_mode` and no `sslmode=` in the URL, Aqueduct
  now requires TLS and verifies the server certificate and hostname. A Postgres server without TLS fails at startup,
  **including on localhost** (verify-full never falls back to plaintext). To upgrade:
  - Production: enable TLS on the server and set `ssl_root_cert` if the CA is private.
  - Non-TLS dev Postgres: set `ssl_mode = "disable"` (or `sslmode=disable` in the URL) explicitly.
  - Plaintext to a non-loopback host is refused unless `ssl_mode = "disable"` is explicit, and then logs a warning.
- **Admin server fails closed** (#10, PR #15). With a non-loopback `health_addr` (the default `0.0.0.0:8080`), startup
  is refused unless `admin_token` (or `AQUEDUCT_ADMIN_TOKEN`) is set to a non-empty value. To upgrade, set a token, bind
  `health_addr` to `127.0.0.1`, or set `admin_insecure_no_auth = true` (logged as a warning; not for production).
- **Admin token is accepted only as a Bearer header** (#11, PR #25). `?token=` is rejected on `/metrics` and `/api/*`
  with `401`, because URLs leak into proxy logs, browser history and Referer. Open the cockpit with
  `http://host:8080/#token=<token>` (the fragment is never sent to the server). Prometheus or other scrapers that used
  `/metrics?token=...` must send the header instead:

  ```yaml
  scrape_configs:
    - job_name: aqueduct
      metrics_path: /metrics
      authorization:
        type: Bearer
        credentials_file: /etc/prometheus/aqueduct-token   # or: credentials: <token>
      static_configs:
        - targets: ["aqueduct:8080"]
  ```

  `curl`: `curl -H "Authorization: Bearer $AQUEDUCT_ADMIN_TOKEN" http://host:8080/metrics`.
- **Compose files no longer default the admin token** (#19). `docker-compose.yml` and `docker-compose.sim.yml` used to
  fall back to the publicly known `dev-token-change-me`. `docker compose up` now fails with
  `set AQUEDUCT_ADMIN_TOKEN to a secret value` unless `AQUEDUCT_ADMIN_TOKEN` is set. To upgrade, export one (or put it in
  a `.env` file next to the compose file):

  ```sh
  export AQUEDUCT_ADMIN_TOKEN=$(openssl rand -hex 32)
  docker compose up --build
  ```

  Open the cockpit with `http://localhost:8080/#token=$AQUEDUCT_ADMIN_TOKEN`. Existing deployments that relied on the
  default token should rotate it, because it was public.
- **The Kafka source is removed** (#20). `[source] type = "kafka"` no longer exists, and the
  `aqueduct-source-kafka` crate, its `AQUEDUCT_KAFKA_SOURCE_SASL_USERNAME` / `AQUEDUCT_KAFKA_SOURCE_SASL_PASSWORD`
  variables and the `aqueduct-kafka` service in `docker-compose.sim.yml` (port 8089) are gone. A config that still sets
  `type = "kafka"` now fails at startup with an unknown-variant error. Kafka remains the sink. To upgrade, switch
  `[source]` to `postgres` or `mongodb`, or pin the previous release if you depend on reading Kafka topics. Checkpoint
  files written by the Kafka source are not readable by the other sources, so delete or move them when changing source type.

- **Weak admin tokens are refused at startup** (#39). A non-empty `admin_token` (or `AQUEDUCT_ADMIN_TOKEN`) must be at
  least 16 characters, must not be whitespace-only or have leading/trailing whitespace, must not contain `${` (an
  unexpanded compose or shell placeholder would otherwise become the token), and must not be the published
  placeholders `dev-token-change-me` or `change-me` (case-insensitive). This applies on loopback binds too. An empty value still means "unset". To
  upgrade, replace short tokens such as `change-me` with a random one: `openssl rand -hex 32`.

### Added

- **Routing: include, exclude, mask, hash and rename fields to protect personal data** (#84, ADR 0002).
  `[[sink.routes]]` takes `include` or `exclude` (top-level columns), `mask` (strings become `"***"`, other values
  `null`), `hash` (HMAC-SHA256 hex over the value's canonical text, so a Postgres `42` and a MongoDB `42` give the
  same digest) and `rename`. They apply to `before` and `after`, to the route's dead-letter records and, unless the
  route sets `key`, to the primary-key object in the record key, which the partition is then hashed from. The hash
  key comes only from `AQUEDUCT_HASH_KEY` or the new `[sink] hash_key_file` (at least 32 bytes; a `hash_key` in the
  TOML fails startup), and the new `hash_key_id` is sent as the `aqueduct-hash-key-id` header. A `key` column, or
  a column a content-routed `topic` reads, may not be excluded, masked, hashed or left out of `include`, and a rename target may not be listed in `mask`, `hash`
  or `exclude`. A row whose renamed column collides goes to `dead_letter_topic`. `/api/routes`
  lists each route's field operations and the key id, never the key. Additive: routes without these keys are
  unchanged.
- **Routing: choose the topic from the row's contents** (#82, ADR 0002). A route's `topic` can read row fields, such as
  `orders.{row.region}`, and also takes `{op}`. Such a template must start with fixed text and needs a fixed
  `topic_fallback`. An optional `topic_values` allow-list limits the values. No fixed topic in the config may match the
  template read as a glob. Each value must be one segment (`[A-Za-z0-9_-]{1,64}`). These events go to the fallback,
  counted under `fallback`, so one bad row never stalls the pipeline:
  - null or missing values;
  - values that are not one segment or not in `topic_values`;
  - names that differ from an existing topic only by `.` vs `_` (Kafka would refuse them);
  - creates the broker refuses for good (invalid name, create policy, missing ACL); the name is remembered until a
    restart, so later rows with that value cause no broker call;
  - new topics beyond the new `[sink] max_dynamic_topics` (default 100; `0` means existing topics only), which is
    counted from the broker at startup, so it holds across restarts.

  Passing create errors still fail the batch and are retried. A content-routed template may not overlap a
  `{schema}`/`{table}` template. Topic names hold the row value, so they show in Kafka and in log lines about that
  topic. Without create rights, pre-create the topics and set `topic_values` or `max_dynamic_topics = 0`.

  Fallbacks are opened at startup. The inspector reads the content-routed topics, and `GET /api/routes` shows
  `topic_fallback` and `topic_values`. Once any route reads a row field, no topic in the config (route topics,
  `sink.topic`, fallbacks) may start with `_`; configs without content routing are unaffected.
- **Topic names are checked at startup** (#82). A route `topic`, `sink.topic` or `topic_fallback` with characters other
  than `A-Z a-z 0-9 . _ -` in its fixed text, or braces that are not a known placeholder (`{schema}`, `{table}`, `{op}`,
  `{row.<col>}`, ...), now fails startup naming the key. Kafka refuses such names anyway, so these configs used to fail
  on the first send instead.
- **Routing: set the Kafka message key per route** (#83, ADR 0002). `key = ["col", ...]` on a `[[sink.routes]]`
  entry builds the record key from those columns of the row (`after`, else `before`, so deletes key by their
  before-image): `{"db", "schema", "table", "key": {"col": value, ...}}`, values copied as JSON, a missing column
  `null`. The partition is hashed from that whole key, overriding `partition_by` for the route. Truncates keep the
  table-only key. `GET /api/routes` shows each route's `key`. Additive: routes without `key` are unchanged.
- **Cockpit: a Routing card draws the routing graph** (#87, ADR 0002). Tables on the left, routes in match order
  (numbered, first match wins) in the middle, topics and the dead-letter topic on the right. Each route shows its
  `match`, `ops`, `where`, per-route counts (produced, filtered, fallback, errors) with a bar, and an amber warning
  listing fields its `where` reads that no row has had. Edge width follows the counts and edges with recent traffic
  animate (static under reduced motion). Hover a table to light up its path, click a route for its details, click a
  topic to open it in the event inspector. Busy routes collapse to the 6 busiest tables and topics with "+N more".
  No new server route: it reads `/api/routes`, `/api/tables` and the stream.
- **Routing: filter events per route and send failures to a dead-letter topic** (#81, ADR 0002). `[[sink.routes]]`
  takes `ops` (insert, update, delete, truncate, read) and `where`, a typed predicate with SQL-style three-valued
  logic (an unknown result is filtered). The first matching route owns the event; nothing falls through. Rows a
  `where` cannot evaluate (a type error) go to the new `[sink] dead_letter_topic` with an `aqueduct-error` header and
  a table-only key; it is required once a route uses `where` and is opened at startup. Per-route counters
  (`matched`, `produced`, `filtered`, `fallback`, `errors`) and unseen-field warnings appear in `/api/metrics`, the new
  `GET /api/routes` (the compiled plan with live counts) and Prometheus (`aqueduct_route_records_total`,
  `aqueduct_route_field_unseen`). Additive: routes without the new keys behave exactly as before.
- **Route plan inside the Kafka sink** (#79). Routing now compiles once into a `RoutePlan` with a pure `render` step;
  output (topic, key, partition, value, headers) is unchanged, which a golden test pins. `[[sink.routes]]` accepts an
  optional unique `name` that labels the route in the per-route counters (`matched`, `produced`; shown in a later
  change). `aqueduct-bench --sink render` measures the render cost without a broker.
- **Cockpit: per-table high-water marks.** The per-table card now shows, for each table, its topic, counts per
  operation, the newest delivered LSN, how long ago it committed and was delivered (amber/red when it falls behind
  the freshest table while streaming), and the topic's Kafka high-water mark per partition. New route
  `GET /api/tables` (Bearer token) and Prometheus series `aqueduct_table_last_commit_timestamp_seconds{table}`;
  `/api/metrics` and `/api/stream` gain `table_marks` and `checkpoint`. See DEPLOYMENT.md, "Tables · high-water marks".
- **Postgres snapshots: copy rows that already exist while changes stream** (#74, ADR 0001). New `[postgres]` keys
  `snapshot` (`never` by default, `on_demand`, `initial`), `snapshot_chunk_size`, `snapshot_max_rows_per_sec` and
  `snapshot_key_columns`. Copied rows arrive as `"op": "r"`, encoded like an `insert` of the same row. Needs
  Postgres 14+ and `EXECUTE` on `pg_logical_emit_message`. See DEPLOYMENT.md, "Snapshots".
  - **New op value `r` (snapshot rows): consumers that decode `op` as a closed enum must accept it.**
- **Postgres streaming transport** (#109, ADR 0003): `[postgres] transport = "stream"` receives changes over a
  replication connection as they commit instead of polling the slot every 500 ms. Same events, keys and checkpoints
  as `poll`, which stays the default. Needs one more walsender per instance (`max_wal_senders`), a `pg_hba.conf` line
  for the role on the database, and a direct connection (no pooler). `stream_buffer_bytes` (64 MiB) bounds what is
  buffered ahead of the sink. See DEPLOYMENT.md, "Streaming transport".
- **MongoDB snapshots: copy documents that already exist while changes stream** (#77, ADR 0001). New MongoDB
  `[source]` keys `snapshot` (`never` by default, `on_demand`, `initial`), `snapshot_chunk_size` and
  `snapshot_max_rows_per_sec`, the same as for Postgres, and `/api/snapshot` accepts `database.collection`. Documents
  arrive as `"op": "r"`, encoded like an `insert` of the same document, in `_id` order (mixed `_id` types work).
  With snapshots on, Aqueduct writes watermark documents into `<database>.aqueduct_watermarks` and needs `insert`,
  `remove` and `createIndex` on that collection; startup checks it. See DEPLOYMENT.md, "MongoDB snapshots".
  - While a MongoDB snapshot runs the checkpoint file holds JSON (`{"pos": <resume token>, "snap": {...}}`); a
    version without MongoDB snapshots cannot resume from it. Let the snapshot finish before rolling back, or delete
    the checkpoint.
  - An older Aqueduct watching the same whole database (no `collections` list) forwards the inserts and deletes of
    `aqueduct_watermarks` as ordinary events. Upgrade every instance on that database first, or give the old ones a
    `collections` list.
- **Snapshots on Postgres 12 and 13** (#92): new `[postgres] snapshot_watermark_table` names a published table whose
  rows carry the watermarks instead of logical messages (Postgres 14+). See DEPLOYMENT.md, "Watermark table".
- **Cockpit snapshot card** (#78): progress of the running or last snapshot, skipped tables and a Cancel button. Snapshot
  rows count as `READ` in the operations bar, the per-op event-rate chart and the inspector's op filter.
- **Admin API for snapshots** (#75): `GET`/`POST`/`DELETE /api/snapshot` shows, starts and cancels a Postgres table
  snapshot (with `snapshot = "on_demand"` or `"initial"`), behind the admin token. Progress is in `/api/metrics`
  (`snapshot`) and Prometheus (`aqueduct_snapshot_*`). A request is not durable until the first chunk is
  checkpointed. The admin server now reads a request split over several TCP segments whole (up to 16 KiB).
  - While a snapshot runs the checkpoint file holds JSON (`{"pos": ..., "snap": {...}}`); a version without snapshots
    refuses it. Let a snapshot finish before rolling back, or delete the checkpoint.
- **Cockpit replication-health tiles** (#54, UI half). Slot lag, last-heartbeat age and skipped heartbeats, read from `slot_lag_bytes`, `confirmed_flush_lsn` and `heartbeat` in the `/api/metrics` and `/api/stream` snapshots. Each tile shows `–` until the server reports the field, so this ships independently of the backend half.
- **Postgres idle heartbeat** (#47). Writes to tables outside the publication no longer pin WAL: every `heartbeat_interval_secs`
  (default 10, `0` disables) an idle source advances the slot to the server's WAL position, never past an event that is
  not yet checkpointed. Optional `heartbeat_action_query` for servers that need a write to move the slot. No action needed
  on upgrade; see DEPLOYMENT.md for the heartbeat-table recipe.

### Changed

- **Snapshots: the cockpit no longer shows primary-key values of the snapshot cursor.** While a snapshot ran, the
  checkpoint in `/api/metrics`, `/api/stream`, `/api/tables` and the cockpit header was the whole composite
  checkpoint, including `snap.after`, the last chunk's primary-key values (an email or other personal data when that
  is the key). It now shows the stream position plus `(snapshot: <table> <i>/<n>)`. The checkpoint file still holds the
  full value, which resuming needs. No config change.
- **MongoDB: a restart soon after the first start no longer skips changes** (#77). Without a checkpoint the change
  stream opens at "now", but nothing saved that position until the first event or 30 s heartbeat, so a restart before
  then opened at a later "now" and lost the changes in between. The opening position is now checkpointed before
  anything else is read. No config change.
- **A multi-table `TRUNCATE` now emits one event per table** (#55). `TRUNCATE a, b` is one WAL message, and only the
  first table's event was emitted, so consumers of the others never saw the truncate. No config change; expect extra
  `truncate` events (all with the same `lsn`) for statements that name several tables.
- **Postgres source errors now include the cause** (#18). A failed connect previously showed only
  `error performing TLS handshake`; it now reads e.g. `...: invalid peer certificate: UnknownIssuer`. This text appears in
  logs and the cockpit's `last_error`. Server-side errors add only the SQLSTATE and message, never `DETAIL`/`HINT`.
- **`partition_by = "key"` no longer hashes the whole row on `REPLICA IDENTITY FULL` tables** (#45). Under FULL every
  column is flagged as identity, so each update produced a new key and could move the row to another partition. Those
  tables now use the `schema.table` key in `key` mode (per-table order, as with `partition_by = "table"`). A key column
  stored as unchanged TOAST is now read from the update's old key, so the row keeps its insert key; if an update carries
  no old value for it, that event uses the `schema.table` key. Other identities are unchanged. **Upgrade:** events of
  FULL tables that are still unconsumed sit on their old hash partitions while new ones go to the `schema.table`
  partition, so a consumer reading partitions in parallel can see one reorder window at the cutover. Drain the topic
  first, or accept it. The same window opens whenever you run `ALTER TABLE ... REPLICA IDENTITY` on a streaming table.
  An `info` log names each FULL table the first time its Relation message is seen (once per process); it only matters if
  you use `partition_by = "key"`.

Details: [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) (config reference and the "Securing connections" checklist).
