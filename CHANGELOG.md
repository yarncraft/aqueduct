# Changelog

Notable changes to Aqueduct. Newest first.

## Unreleased

### Breaking changes and upgrade notes

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

- **Cockpit replication-health tiles** (#54, UI half). Slot lag, last-heartbeat age and skipped heartbeats, read from `slot_lag_bytes`, `confirmed_flush_lsn` and `heartbeat` in the `/api/metrics` and `/api/stream` snapshots. Each tile shows `–` until the server reports the field, so this ships independently of the backend half.
- **Postgres idle heartbeat** (#47). Writes to tables outside the publication no longer pin WAL: every `heartbeat_interval_secs`
  (default 10, `0` disables) an idle source advances the slot to the server's WAL position, never past an event that is
  not yet checkpointed. Optional `heartbeat_action_query` for servers that need a write to move the slot. No action needed
  on upgrade; see DEPLOYMENT.md for the heartbeat-table recipe.

### Changed

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
