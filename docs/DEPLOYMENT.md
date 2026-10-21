# Deployment guide

Aqueduct runs only as a container. This guide covers preparing Postgres, running the image and operating it.
Settings are in [CONFIGURATION.md](CONFIGURATION.md), the web UI in [COCKPIT.md](COCKPIT.md), failure handling in [RUNBOOK.md](RUNBOOK.md).

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

Tables without a primary key and with the default identity reject `UPDATE`/`DELETE` once they are published; set `REPLICA IDENTITY FULL` or add a key. Records are keyed by the primary key (see [Kafka sink](CONFIGURATION.md#kafka-sink)), so give tables one: a table without a
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
- Port 8080: cockpit, API and health probe. `HEALTHCHECK` runs `aqueduct --healthcheck`, which calls `/healthz` on the
  port of `health_addr`, read from the same config file (`AQUEDUCT_CONFIG`): a wildcard bind (`0.0.0.0`, `[::]`) is
  probed on `127.0.0.1` / `[::1]`, any other address as is; without a readable config file it probes `127.0.0.1:8080`.
  `AQUEDUCT_HEALTH_PROBE=<host:port>` overrides the address. It prints nothing; only the exit code counts.
- Logs are JSON lines on stdout/stderr (`RUST_LOG=debug` for more detail).

## 3. Operating Aqueduct

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
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -X POST http://host:8080/api/snapshot \
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
  drop or rename of the collection, or a dropped database drops the whole chunk; a change without a `documentKey._id`
  makes the chunk be read again.
- **Renames onto a collection:** a rename of another collection onto the collection being copied (`renameCollection`
  with `dropTarget`, or an `$out` aggregation) replaces its documents without any insert events, so its copy starts
  again at the first `_id`, whether a chunk is in flight or not, and the reset cursor is checkpointed at once. Documents
  copied before the rename are sent again (duplicates, never gaps). A collection still queued needs nothing; one whose
  copy already finished is not copied again (request a new snapshot of it). With a `collections` list, the change
  stream also asks for renames into a watched collection and `dropDatabase` for this (neither is forwarded).
- **Admin requests** while a cancelled snapshot finishes its last chunk get `409` ("try again in a moment").
- **Checkpoint:** while a snapshot runs, the checkpoint file holds `{"pos": <resume token>, "snap": {...}}`; the last
  `_id` is kept as canonical extended JSON, so its BSON type survives a restart.

## 4. Troubleshooting

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
