# Postgres operations deep dive

Idle heartbeat, the checkpoint origin check and the streaming transport. The short version is [DEPLOYMENT.md](../DEPLOYMENT.md).

## Idle heartbeat

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

## Checkpoint origin check

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
remove the checkpoint and take a [snapshot](snapshots.md#snapshots). If you have verified that nothing was lost (a planned
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

## Streaming transport

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
