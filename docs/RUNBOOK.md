# Aqueduct operations runbook

Failure handling, recovery and disaster recovery. Read `docs/DEPLOYMENT.md` first for the delivery model.
Chaos scenarios that exercise the cases below live in `scripts/chaos/` (`scripts/chaos/run.sh`).

**Delivery contract:** at-least-once. After any crash, the events of at most the in-flight transaction are replayed
(deduplicate on `lsn` plus row key). Gaps are never expected; a gap is a bug or a lost slot (section 4).

**State that matters**

| State | Where | If lost |
|-------|-------|---------|
| Replication slot | Postgres | Position and unconsumed changes are gone. **Data loss.** See section 4 |
| Checkpoint file | `/var/lib/aqueduct/checkpoint` | Harmless: the slot decides what is pending; at most replay |

## 1. Failure matrix

| Event | What happens | Operator action |
|-------|--------------|-----------------|
| Container `kill -9` / OOM / node reboot | Restarts (`restart: unless-stopped` or orchestrator). Resumes from slot + checkpoint. Replay of the in-flight transaction possible | None |
| Postgres restart or brief outage | The source has no reconnect loop: the replication connection breaks, the pipeline fails, the process exits non-zero and is restarted by the orchestrator, retrying until Postgres is back | None; make sure the restart policy is on, and alert if restarts loop |
| Kafka down | Sink send fails, pipeline fails, process restarts and retries. Nothing is acknowledged, WAL accumulates in Postgres | Restore the sink; watch slot lag (section 3) |
| State volume full | Checkpoint or file-sink write fails, pipeline stops without acknowledging; slot is not advanced | Free space; the process recovers on its own |
| Checkpoint file deleted | Slot position is authoritative; replay limited to un-advanced transactions | None |
| Checkpoint file corrupt (unparseable) | Startup fails with a checkpoint error, crash loop | Delete the file (replay from slot) and restart |
| Rolled back to a version older than the commit-order key (#94): the older binary refuses the new checkpoint (`not a Postgres LSN`) and crash-loops | Startup fails with a checkpoint error | Delete the checkpoint file (section "Corrupt checkpoint") and accept a replay from the slot |
| Replication slot dropped, checkpoint present | With the `feat/hardening` guard: refuses to start. Without it: a new slot is created from "now" and the gap is silent | Section 4 |
| Two instances on one slot | The second cannot start: the first holds the slot, and the second retries for 15 s, fails and is restarted | Never do this; one instance per slot |
| MongoDB: watched database dropped (the change stream is invalidated) | The source fails with `MongoDB invalidated the change stream`, the process exits non-zero, and every restart fails the same way: nothing is captured until the checkpoint is reset | Section "MongoDB change stream invalidated" |
| MongoDB: a change event over 16 MB (`BSONObjectTooLarge`, code 10334) | The source fails with `larger than MongoDB's 16 MB document limit` at the same event on every restart | Section "MongoDB change event larger than 16 MB" |

## 2. Recovery procedures

### Restart after a crash loop
1. `docker logs aqueduct --tail 100`: logs are JSON; look for `pipeline failed` and the error text.
2. Fix the cause (sink reachable, Postgres reachable, disk space, config), then `docker restart aqueduct`.

### Corrupt checkpoint
```sh
docker stop aqueduct
docker run --rm -v aqueduct-state:/v busybox rm -f /v/checkpoint
docker start aqueduct       # resumes from the slot; may replay recent transactions
```

### Deliberately reset a slot (accepting loss of everything not yet delivered)
Only when the pipeline's backlog is unwanted, or after a restore (section 4).
```sh
docker stop aqueduct
psql -c "SELECT pg_drop_replication_slot('aqueduct');"
docker run --rm -v aqueduct-state:/v busybox rm -f /v/checkpoint
# acknowledge the slot recreation explicitly (guard from feat/hardening):
#   [postgres] allow_slot_recreate = true     (remove again after the first start)
docker start aqueduct       # creates a new slot from "now"
```
Anything committed between the drop and the new slot is not delivered: take a snapshot/backfill of the affected tables
from the database for consumers that need it (Aqueduct has no initial snapshot).

### Stuck on a poison event (oversized Kafka record)
The pipeline stops on non-retryable errors by design (no silent skip). Fix the receiver or the event source, then restart.
Skipping requires advancing the slot manually and is data loss for that event:
`SELECT pg_replication_slot_advance('aqueduct', '<lsn after the bad txn>');` with Aqueduct stopped.

### MongoDB change stream invalidated
Dropping the watched database (also when it is recreated right after) makes MongoDB close the change stream with an
`invalidate` event. Aqueduct does not skip past it on its own: the log shows `pipeline failed` with
`MongoDB invalidated the change stream of database "app" at resume token {"_data":"82..."}`, and each restart replays
to the same event and fails again. Nothing done to the database after the drop is captured until you choose:

1. Stop Aqueduct and make sure the database exists again (with the collections you expect).
2. Either resume right after the invalidate, so that everything done since the database was recreated is delivered:
   write the token from the error into the checkpoint, prefixed with `~` (the stream reopens with `startAfter`, which
   MongoDB allows for an invalidate token):
   ```sh
   echo '~{"_data":"82..."}' > checkpoint     # the token from the error, prefixed with ~
   docker run --rm -v aqueduct-state:/v -v "$PWD:/in:ro" busybox cp /in/checkpoint /v/checkpoint
   ```
   or delete the checkpoint (section "Corrupt checkpoint") to start at "now": changes made between the recreation and
   the start are then not delivered, so copy the collections with a snapshot (`snapshot = "initial"` on that start, or
   `POST /api/snapshot`).
3. Start Aqueduct. A snapshot that was running is dropped by either reset; request it again.

Consumers still hold the documents of the dropped database: no delete or truncate events are sent for a drop.
When the error has no token (the server closed the stream without an `invalidate` event), only the second choice applies.

### MongoDB change event larger than 16 MB
A change event carries the whole document (`updateLookup`) and, with `pre_images = true`, its pre-image too, so a
document near the 16 MB BSON limit can make an event the server cannot return: every getMore fails with
`BSONObjectTooLarge` (code 10334), and Aqueduct fails with `a change event ... is larger than MongoDB's 16 MB
document limit` at the same place on every restart. The error names the resume token after which the event lies.

- Set `pre_images = false` in `[source]` and restart: the event loses `fullDocumentBeforeChange`, which is usually
  enough. Deletes then carry only the `documentKey`, updates no `before`.
- Or shrink the document: `updateLookup` reads the document as it is now, so a smaller current version unblocks an
  update event (not an insert or a pre-image).
- If neither helps (an insert of a document close to 16 MB), the event can only be skipped: delete the checkpoint and
  start at "now", which loses every change since the checkpoint, then snapshot the affected collections.

MongoDB 7.0+ can split such events with `$changeStreamSplitLargeEvent`; Aqueduct does not use it yet.

## 3. Monitoring

### Slot lag and WAL retention (alert on these in Postgres)
```sql
-- bytes of WAL the slot is holding back; alert above your disk headroom (for example 1 GB, and rising)
SELECT slot_name, active, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag,
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)               AS lag_bytes
FROM pg_replication_slots WHERE slot_type = 'logical';
```
- `wal_status = 'lost'` means the slot was invalidated (`max_slot_wal_keep_size` exceeded): treat as slot loss (section 4).
  Alert on `unreserved` too: invalidation is imminent.
- Set `max_slot_wal_keep_size` (PG13+). It protects the database disk at the price of invalidating a stuck slot.
  Size it to the longest outage you want to survive without data loss.
- `active` is `true` while Aqueduct is connected: its replication connection holds the slot. `false` for longer than a
  restart means it is down. Alert on that, and on lag growth plus no recent progress.

### Aqueduct signals
| Signal | Alert when |
|--------|------------|
| `GET /healthz` != 200 / container unhealthy | 2 consecutive failures |
| Container restart count | more than 3 in 10 minutes (crash loop) |
| `GET /api/metrics`: `state` | not `streaming` for more than 1 minute |
| `GET /api/metrics`: error count / `last_error` | increases |
| Throughput stays 0 while slot lag grows | 5 minutes |
| Sink latency (p95) | sustained above your SLO |
| State volume free space | below 20% |

`GET /metrics` serves the same signals in Prometheus format (Bearer token required when `admin_token` is set):
`aqueduct_state{state=...}`, `aqueduct_errors_total`, `aqueduct_events_total{op=...}`, `aqueduct_table_events_total{table=...}`,
`aqueduct_lag_seconds`, `aqueduct_last_event_timestamp_seconds`, `aqueduct_sink_latency_seconds` and `aqueduct_uptime_seconds`
(see `prometheus()` in `crates/aqueduct/src/admin.rs`). Use `GET /readyz` (`503` unless streaming) as the readiness probe.
With OTLP export on ([DEPLOYMENT.md](DEPLOYMENT.md#opentelemetry-otlp)) the same series, under the same names, are pushed to
your OpenTelemetry Collector, so these alerts can live in your OTel backend instead.

## 4. Disaster recovery

### Where the guarantees stop
The slot lives inside Postgres. Anything that loses or rewinds Postgres state (failover to a replica without the slot,
restore from backup, `pg_basebackup`, major upgrade by dump/restore, slot invalidation) loses the slot, and the changes
between the last delivered LSN and the new slot position are **not recoverable from Aqueduct**. Recovery means
re-syncing consumers from the database, then starting a fresh stream.

### Postgres failover to a replica
- Physical replicas do **not** carry logical slots before PG17. After promotion the slot is missing.
- PG17+ with failover slots: create the slot with `failover = true` (`pg_create_logical_replication_slot(..., failover => true)`),
  set `sync_replication_slots = on` and `primary_slot_name` on the standby, and keep `hot_standby_feedback = on`. Aqueduct creates its slot
  without this flag today: create the slot yourself with `failover => true` before the first start, Aqueduct then reuses it.
  Verify on the standby: `SELECT slot_name, synced FROM pg_replication_slots;` before relying on it.
- Procedure after an unsynced failover:
  1. Keep Aqueduct stopped (the guard refuses to start without the slot, which is what you want).
  2. Promote, point `AQUEDUCT_PG_URL` at the new primary. Confirm the publication exists (publications replicate physically).
  3. Decide the recovery point: last delivered change = the value in the checkpoint file (`cat /var/lib/aqueduct/checkpoint`, `<commit>@<change>#<ord>`, decimal LSNs; older versions wrote a plain LSN) or the newest event at the sink.
  4. Re-sync consumers for rows changed since then (`updated_at > ...` backfill, or full table reload), then reset the slot (section 2) and start.
  5. Make consumers idempotent on key during the overlap.

### Restore Postgres from backup / PITR
Same as failover: the slot is gone, the restored database may be *behind* what consumers already received.
Consumers can hold changes the restored database never had. Reset the slot, decide per consumer whether to roll back or reload, then start.

### Backup of Aqueduct state
- The checkpoint is a single small file and is optional for correctness; back it up with the volume snapshot if convenient.
  Do not restore an old checkpoint over a newer slot position blindly: the source skips every change at or below the checkpoint LSN,
  so a checkpoint **ahead** of the real delivery position (restored from a different cluster/timeline) silently drops events.
  When restoring a database from backup, delete the checkpoint.
- Config and secrets: keep the TOML, `AQUEDUCT_PG_URL` and `AQUEDUCT_ADMIN_TOKEN` in your secret store, not only on the host.

### Region / host loss
Aqueduct is stateless apart from the above. Recreate the container anywhere that can reach Postgres and the sink and run
the failover procedure if Postgres moved. RTO is the container start time; RPO is zero while the slot survives, and is
"everything since the last delivered LSN, re-synced from the database" when it does not.

## 5. Planned maintenance
- **Upgrade Aqueduct:** `docker stop` (graceful, finishes the in-flight event), deploy the new image on the same volume, start.
  Checks before and after, and rollback: [UPGRADING.md](UPGRADING.md).
- **Postgres minor upgrade/restart:** nothing to do; Aqueduct restarts itself. Major upgrade: treat as a restore (slot is lost, section 4).
- **Decommission:** stop the container, then `SELECT pg_drop_replication_slot('aqueduct');`, or the slot holds WAL forever.
- **Drain before risky changes:** wait until `lag_bytes` is near 0 so a slot loss costs nothing.

## 6. Verifying this runbook

`scripts/chaos/run.sh` (needs Docker/Podman with compose v2, `curl`, `bash`) runs each scenario against the compose stack with a file sink
and checks for no gaps and bounded duplicates: `kill9`, `postgres-restart`, `sink-volume-full`, `checkpoint-delete`,
`checkpoint-corrupt`, `slot-loss`. Run one with `scripts/chaos/run.sh kill9`.
