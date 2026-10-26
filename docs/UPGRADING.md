# Upgrading and rolling back

How to move Aqueduct to a new image and back again without losing the replication slot or the checkpoint.

0.2.0 is the first release, so it carries every entry in
[`changelog/unreleased/breaking/`](../changelog/unreleased/breaking/). Read each one that is newer than the build you run
now. This page gives the procedure, the checks for the three newest changes (#89, #213, #214), and the extra steps to
[roll back past #94 or #63](#rolling-back-past-94-or-63), which changed the checkpoint format.

## What carries state

| State | Where | Survives an upgrade or rollback when you ... |
|---|---|---|
| Replication slot (Postgres) | inside Postgres, named by `[postgres] slot` | keep `slot` unchanged and never drop it |
| Checkpoint (last delivered position) | `checkpoint_path`, default `/var/lib/aqueduct/checkpoint`, on the state volume | mount the same volume and leave the file alone |
| Resume token (MongoDB) | the same checkpoint file | the same |
| Topics and their records | Kafka | nothing to do |

Neither #89, #213 nor #214 changes the checkpoint format or how the slot is used, so a build from before them and a
build with them read each other's checkpoint. #94 and #63 did change it: builds before them refuse the newer checkpoint
and do not start, see [Rolling back past #94 or #63](#rolling-back-past-94-or-63).

## Before you upgrade

| Change | You are affected if ... | Do this before upgrading |
|---|---|---|
| **#213** The Kafka record timestamp is the produce time, not the commit time | a consumer reads the record timestamp as the time of the change: Kafka Streams or ksqlDB windows (`ROWTIME`), `offsetsForTimes` lookups, time-based retention or tiering you reason about | Read `source.ts_ms` from the event instead. Or set `[sink] record_timestamp = "commit"` to keep the old behaviour; then a backlog replayed after a long outage can again be deleted by `retention.ms` on `CreateTime` topics, so keep retention well above your longest outage |
| **#214** MongoDB refuses plaintext to a remote host | `[source] type = "mongodb"`, a `mongodb://` URI, a host other than `localhost` or a loopback address, and no `tls=` or `ssl=` option | Add `tls=true` (and `tlsCAFile` for a private CA). To keep plaintext, add `tls=false`; it logs a warning. `mongodb+srv://` URIs are not affected |
| **#89** FULL tables carry their unchanged large (TOAST) columns | a Postgres table with `REPLICA IDENTITY FULL` has `text`, `jsonb`, `bytea` or similar columns large enough to be stored out of line | Nothing, unless a consumer or route depended on those columns being missing. A route with `unchanged = "dead_letter"` no longer dead-letters those updates |

Find the FULL tables in the publication (use your `[postgres] publication` value instead of `aqueduct`):

```sql
SELECT format('%I.%I', n.nspname, c.relname) FROM pg_publication_tables p
JOIN pg_namespace n ON n.nspname = p.schemaname
JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = p.tablename
WHERE p.pubname = 'aqueduct' AND c.relreplident = 'f';
```

Upgrade order for #213: change the consumers first, then Aqueduct. The topic then holds commit-time records followed by
produce-time records, and the unacknowledged tail that the new image replays after the switch carries the produce time.

## Upgrade

1. Read every entry in `breaking/` that is newer than the image you run, and make the config changes above.
2. Optional (Postgres): wait until `slot_lag_bytes` on `/api/metrics` (`aqueduct_slot_lag_bytes` on `/metrics`) is near
   zero, so the switch replays almost nothing.
3. Note the current position, so you can tell later that nothing went backwards:
   - `GET /api/metrics` → `checkpoint` (with `Authorization: Bearer <token>`)
   - `SELECT slot_name, active, confirmed_flush_lsn FROM pg_replication_slots;`
4. Stop Aqueduct gracefully: `docker stop -t 30` (the compose files allow 30 s already), or a Kubernetes rollout with
   `strategy: Recreate`. Never run the old and the new image at the same time on one slot: a rolling update does that.
5. Start the new image, pinned by digest, with the same state volume and the same config.
6. Check:
   - `GET /readyz` returns `200` and the cockpit state is `streaming`.
   - `pg_replication_slots` still has exactly one slot with that name, and its `confirmed_flush_lsn` moves forward as
     changes are delivered. A second slot means `slot` changed: stop and fix the config. `active` is `t` while
     Aqueduct is connected.
   - The `checkpoint` in `/api/metrics` moves forward from the value you noted.
   - The logs have no `ERROR`. A warning about a checkpoint without origin fields is expected once, after an upgrade
     from a build before #63.

Delivery stays at-least-once across the switch: events after the last checkpoint are delivered again, never skipped.

## Roll back

Rolling back is the same procedure in the other direction. The state stays where it is.

1. Stop the new image gracefully, as in step 4 above.
2. Postgres source only: if the previous build is older than #63, prepare the checkpoint first, see
   [Rolling back past #94 or #63](#rolling-back-past-94-or-63).
3. Start the previous image, pinned by its digest, on **the same state volume** with **the same `slot`**.
4. Check as in step 6 above.

Do not:
- delete the checkpoint file or the state volume, except as described in the next section,
- drop the slot, change `slot`, or set `allow_slot_recreate`,
- start the previous image while the new one still runs.

Rolling back past any entry in `breaking/` undoes that entry too. For example, a build before #138 publishes the old
event format, which every consumer reads. Check each entry between the previous build and this one.

### Rolling back past #94 or #63

**Postgres sources only.** #94 and #63 do not change the MongoDB checkpoint, so never edit or delete it for a MongoDB
source: that file holds the resume token, there is no slot behind it, and without it the stream starts at the current
position and silently skips everything in between.

**Rolling back past #94 brings back a data-loss bug.** Builds before #94 drop rows when transactions overlap, on
multi-row `COPY` and on multi-table `TRUNCATE`. Do it only if you accept that, and after rolling forward again,
re-snapshot or backfill the tables those statements touched in the meantime. Builds before #63 also no longer refuse a
checkpoint from another cluster, database or timeline.

Find out where the previous build stands, from its commit:

```sh
git merge-base --is-ancestor 8573148 <commit> && echo "has #63"   # checkpoint origin check
git merge-base --is-ancestor 45fd45b <commit> && echo "has #94"   # (commit, change, ord) checkpoint
```

| Previous build | It reads | Before starting it |
|---|---|---|
| has #63 | the current checkpoint | nothing |
| has #94, not #63 | `~<commit>@<change>#<ord>` | strip `;sys=` and everything after it from the checkpoint file. Otherwise it stops with `not a Postgres checkpoint` |
| before #94 | a plain LSN | delete the checkpoint file. Otherwise it stops with `not a Postgres LSN` |

These are the only cases where you change the checkpoint. The image is distroless, so edit the file from a helper
container on the same volume, as the same user (uid 65532), while Aqueduct is stopped:

```sh
# has #94, not #63: keep the key, drop the origin fields
docker run --rm --user 65532:65532 -v <state-volume>:/var/lib/aqueduct busybox sed -i 's/;sys=.*//' /var/lib/aqueduct/checkpoint
# before #94: remove the checkpoint
docker run --rm --user 65532:65532 -v <state-volume>:/var/lib/aqueduct busybox rm /var/lib/aqueduct/checkpoint
```

Nothing is skipped either way. A stripped checkpoint resumes from the same position. Without a checkpoint, the slot's
`confirmed_flush_lsn` decides where the stream resumes. The slot never moves past an undelivered event, so events
after the last checkpoint are delivered again. Do not drop or recreate the slot: it is what keeps this safe.

Rolling forward again needs nothing extra. Current builds accept both older formats (a plain LSN replays from the slot,
and a checkpoint without origin fields logs one warning).

What changes back when you roll back past these three:

| Change | After the rollback |
|---|---|
| #213 | Records carry the commit time again. Older builds silently ignore `record_timestamp`, so remove it to avoid confusion. The topic now holds produce-time records followed by commit-time records. If a backlog is replayed at the switch, its records carry their older commit times, so timestamps can go backwards there, and retention can again delete that backlog |
| #214 | Plaintext to a remote MongoDB host is accepted again without a warning. A `tls=true` or `tls=false` you added keeps working |
| #89 | Updates on FULL tables omit unchanged TOAST columns again, and `unchanged = "dead_letter"` routes dead-letter them again |

## Tested

Rolled back once and forward again on 2026-10-23, with images built from `ca8d048` (new) and `4c6b284` (main just
before #89, the last build without the three changes), against Postgres 16 and Kafka 3.8 in Podman. Config as in
`config/aqueduct.example.toml`, poll transport, `record_timestamp = "produce"` set throughout.

| Step | Result |
|---|---|
| new image, 100 inserts, graceful stop | checkpoint `~26383696@26383568#0;sys=…;db=16384;tli=1` |
| 100 inserts while stopped, then the **old** image on the same volume and slot, 100 inserts and 10 updates | starts without a warning, reads the new checkpoint, delivers the 200 rows; ignores `record_timestamp` |
| 100 inserts while stopped, then the new image again, 100 inserts | reads the old image's checkpoint and resumes |
| Topic audit | 500 of 500 inserted ids and the 10 updates, no gaps, no duplicates. Records 0–99 and 310–509 carry the produce time, 100–309 the commit time (`= source.ts_ms`) |
| Slot | one slot `aqueduct` throughout; `confirmed_flush_lsn` only moved forward (`0/19295B8` → `0/19766E0` → `0/19829B0`) |

Not tested: a MongoDB source, `transport = "stream"`, a backlog at the switch (so timestamps going backwards after a
rollback is reasoned, not observed), and rolling back past #94 or #63. The checkpoint edit above was checked on a
copy of a real checkpoint file, as uid 65532: the file keeps its owner and only the key is left.
