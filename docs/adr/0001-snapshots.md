# ADR 0001: Initial and incremental snapshots

- Status: proposed
- Issue: #46
- Deciders: Lucas (approval), Megatron (review), Perceptor (author)
- Date: 2026-10-19

## Context

Aqueduct only streams changes made after its slot (Postgres) or change stream (MongoDB) exists, so it
cannot bootstrap a table that already has rows. #46 asks for a consistent initial snapshot and resumable,
chunked re-snapshots of selected tables, Postgres first, without weakening at-least-once.

Constraints from the code as of `e57a7b3`:

- The Postgres source reads the slot over a **plain SQL connection** with
  `pg_logical_slot_peek_binary_changes` and advances it with `pg_replication_slot_advance`
  (`aqueduct-source-postgres/src/source.rs`). `replication=database` is ruled out (CLAUDE.md: tokio-postgres
  rejects it). The replication-protocol command `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT` is therefore
  not available, and `pg_create_logical_replication_slot()` exports no snapshot.
- The pipeline checkpoints one `Position` per batch (the last event's), after `send_batch` + `flush`, then
  calls `ack` (`aqueduct-core/src/pipeline.rs`). Sources own their position encoding.
- The event's `position` is on the wire: JSON field `lsn` and Kafka header `lsn`.
- Postgres 10+ is supported (DEPLOYMENT.md); the role has `REPLICATION` + `SELECT` on published tables.
  MongoDB needs a replica set; the role has `read`.

## Decision

### 1. One mechanism: watermark-based chunked snapshots (DBLog), for initial and incremental

A snapshot of a table is a sequence of **chunks**, each read while the live stream keeps flowing. Per chunk
the source:

1. writes a **low watermark** into the change stream,
2. reads the next chunk with keyset pagination: `SELECT ... WHERE (pk) > ($last) ORDER BY pk LIMIT $n`,
3. writes a **high watermark**,
4. keeps consuming the stream. Between low and high it emits live events as usual and **drops every
   chunk row whose primary key appears in a live event** (insert, update with old or new key, delete).
   A `TRUNCATE` of the table in the window drops the whole chunk.
5. On reaching the high watermark it emits the surviving chunk rows, then continues with the stream.

Why this is correct: the chunk's statement snapshot is taken after the low watermark commits and before
the high one, so every change to a key that the chunk might have missed or seen half of is a live event in
the window. The last such event carries the row's state as of the chunk read or newer, so the dropped row is
already on the stream (or deleted). Rows not touched in the
window are exactly as the chunk read them, and every later change arrives after them on the stream. Rows
inserted after the chunk are delivered by the stream. Nothing is lost, and per-key order never regresses.

An **initial snapshot** is the same thing run over every table in the publication (or every watched
collection) on first start. There is no separate "export snapshot then stream" mode.

Chosen over `EXPORT_SNAPSHOT` / `SET TRANSACTION SNAPSHOT` because:

- it is not reachable from the SQL interface we are committed to;
- it holds one transaction open for the whole snapshot: xmin is pinned (vacuum blocked), and on a crash
  the whole snapshot restarts, because the exported snapshot dies with the connection;
- it covers only the initial case; incremental re-snapshots need watermarks anyway.

Only one chunk is in flight at a time, so memory is bounded by `snapshot_chunk_size` rows.

### 2. Watermarks

- **Postgres:** `pg_logical_emit_message(true, 'aqueduct', '<json>')`. It is transactional, so it is
  decoded in commit order with the data. The peek query adds `'messages', 'true'`, and the decoder learns
  the `M` message (it is `Ignored` today). This needs **Postgres 14+** for snapshots only; streaming keeps
  supporting 10+. No table and no publication change are needed. The payload carries
  `{slot, snapshot_id, chunk, mark}`, and the source ignores messages for other slots, so several Aqueduct
  instances on one database do not see each other's watermarks. Before implementation, verify which
  privilege `pg_logical_emit_message` needs on 14–17 and on RDS/Cloud SQL. The role already has
  `REPLICATION`.
- **MongoDB:** an insert into `<database>.aqueduct_watermarks` with `_id = "<snapshot_id>:<chunk>:<mark>"`.
  The source adds that collection to its `$match`, recognises these events before `map_event`, never
  forwards them, and deletes each watermark document when the snapshot ends (a TTL index is the safety net).
  This needs `insert` + `remove` on that one collection, but only when snapshots are used.

Rejected for MongoDB: snapshot reads at `atClusterTime` with no write. Locating the handoff point needs the
cluster time inside the opaque resume token, and the read is limited by `minSnapshotHistoryWindowInSeconds`.

### 3. Event shape

- New `Op::Read`, serialised `"read"` in JSON and in the Kafka `op` header (consistent with
  `insert`/`update`/`delete`). `before = null`, `after` = the full row, `xid = 0`,
  `commit_ts_micros` = the chunk read time.
- `position` = the high-watermark LSN (or token), so the wire `lsn` stays a real stream position. Like every
  Postgres `lsn`, it is not monotonic across transactions (ADR 0003 §4).
- **Postgres checkpoint key:** a `read` row gets K = `(hw_commit, hw_lsn, ord)` (ADR 0003 §4), where `hw_commit` is
  the `final_lsn` of the high watermark's transaction (`pg_logical_emit_message(true, ..)` is transactional),
  `hw_lsn` is the message's LSN, and `ord` is the row's index among the rows emitted at that watermark. So
  `read` rows sort after every live event of the window and before the next transaction, and every row has its
  own K.
- **Row encoding must be the decoder's.** The chunk `SELECT` casts each published column to `::text` and
  feeds it through the same `push_text(type_oid, ..)` as pgoutput tuples, so a `read` row and an
  `insert` of the same row are byte-identical. Column set = the publication's column list
  (`pg_publication_tables.attnames`, PG15+). If the publication has a row filter, it becomes the
  chunk's `WHERE` (PG15+).
- `key` = computed exactly as for live events of that table, so `read` rows land on the same partition as
  the row's later changes in `partition_by = "key"` mode. Under `REPLICA IDENTITY FULL` that inherits #45.
  Snapshot key mode depends on #45 landing (or ships documented as per-table order only).
- Consumers tell snapshot rows apart by `op == "read"`. A snapshot row can follow live events for the same
  key only when they did not touch it in its window, so "last write wins" consumers converge.

### 4. Checkpoint and resumability

Snapshot progress must be durable in the **same atomic write** as the stream position, so there is no
second file that can disagree after a crash.

- `ChangeEvent` gets `#[serde(skip)] pub checkpoint: Option<Position>`. The pipeline saves and acks
  `last.checkpoint.unwrap_or(last.position)`. Nothing else in core changes, and the wire format is untouched.
- While a snapshot is active, every event the source emits (live and `read`) carries
  `checkpoint = Position::Text(json)`, with the shape
  `{"pos": <K | token>, "snap": {"id", "tables": [...], "table": i, "after": [pk values] | null}}`, where
  `pos` is the Postgres K text `"<commit>@<change>#<ord>"` (ADR 0003 §4) or the MongoDB resume token, and
  `after` is the last PK of the last **fully emitted** chunk. When the snapshot completes, the source emits a
  heartbeat with the plain K (Postgres) or token (MongoDB), so the checkpoint returns to the non-snapshot
  format.
- `start()` accepts the plain-LSN format (duplicates-only replay, ADR 0003 §4), the K text and the snapshot
  JSON. `ack()` parses `pos` out of it.

Crash cases (Ratchet's kill points):

| Crash point | On restart |
|---|---|
| before the first chunk's checkpoint | the snapshot request is not durable yet; `initial` re-triggers because there is still no checkpoint, and an ad-hoc request must be re-sent (the API says so) |
| inside a chunk window (after low, before high is checkpointed) | stream replays from `pos`; stale watermarks are ignored (not the pending chunk); the chunk after `after` is redone. Duplicates only |
| after chunk rows reach the sink, before `save` | same as above: rows re-read and re-sent. Duplicates only |
| between snapshot end and the plain-position heartbeat | the last chunk is redone, finds nothing more, and ends. Duplicates only |

The slot needs no special protection: chunks read **current** table state, so advancing the slot never
makes a row unreachable. Every chunk is acked like any batch, and WAL is retained only as long as the sink
lags, exactly as today. #47's rule (never confirm past the checkpoint) is unaffected.

### 5. Triggering

- `snapshot = "never" | "initial"` in `[postgres]`/`[source]`, default **`never`** (no behaviour change on
  upgrade). `initial` runs a snapshot of every published table / watched collection when there is no
  checkpoint.
- Ad-hoc incremental snapshots via the admin server, behind the Bearer token:
  - `POST /api/snapshot {"tables": ["public.orders"]}` → `202` (queued). It is rejected when a table is not
    in the publication or watch list.
  - `GET /api/snapshot` → state, current table, rows done / estimated.
  - `DELETE /api/snapshot` → cancel after the current chunk.
  Admin hands requests to the source over an `mpsc` channel that the source drains in `next()`. One
  snapshot runs at a time, and later requests are appended to its table list.
- No signal table in v1 (Debezium-style). It can be added later on top of the same channel.

### 6. Chunking and tables without a primary key

- Keyset pagination on the primary key (`pg_index.indisprimary`; this catalog helper is shared with #45),
  `snapshot_chunk_size` default 1024.
- **No PK:** v1 skips the table with a `WARN` and marks it `skipped: no primary key` in the status.
  Follow-up: a per-table key-column override. A unique `NOT NULL` index is a candidate too.
- MongoDB: chunk on `_id` (always present and indexed). `$gt` is type-bracketed, so a collection with
  mixed `_id` BSON types must paginate by index bounds (`hint({_id: 1})` + `min`) and skip the
  equal key.

### 7. Operations

- Throttle: `snapshot_chunk_size` plus optional `snapshot_max_rows_per_sec` (unset = unthrottled). A chunk
  goes through the normal batch → sink → checkpoint loop, so a slow sink back-pressures the snapshot.
- Metrics: `aqueduct_snapshot_rows_total`, `..._tables_done`, `..._tables_total`, `..._rows_estimated`
  (`pg_class.reltuples` / `estimatedDocumentCount`), and the state, in `/api/metrics` and Prometheus. `read`
  events count in per-table ops as `read`. Cockpit: a progress card (Bumblebee, after approval).
- WAL growth: no long transaction, 2 small WAL records per chunk; retention behaves as in live streaming.
- TOAST caveat (unchanged from live): under the default replica identity, a live update in the window
  omits unchanged TOASTed columns, and that event replaces the chunk row. DEPLOYMENT.md already
  recommends `FULL` for complete images.

## Consequences

- Positive: one algorithm for initial and incremental, for both sources. No long transactions. Resumable
  per chunk. The slot keeps advancing during a snapshot. No core trait change besides one skipped field
  and one `Op`.
- Negative: snapshots need Postgres 14+. MongoDB needs write access to one collection. Tables without a PK
  are not snapshotted in v1. Key-mode ordering for FULL tables waits on #45. Snapshot rows go to the same
  topics as live events (consumers that cannot handle `read` must filter on `op`).
- Breaking: none by default (`snapshot = "never"`). `Op::Read` is a new value consumers may see once
  they enable snapshots; that goes in the CHANGELOG.

## Implementation issues (after approval)

1. **core:** `Op::Read`, `ChangeEvent.checkpoint`, pipeline saves/acks it, metrics and Kafka header handle
   `read`. Unit tests.
2. **pg catalog:** publication tables, column lists, row filters, PK columns, `reltuples` (shared with #45).
3. **pg decoder:** `M` (logical message) decoding; peek with `messages=true` when snapshots are enabled.
4. **pg snapshot engine:** watermark emit, chunk query + text encoding via `push_text`, window dedup, emit at
   high, composite checkpoint encode/decode in `start`/`ack`, resume. Unit tests for the window logic
   (pure function over a scripted stream).
5. **config + trigger:** `snapshot`, `snapshot_chunk_size`, `snapshot_max_rows_per_sec`, the `initial`
   trigger, and the PG <14 startup error when a snapshot is requested.
6. **admin API:** `POST/GET/DELETE /api/snapshot`, the channel to the source, metrics and Prometheus,
   tests in `admin.rs`.
7. **integration tests (Ratchet):** pre-populated table arrives complete; live updates/deletes during the
   snapshot never regress; crash at each kill point above resumes without loss.
8. **MongoDB snapshot engine:** watermark collection, `_id` chunking, the same window logic, and the same
   tests.
9. **docs:** DEPLOYMENT.md (keys, PG14+, privileges, `read` op), README, CHANGELOG.
10. **cockpit:** snapshot progress card (Bumblebee).

Order: 1 → 2/3 → 4 → 5 → 6, with 7 alongside 4. 8 can start after 1 and reuses the window logic from 4.

## Open questions for approval

1. Default `snapshot = "never"` (safe upgrade) or `initial` (better first run)? Recommendation: `never`.
2. Is "snapshots need Postgres 14+" acceptable? The alternative for 10–13 is a watermark table added to the
   publication, which needs write access and a publication change. Recommendation: 14+ only.
3. Is write access to `aqueduct_watermarks` acceptable for MongoDB snapshots? Recommendation: yes, opt-in.
