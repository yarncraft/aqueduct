# Snapshots deep dive

Copying rows that already exist, for Postgres and MongoDB. Short version: [DEPLOYMENT.md](../DEPLOYMENT.md).

## Snapshots

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

### Watermark table (Postgres 12 and 13)

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

### Starting a snapshot on request

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

### MongoDB snapshots

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
