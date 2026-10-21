//! Watermark-based snapshots of MongoDB collections (ADR 0001, #77).
//!
//! A snapshot copies each collection in `_id` chunks while the change stream keeps flowing. Per
//! chunk the engine inserts a low watermark document into [`WATERMARK_COLLECTION`], reads the
//! chunk, inserts a high watermark, and hands the chunk to the shared [`Window`]. Live changes of
//! the collection between the watermarks remove the documents they touch; at the high watermark
//! the surviving documents are emitted as `read` events. Documents are encoded by the same
//! [`map_event`] as live inserts, so a `read` event's `after` is byte-identical to an `insert` of
//! the same document. Its Kafka key is the `documentKey` the stream would send: `{_id}`, or on a
//! sharded collection (seen through `mongos`) the shard key fields from `config.collections` in key
//! order, then `_id`. The shard key is read best effort: without access to `config.collections`,
//! a sharded collection's `read` rows are keyed by `_id` alone and may land on another partition
//! than its live changes with `partition_by = "key"` (a warning says so).
//!
//! **Consistency.** Both watermarks are written with `w: majority` and the chunk is read with
//! `readConcern: majority` on the primary, all in one causally consistent session. So the read
//! sees at least everything up to the low watermark, and since it finishes before the high
//! watermark is written, every change it may have missed or seen half of is a live event between
//! the two watermarks on the stream (which only carries majority-committed changes, in oplog
//! order).
//!
//! **Pagination over mixed `_id` types.** `{_id: {$gt: x}}` only matches `_id`s of `x`'s BSON
//! type, so a collection with, say, numeric and string `_id`s would never get past the numbers.
//! Chunks instead walk the `_id` index itself: `hint({_id: 1})` plus `min({_id: x})` starts the
//! index scan at `x` in BSON comparison order, across types. `min` is inclusive, so the chunk asks
//! for one document more and drops the first one if it is the cursor document itself (compared by
//! canonical extended JSON, which keeps the BSON type). If that document was deleted and an equal
//! `_id` of another numeric type inserted, it is kept: a duplicate at worst, never a loss.
//!
//! **Failures.** A chunk read the server refuses (a command or write-concern error, a write
//! concern timeout) is retried with a growing pause; after [`MAX_CHUNK_FAILURES`] in a row the
//! collection is skipped with the error as the reason, so one collection cannot crash-loop the
//! whole source. Connection errors still fail the source, as for live reads.
//!
//! The engine does no streaming itself: `MongoSource` feeds it the live changes and watermarks it
//! reads and asks it for the next chunk when none is in flight.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aqueduct_core::{ChunkRow, Mark, Outcome, SkippedTable, SnapState, SnapshotStatus, Window};
use aqueduct_core::{Error, Result};
use mongodb::bson::{doc, Bson, DateTime, Document};
use mongodb::options::{
    Acknowledgment, Hint, ReadConcern, ReadPreference, SelectionCriteria, WriteConcern,
};
use mongodb::{Client, ClientSession};
use serde_json::value::{to_raw_value, RawValue};
use serde_json::Value;

use crate::mapping::map_event;

/// Where watermark documents go, in the watched database. Its change events are never forwarded.
pub const WATERMARK_COLLECTION: &str = "aqueduct_watermarks";

/// Watermark documents are deleted when their chunk closes; this TTL removes any a crash left.
pub const WATERMARK_TTL: Duration = Duration::from_secs(24 * 3600);

/// Name of the TTL index on [`WATERMARK_COLLECTION`].
pub const TTL_INDEX: &str = "aqueduct_ttl";

/// A watermark write waits this long for a majority, then the chunk counts as failed.
pub const WATERMARK_WTIMEOUT: Duration = Duration::from_secs(30);

/// A chunk read is cut off after this long, then the chunk counts as failed.
pub const CHUNK_MAX_TIME: Duration = Duration::from_secs(60);

/// Consecutive failed chunk reads of one collection before it is skipped.
pub const MAX_CHUNK_FAILURES: u32 = 3;

/// The write concern of watermark inserts: majority, so the chunk read that follows sees them,
/// bounded so an unreachable majority fails the chunk instead of stalling the stream.
pub fn watermark_write_concern() -> WriteConcern {
    WriteConcern::builder()
        .w(Acknowledgment::Majority)
        .w_timeout(WATERMARK_WTIMEOUT)
        .build()
}

/// Why a chunk could not be read.
#[derive(Debug)]
pub enum ChunkError {
    /// The server or the data refused it (counts toward skipping the collection).
    Failed(String),
    /// Anything else (connection, server selection): fails the source like a live read would.
    Fatal(Error),
}

/// Sorts a driver error: what the server answered about this request is a chunk failure;
/// trouble reaching the server is fatal.
pub fn chunk_error(e: mongodb::error::Error) -> ChunkError {
    use mongodb::error::ErrorKind;
    match *e.kind {
        ErrorKind::Command(_)
        | ErrorKind::Write(_)
        | ErrorKind::InvalidArgument { .. }
        | ErrorKind::BsonDeserialization(_) => ChunkError::Failed(e.to_string()),
        _ => ChunkError::Fatal(src_err(e)),
    }
}

/// What a failed chunk did to the snapshot.
#[derive(Debug, PartialEq, Eq)]
pub enum Failure {
    /// The chunk is read again after a pause.
    Retry,
    /// The collection was skipped; `state` is the new progress, `None` when nothing is left.
    Skipped { state: Option<SnapState> },
}

fn src_err<E: std::fmt::Display>(e: E) -> Error {
    Error::Source(e.to_string())
}

/// `db.coll`, as both the stream and the snapshot name a collection: one function, so the window
/// never misses a live event over a spelling difference.
pub fn table_id(db: &str, coll: &str) -> String {
    format!("{db}.{coll}")
}

/// A watermark as it appears in the change stream: an insert into [`WATERMARK_COLLECTION`] whose
/// `_id` is `"<snapshot_id>:<chunk>:<low|high>"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watermark {
    pub snapshot_id: String,
    pub chunk: u64,
    pub mark: Mark,
}

impl Watermark {
    pub fn id(snapshot_id: &str, chunk: u64, high: bool) -> String {
        let mark = if high { "high" } else { "low" };
        format!("{snapshot_id}:{chunk}:{mark}")
    }

    /// The watermark an `_id` names, if it is one.
    pub fn parse(id: &str) -> Option<Self> {
        let mut parts = id.rsplitn(3, ':');
        let mark = parts.next()?;
        let chunk = parts.next()?.parse().ok()?;
        let snapshot_id = parts.next()?;
        let mark = match mark {
            "low" => Mark::Low(chunk),
            "high" => Mark::High(chunk),
            _ => return None,
        };
        (!snapshot_id.is_empty()).then(|| Self {
            snapshot_id: snapshot_id.to_owned(),
            chunk,
            mark,
        })
    }
}

/// The window key of an `_id`: its canonical extended JSON, which keeps the BSON type, so a chunk
/// document and a live event's `documentKey._id` compare equal exactly when they are the same
/// document. It is also the snapshot cursor (`SnapState.after`).
pub fn window_key(id: &Bson) -> String {
    id.clone().into_canonical_extjson().to_string()
}

/// The `_id` a saved cursor names.
pub fn parse_key(key: &str) -> Result<Bson> {
    let v: Value = serde_json::from_str(key)
        .map_err(|e| Error::Source(format!("bad snapshot cursor {key:?}: {e}")))?;
    Bson::try_from(v).map_err(|e| Error::Source(format!("bad snapshot cursor {key:?}: {e}")))
}

/// A document read by a chunk, encoded like a live insert.
#[derive(Debug, Clone)]
pub struct ReadRow {
    pub schema: String,
    pub table: String,
    pub row: Box<RawValue>,
    /// Kafka key part, exactly as for a live event of the document.
    pub key: Option<String>,
    pub commit_ts_micros: i64,
}

/// The value at a dotted path of `doc`.
fn path<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut parts = path.split('.');
    let mut cur = doc.get(parts.next()?)?;
    for p in parts {
        cur = cur.as_document()?.get(p)?;
    }
    Some(cur)
}

/// The `documentKey` the change stream sends for `doc`: the shard key fields in key order (a
/// missing one is left out), then `_id` unless the shard key holds it. `{_id}` when unsharded.
pub fn document_key(doc: &Document, shard_key: &[String]) -> Option<Document> {
    let id = doc.get("_id")?.clone();
    let mut key = Document::new();
    for f in shard_key {
        if f == "_id" {
            key.insert("_id", id.clone());
        } else if let Some(v) = path(doc, f) {
            key.insert(f.clone(), v.clone());
        }
    }
    if !key.contains_key("_id") {
        key.insert("_id", id);
    }
    Some(key)
}

/// Encodes `doc` of `db.coll` exactly as the stream encodes an insert of it: the same synthesized
/// change event goes through [`map_event`], and the key is the `documentKey` JSON (`shard_key`
/// is the collection's shard key pattern, empty when unsharded).
pub fn read_row(
    db: &str,
    coll: &str,
    doc: &Document,
    shard_key: &[String],
    commit_ts_micros: i64,
) -> Result<ReadRow> {
    let key = document_key(doc, shard_key)
        .ok_or_else(|| Error::Source(format!("document of {db}.{coll} without _id")))?;
    let change = doc! {
        "operationType": "insert",
        "ns": {"db": db, "coll": coll},
        "fullDocument": doc.clone(),
        "documentKey": key,
    };
    let ev = Bson::Document(change).into_relaxed_extjson();
    let m = map_event(&ev).ok_or_else(|| Error::Source("cannot encode snapshot row".into()))?;
    let after = m
        .after
        .ok_or_else(|| Error::Source("cannot encode snapshot row".into()))?;
    Ok(ReadRow {
        schema: m.schema,
        table: m.table,
        row: to_raw_value(&after).map_err(src_err)?,
        key: ev.get("documentKey").map(Value::to_string),
        commit_ts_micros,
    })
}

/// A live change of a snapshotted collection, as the window needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Live {
    /// An insert, update, replace or delete of the document with this window key.
    Key(String),
    /// A change of a document that cannot be told (no `documentKey._id`).
    Unknown,
    /// The collection was dropped or renamed away, or its database dropped: every document the
    /// chunk read may be gone. (A rename *onto* the collection is
    /// [`Engine::collection_replaced`]: it brings documents that no live event announces.)
    Truncate,
}

/// What closing a chunk produced.
pub struct Closed {
    pub rows: Vec<ReadRow>,
    /// Progress once these rows are delivered; `None` when the snapshot is finished.
    pub state: Option<SnapState>,
    /// The ids of the closed chunk's watermark documents, to delete.
    pub watermarks: Vec<String>,
}

/// What a collection is, for planning: from `listCollections`, plus the watch list.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    pub database: String,
    /// The configured watch list; empty means the whole database.
    pub watched: Vec<String>,
    /// `listCollections` type (`collection`, `view`, `timeseries`) by collection name.
    pub kinds: HashMap<String, String>,
    /// `estimatedDocumentCount` by table id, where read.
    pub estimates: HashMap<String, u64>,
    /// Shard key field names in key order by collection name, for sharded collections.
    pub shard_keys: HashMap<String, Vec<String>>,
}

impl Catalog {
    /// The collection name of `table`, or why it cannot be snapshotted. A watched collection that
    /// does not exist (yet) is fine: it is empty.
    pub fn check(&self, table: &str) -> std::result::Result<String, String> {
        let coll = table
            .strip_prefix(&self.database)
            .and_then(|r| r.strip_prefix('.'))
            .filter(|c| !c.is_empty())
            .ok_or_else(|| format!("not in database {:?}", self.database))?;
        if coll == WATERMARK_COLLECTION || coll.starts_with("system.") {
            return Err("not a user collection".into());
        }
        if !self.watched.is_empty() && !self.watched.iter().any(|w| w == coll) {
            return Err("not in source.collections".into());
        }
        match self.kinds.get(coll).map(String::as_str) {
            None | Some("collection") => Ok(coll.to_owned()),
            Some(kind) => Err(format!("is a {kind}, not a collection")),
        }
    }

    /// Every collection an initial snapshot copies: the watch list, or every collection of the
    /// database.
    pub fn initial_tables(&self) -> Vec<String> {
        let mut colls: Vec<String> = if self.watched.is_empty() {
            self.kinds.keys().cloned().collect()
        } else {
            self.watched.clone()
        };
        if self.watched.is_empty() {
            colls.sort();
        }
        colls
            .into_iter()
            .map(|c| table_id(&self.database, &c))
            .filter(|t| self.check(t).is_ok())
            .collect()
    }
}

/// A running snapshot.
pub struct Engine {
    database: String,
    state: SnapState,
    window: Window<ReadRow>,
    /// Chunk ids must never repeat across restarts (a replayed watermark of an old chunk would
    /// close a new one), and must not be guessable (anyone who may insert into the watermark
    /// collection could close a window early): a random start, counting up.
    next_chunk: u64,
    in_flight: Option<u64>,
    /// Consecutive retries of the current chunk, for the backoff.
    retries: u32,
    /// Consecutive failed reads of the current collection's chunk.
    failures: u32,
    shard_keys: HashMap<String, Vec<String>>,
    chunk_size: u32,
    max_rows_per_sec: Option<u32>,
    not_before: Instant,
    rows: u64,
    estimates: HashMap<String, u64>,
    skipped: Vec<(String, String)>,
    /// Stop after the chunk in flight (admin `DELETE /api/snapshot`).
    cancelled: bool,
    /// The collection of the chunk in flight was replaced: closing the chunk restarts the
    /// collection from its first `_id` instead of moving the cursor.
    restart: bool,
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// 64 random bits from the standard library's per-process hash keys (no extra dependency).
fn random_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(now_micros());
    h.finish()
}

/// A fresh snapshot id. No `:`, which separates the watermark `_id`'s parts.
pub fn new_snapshot_id() -> String {
    format!("s{}", now_micros())
}

impl Engine {
    /// A snapshot of `state.tables` (resuming at `state.table` after `state.after`). Tables that
    /// cannot be snapshotted are skipped with a warning. `None` if nothing is left to copy.
    pub fn new(
        mut state: SnapState,
        catalog: &Catalog,
        chunk_size: u32,
        max_rows_per_sec: Option<u32>,
    ) -> Option<Self> {
        let mut skipped = Vec::new();
        let mut kept = Vec::new();
        for (i, id) in state.tables.iter().enumerate() {
            match catalog.check(id) {
                Ok(_) => kept.push((i, id.clone())),
                Err(why) => {
                    tracing::warn!(table = %id, reason = %why, "snapshot skips this collection");
                    skipped.push((id.clone(), why));
                }
            }
        }
        // Keep the table index pointing at the same table after dropping skipped ones; the
        // cursor only belongs to the table it was saved for.
        let cur = state.table;
        let new_index = kept.iter().position(|(i, _)| *i >= cur);
        if new_index.map(|n| kept[n].0) != Some(cur) {
            state.after = None;
        }
        state.tables = kept.into_iter().map(|(_, id)| id).collect();
        state.table = new_index?;
        Some(Self {
            database: catalog.database.clone(),
            state,
            window: Window::new(),
            next_chunk: random_u64() >> 1,
            in_flight: None,
            retries: 0,
            failures: 0,
            shard_keys: catalog.shard_keys.clone(),
            chunk_size: chunk_size.max(1),
            max_rows_per_sec: max_rows_per_sec.filter(|n| *n > 0),
            not_before: Instant::now(),
            rows: 0,
            estimates: catalog.estimates.clone(),
            skipped,
            cancelled: false,
            restart: false,
        })
    }

    /// Progress as of the last fully emitted chunk, for the checkpoint of events emitted now.
    pub fn state(&self) -> &SnapState {
        &self.state
    }

    pub fn in_flight(&self) -> bool {
        self.in_flight.is_some()
    }

    /// Progress for the admin API and metrics, in the state `state` (`running`, `finished`,
    /// `cancelled`).
    pub fn status(&self, state: &str) -> SnapshotStatus {
        let running = state == "running";
        SnapshotStatus {
            id: self.state.id.clone(),
            state: state.to_owned(),
            tables: self.state.tables.clone(),
            tables_done: if state == "finished" {
                self.state.tables.len()
            } else {
                self.state.table
            },
            table: running
                .then(|| self.state.tables.get(self.state.table).cloned())
                .flatten(),
            rows: self.rows,
            rows_estimated: self
                .state
                .tables
                .iter()
                .map(|t| self.estimates.get(t).copied())
                .sum(),
            skipped: self
                .skipped
                .iter()
                .map(|(table, reason)| SkippedTable {
                    table: table.clone(),
                    reason: reason.clone(),
                })
                .collect(),
        }
    }

    /// Whether the next chunk may start now: none in flight, not cancelled, and the throttle
    /// allows it.
    pub fn due(&self) -> bool {
        self.in_flight.is_none() && !self.cancelled && Instant::now() >= self.not_before
    }

    /// Stops the snapshot once the chunk in flight (if any) is closed.
    pub fn cancel(&mut self) {
        self.cancelled = true;
    }

    pub fn cancelled(&self) -> bool {
        self.cancelled
    }

    pub fn id(&self) -> &str {
        &self.state.id
    }

    /// Appends tables to the running snapshot (an admin request while one runs).
    pub fn append(&mut self, tables: Vec<String>, catalog: &Catalog) {
        for id in tables {
            if self.state.tables.contains(&id) {
                continue;
            }
            match catalog.check(&id) {
                Ok(coll) => {
                    if let Some(n) = catalog.estimates.get(&id) {
                        self.estimates.insert(id.clone(), *n);
                    }
                    if let Some(k) = catalog.shard_keys.get(&coll) {
                        self.shard_keys.insert(coll, k.clone());
                    }
                    self.state.tables.push(id);
                }
                Err(why) => {
                    tracing::warn!(table = %id, reason = %why, "snapshot skips this collection");
                    self.skipped.push((id, why));
                }
            }
        }
    }

    /// The collection being read.
    fn collection(&self) -> String {
        let id = &self.state.tables[self.state.table];
        id.strip_prefix(&self.database)
            .and_then(|r| r.strip_prefix('.'))
            .unwrap_or(id)
            .to_owned()
    }

    async fn write_watermark(
        &self,
        client: &Client,
        session: &mut ClientSession,
        chunk: u64,
        high: bool,
    ) -> std::result::Result<(), ChunkError> {
        client
            .database(&self.database)
            .collection::<Document>(WATERMARK_COLLECTION)
            .insert_one(doc! {
                "_id": Watermark::id(&self.state.id, chunk, high),
                "at": DateTime::now(),
            })
            .write_concern(watermark_write_concern())
            .session(&mut *session)
            .await
            .map_err(chunk_error)?;
        Ok(())
    }

    /// Reads the next chunk between a low and a high watermark (see the module docs for why the
    /// read sees exactly what the window needs). On an error nothing stays in flight; the source
    /// reports a [`ChunkError::Failed`] to [`Engine::chunk_failed`].
    pub async fn start_chunk(&mut self, client: &Client) -> std::result::Result<(), ChunkError> {
        let id = self.next_chunk;
        self.next_chunk += 1;
        // A replacement seen before this chunk already reset the cursor it reads from.
        self.restart = false;
        let table = self.state.tables[self.state.table].clone();
        self.window
            .expect(id, &table)
            .map_err(|e| ChunkError::Fatal(Error::Source(format!("snapshot window: {e}"))))?;
        self.in_flight = Some(id);
        let read = self.read_chunk(client, id, &table).await;
        if read.is_err() {
            self.window = Window::new();
            self.in_flight = None;
        }
        read
    }

    /// After a failed chunk read: pause and read it again, or skip the collection once it failed
    /// [`MAX_CHUNK_FAILURES`] times in a row. Also returns the failed chunk's watermark ids.
    pub fn chunk_failed(&mut self, why: &str) -> (Failure, Vec<String>) {
        let chunk = self.next_chunk - 1;
        let watermarks = vec![
            Watermark::id(&self.state.id, chunk, false),
            Watermark::id(&self.state.id, chunk, true),
        ];
        self.failures += 1;
        let table = self.state.tables[self.state.table].clone();
        if self.failures < MAX_CHUNK_FAILURES {
            let wait = Duration::from_secs(1u64 << self.failures);
            self.not_before = Instant::now() + wait;
            tracing::warn!(table = %table, error = %why, attempt = self.failures, "snapshot chunk read failed; retrying");
            return (Failure::Retry, watermarks);
        }
        tracing::warn!(table = %table, error = %why, "snapshot chunk read failed {} times; skipping this collection", self.failures);
        self.failures = 0;
        self.restart = false;
        self.skipped
            .push((table, format!("chunk read failed: {why}")));
        // Out of the list, so a restart does not try it again; the index now names the next one.
        self.state.tables.remove(self.state.table);
        self.state.after = None;
        let state = (self.state.table < self.state.tables.len()).then(|| self.state.clone());
        (Failure::Skipped { state }, watermarks)
    }

    async fn read_chunk(
        &mut self,
        client: &Client,
        id: u64,
        table: &str,
    ) -> std::result::Result<(), ChunkError> {
        let mut session = client
            .start_session()
            .causal_consistency(true)
            .await
            .map_err(chunk_error)?;
        self.write_watermark(client, &mut session, id, false)
            .await?;

        let coll = self.collection();
        let shard_key = self.shard_keys.get(&coll).cloned().unwrap_or_default();
        let after = match self.state.after.as_ref().and_then(|a| a.first()) {
            Some(k) => Some((
                k.clone(),
                parse_key(k).map_err(|e| ChunkError::Failed(e.to_string()))?,
            )),
            None => None,
        };
        // One more than a chunk when `min` (inclusive) may return the cursor document itself.
        let limit = self.chunk_size + u32::from(after.is_some());
        let source = client
            .database(&self.database)
            .collection::<Document>(&coll);
        let mut find = source
            .find(doc! {})
            .hint(Hint::Keys(doc! {"_id": 1}))
            .sort(doc! {"_id": 1})
            .limit(i64::from(limit))
            .batch_size(limit)
            .max_time(CHUNK_MAX_TIME)
            .read_concern(ReadConcern::majority())
            .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary));
        if let Some((_, b)) = &after {
            find = find.min(doc! {"_id": b.clone()});
        }
        let mut cursor = find.session(&mut session).await.map_err(chunk_error)?;
        let ts = now_micros() as i64;
        let mut chunk = Vec::new();
        while let Some(d) = cursor.next(&mut session).await {
            let d = d.map_err(chunk_error)?;
            let key = d
                .get("_id")
                .map(window_key)
                .ok_or_else(|| ChunkError::Failed(format!("document of {table} without _id")))?;
            if chunk.is_empty() && after.as_ref().is_some_and(|(k, _)| *k == key) {
                continue;
            }
            if chunk.len() as u32 >= self.chunk_size {
                break;
            }
            chunk.push(ChunkRow {
                key,
                row: read_row(&self.database, &coll, &d, &shard_key, ts)
                    .map_err(|e| ChunkError::Failed(e.to_string()))?,
            });
        }
        drop(cursor);
        let n = chunk.len() as u64;
        self.window
            .set_chunk(chunk)
            .map_err(|e| ChunkError::Fatal(Error::Source(format!("snapshot window: {e}"))))?;
        self.write_watermark(client, &mut session, id, true).await?;
        self.failures = 0;
        if let Some(rate) = self.max_rows_per_sec {
            self.not_before = Instant::now() + Duration::from_secs_f64(n as f64 / f64::from(rate));
        }
        Ok(())
    }

    /// A live change of `table` passed on the stream.
    pub fn observe(&mut self, table: &str, change: Live) {
        if self.in_flight.is_none() {
            return;
        }
        match change {
            Live::Key(k) => self.window.live_key(table, &k),
            Live::Unknown => self.window.live_unknown(table),
            Live::Truncate => self.window.live_truncate(table),
        }
    }

    /// Another collection was renamed onto `table` (`renameCollection` with `dropTarget`, or an
    /// `$out` aggregation): it now holds a different set of documents, and no live event
    /// announces them. If `table` is the collection being read, its copy starts again at its
    /// first `_id`: the chunk in flight is voided and closes without moving the cursor, and the
    /// saved cursor is reset now, so a crash also starts it again. Documents already copied are
    /// copied twice (at-least-once); none is skipped. A queued collection needs nothing, as its
    /// copy has not started. Returns whether the saved progress changed, to be checkpointed.
    pub fn collection_replaced(&mut self, table: &str) -> bool {
        if self.state.tables.get(self.state.table).map(String::as_str) != Some(table) {
            return false;
        }
        tracing::info!(
            table = %table,
            "a collection was renamed onto the collection being snapshotted; copying it again from the start"
        );
        if self.in_flight.is_some() {
            self.window.live_truncate(table);
            self.restart = true;
        }
        self.state.after.take().is_some()
    }

    /// The database was dropped: the collection being read is gone.
    pub fn database_dropped(&mut self) {
        if let Some(t) = self.state.tables.get(self.state.table).cloned() {
            self.observe(&t, Live::Truncate);
        }
    }

    /// A watermark passed on the stream. Returns the rows to emit and the new progress when it
    /// closes the chunk in flight.
    pub fn mark(&mut self, wm: &Watermark) -> Result<Option<Closed>> {
        if wm.snapshot_id != self.state.id {
            return Ok(None);
        }
        let outcome = self
            .window
            .mark(wm.mark)
            .map_err(|e| Error::Source(format!("snapshot window: {e}")))?;
        let watermarks = vec![
            Watermark::id(&self.state.id, wm.chunk, false),
            Watermark::id(&self.state.id, wm.chunk, true),
        ];
        let (rows, last_key) = match outcome {
            Outcome::None => return Ok(None),
            Outcome::Retry => {
                // Back off, so a collection that keeps producing unknown keys is not hammered.
                self.retries = self.retries.saturating_add(1);
                let wait = Duration::from_millis(100u64 << self.retries.min(7))
                    .min(Duration::from_secs(10));
                self.not_before = self.not_before.max(Instant::now() + wait);
                tracing::info!(
                    table = %self.state.tables[self.state.table],
                    "a live change in the snapshot window had no readable key; reading the chunk again"
                );
                self.in_flight = None;
                return Ok(Some(Closed {
                    rows: Vec::new(),
                    state: Some(self.state.clone()),
                    watermarks,
                }));
            }
            Outcome::Emit { rows, last_key } => (rows, last_key),
        };
        self.in_flight = None;
        self.retries = 0;
        match last_key {
            // The collection was replaced while the chunk was read: start it again.
            _ if std::mem::take(&mut self.restart) => self.state.after = None,
            Some(k) => self.state.after = Some(vec![k]),
            None => {
                // An empty chunk: the collection is done.
                self.state.after = None;
                self.state.table += 1;
            }
        }
        self.rows += rows.len() as u64;
        let rows = rows.into_iter().map(|r| r.row).collect();
        let state = (self.state.table < self.state.tables.len()).then(|| self.state.clone());
        Ok(Some(Closed {
            rows,
            state,
            watermarks,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::oid::ObjectId;

    fn catalog() -> Catalog {
        Catalog {
            database: "app".into(),
            watched: vec![],
            kinds: [
                ("users".to_string(), "collection".to_string()),
                ("v".to_string(), "view".to_string()),
                ("orders".to_string(), "collection".to_string()),
            ]
            .into(),
            estimates: [("app.users".to_string(), 7)].into(),
            shard_keys: Default::default(),
        }
    }

    fn state(tables: &[&str]) -> SnapState {
        SnapState {
            id: "s1".into(),
            tables: tables.iter().map(|t| t.to_string()).collect(),
            table: 0,
            after: None,
        }
    }

    #[test]
    fn watermark_ids_round_trip_and_reject_others() {
        let id = Watermark::id("s17", 42, true);
        assert_eq!(id, "s17:42:high");
        assert_eq!(
            Watermark::parse(&id),
            Some(Watermark {
                snapshot_id: "s17".into(),
                chunk: 42,
                mark: Mark::High(42)
            })
        );
        assert_eq!(
            Watermark::parse("s17:7:low").map(|w| w.mark),
            Some(Mark::Low(7))
        );
        for bad in ["", "s1:x:low", "s1:1:mid", ":1:low", "probe:abc", "1:low"] {
            assert_eq!(Watermark::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn window_keys_keep_the_bson_type_and_round_trip() {
        let oid = ObjectId::new();
        for b in [
            Bson::Int32(1),
            Bson::Int64(1),
            Bson::Double(1.0),
            Bson::String("1".into()),
            Bson::ObjectId(oid),
            Bson::Document(doc! {"a": 1, "b": "x"}),
        ] {
            let k = window_key(&b);
            assert_eq!(parse_key(&k).unwrap(), b, "{k}");
        }
        assert_ne!(window_key(&Bson::Int32(1)), window_key(&Bson::Int64(1)));
        assert_ne!(
            window_key(&Bson::Int32(1)),
            window_key(&Bson::String("1".into()))
        );
        assert!(parse_key("nope").is_err());
    }

    #[test]
    fn read_rows_are_encoded_like_live_inserts() {
        let d = doc! {"_id": 5_i64, "n": "a", "when": DateTime::from_millis(0), "z": 1.5};
        let r = read_row("app", "users", &d, &[], 9).unwrap();
        let change = doc! {"_id": {"_data": "82"}, "operationType": "insert",
        "ns": {"db": "app", "coll": "users"}, "fullDocument": d.clone(),
        "documentKey": {"_id": 5_i64}};
        let live = crate::source::to_change_event(&change).unwrap().unwrap();
        assert_eq!(r.row.get(), live.after.unwrap().get());
        assert_eq!(r.key, live.key);
        assert_eq!((r.schema.as_str(), r.table.as_str()), ("app", "users"));
        assert_eq!(r.commit_ts_micros, 9);
        assert!(read_row("app", "users", &doc! {"n": 1}, &[], 0).is_err());
    }

    #[test]
    fn sharded_document_keys_follow_the_shard_key() {
        let d = doc! {"_id": 5, "region": "eu", "a": {"b": 2}, "n": 1};
        let key = |k: &[&str]| {
            let k: Vec<String> = k.iter().map(|s| s.to_string()).collect();
            Bson::Document(document_key(&d, &k).unwrap())
                .into_relaxed_extjson()
                .to_string()
        };
        assert_eq!(key(&[]), r#"{"_id":5}"#);
        assert_eq!(key(&["region"]), r#"{"region":"eu","_id":5}"#);
        assert_eq!(key(&["a.b", "_id"]), r#"{"a.b":2,"_id":5}"#);
        assert_eq!(key(&["missing", "region"]), r#"{"region":"eu","_id":5}"#);
        let r = read_row("app", "u", &d, &["region".to_string()], 0).unwrap();
        assert_eq!(r.key.as_deref(), Some(r#"{"region":"eu","_id":5}"#));
    }

    #[test]
    fn failing_chunks_retry_then_skip_the_collection() {
        let mut e = Engine::new(state(&["app.users", "app.orders"]), &catalog(), 10, None).unwrap();
        e.state.after = Some(vec!["7".into()]);
        for _ in 1..MAX_CHUNK_FAILURES {
            e.next_chunk += 1;
            assert_eq!(e.chunk_failed("boom").0, Failure::Retry);
            assert!(!e.due(), "pauses before the next attempt");
        }
        e.next_chunk += 1;
        let (f, ids) = e.chunk_failed("boom");
        let Failure::Skipped { state: Some(st) } = f else {
            panic!("expected a skip, got {f:?}");
        };
        assert_eq!(ids.len(), 2);
        assert_eq!(
            (st.tables.clone(), st.table, st.after),
            (vec!["app.orders".to_string()], 0, None)
        );
        let status = e.status("running");
        assert_eq!(status.skipped[0].table, "app.users");
        assert!(status.skipped[0].reason.contains("boom"));
        for _ in 0..MAX_CHUNK_FAILURES {
            e.next_chunk += 1;
            e.chunk_failed("again");
        }
        assert!(e.state().tables.is_empty());
    }

    #[test]
    fn catalog_checks_collections() {
        let c = catalog();
        assert_eq!(c.check("app.users"), Ok("users".into()));
        assert_eq!(c.check("app.missing"), Ok("missing".into()));
        assert_eq!(c.check("app.a.b"), Ok("a.b".into()));
        assert!(c.check("other.users").is_err());
        assert!(c.check("app.").is_err());
        assert!(c.check("app.v").unwrap_err().contains("view"));
        assert!(c.check("app.aqueduct_watermarks").is_err());
        assert!(c.check("app.system.profile").is_err());
        assert_eq!(c.initial_tables(), ["app.orders", "app.users"]);
        let mut w = catalog();
        w.watched = vec!["users".into(), "gone".into()];
        assert!(w.check("app.orders").unwrap_err().contains("collections"));
        assert_eq!(w.initial_tables(), ["app.users", "app.gone"]);
    }

    fn row(key: &str) -> ChunkRow<ReadRow> {
        ChunkRow {
            key: key.into(),
            row: ReadRow {
                schema: "app".into(),
                table: "users".into(),
                row: RawValue::from_string("{}".into()).unwrap(),
                key: None,
                commit_ts_micros: 0,
            },
        }
    }

    /// Drives the window as `start_chunk` would, without a server.
    fn open(e: &mut Engine, rows: Vec<ChunkRow<ReadRow>>) -> u64 {
        let id = e.next_chunk;
        e.next_chunk += 1;
        let t = e.state.tables[e.state.table].clone();
        e.window.expect(id, &t).unwrap();
        e.in_flight = Some(id);
        e.window.set_chunk(rows).unwrap();
        id
    }

    fn wm(chunk: u64, high: bool) -> Watermark {
        Watermark::parse(&Watermark::id("s1", chunk, high)).unwrap()
    }

    #[test]
    fn closing_chunks_moves_the_cursor_then_the_table_then_finishes() {
        let mut e = Engine::new(state(&["app.users", "app.v"]), &catalog(), 2, None).unwrap();
        assert_eq!(e.state().tables, ["app.users"], "the view is skipped");
        assert_eq!(e.status("running").skipped.len(), 1);
        assert_eq!(e.status("running").rows_estimated, Some(7));
        let id = open(&mut e, vec![row("1"), row("2")]);
        assert!(e.mark(&wm(id, false)).unwrap().is_none());
        // A live update of `2` in the window removes that document; other collections do not.
        e.observe("app.users", Live::Key("2".into()));
        e.observe("app.orders", Live::Key("1".into()));
        // Another snapshot's watermark is ignored.
        let mut foreign = wm(id, true);
        foreign.snapshot_id = "s0".into();
        assert!(e.mark(&foreign).unwrap().is_none());
        let closed = e.mark(&wm(id, true)).unwrap().unwrap();
        assert_eq!(closed.rows.len(), 1);
        assert_eq!(
            closed.watermarks,
            [
                Watermark::id("s1", id, false),
                Watermark::id("s1", id, true)
            ]
        );
        assert_eq!(closed.state.unwrap().after, Some(vec!["2".into()]));

        // A change without a key retries: same cursor, nothing emitted, after a backoff.
        let id = open(&mut e, vec![row("3")]);
        e.mark(&wm(id, false)).unwrap();
        e.observe("app.users", Live::Unknown);
        let closed = e.mark(&wm(id, true)).unwrap().unwrap();
        assert!(closed.rows.is_empty());
        assert_eq!(closed.state.unwrap().after, Some(vec!["2".into()]));
        assert!(!e.due());

        // A drop voids the chunk but moves the cursor past it.
        let id = open(&mut e, vec![row("3"), row("4")]);
        e.mark(&wm(id, false)).unwrap();
        e.database_dropped();
        let closed = e.mark(&wm(id, true)).unwrap().unwrap();
        assert!(closed.rows.is_empty());
        assert_eq!(closed.state.unwrap().after, Some(vec!["4".into()]));

        // An empty chunk ends the collection, and with it the snapshot.
        let id = open(&mut e, vec![]);
        e.mark(&wm(id, false)).unwrap();
        let closed = e.mark(&wm(id, true)).unwrap().unwrap();
        assert!(closed.rows.is_empty());
        assert!(closed.state.is_none(), "finished");
        assert_eq!(e.status("finished").rows, 1);
        assert_eq!(e.status("finished").tables_done, 1);
    }

    #[test]
    fn a_rename_onto_the_collection_being_read_starts_it_again() {
        let mut e = Engine::new(state(&["app.users", "app.orders"]), &catalog(), 2, None).unwrap();
        let id = open(&mut e, vec![row("1"), row("2")]);
        e.mark(&wm(id, false)).unwrap();
        e.mark(&wm(id, true)).unwrap().unwrap();
        assert_eq!(e.state().after, Some(vec!["2".into()]));

        // In flight: the chunk is voided and the cursor goes back to the start, not to `4`.
        let id = open(&mut e, vec![row("3"), row("4")]);
        e.mark(&wm(id, false)).unwrap();
        assert!(e.collection_replaced("app.users"));
        assert_eq!(e.state().after, None, "a crash now also starts again");
        let closed = e.mark(&wm(id, true)).unwrap().unwrap();
        assert!(closed.rows.is_empty());
        let st = closed.state.unwrap();
        assert_eq!((st.table, st.after), (0, None));

        // An empty chunk after the replacement still restarts, rather than finishing the table.
        let id = open(&mut e, vec![row("1")]);
        e.mark(&wm(id, false)).unwrap();
        e.mark(&wm(id, true)).unwrap().unwrap();
        let id = open(&mut e, vec![]);
        e.mark(&wm(id, false)).unwrap();
        e.collection_replaced("app.users");
        let st = e.mark(&wm(id, true)).unwrap().unwrap().state.unwrap();
        assert_eq!((st.table, st.after), (0, None));

        // Between chunks: the saved cursor is reset at once, and the next chunk moves on as usual.
        let id = open(&mut e, vec![row("1")]);
        e.mark(&wm(id, false)).unwrap();
        e.mark(&wm(id, true)).unwrap().unwrap();
        assert!(e.collection_replaced("app.users"));
        assert_eq!(e.state().after, None);
        assert!(!e.collection_replaced("app.users"), "nothing more to save");
        let id = open(&mut e, vec![row("1"), row("2")]);
        e.mark(&wm(id, false)).unwrap();
        let closed = e.mark(&wm(id, true)).unwrap().unwrap();
        assert_eq!(closed.rows.len(), 2);
        assert_eq!(closed.state.unwrap().after, Some(vec!["2".into()]));

        // A queued collection has not started, and another collection is not this one.
        assert!(!e.collection_replaced("app.orders"));
        assert!(!e.collection_replaced("app.other"));
        assert_eq!(e.state().after, Some(vec!["2".into()]));
    }

    #[test]
    fn engine_keeps_the_cursor_on_its_table() {
        let mut st = state(&["app.v", "app.users", "app.orders"]);
        st.table = 1;
        st.after = Some(vec!["9".into()]);
        let e = Engine::new(st, &catalog(), 10, None).unwrap();
        assert_eq!(e.state().tables, ["app.users", "app.orders"]);
        assert_eq!(
            (e.state().table, e.state().after.clone()),
            (0, Some(vec!["9".into()]))
        );
        // The saved table is gone: the cursor does not carry over to the next one.
        let mut st = state(&["app.users", "app.v", "app.orders"]);
        st.table = 1;
        st.after = Some(vec!["9".into()]);
        let e = Engine::new(st, &catalog(), 10, None).unwrap();
        assert_eq!((e.state().table, e.state().after.clone()), (1, None));
        assert!(Engine::new(state(&["app.v"]), &catalog(), 10, None).is_none());
    }

    #[test]
    fn append_adds_new_tables_once() {
        let mut e = Engine::new(state(&["app.users"]), &catalog(), 10, None).unwrap();
        e.append(
            vec!["app.users".into(), "app.orders".into(), "app.v".into()],
            &catalog(),
        );
        assert_eq!(e.state().tables, ["app.users", "app.orders"]);
        assert_eq!(e.status("running").skipped.len(), 1);
    }
}
