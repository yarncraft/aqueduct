use crate::config::{MongoConfig, SnapshotMode};
use crate::mapping::{map_event, Mapped};
use crate::snapshot::{
    new_snapshot_id, table_id, window_key, Catalog, Closed, Engine, Live, Watermark, TTL_INDEX,
    WATERMARK_COLLECTION, WATERMARK_TTL,
};
use aqueduct_core::{ChangeEvent, Error, Metrics, Op, Position, Result, Source};
use aqueduct_core::{Checkpoint, SnapState, SnapshotCommand, SnapshotReceiver, SnapshotStatus};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use mongodb::bson::{doc, Bson, DateTime, Document};
use mongodb::change_stream::event::ResumeToken;
use mongodb::change_stream::ChangeStream;
use mongodb::options::{FullDocumentBeforeChangeType, FullDocumentType, IndexOptions};
use mongodb::{Client, IndexModel};
use serde_json::value::to_raw_value;
use serde_json::{Map, Value};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

type Stream = ChangeStream<Document>;

/// Reads a MongoDB change stream. The position is the event's resume token (as JSON text),
/// so a restart resumes just after the last checkpointed event. The server keeps no
/// consumer state, so `ack` has nothing to commit; durability lives in the checkpoint store.
///
/// With snapshots on (ADR 0001), existing documents are copied in `_id` chunks between watermark
/// inserts into [`WATERMARK_COLLECTION`] while the stream keeps flowing (see
/// [`crate::snapshot`]). Snapshot progress rides in the checkpoint of every event:
/// `{"pos": <resume token>, "snap": {...}}`.
pub struct MongoSource {
    cfg: MongoConfig,
    client: Option<Client>,
    stream: Option<Stream>,
    /// How often an idle or filtered stream may emit a heartbeat that moves the checkpoint.
    heartbeat_every: Duration,
    /// Token text of the last position handed to the pipeline (event or heartbeat).
    last_token: Option<String>,
    last_heartbeat: Instant,
    /// Started without a checkpoint: hand out the stream's opening token before anything else,
    /// so the start position is durable (otherwise a restart before the first checkpoint would
    /// open the stream at a later "now" and lose what happened in between).
    announce_start: bool,
    /// Events ready to hand out (snapshot rows, heartbeats), before the stream is read again.
    buffer: VecDeque<ChangeEvent>,
    /// The running snapshot, if any.
    snapshot: Option<Engine>,
    /// Snapshot requests from the admin API.
    control: Option<SnapshotReceiver>,
    /// Status of the last snapshot that ended, kept for the admin API.
    last_status: Option<SnapshotStatus>,
    metrics: Option<Arc<Metrics>>,
    /// Watermark documents to delete: exact ids, or every id of an ended snapshot.
    cleanup: Vec<Cleanup>,
}

enum Cleanup {
    Ids(Vec<String>),
    Snapshot(String),
}

/// Long enough to be cheap, far inside any realistic oplog window.
const HEARTBEAT_EVERY: Duration = Duration::from_secs(30);

/// Server-side wait of each getMore. Bounds how often an idle stream reaches the heartbeat
/// check and how long shutdown waits on `next()`, instead of relying on the server default.
const MAX_AWAIT: Duration = Duration::from_secs(1);

impl MongoSource {
    pub fn new(cfg: MongoConfig) -> Self {
        Self {
            cfg,
            client: None,
            stream: None,
            heartbeat_every: HEARTBEAT_EVERY,
            last_token: None,
            last_heartbeat: Instant::now(),
            announce_start: false,
            buffer: VecDeque::new(),
            snapshot: None,
            control: None,
            last_status: None,
            metrics: None,
            cleanup: Vec::new(),
        }
    }

    /// Override the heartbeat interval (tests).
    pub fn with_heartbeat_every(mut self, every: Duration) -> Self {
        self.heartbeat_every = every;
        self
    }

    /// Take snapshot requests (start, cancel) from the admin API.
    pub fn with_snapshot_control(mut self, control: SnapshotReceiver) -> Self {
        self.control = Some(control);
        self
    }

    /// Report snapshot progress to `metrics`.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn client(&self) -> Result<&Client> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::Source("source not started".into()))
    }

    /// The checkpoint of an event at `token`: the token itself, wrapped with the snapshot
    /// progress while a snapshot runs, so both are saved in one atomic write.
    fn checkpoint_at(&self, token: &str) -> Option<Position> {
        self.snapshot.as_ref().map(|e| {
            Position::Text(
                Checkpoint {
                    pos: token.to_owned(),
                    snap: Some(e.state().clone()),
                }
                .encode(),
            )
        })
    }

    /// A heartbeat carrying the stream's current resume token, if it moved since the last
    /// position handed out. The token includes the server's postBatchResumeToken, so it
    /// advances even when every event was filtered out or nothing happened.
    fn heartbeat(&mut self) -> Result<Option<ChangeEvent>> {
        self.last_heartbeat = Instant::now();
        let Some(token) = self.stream.as_ref().and_then(Stream::resume_token) else {
            return Ok(None);
        };
        let text = token_text(&token)?;
        Ok(self.heartbeat_at(text))
    }

    /// A heartbeat at `text`, unless that is already the last position handed out.
    fn heartbeat_at(&mut self, text: String) -> Option<ChangeEvent> {
        self.last_heartbeat = Instant::now();
        if self.last_token.as_deref() == Some(text.as_str()) {
            return None;
        }
        let mut hb = ChangeEvent::heartbeat(Position::Text(text.clone()), 0);
        hb.checkpoint = self.checkpoint_at(&text);
        self.last_token = Some(text);
        Some(hb)
    }

    /// Queues a heartbeat at the last position handed out, whatever its checkpoint was: it
    /// saves a change of snapshot progress (or its end) that no event carries.
    fn queue_progress(&mut self) {
        let token = self.last_token.clone().or_else(|| {
            self.stream
                .as_ref()
                .and_then(Stream::resume_token)
                .and_then(|t| token_text(&t).ok())
        });
        if let Some(t) = token {
            let mut hb = ChangeEvent::heartbeat(Position::Text(t.clone()), 0);
            hb.checkpoint = self.checkpoint_at(&t);
            self.last_token = Some(t);
            self.buffer.push_back(hb);
        }
    }

    /// Reports the running or last snapshot to the metrics.
    fn publish_status(&self) {
        if let Some(m) = &self.metrics {
            let status = match &self.snapshot {
                Some(e) => Some(e.status("running")),
                None => self.last_status.clone(),
            };
            m.set_snapshot(status);
        }
    }

    /// Answers the admin API's snapshot requests. Runs between events, never while a chunk is
    /// half set up.
    async fn handle_requests(&mut self) -> Result<()> {
        loop {
            let Some(req) = self.control.as_mut().and_then(|c| c.try_recv().ok()) else {
                return Ok(());
            };
            let answer = match req.command {
                SnapshotCommand::Start(tables) => self.request_snapshot(tables).await,
                SnapshotCommand::Cancel => self.cancel_snapshot(),
            };
            let _ = req.reply.send(answer);
            self.publish_status();
        }
    }

    /// Starts a snapshot of `tables` (`database.collection`), or appends them to the running
    /// one. The answer is the snapshot id. The request is durable only once an event carrying
    /// the snapshot is checkpointed; a crash before that forgets it.
    async fn request_snapshot(
        &mut self,
        tables: Vec<String>,
    ) -> std::result::Result<String, String> {
        if self.cfg.snapshot == SnapshotMode::Never {
            return Err(
                "snapshots are off; set source.snapshot = \"on_demand\" or \"initial\"".into(),
            );
        }
        let client = self.client().map_err(|e| e.to_string())?.clone();
        let catalog = load_catalog(client, &self.cfg, &tables)
            .await
            .map_err(|e| e.to_string())?;
        let mut refused = Vec::new();
        for t in &tables {
            match catalog.check(t) {
                // With no watch list, only an existing collection can be meant.
                Ok(coll) if catalog.watched.is_empty() && !catalog.kinds.contains_key(&coll) => {
                    refused.push(format!("{t} (no such collection)"))
                }
                Ok(_) => {}
                Err(why) => refused.push(format!("{t} ({why})")),
            }
        }
        if !refused.is_empty() {
            return Err(format!("cannot snapshot: {}", refused.join(", ")));
        }
        if let Some(e) = self.snapshot.as_mut() {
            if !e.cancelled() {
                e.append(tables, &catalog);
                return Ok(e.id().to_owned());
            }
        }
        let id = new_snapshot_id();
        let state = SnapState {
            id: id.clone(),
            tables,
            table: 0,
            after: None,
        };
        match Engine::new(
            state,
            &catalog,
            self.cfg.snapshot_chunk_size,
            self.cfg.snapshot_max_rows_per_sec,
        ) {
            Some(e) => {
                tracing::info!(snapshot = %id, tables = e.state().tables.len(), "snapshot requested");
                self.snapshot = Some(e);
                Ok(id)
            }
            None => Err("none of these collections can be snapshotted".into()),
        }
    }

    /// Stops the running snapshot after the chunk in flight.
    fn cancel_snapshot(&mut self) -> std::result::Result<String, String> {
        let Some(e) = self.snapshot.as_mut() else {
            return Err("no snapshot is running".into());
        };
        e.cancel();
        let id = e.id().to_owned();
        if !e.in_flight() {
            // Nothing in flight: end now, and return the checkpoint to the plain form.
            self.end_snapshot();
            self.queue_progress();
        }
        Ok(id)
    }

    /// Forgets the running snapshot, keeping its final status for the admin API.
    fn end_snapshot(&mut self) {
        if let Some(e) = self.snapshot.take() {
            let state = if e.cancelled() {
                "cancelled"
            } else {
                "finished"
            };
            let st = e.status(state);
            tracing::info!(snapshot = %st.id, rows = st.rows, "snapshot {state}");
            self.cleanup.push(Cleanup::Snapshot(st.id.clone()));
            self.last_status = Some(st);
        }
        self.publish_status();
    }

    /// Deletes watermark documents that are no longer needed. Failures only leave them to the
    /// TTL index, so they are logged, never fatal.
    async fn run_cleanup(&mut self) {
        if self.cleanup.is_empty() {
            return;
        }
        let Ok(client) = self.client() else { return };
        let coll = client
            .database(&self.cfg.database)
            .collection::<Document>(WATERMARK_COLLECTION);
        for c in std::mem::take(&mut self.cleanup) {
            let filter = match c {
                Cleanup::Ids(ids) => doc! {"_id": {"$in": ids}},
                // Snapshot ids hold no regex metacharacters (`s<digits>`).
                Cleanup::Snapshot(id) => doc! {"_id": {"$regex": format!("^{id}:")}},
            };
            if let Err(e) = coll.delete_many(filter).await {
                tracing::warn!(error = %e, "could not delete snapshot watermarks; the TTL index removes them later");
            }
        }
    }

    /// Queues what closing a chunk produced, at the high watermark's token. Rows before the last
    /// carry the progress from before the chunk (a crash part-way reads the chunk again:
    /// duplicates only), the last row the new progress. A chunk without rows saves its progress
    /// with a heartbeat.
    fn queue_closed(&mut self, mut closed: Closed, before: SnapState, token: &str) {
        self.cleanup.push(Cleanup::Ids(closed.watermarks));
        // A cancelled snapshot ends with this chunk: its rows are delivered, then the checkpoint
        // returns to the plain form.
        if self.snapshot.as_ref().is_some_and(Engine::cancelled) {
            closed.state = None;
        }
        let progress = |snap: Option<SnapState>| {
            snap.map(|s| {
                Position::Text(
                    Checkpoint {
                        pos: token.to_owned(),
                        snap: Some(s),
                    }
                    .encode(),
                )
            })
            // Without progress the plain token, kept in `checkpoint` so every row sharing this
            // position has its own resume key.
            .unwrap_or_else(|| Position::Text(token.to_owned()))
        };
        let n = closed.rows.len();
        for (i, r) in closed.rows.into_iter().enumerate() {
            let snap = if i + 1 == n {
                closed.state.clone()
            } else {
                Some(before.clone())
            };
            self.buffer.push_back(ChangeEvent {
                position: Position::Text(token.to_owned()),
                xid: 0,
                commit_ts_micros: r.commit_ts_micros,
                schema: r.schema,
                table: r.table,
                op: Op::Read,
                before: None,
                after: Some(r.row),
                key: r.key,
                checkpoint: Some(progress(snap)),
            });
        }
        self.last_token = Some(token.to_owned());
        if closed.state.is_none() {
            self.end_snapshot();
            if n == 0 {
                self.queue_progress();
            }
        } else {
            if n == 0 && closed.state.as_ref() != Some(&before) {
                self.queue_progress();
            }
            self.publish_status();
        }
    }

    /// Feeds a change-stream document to the running snapshot. Returns `true` for a watermark,
    /// which is never forwarded.
    fn observe(&mut self, doc: &Document, token: &str) -> Result<bool> {
        let coll = doc
            .get_document("ns")
            .ok()
            .and_then(|ns| ns.get_str("coll").ok());
        let op = doc.get_str("operationType").unwrap_or_default();
        if coll == Some(WATERMARK_COLLECTION) {
            // Ours or another instance's: either way not a change of user data.
            let wm = (op == "insert")
                .then(|| doc.get_document("documentKey").ok())
                .flatten()
                .and_then(|k| k.get_str("_id").ok())
                .and_then(Watermark::parse);
            if let (Some(wm), Some(e)) = (wm, self.snapshot.as_mut()) {
                let before = e.state().clone();
                if let Some(closed) = e.mark(&wm)? {
                    self.queue_closed(closed, before, token);
                }
            }
            return Ok(true);
        }
        let Some(e) = self.snapshot.as_mut() else {
            return Ok(false);
        };
        let db = &self.cfg.database;
        let Some(coll) = coll else {
            if op == "dropDatabase" {
                e.database_dropped();
            }
            return Ok(false);
        };
        let id = table_id(db, coll);
        match op {
            "insert" | "update" | "replace" | "delete" => {
                let key = doc
                    .get_document("documentKey")
                    .ok()
                    .and_then(|k| k.get("_id"))
                    .map(window_key);
                e.observe(&id, key.map_or(Live::Unknown, Live::Key));
            }
            "drop" => e.observe(&id, Live::Truncate),
            "rename" => {
                e.observe(&id, Live::Truncate);
                let to = doc
                    .get_document("to")
                    .ok()
                    .and_then(|t| Some((t.get_str("db").ok()?, t.get_str("coll").ok()?)));
                if let Some((db, coll)) = to {
                    e.observe(&table_id(db, coll), Live::Truncate);
                }
            }
            "dropDatabase" => e.database_dropped(),
            _ => {}
        }
        Ok(false)
    }

    /// Makes sure the watermark collection can be written: creates its TTL index, then inserts
    /// and deletes a probe document. Fails at startup, not in the middle of a snapshot.
    async fn prepare_watermarks(client: &Client, cfg: &MongoConfig) -> Result<()> {
        let coll = client
            .database(&cfg.database)
            .collection::<Document>(WATERMARK_COLLECTION);
        let hint = |e: mongodb::error::Error| {
            Error::Config(format!(
                "source.snapshot = {:?} writes watermarks into {}.{WATERMARK_COLLECTION}, which needs \
                 the insert, remove and createIndex privileges on that collection: {e}. Grant them, \
                 or set source.snapshot = \"never\"",
                cfg.snapshot, cfg.database
            ))
        };
        let ttl = IndexModel::builder()
            .keys(doc! {"at": 1})
            .options(
                IndexOptions::builder()
                    .name(TTL_INDEX.to_string())
                    .expire_after(WATERMARK_TTL)
                    .build(),
            )
            .build();
        coll.create_index(ttl).await.map_err(hint)?;
        let probe = format!("probe:{}", new_snapshot_id());
        coll.insert_one(doc! {"_id": &probe, "at": DateTime::now()})
            .await
            .map_err(hint)?;
        coll.delete_one(doc! {"_id": &probe}).await.map_err(hint)?;
        Ok(())
    }

    /// Sets up the snapshot this start needs: resumes the one the checkpoint carries, or starts
    /// the initial one when there is no checkpoint.
    async fn start_snapshot(&mut self, resume: Option<SnapState>, first_start: bool) -> Result<()> {
        if self.cfg.snapshot == SnapshotMode::Never {
            if let Some(s) = resume {
                tracing::warn!(
                    snapshot = %s.id,
                    "the checkpoint carries an unfinished snapshot, but snapshot = \"never\"; \
                     it is abandoned (documents not copied yet are not delivered)"
                );
            }
            return Ok(());
        }
        let client = self.client()?.clone();
        Self::prepare_watermarks(&client, &self.cfg).await?;
        let state = match resume {
            Some(s) => Some(s),
            None if first_start && self.cfg.snapshot == SnapshotMode::Initial => {
                let cat = load_catalog(self.client()?.clone(), &self.cfg, &[]).await?;
                Some(SnapState {
                    id: new_snapshot_id(),
                    tables: cat.initial_tables(),
                    table: 0,
                    after: None,
                })
            }
            None => None,
        };
        if let Some(state) = state {
            let id = state.id.clone();
            let catalog = load_catalog(self.client()?.clone(), &self.cfg, &state.tables).await?;
            self.snapshot = Engine::new(
                state,
                &catalog,
                self.cfg.snapshot_chunk_size,
                self.cfg.snapshot_max_rows_per_sec,
            );
            match &self.snapshot {
                Some(e) => {
                    tracing::info!(snapshot = %id, tables = e.state().tables.len(), "snapshot running")
                }
                None => tracing::info!(snapshot = %id, "nothing to snapshot"),
            }
        }
        self.publish_status();
        Ok(())
    }
}

/// What the database holds, for planning a snapshot of `tables` (estimates only for those).
/// A free function: `MongoSource` is not `Sync`, so its futures must not hold `&self`.
async fn load_catalog(client: Client, cfg: &MongoConfig, tables: &[String]) -> Result<Catalog> {
    let db = client.database(&cfg.database);
    let specs: Vec<_> = db
        .list_collections()
        .await
        .map_err(src_err)?
        .try_collect()
        .await
        .map_err(src_err)?;
    let mut cat = Catalog {
        database: cfg.database.clone(),
        watched: cfg.collections.clone(),
        kinds: specs
            .into_iter()
            .map(|s| {
                let kind = match s.collection_type {
                    mongodb::results::CollectionType::View => "view",
                    mongodb::results::CollectionType::Timeseries => "timeseries",
                    _ => "collection",
                };
                (s.name, kind.to_owned())
            })
            .collect(),
        estimates: Default::default(),
    };
    for t in tables {
        if let Ok(coll) = cat.check(t) {
            // Telemetry only: a failure leaves the estimate unknown.
            if let Ok(n) = db
                .collection::<Document>(&coll)
                .estimated_document_count()
                .await
            {
                cat.estimates.insert(t.clone(), n);
            }
        }
    }
    Ok(cat)
}

fn src_err<E: std::fmt::Display>(e: E) -> Error {
    Error::Source(e.to_string())
}

/// Resume token JSON text -> driver token.
fn parse_token(text: &str) -> Result<ResumeToken> {
    let json: Value = serde_json::from_str(text).map_err(src_err)?;
    let bson = Bson::try_from(json).map_err(src_err)?;
    mongodb::bson::from_bson(bson).map_err(src_err)
}

/// Driver token -> the same JSON text that change events use as their position.
fn token_text(token: &ResumeToken) -> Result<String> {
    let bson = mongodb::bson::to_bson(token).map_err(src_err)?;
    Ok(bson.into_relaxed_extjson().to_string())
}

/// A change doc's own resume token in the same text form as event positions.
fn doc_token_text(doc: &Document) -> Result<String> {
    let ev = Bson::Document(doc.clone()).into_relaxed_extjson();
    ev.get("_id")
        .map(Value::to_string)
        .ok_or_else(|| Error::Source("change event without resume token".into()))
}

/// `clusterTime` (a BSON timestamp, seconds in the high word) to microseconds.
fn commit_micros(ev: &Value) -> i64 {
    ev.pointer("/clusterTime/$timestamp/t")
        .and_then(Value::as_i64)
        .map_or(0, |s| s * 1_000_000)
}

pub(crate) fn to_change_event(doc: &Document) -> Result<Option<ChangeEvent>> {
    let ev = Bson::Document(doc.clone()).into_relaxed_extjson();
    let Some(Mapped {
        schema,
        table,
        op,
        before,
        after,
    }) = map_event(&ev)
    else {
        return Ok(None);
    };
    let token = ev
        .get("_id")
        .ok_or_else(|| Error::Source("change event without resume token".into()))?;
    let raw = |m: Option<Map<String, Value>>| -> Result<_> {
        m.map(|m| to_raw_value(&m).map_err(src_err)).transpose()
    };
    Ok(Some(ChangeEvent {
        position: Position::Text(token.to_string()),
        xid: 0,
        commit_ts_micros: commit_micros(&ev),
        schema,
        table,
        op,
        before: raw(before)?,
        after: raw(after)?,
        key: ev.get("documentKey").map(Value::to_string),
        checkpoint: None,
    }))
}

#[async_trait]
impl Source for MongoSource {
    async fn start(&mut self, start: Option<Position>) -> Result<()> {
        self.cfg.validate().map_err(Error::Config)?;
        let client = Client::with_uri_str(&self.cfg.uri).await.map_err(src_err)?;
        self.client = Some(client.clone());
        let (token, resume) = match &start {
            Some(Position::Text(t)) => {
                let c = Checkpoint::decode(t).map_err(Error::Source)?;
                (Some(c.pos), c.snap)
            }
            Some(Position::Lsn(l)) => {
                return Err(Error::Source(format!(
                    "checkpoint {l} is a Postgres LSN, not a MongoDB resume token"
                )))
            }
            None => (None, None),
        };
        let db = client.database(&self.cfg.database);
        let mut pipeline = Vec::new();
        if !self.cfg.collections.is_empty() {
            let mut colls = self.cfg.collections.clone();
            if self.cfg.snapshot != SnapshotMode::Never {
                colls.push(WATERMARK_COLLECTION.to_owned());
            }
            pipeline.push(doc! {"$match": {"ns.coll": {"$in": colls}}});
        }
        let mut watch = db
            .watch()
            .pipeline(pipeline)
            .max_await_time(MAX_AWAIT)
            .full_document(FullDocumentType::UpdateLookup);
        if self.cfg.pre_images {
            watch = watch.full_document_before_change(FullDocumentBeforeChangeType::WhenAvailable);
        }
        if let Some(t) = &token {
            watch = watch.start_after(parse_token(t)?);
            self.last_token = Some(t.clone());
        }
        self.last_heartbeat = Instant::now();
        // The stream opens before the snapshot's first watermark is written, so it sees it.
        self.stream = Some(watch.await.map_err(src_err)?.with_type::<Document>());
        self.announce_start = token.is_none();
        self.start_snapshot(resume, token.is_none()).await?;
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<ChangeEvent>> {
        loop {
            if let Some(ev) = self.buffer.pop_front() {
                return Ok(Some(ev));
            }
            if self.announce_start {
                self.announce_start = false;
                // An empty first batch gives the stream's opening token. A non-empty one gives
                // none, but then the first event carries a position just as early.
                if let Some(hb) = self.heartbeat()? {
                    return Ok(Some(hb));
                }
            }
            self.run_cleanup().await;
            self.handle_requests().await?;
            if let Some(ev) = self.buffer.pop_front() {
                return Ok(Some(ev));
            }
            // The next chunk starts only when everything before it is handed on, so memory stays
            // at one chunk.
            if let (Some(e), Some(c)) = (self.snapshot.as_mut(), self.client.as_ref()) {
                if e.due() {
                    e.start_chunk(c).await?;
                }
            }
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| Error::Source("source not started".into()))?;
            // `next_if_any` makes at most one getMore (the server holds it for maxAwaitTime)
            // and returns `None` for an empty batch. Do NOT wrap `stream.next()` in a timeout
            // here: dropping that future mid-poll leaves the driver's cursor state taken, and
            // `resume_token()` then panics ("cursor state access while streaming").
            match stream.next_if_any().await.map_err(src_err)? {
                Some(doc) => {
                    let token = doc_token_text(&doc)?;
                    if !self.observe(&doc, &token)? {
                        if let Some(mut ev) = to_change_event(&doc)? {
                            ev.checkpoint = self.checkpoint_at(&token);
                            self.last_token = Some(token);
                            return Ok(Some(ev));
                        }
                    }
                    if !self.buffer.is_empty() {
                        continue;
                    }
                    // Unmapped doc (drop, rename, index DDL, watermark, ...). A steady run of
                    // these never reaches the empty-batch branch, so heartbeat at the doc's own
                    // token: it is already consumed, unlike the driver's token, which may sit
                    // past buffered docs.
                    if self.last_heartbeat.elapsed() >= self.heartbeat_every {
                        if let Some(hb) = self.heartbeat_at(token) {
                            return Ok(Some(hb));
                        }
                    }
                }
                None => {
                    if !stream.is_alive() {
                        return Ok(None);
                    }
                    // Empty batch: the token is the server's postBatchResumeToken, so the
                    // checkpoint can follow an idle or fully filtered stream. Only here is it
                    // safe, as the driver's token may point past undelivered events otherwise.
                    if self.last_heartbeat.elapsed() >= self.heartbeat_every {
                        if let Some(hb) = self.heartbeat()? {
                            return Ok(Some(hb));
                        }
                    }
                }
            }
        }
    }

    fn try_next(&mut self) -> Option<ChangeEvent> {
        self.buffer.pop_front()
    }

    async fn ack(&mut self, _position: Position) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trips() {
        let t = r#"{"_data":"8265A1"}"#;
        assert!(parse_token(t).is_ok());
        assert!(parse_token("not json").is_err());
    }

    #[test]
    fn token_text_matches_event_position_form() {
        let t = r#"{"_data":"8265A1"}"#;
        assert_eq!(token_text(&parse_token(t).unwrap()).unwrap(), t);
    }

    #[test]
    fn converts_insert_event() {
        let d = doc! {"_id": {"_data": "8265A1"}, "operationType": "insert",
        "ns": {"db": "app", "coll": "u"}, "fullDocument": {"_id": 1, "n": "a"},
        "documentKey": {"_id": 1}};
        let e = to_change_event(&d).unwrap().unwrap();
        assert_eq!(e.table, "u");
        assert_eq!(e.position, Position::Text(r#"{"_data":"8265A1"}"#.into()));
        assert!(e.after.unwrap().get().contains("\"n\":\"a\""));
        assert_eq!(e.key.as_deref(), Some(r#"{"_id":1}"#));
    }

    #[test]
    fn skipped_doc_token_is_its_own_id() {
        let d = doc! {"_id": {"_data": "8265A2"}, "operationType": "drop",
        "ns": {"db": "app", "coll": "u"}};
        assert!(to_change_event(&d).unwrap().is_none());
        assert_eq!(doc_token_text(&d).unwrap(), r#"{"_data":"8265A2"}"#);
        assert!(doc_token_text(&doc! {"operationType": "drop"}).is_err());
    }

    #[test]
    fn a_resume_token_checkpoint_is_plain_and_a_snapshot_one_is_composite() {
        // The token text is JSON itself; only a `snap` key makes the composite form.
        let tok = r#"{"_data":"8265A1"}"#;
        let c = Checkpoint::decode(tok).unwrap();
        assert_eq!((c.pos.as_str(), c.snap), (tok, None));
        let mut src = MongoSource::new(MongoConfig::new("mongodb://h/", "app"));
        assert_eq!(src.checkpoint_at(tok), None, "plain outside a snapshot");
        let cat = Catalog {
            database: "app".into(),
            ..Default::default()
        };
        let st = SnapState {
            id: "s1".into(),
            tables: vec!["app.u".into()],
            table: 0,
            after: Some(vec![r#"{"$numberInt":"3"}"#.into()]),
        };
        src.snapshot = Engine::new(st.clone(), &cat, 10, None);
        let Some(Position::Text(text)) = src.checkpoint_at(tok) else {
            panic!("composite expected");
        };
        let back = Checkpoint::decode(&text).unwrap();
        assert_eq!((back.pos.as_str(), back.snap), (tok, Some(st)));
    }

    fn snapshotting_source() -> MongoSource {
        let mut src = MongoSource::new(MongoConfig::new("mongodb://h/", "app"));
        let cat = Catalog {
            database: "app".into(),
            ..Default::default()
        };
        let st = SnapState {
            id: "s1".into(),
            tables: vec!["app.u".into()],
            table: 0,
            after: None,
        };
        src.snapshot = Engine::new(st, &cat, 10, None);
        src
    }

    #[test]
    fn watermark_events_are_never_forwarded() {
        let mut src = snapshotting_source();
        for d in [
            doc! {"_id": {"_data": "01"}, "operationType": "insert",
            "ns": {"db": "app", "coll": WATERMARK_COLLECTION}, "documentKey": {"_id": "s9:1:low"}},
            doc! {"_id": {"_data": "02"}, "operationType": "delete",
            "ns": {"db": "app", "coll": WATERMARK_COLLECTION}, "documentKey": {"_id": "probe:x"}},
        ] {
            assert!(src.observe(&d, "t").unwrap());
        }
        // Without a snapshot too (another instance's watermarks).
        let mut plain = MongoSource::new(MongoConfig::new("mongodb://h/", "app"));
        let d = doc! {"_id": {"_data": "03"}, "operationType": "insert",
        "ns": {"db": "app", "coll": WATERMARK_COLLECTION}, "documentKey": {"_id": "s1:1:low"}};
        assert!(plain.observe(&d, "t").unwrap());
        let user = doc! {"_id": {"_data": "04"}, "operationType": "insert",
        "ns": {"db": "app", "coll": "u"}, "documentKey": {"_id": 1}};
        assert!(!plain.observe(&user, "t").unwrap());
    }

    #[test]
    fn closing_rows_carry_progress_and_the_end_returns_to_plain() {
        let mut src = snapshotting_source();
        let before = src.snapshot.as_ref().unwrap().state().clone();
        let mut after = before.clone();
        after.after = Some(vec!["2".into()]);
        let row = |k: &str| crate::snapshot::read_row("app", "u", &doc! {"_id": k}, 0).unwrap();
        src.queue_closed(
            Closed {
                rows: vec![row("1"), row("2")],
                state: Some(after.clone()),
                watermarks: vec![],
            },
            before.clone(),
            "T",
        );
        let first = src.buffer.pop_front().unwrap();
        let second = src.buffer.pop_front().unwrap();
        assert_eq!(first.op, Op::Read);
        assert_eq!(first.position, Position::Text("T".into()));
        let snap = |e: &ChangeEvent| match &e.checkpoint {
            Some(Position::Text(t)) => Checkpoint::decode(t).unwrap().snap,
            _ => panic!("no checkpoint"),
        };
        assert_eq!(snap(&first), Some(before));
        assert_eq!(snap(&second), Some(after));

        // The last (empty) chunk ends the snapshot with a plain heartbeat.
        src.queue_closed(
            Closed {
                rows: vec![],
                state: None,
                watermarks: vec![],
            },
            src.snapshot.as_ref().unwrap().state().clone(),
            "U",
        );
        let hb = src.buffer.pop_front().unwrap();
        assert_eq!(hb.op, Op::Heartbeat);
        assert_eq!(hb.position, Position::Text("U".into()));
        assert_eq!(hb.checkpoint, None);
        assert!(src.snapshot.is_none());
        assert_eq!(src.last_status.as_ref().unwrap().state, "finished");
    }

    #[test]
    fn live_changes_reach_the_window_by_id() {
        let mut src = snapshotting_source();
        let e = src.snapshot.as_mut().unwrap();
        // Not in flight: nothing is recorded, and nothing fails.
        e.observe("app.u", Live::Key("1".into()));
        let ev = doc! {"_id": {"_data": "05"}, "operationType": "update",
        "ns": {"db": "app", "coll": "u"}, "documentKey": {"_id": 1}};
        assert!(!src.observe(&ev, "t").unwrap());
        let drop = doc! {"_id": {"_data": "06"}, "operationType": "rename",
        "ns": {"db": "app", "coll": "u"}, "to": {"db": "app", "coll": "v"}};
        assert!(!src.observe(&drop, "t").unwrap());
    }

    #[tokio::test]
    async fn srv_uri_is_resolved_not_rejected() {
        // Needs the driver's `dns-resolver` feature; without it the error names that feature.
        let err = Client::with_uri_str("mongodb+srv://aqueduct.invalid/")
            .await
            .unwrap_err()
            .to_string();
        assert!(!err.contains("dns-resolver"), "{err}");
    }
}
