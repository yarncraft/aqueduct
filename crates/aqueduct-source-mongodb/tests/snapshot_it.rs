//! Snapshots against a real replica set (ADR 0001, #77). Needs `MONGO_URI`, see `replica_set.rs`:
//! `MONGO_URI='mongodb://localhost:27017/?replicaSet=rs0&directConnection=true' cargo test -p aqueduct-source-mongodb --test snapshot_it -- --ignored`
use aqueduct_core::{
    snapshot_channel, ChangeEvent, Checkpoint, Metrics, Op, Position, SnapshotCommand,
    SnapshotRequest, SnapshotSender, Source,
};
use aqueduct_source_mongodb::{MongoConfig, MongoSource, SnapshotMode};
use mongodb::bson::{doc, oid::ObjectId, Bson, DateTime, Document};
use mongodb::Client;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

fn uri() -> String {
    std::env::var("MONGO_URI").expect("set MONGO_URI to a replica-set URI")
}

fn db_name(test: &str) -> String {
    // Process ids repeat across container runs; an ObjectId does not.
    format!("aq_snap_{test}_{}", ObjectId::new().to_hex())
}

fn cfg(db: &str, mode: SnapshotMode, chunk: u32) -> MongoConfig {
    let mut c = MongoConfig::new(uri(), db);
    c.pre_images = false;
    c.snapshot = mode;
    c.snapshot_chunk_size = chunk;
    c
}

/// Drives a source the way the pipeline does: a batch is `next` plus everything `try_next`
/// has, its last checkpoint is "saved" and acked, heartbeats are not delivered.
struct Driver {
    src: MongoSource,
    metrics: Arc<Metrics>,
    control: SnapshotSender,
    checkpoint: Option<Position>,
    events: Vec<ChangeEvent>,
    /// Writes here wake `next` up (the whole database is watched where requests are sent).
    ping: mongodb::Collection<Document>,
}

impl Driver {
    async fn start(cfg: MongoConfig, from: Option<Position>) -> Self {
        let metrics = Arc::new(Metrics::new());
        let (control, rx) = snapshot_channel();
        let ping = Client::with_uri_str(uri())
            .await
            .unwrap()
            .database(&cfg.database)
            .collection("ping");
        let mut src = MongoSource::new(cfg)
            .with_heartbeat_every(Duration::from_secs(1))
            .with_metrics(metrics.clone())
            .with_snapshot_control(rx);
        src.start(from.clone()).await.unwrap();
        Self {
            src,
            metrics,
            control,
            checkpoint: from,
            events: Vec::new(),
            ping,
        }
    }

    /// One batch; returns it, heartbeats included.
    async fn step(&mut self) -> Vec<ChangeEvent> {
        let first = tokio::time::timeout(Duration::from_secs(30), self.src.next())
            .await
            .expect("timed out waiting for an event")
            .unwrap()
            .expect("the stream ended");
        let mut batch = vec![first];
        while let Some(e) = self.src.try_next() {
            batch.push(e);
        }
        let last = batch.last().unwrap();
        let cp = last.checkpoint.clone().unwrap_or(last.position.clone());
        self.checkpoint = Some(cp.clone());
        self.src.ack(cp).await.unwrap();
        self.events
            .extend(batch.iter().filter(|e| e.op != Op::Heartbeat).cloned());
        batch
    }

    async fn until(&mut self, what: &str, done: impl Fn(&Self) -> bool) {
        let end = Instant::now() + Duration::from_secs(180);
        while !done(self) {
            assert!(Instant::now() < end, "timed out waiting until {what}");
            self.step().await;
        }
    }

    fn snapshot_state(&self) -> Option<String> {
        self.metrics.snapshot().snapshot.map(|s| s.state)
    }

    fn reads(&self) -> usize {
        self.events.iter().filter(|e| e.op == Op::Read).count()
    }

    /// Sends a request; the source answers it inside `next`, which a write then wakes up.
    async fn request(&mut self, command: SnapshotCommand) -> Result<String, String> {
        let (reply, mut answer) = oneshot::channel();
        self.control
            .try_send(SnapshotRequest { command, reply })
            .unwrap();
        let end = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(a) = answer.try_recv() {
                return a;
            }
            assert!(Instant::now() < end, "no answer to the snapshot request");
            self.ping.insert_one(doc! {}).await.unwrap();
            self.step().await;
        }
    }

    /// Inserts a marker document and drives until its insert arrives: everything before it has.
    async fn sync(&mut self, client: &Client, db: &str, coll: &str) {
        let id = format!("sync-{}", ObjectId::new());
        client
            .database(db)
            .collection::<Document>(coll)
            .insert_one(doc! {"_id": &id})
            .await
            .unwrap();
        let key = format!(r#"{{"_id":"{id}"}}"#);
        self.until("the marker arrived", |d| {
            d.events.iter().any(|e| e.key.as_deref() == Some(&key))
        })
        .await;
    }
}

/// The documentKey JSON of a document, as events carry it in `key`.
fn key_of(doc: &Document) -> String {
    Bson::Document(doc! {"_id": doc.get("_id").unwrap().clone()})
        .into_relaxed_extjson()
        .to_string()
}

/// Last write wins over `events` of `table`, by `key`.
fn fold(events: &[ChangeEvent], table: &str) -> BTreeMap<String, Value> {
    let mut m = BTreeMap::new();
    for e in events.iter().filter(|e| e.table == table) {
        let k = e.key.clone().expect("every event has a key");
        match e.op {
            Op::Delete => {
                m.remove(&k);
            }
            _ => {
                let after: Value = serde_json::from_str(e.after.as_ref().unwrap().get()).unwrap();
                m.insert(k, after);
            }
        }
    }
    m
}

async fn contents(client: &Client, db: &str, coll: &str) -> BTreeMap<String, Value> {
    let mut cur = client
        .database(db)
        .collection::<Document>(coll)
        .find(doc! {})
        .await
        .unwrap();
    let mut m = BTreeMap::new();
    while cur.advance().await.unwrap() {
        let d = cur.deserialize_current().unwrap();
        m.insert(key_of(&d), Bson::Document(d).into_relaxed_extjson());
    }
    m
}

async fn populate(client: &Client, db: &str, coll: &str, n: i32) {
    let docs: Vec<Document> = (0..n)
        .map(|i| doc! {"_id": i, "v": 0, "name": format!("doc {i}")})
        .collect();
    client
        .database(db)
        .collection::<Document>(coll)
        .insert_many(docs)
        .await
        .unwrap();
}

fn snap_of(p: &Option<Position>) -> Option<aqueduct_core::SnapState> {
    match p {
        Some(Position::Text(t)) => Checkpoint::decode(t).unwrap().snap,
        _ => None,
    }
}

/// A pre-populated collection arrives complete while updates, deletes and inserts run: folding
/// the events (last write wins) gives exactly the collection. Watermarks never reach the sink and
/// are deleted at the end.
#[tokio::test]
#[ignore]
async fn initial_snapshot_is_complete_while_writes_run() {
    let db = db_name("initial");
    let client = Client::with_uri_str(uri()).await.unwrap();
    populate(&client, &db, "items", 5000).await;

    let mut c = cfg(&db, SnapshotMode::Initial, 50);
    c.collections = vec!["items".into()];
    let mut d = Driver::start(c, None).await;
    // The first thing handed out is the start position, already carrying the snapshot.
    let first = d.step().await;
    assert_eq!(first[0].op, Op::Heartbeat);
    let snap = snap_of(&d.checkpoint).expect("the start checkpoint carries the snapshot");
    assert_eq!(snap.tables, [format!("{db}.items")]);

    // Writers hit the documents just ahead of the snapshot's progress, so changes land inside
    // chunk windows (between the chunk read and its high watermark) and the window must drop
    // the rows they touch. Without that the fold below ends on stale snapshot rows.
    let stop = Arc::new(AtomicBool::new(false));
    let frontier = Arc::new(AtomicI64::new(0));
    let mut writers = Vec::new();
    for w in 0..3u64 {
        let (stop, frontier, coll) = (
            stop.clone(),
            frontier.clone(),
            client.database(&db).collection::<Document>("items"),
        );
        writers.push(tokio::spawn(async move {
            let mut x: u64 = 0x2545_f491_4f6c_dd1d ^ (w << 32);
            let mut next_id = 10_000 * (w as i32 + 1);
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let id = frontier.load(Ordering::Relaxed) as i32 + (x % 250) as i32;
                match x % 10 {
                    0 => {
                        coll.delete_one(doc! {"_id": id}).await.unwrap();
                    }
                    1 => {
                        coll.insert_one(doc! {"_id": next_id, "v": 1, "name": "new"})
                            .await
                            .unwrap();
                        next_id += 1;
                    }
                    2 => {
                        // Re-creates a deleted document (or replaces a live one).
                        coll.replace_one(doc! {"_id": id}, doc! {"v": -1, "name": "again"})
                            .upsert(true)
                            .await
                            .unwrap();
                    }
                    _ => {
                        coll.update_one(doc! {"_id": id}, doc! {"$inc": {"v": 1}})
                            .await
                            .unwrap();
                    }
                }
                n += 1;
            }
            n
        }));
    }

    let end = Instant::now() + Duration::from_secs(180);
    while d.snapshot_state().as_deref() != Some("finished") {
        assert!(Instant::now() < end, "timed out waiting for the snapshot");
        d.step().await;
        frontier.store(d.reads() as i64, Ordering::Relaxed);
    }
    stop.store(true, Ordering::Relaxed);
    let mut writes = 0;
    for w in writers {
        writes += w.await.unwrap();
    }
    d.sync(&client, &db, "items").await;

    assert!(
        writes > 200,
        "too few concurrent writes ({writes}) to test anything"
    );
    assert!(d.reads() > 2000, "only {} reads", d.reads());
    assert!(d.events.iter().all(|e| e.table != "aqueduct_watermarks"));
    assert_eq!(
        fold(&d.events, "items"),
        contents(&client, &db, "items").await
    );
    // The checkpoint is back to the plain resume token.
    assert_eq!(snap_of(&d.checkpoint), None);
    let left = client
        .database(&db)
        .collection::<Document>("aqueduct_watermarks")
        .count_documents(doc! {"_id": {"$regex": format!("^{}:", snap.id)}})
        .await
        .unwrap();
    assert_eq!(left, 0, "watermark documents are deleted");

    client.database(&db).drop().await.unwrap();
}

/// `_id`s of many BSON types, paged two at a time: every document is read exactly once, and each
/// `read` row is byte-identical to the `insert` of the same document (same `after`, same key).
#[tokio::test]
#[ignore]
async fn mixed_id_types_are_all_read_and_encoded_like_inserts() {
    let db = db_name("mixed");
    let client = Client::with_uri_str(uri()).await.unwrap();
    let docs = vec![
        doc! {"_id": 1_i32, "t": "int"},
        doc! {"_id": 2_i64, "t": "long"},
        doc! {"_id": 2.5_f64, "t": "double"},
        doc! {"_id": "a", "t": "string"},
        doc! {"_id": "b", "t": "string", "nested": {"x": [1, 2, {"y": null}]}},
        doc! {"_id": {"k": 1}, "t": "document"},
        doc! {"_id": ObjectId::new(), "t": "oid", "when": DateTime::from_millis(1_700_000_000_000)},
        doc! {"_id": true, "t": "bool"},
        doc! {"_id": DateTime::from_millis(5), "t": "date"},
        doc! {"_id": -7_i32, "t": "negative"},
    ];
    let coll = client.database(&db).collection::<Document>("mixed");
    coll.insert_many(docs.clone()).await.unwrap();

    let mut d = Driver::start(cfg(&db, SnapshotMode::OnDemand, 2), None).await;
    d.step().await; // the start position
                    // Validation: unknown and foreign collections are refused.
    let err = d
        .request(SnapshotCommand::Start(vec![format!("{db}.nope")]))
        .await
        .unwrap_err();
    assert!(err.contains("no such collection"), "{err}");
    let err = d
        .request(SnapshotCommand::Start(vec!["other.mixed".into()]))
        .await
        .unwrap_err();
    assert!(err.contains("not in database"), "{err}");
    d.request(SnapshotCommand::Start(vec![format!("{db}.mixed")]))
        .await
        .unwrap();
    d.until("the snapshot finished", |d| {
        d.snapshot_state().as_deref() == Some("finished")
    })
    .await;
    let reads: Vec<ChangeEvent> = d
        .events
        .iter()
        .filter(|e| e.op == Op::Read)
        .cloned()
        .collect();
    let mut seen: HashMap<String, usize> = HashMap::new();
    for r in &reads {
        *seen.entry(r.key.clone().unwrap()).or_default() += 1;
    }
    let want: HashMap<String, usize> = docs.iter().map(|d| (key_of(d), 1)).collect();
    assert_eq!(seen, want, "every _id exactly once");

    // Insert every document again: the live inserts must match the reads byte for byte.
    coll.delete_many(doc! {}).await.unwrap();
    coll.insert_many(docs.clone()).await.unwrap();
    let n = d.events.len();
    d.until("the inserts arrived", |d| {
        d.events[n..].iter().filter(|e| e.op == Op::Insert).count() == docs.len()
    })
    .await;
    for ins in d.events[n..].iter().filter(|e| e.op == Op::Insert) {
        let read = reads.iter().find(|r| r.key == ins.key).unwrap();
        assert_eq!(
            read.after.as_ref().unwrap().get(),
            ins.after.as_ref().unwrap().get()
        );
        assert_eq!(
            (&read.schema, &read.table, read.before.is_none()),
            (&ins.schema, &ins.table, true)
        );
    }

    client.database(&db).drop().await.unwrap();
}

/// A crash mid-snapshot: the restart resumes from the checkpoint's cursor and the union of what
/// both runs delivered is the whole collection (duplicates only, nothing lost).
#[tokio::test]
#[ignore]
async fn restart_mid_snapshot_resumes_with_duplicates_only() {
    let db = db_name("restart");
    let client = Client::with_uri_str(uri()).await.unwrap();
    populate(&client, &db, "items", 1000).await;
    let mut c = cfg(&db, SnapshotMode::Initial, 50);
    c.collections = vec!["items".into()];

    let mut d = Driver::start(c.clone(), None).await;
    d.until("some rows were read", |d| d.reads() >= 300).await;
    let saved = d.checkpoint.clone();
    let snap = snap_of(&saved).expect("mid-snapshot checkpoint carries progress");
    assert!(snap.after.is_some(), "{snap:?}");
    let first_run = d.events.clone();
    drop(d); // the crash: nothing after the saved checkpoint counts

    // A change while down is delivered after the restart.
    client
        .database(&db)
        .collection::<Document>("items")
        .update_one(doc! {"_id": 999}, doc! {"$set": {"v": 42}})
        .await
        .unwrap();

    let mut d = Driver::start(c, saved).await;
    d.until("the snapshot finished", |d| {
        d.snapshot_state().as_deref() == Some("finished")
    })
    .await;
    d.sync(&client, &db, "items").await;
    assert!(
        d.reads() < 1000,
        "the restart resumed instead of starting over ({} reads)",
        d.reads()
    );
    let mut all = first_run;
    all.extend(d.events.iter().cloned());
    assert_eq!(fold(&all, "items"), contents(&client, &db, "items").await);

    client.database(&db).drop().await.unwrap();
}

/// `DELETE /api/snapshot` reaches the source as `Cancel`: it stops after the chunk in flight and
/// the checkpoint returns to the plain token. A restart does not resume it.
#[tokio::test]
#[ignore]
async fn cancel_stops_the_snapshot() {
    let db = db_name("cancel");
    let client = Client::with_uri_str(uri()).await.unwrap();
    populate(&client, &db, "items", 2000).await;
    let mut c = cfg(&db, SnapshotMode::OnDemand, 10);
    c.snapshot_max_rows_per_sec = Some(200);

    let mut d = Driver::start(c.clone(), None).await;
    d.step().await;
    assert_eq!(
        d.request(SnapshotCommand::Cancel).await.unwrap_err(),
        "no snapshot is running"
    );
    let id = d
        .request(SnapshotCommand::Start(vec![format!("{db}.items")]))
        .await
        .unwrap();
    d.until("some rows were read", |d| d.reads() >= 20).await;
    assert_eq!(d.request(SnapshotCommand::Cancel).await.unwrap(), id);
    d.until("the snapshot was cancelled", |d| {
        d.snapshot_state().as_deref() == Some("cancelled")
    })
    .await;
    d.sync(&client, &db, "items").await;
    assert!(d.reads() < 2000, "cancelled early ({} reads)", d.reads());
    assert_eq!(snap_of(&d.checkpoint), None, "back to the plain token");

    let reads = d.reads();
    let mut again = Driver::start(c, d.checkpoint.clone()).await;
    again.sync(&client, &db, "items").await;
    assert_eq!(again.reads(), 0);
    assert!(reads > 0);

    client.database(&db).drop().await.unwrap();
}

/// `snapshot = "never"` copies nothing, writes nothing, refuses requests, and abandons a
/// snapshot a checkpoint still carries.
#[tokio::test]
#[ignore]
async fn never_copies_nothing() {
    let db = db_name("never");
    let client = Client::with_uri_str(uri()).await.unwrap();
    populate(&client, &db, "items", 50).await;

    let mut d = Driver::start(cfg(&db, SnapshotMode::Never, 10), None).await;
    let err = d
        .request(SnapshotCommand::Start(vec![format!("{db}.items")]))
        .await
        .unwrap_err();
    assert!(err.contains("snapshots are off"), "{err}");
    d.sync(&client, &db, "items").await;
    assert_eq!(d.reads(), 0);
    let names = client.database(&db).list_collection_names().await.unwrap();
    assert!(!names.contains(&"aqueduct_watermarks".to_string()));

    // A checkpoint from a run with snapshots on: its snapshot is dropped, the stream resumes.
    let Some(Position::Text(tok)) = d.checkpoint.clone() else {
        panic!("no checkpoint")
    };
    let composite = Checkpoint {
        pos: tok,
        snap: Some(aqueduct_core::SnapState {
            id: "s1".into(),
            tables: vec![format!("{db}.items")],
            table: 0,
            after: None,
        }),
    }
    .encode();
    let mut again = Driver::start(
        cfg(&db, SnapshotMode::Never, 10),
        Some(Position::Text(composite)),
    )
    .await;
    again.sync(&client, &db, "items").await;
    assert_eq!(again.reads(), 0);
    assert_eq!(snap_of(&again.checkpoint), None);

    client.database(&db).drop().await.unwrap();
}

/// A chunk read that fails every time (here: a cursor that cannot be parsed) does not crash-loop
/// the source: after a few attempts the collection is skipped with the reason, the snapshot moves
/// on, and live changes keep flowing.
#[tokio::test]
#[ignore]
async fn a_chunk_that_always_fails_skips_its_collection() {
    let db = db_name("failing");
    let client = Client::with_uri_str(uri()).await.unwrap();
    populate(&client, &db, "bad", 10).await;
    populate(&client, &db, "good", 10).await;
    let mut c = cfg(&db, SnapshotMode::OnDemand, 5);
    c.collections = vec!["bad".into(), "good".into()];

    // A checkpoint whose snapshot cursor for `bad` is garbage.
    let mut first = Driver::start(c.clone(), None).await;
    first.step().await;
    let Some(Position::Text(tok)) = first.checkpoint.clone() else {
        panic!("no start checkpoint")
    };
    drop(first);
    let composite = Checkpoint {
        pos: tok,
        snap: Some(aqueduct_core::SnapState {
            id: "s1".into(),
            tables: vec![format!("{db}.bad"), format!("{db}.good")],
            table: 0,
            after: Some(vec!["{not json".into()]),
        }),
    }
    .encode();
    let mut d = Driver::start(c, Some(Position::Text(composite))).await;
    d.until("the snapshot finished", |d| {
        d.snapshot_state().as_deref() == Some("finished")
    })
    .await;
    let st = d.metrics.snapshot().snapshot.unwrap();
    assert_eq!(st.skipped.len(), 1, "{st:?}");
    assert_eq!(st.skipped[0].table, format!("{db}.bad"));
    assert!(st.skipped[0].reason.contains("chunk read failed"), "{st:?}");
    assert!(d
        .events
        .iter()
        .all(|e| e.table != "bad" || e.op != Op::Read));
    assert_eq!(
        d.events
            .iter()
            .filter(|e| e.table == "good" && e.op == Op::Read)
            .count(),
        10
    );
    d.sync(&client, &db, "bad").await;

    client.database(&db).drop().await.unwrap();
}

/// An existing TTL index on `{at: 1}` under another name is accepted; a non-TTL index there is
/// reported as a conflict, not as a missing privilege.
#[tokio::test]
#[ignore]
async fn an_existing_watermark_index_is_accepted_or_reported() {
    use mongodb::options::IndexOptions;
    use mongodb::IndexModel;
    let db = db_name("ttl");
    let client = Client::with_uri_str(uri()).await.unwrap();
    let wm = client
        .database(&db)
        .collection::<Document>("aqueduct_watermarks");
    let index = |ttl: Option<u64>| {
        IndexModel::builder()
            .keys(doc! {"at": 1})
            .options(
                IndexOptions::builder()
                    .name("theirs".to_string())
                    .expire_after(ttl.map(Duration::from_secs))
                    .build(),
            )
            .build()
    };
    wm.create_index(index(Some(60))).await.unwrap();
    let mut src = MongoSource::new(cfg(&db, SnapshotMode::OnDemand, 10));
    src.start(None).await.unwrap();
    drop(src);

    wm.drop_index("theirs").await.unwrap();
    wm.create_index(index(None)).await.unwrap();
    let mut src = MongoSource::new(cfg(&db, SnapshotMode::OnDemand, 10));
    let err = src.start(None).await.unwrap_err().to_string();
    assert!(err.contains("conflicts"), "{err}");
    assert!(!err.contains("privileges"), "{err}");

    client.database(&db).drop().await.unwrap();
}
