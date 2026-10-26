//! Needs a replica set: `podman compose -f docker-compose.test.yml up -d`, then
//! `MONGO_URI='mongodb://localhost:27017/?replicaSet=rs0&directConnection=true' cargo test -p aqueduct-source-mongodb -- --ignored`
use aqueduct_core::{ChangeEvent, LagState, Metrics, Op, Source};
use aqueduct_source_mongodb::{MongoConfig, MongoSource};
use mongodb::bson::{doc, Document};
use mongodb::Client;
use std::sync::Arc;
use std::time::Duration;

fn uri() -> String {
    std::env::var("MONGO_URI").expect("set MONGO_URI to a replica-set URI")
}

fn cfg(db: &str) -> MongoConfig {
    let mut c = MongoConfig::new(uri(), db);
    c.pre_images = false;
    c
}

async fn next(s: &mut MongoSource) -> ChangeEvent {
    tokio::time::timeout(Duration::from_secs(15), s.next())
        .await
        .expect("timed out waiting for an event")
        .unwrap()
        .unwrap()
}

#[tokio::test]
#[ignore]
async fn insert_update_delete_then_resume() {
    let db_name = format!("aq_it_{}", std::process::id());
    let client = Client::with_uri_str(uri()).await.unwrap();
    let coll = client.database(&db_name).collection::<Document>("users");
    coll.insert_one(doc! {"_id": 0, "seed": true})
        .await
        .unwrap(); // creates the db

    let mut src = MongoSource::new(cfg(&db_name));
    src.start(None).await.unwrap();
    // Without a checkpoint the start position comes first, before any change.
    assert_eq!(next(&mut src).await.op, Op::Heartbeat);

    // One change at a time: `updateLookup` reads the current document, so changes made
    // faster than the source consumes them would see the later state (or none after a delete).
    coll.insert_one(doc! {"_id": 1, "n": "a"}).await.unwrap();
    let ins = next(&mut src).await;
    assert_eq!((ins.op, &*ins.table), (Op::Insert, "users"));

    coll.update_one(doc! {"_id": 1}, doc! {"$set": {"n": "b"}})
        .await
        .unwrap();
    let upd = next(&mut src).await;
    assert_eq!(upd.op, Op::Update);
    assert!(upd.after.as_ref().unwrap().get().contains("\"b\""));

    coll.delete_one(doc! {"_id": 1}).await.unwrap();
    let del = next(&mut src).await;
    assert_eq!(del.op, Op::Delete);
    assert!(del.before.as_ref().unwrap().get().contains("\"_id\":1"));

    // Restart from the update's position: only the delete is redelivered.
    let mut again = MongoSource::new(cfg(&db_name));
    again.start(Some(upd.position.clone())).await.unwrap();
    let redelivered = next(&mut again).await;
    assert_eq!(redelivered.op, Op::Delete);
    assert_eq!(redelivered.position, del.position);

    client.database(&db_name).drop().await.unwrap();
}

/// Writes to a collection the source filters out; the source must still hand the pipeline a
/// heartbeat so the checkpoint follows the stream. Not run here (needs a replica set).
#[tokio::test]
#[ignore]
async fn filtered_writes_produce_a_heartbeat() {
    let db_name = format!("aq_hb_{}", std::process::id());
    let client = Client::with_uri_str(uri()).await.unwrap();
    let db = client.database(&db_name);
    db.collection::<Document>("watched")
        .insert_one(doc! {"_id": 0})
        .await
        .unwrap();

    let mut c = cfg(&db_name);
    c.collections = vec!["watched".into()];
    let mut src = MongoSource::new(c).with_heartbeat_every(Duration::from_secs(1));
    src.start(None).await.unwrap();
    let other = db.collection::<Document>("other");
    other.insert_one(doc! {"_id": 1}).await.unwrap();

    // The first heartbeat may predate the insert above, so it proves only the path.
    let first = next(&mut src).await;
    assert_eq!(first.op, Op::Heartbeat);
    assert!(matches!(first.position, aqueduct_core::Position::Text(_)));

    // Only filtered writes happen from here, so a second heartbeat with a different token
    // shows the token advances past events the source never emits.
    other.insert_one(doc! {"_id": 2}).await.unwrap();
    let second = next(&mut src).await;
    assert_eq!(second.op, Op::Heartbeat);
    assert_ne!(first.position.to_string(), second.position.to_string());

    db.drop().await.unwrap();
}

/// A steady run of unmapped events (`drop` of a watched collection) never produces an empty
/// batch, yet the source must still heartbeat, at the skipped doc's own token. Not run here
/// (needs a replica set).
#[tokio::test]
#[ignore]
async fn skipped_ddl_still_produces_a_heartbeat() {
    let db_name = format!("aq_ddl_{}", std::process::id());
    let client = Client::with_uri_str(uri()).await.unwrap();
    let db = client.database(&db_name);
    db.collection::<Document>("watched")
        .insert_one(doc! {"_id": 0})
        .await
        .unwrap();

    let mut c = cfg(&db_name);
    c.collections = vec!["watched".into()];
    let mut src = MongoSource::new(c).with_heartbeat_every(Duration::from_secs(1));
    src.start(None).await.unwrap();
    // The start position; the heartbeat under test comes after it.
    assert_eq!(next(&mut src).await.op, Op::Heartbeat);

    // Drop, then recreate, each round: `watched` exists at start, so the first drop is real and
    // so is every later one. `create` events need `showExpandedEvents`, which the source does
    // not set, so only unmapped `drop` events arrive, and getMores keep returning data for
    // longer than the 1s heartbeat interval.
    let churn_db = db.clone();
    let churn = tokio::spawn(async move {
        let end = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < end {
            churn_db
                .collection::<Document>("watched")
                .drop()
                .await
                .unwrap();
            churn_db.create_collection("watched").await.unwrap();
        }
    });
    // Smoke test only: it cannot show which branch emitted the heartbeat. The unit test
    // `skipped_doc_token_is_its_own_id` and code review cover that.
    let hb = next(&mut src).await;
    assert_eq!(hb.op, Op::Heartbeat);
    assert!(matches!(hb.position, aqueduct_core::Position::Text(_)));

    // Bounded by the 5s loop; a panic in the churn task fails the test.
    churn.await.unwrap();
    db.drop().await.unwrap();
}

/// Without a checkpoint the stream opens at "now". Its opening token must be handed out (and so
/// checkpointed) before anything else: otherwise a restart before the first checkpoint would open
/// at a later "now" and silently skip every change made in between.
#[tokio::test]
#[ignore]
async fn start_position_is_durable_before_the_first_change() {
    let db_name = format!("aq_start_{}", mongodb::bson::oid::ObjectId::new().to_hex());
    let client = Client::with_uri_str(uri()).await.unwrap();
    let coll = client.database(&db_name).collection::<Document>("users");
    coll.insert_one(doc! {"_id": 0}).await.unwrap();

    let mut src = MongoSource::new(cfg(&db_name));
    src.start(None).await.unwrap();
    let hb = next(&mut src).await;
    assert_eq!(hb.op, Op::Heartbeat);
    let saved = hb.checkpoint.clone().unwrap_or(hb.position.clone());
    // The process dies before it reads anything else; this change happens meanwhile.
    drop(src);
    coll.insert_one(doc! {"_id": 1}).await.unwrap();

    let mut again = MongoSource::new(cfg(&db_name));
    again.start(Some(saved)).await.unwrap();
    let ev = next(&mut again).await;
    assert_eq!(ev.op, Op::Insert);
    assert_eq!(ev.key.as_deref(), Some(r#"{"_id":1}"#));

    client.database(&db_name).drop().await.unwrap();
}

/// "Behind source" (#150): `hello` gives the server's newest cluster time and every ack the
/// delivered one, both in ms; right after a change is delivered the source is caught up.
#[tokio::test]
#[ignore]
async fn reports_cluster_times_for_the_behind_source_card() {
    let db_name = format!("aq_lag_{}", std::process::id());
    let client = Client::with_uri_str(uri()).await.unwrap();
    let coll = client.database(&db_name).collection::<Document>("users");
    coll.insert_one(doc! {"_id": 0}).await.unwrap();
    let metrics = Arc::new(Metrics::new());
    let mut src = MongoSource::new(cfg(&db_name)).with_metrics(metrics.clone());
    src.start(None).await.unwrap();
    assert_eq!(next(&mut src).await.op, Op::Heartbeat);
    coll.insert_one(doc! {"_id": 1}).await.unwrap();
    let ev = next(&mut src).await;
    assert_eq!(ev.op, Op::Insert);
    src.ack(ev.checkpoint.clone().unwrap_or_else(|| ev.position.clone()))
        .await
        .unwrap();
    // `hello` runs once a second in its own task.
    let mut snap = metrics.snapshot();
    for _ in 0..50 {
        if snap.source_head_time_ms.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        snap = metrics.snapshot();
    }
    let head = snap
        .source_head_time_ms
        .expect("hello reports the cluster time");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert!(head.abs_diff(now) < 60_000, "head {head}, now {now}");
    let lag = snap.source_lag;
    assert_eq!(lag.rate_unit, Some("s/s"));
    assert_eq!(lag.backlog_bytes, None);
    assert_eq!(lag.state, LagState::CaughtUp, "{lag:?}");
    assert!(lag.behind_secs.unwrap() <= 2.0);
    client.database(&db_name).drop().await.unwrap();
}

/// Dropping the watched database invalidates the change stream. The source must fail, not end
/// cleanly (the pipeline would exit with success and a restart would end at the same place), and
/// a restart from the last checkpoint must fail the same way until the operator resets it.
#[tokio::test]
#[ignore]
async fn a_dropped_database_fails_the_source_instead_of_ending_it() {
    let db_name = format!("aq_inv_{}", mongodb::bson::oid::ObjectId::new().to_hex());
    let client = Client::with_uri_str(uri()).await.unwrap();
    let coll = client.database(&db_name).collection::<Document>("users");
    coll.insert_one(doc! {"_id": 0}).await.unwrap();

    let mut src = MongoSource::new(cfg(&db_name));
    src.start(None).await.unwrap();
    let hb = next(&mut src).await;
    assert_eq!(hb.op, Op::Heartbeat);
    let saved = hb.checkpoint.clone().unwrap_or(hb.position.clone());
    client.database(&db_name).drop().await.unwrap();

    let failed = |r: Result<aqueduct_core::Result<Option<ChangeEvent>>, _>| match r {
        Ok(Err(e)) => e.to_string(),
        other => panic!("expected the source to fail, got {other:?}"),
    };
    let err = failed(tokio::time::timeout(Duration::from_secs(15), src.next()).await);
    assert!(err.contains("invalidated the change stream"), "{err}");

    let mut again = MongoSource::new(cfg(&db_name));
    again.start(Some(saved)).await.unwrap();
    let err = failed(tokio::time::timeout(Duration::from_secs(15), again.next()).await);
    assert!(err.contains("invalidated the change stream"), "{err}");
}
