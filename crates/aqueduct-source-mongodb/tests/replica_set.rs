//! Needs a replica set: `podman compose -f docker-compose.test.yml up -d`, then
//! `MONGO_URI='mongodb://localhost:27017/?replicaSet=rs0&directConnection=true' cargo test -p aqueduct-source-mongodb -- --ignored`
use aqueduct_core::{ChangeEvent, Op, Source};
use aqueduct_source_mongodb::{MongoConfig, MongoSource};
use mongodb::bson::{doc, Document};
use mongodb::Client;
use std::time::Duration;

fn uri() -> String {
    std::env::var("MONGO_URI").expect("set MONGO_URI to a replica-set URI")
}

fn cfg(db: &str) -> MongoConfig {
    MongoConfig {
        uri: uri(),
        database: db.into(),
        collections: vec![],
        pre_images: false,
    }
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

    // One change at a time: `updateLookup` reads the current document, so changes made
    // faster than the source consumes them would see the later state (or none after a delete).
    coll.insert_one(doc! {"_id": 1, "n": "a"}).await.unwrap();
    let ins = next(&mut src).await;
    assert_eq!((ins.op, ins.table.as_str()), (Op::Insert, "users"));

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

    // Recreate the collection each round so every drop writes an oplog entry. `create` events
    // need `showExpandedEvents`, which the source does not set, so only unmapped `drop` events
    // arrive, and getMores keep returning data for longer than the 1s heartbeat interval.
    let churn_db = db.clone();
    let churn = tokio::spawn(async move {
        let end = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < end {
            churn_db.create_collection("watched").await.unwrap();
            churn_db
                .collection::<Document>("watched")
                .drop()
                .await
                .unwrap();
        }
    });
    // Smoke test only: it cannot show which branch emitted the heartbeat. The unit test
    // `skipped_doc_token_is_its_own_id` and code review cover that.
    let hb = next(&mut src).await;
    assert_eq!(hb.op, Op::Heartbeat);
    assert!(matches!(hb.position, aqueduct_core::Position::Text(_)));

    churn.abort();
    db.drop().await.unwrap();
}
