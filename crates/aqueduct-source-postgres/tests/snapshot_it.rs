//! Snapshots against a real server (ADR 0001, #74). Needs the plaintext PG14+ server the
//! integration suite uses (`wal_level=logical`, user postgres / password aq):
//!
//! ```text
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434
//! cargo test -p aqueduct-source-postgres --test snapshot_it -- --ignored
//! ```
//!
//! Each test makes its own tables, publication and slot, and drives the source the way the
//! pipeline does: take events, then ack the last one's checkpoint.
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use aqueduct_core::{ChangeEvent, Op, Position, Source};
use aqueduct_source_postgres::{
    PostgresSource, PostgresSourceConfig, SnapshotConfig, SnapshotMode, SslMode,
};
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls};

fn url() -> String {
    let addr = std::env::var("AQUEDUCT_TEST_PG_PLAIN")
        .unwrap_or_else(|_| panic!("set AQUEDUCT_TEST_PG_PLAIN, see the module docs"));
    format!("postgres://postgres:aq@{addr}/postgres")
}

async fn connect() -> Client {
    let (c, conn) = tokio_postgres::connect(&url(), NoTls).await.unwrap();
    tokio::spawn(conn);
    c
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos % 1_000_000_000)
}

fn source(slot: &str, publication: &str, mode: SnapshotMode, chunk: u32) -> PostgresSource {
    let mut cfg = PostgresSourceConfig::new(url(), slot, publication);
    cfg.ssl_mode = Some(SslMode::Disable);
    cfg.poll_interval = Duration::from_millis(50);
    cfg.snapshot = SnapshotConfig {
        mode,
        chunk_size: chunk,
        ..SnapshotConfig::default()
    };
    PostgresSource::new(cfg)
}

/// Takes events until `done` says so or nothing arrives for `idle`, acking like the pipeline.
async fn collect(
    s: &mut PostgresSource,
    idle: Duration,
    mut done: impl FnMut(&[ChangeEvent]) -> bool,
) -> Vec<ChangeEvent> {
    let mut out = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline && !done(&out) {
        match tokio::time::timeout(idle, s.next()).await {
            Ok(r) => {
                let ev = r.unwrap().expect("the source never ends");
                if let Some(c) = ev.checkpoint.clone() {
                    s.ack(c).await.unwrap();
                }
                if ev.op != Op::Heartbeat {
                    out.push(ev);
                }
            }
            Err(_) => break,
        }
    }
    out
}

async fn drop_slot(db: &Client, slot: &str) {
    for _ in 0..50 {
        match db
            .execute("SELECT pg_drop_replication_slot($1::text)", &[&slot])
            .await
        {
            Err(e) if e.code() == Some(&SqlState::OBJECT_IN_USE) => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            r => {
                r.unwrap();
                return;
            }
        }
    }
    panic!("slot {slot} stayed in use");
}

fn after(ev: &ChangeEvent) -> serde_json::Value {
    serde_json::from_str(ev.after.as_ref().unwrap().get()).unwrap()
}

/// Folds events into the current rows by `id`, as a last-write-wins consumer would.
fn fold(events: &[ChangeEvent], table: &str) -> BTreeMap<i64, serde_json::Value> {
    let mut rows = BTreeMap::new();
    for ev in events.iter().filter(|e| e.table == table) {
        match ev.op {
            Op::Insert | Op::Update | Op::Read => {
                let v = after(ev);
                rows.insert(v["id"].as_i64().unwrap(), v);
            }
            Op::Delete => {
                let v: serde_json::Value =
                    serde_json::from_str(ev.before.as_ref().unwrap().get()).unwrap();
                rows.remove(&v["id"].as_i64().unwrap());
            }
            Op::Truncate => rows.clear(),
            Op::Heartbeat => {}
        }
    }
    rows
}

async fn table_rows(db: &Client, table: &str) -> BTreeMap<i64, serde_json::Value> {
    db.query(
        &format!("SELECT id::int8, to_jsonb(t)::text FROM {table} t"),
        &[],
    )
    .await
    .unwrap()
    .iter()
    .map(|r| {
        let id: i64 = r.get(0);
        let json: String = r.get(1);
        (id, serde_json::from_str(&json).unwrap())
    })
    .collect()
}

/// A pre-populated table arrives complete as `read` events, in several chunks, while live
/// updates, deletes and inserts run against it; a last-write-wins consumer ends up with exactly
/// the table's rows.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn initial_snapshot_copies_every_row_while_changes_stream() {
    let db = connect().await;
    let table = unique("snap_full");
    let publication = unique("snap_full_pub");
    let slot = unique("snap_full");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, v text NOT NULL);
         INSERT INTO {table} SELECT g, 'old ' || g FROM generate_series(1, 2500) g;
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();

    let mut s = source(&slot, &publication, SnapshotMode::Initial, 200);
    s.start(None).await.unwrap();
    // Live writes while the snapshot runs: some rows change, some go, some are new.
    let writer = {
        let table = table.clone();
        tokio::spawn(async move {
            let db = connect().await;
            for i in 0..40 {
                let id = 1 + i * 61;
                db.batch_execute(&format!(
                    "UPDATE {table} SET v = 'new {id}' WHERE id = {id};
                     DELETE FROM {table} WHERE id = {};
                     INSERT INTO {table} VALUES ({}, 'fresh');",
                    id + 1,
                    3000 + i
                ))
                .await
                .unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };
    let events = collect(&mut s, Duration::from_secs(5), |_| false).await;
    writer.await.unwrap();
    // Whatever the writer did after the last chunk is still streaming.
    let mut events = events;
    events.extend(collect(&mut s, Duration::from_secs(3), |_| false).await);

    let reads = events.iter().filter(|e| e.op == Op::Read).count();
    assert!(reads >= 2400, "most rows arrive as reads: {reads}");
    assert!(
        events
            .iter()
            .filter(|e| e.op == Op::Read)
            .all(|e| e.before.is_none() && e.xid != 0),
        "reads have no before image and belong to the watermark's transaction"
    );
    assert_eq!(
        fold(&events, &table),
        table_rows(&db, &table).await,
        "a last-write-wins consumer must end up with the table"
    );
    drop(s);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}

/// A `read` row is byte-identical to an `insert` of the same row, with the same Kafka key part.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn read_rows_are_encoded_exactly_like_inserts() {
    let db = connect().await;
    let table = unique("snap_types");
    let publication = unique("snap_types_pub");
    let slot = unique("snap_types");
    let values = "1, 'text \"q\" \\ x', true, 1.5::float8, 12.30::numeric, \
                  '2024-01-02 03:04:05+00'::timestamptz, '{\"a\": [1, 2]}'::jsonb, \
                  'c3b2e1a0-0000-4000-8000-000000000001'::uuid, NULL::int, 9000000000::int8";
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, t text, b bool, f float8, n numeric,
             ts timestamptz, j jsonb, u uuid, z int, big int8);
         INSERT INTO {table} VALUES ({values});
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();

    let mut s = source(&slot, &publication, SnapshotMode::Initial, 10);
    s.start(None).await.unwrap();
    let read = collect(&mut s, Duration::from_secs(5), |e| !e.is_empty()).await;
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].op, Op::Read);
    // The same row again, this time as a live insert.
    db.batch_execute(&format!(
        "DELETE FROM {table}; INSERT INTO {table} VALUES ({values});"
    ))
    .await
    .unwrap();
    let live = collect(&mut s, Duration::from_secs(5), |e| {
        e.iter().any(|e| e.op == Op::Insert)
    })
    .await;
    let insert = live.iter().find(|e| e.op == Op::Insert).unwrap();
    assert_eq!(
        read[0].after.as_ref().unwrap().get(),
        insert.after.as_ref().unwrap().get()
    );
    assert_eq!(read[0].key, insert.key);
    assert_eq!(
        (read[0].schema.as_str(), read[0].table.as_str()),
        ("public", table.as_str())
    );
    drop(s);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}

/// A publication's column list and row filter (PG15+) apply to snapshot rows as to live ones.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn snapshot_respects_column_list_and_row_filter() {
    let db = connect().await;
    let version: String = db
        .query_one("SHOW server_version_num", &[])
        .await
        .unwrap()
        .get(0);
    if version.parse::<u32>().unwrap() < 150_000 {
        eprintln!("column lists and row filters need PG15; this server is {version}");
        return;
    }
    let table = unique("snap_filter");
    let publication = unique("snap_filter_pub");
    let slot = unique("snap_filter");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, region text NOT NULL, secret text);
         INSERT INTO {table} SELECT g, CASE WHEN g % 2 = 0 THEN 'eu' ELSE 'us' END, 's'
             FROM generate_series(1, 30) g;
         CREATE PUBLICATION {publication} FOR TABLE {table} (id, region) WHERE (region = 'eu');"
    ))
    .await
    .unwrap();
    let mut s = source(&slot, &publication, SnapshotMode::Initial, 4);
    s.start(None).await.unwrap();
    let events = collect(&mut s, Duration::from_secs(3), |_| false).await;
    let ids: Vec<i64> = events
        .iter()
        .filter(|e| e.op == Op::Read)
        .map(|e| {
            let v = after(e);
            assert!(v.get("secret").is_none(), "unpublished column: {v}");
            v["id"].as_i64().unwrap()
        })
        .collect();
    assert_eq!(ids, (1..=15).map(|i| i * 2).collect::<Vec<_>>());
    drop(s);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}

/// A restart in the middle of a snapshot resumes from the checkpoint: duplicates at most, no
/// row lost, and the snapshot finishes.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn a_restart_mid_snapshot_resumes_from_the_checkpoint() {
    let db = connect().await;
    let table = unique("snap_resume");
    let publication = unique("snap_resume_pub");
    let slot = unique("snap_resume");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, v text NOT NULL);
         INSERT INTO {table} SELECT g, 'v' FROM generate_series(1, 1000) g;
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();
    let mut s = source(&slot, &publication, SnapshotMode::Initial, 100);
    s.start(None).await.unwrap();
    let first = collect(&mut s, Duration::from_secs(5), |e| e.len() >= 350).await;
    let last: Position = first.last().unwrap().checkpoint.clone().unwrap();
    let Position::Text(t) = &last else {
        panic!("text checkpoint")
    };
    assert!(t.starts_with('{') && t.contains("\"snap\""), "{t}");
    drop(s);

    // The next start resumes the snapshot from the saved progress.
    let mut s = source(&slot, &publication, SnapshotMode::Initial, 100);
    s.start(Some(last)).await.unwrap();
    let rest = collect(&mut s, Duration::from_secs(3), |_| false).await;
    let mut all = first.clone();
    all.extend(rest.iter().cloned());
    assert_eq!(fold(&all, &table).len(), 1000, "every row arrives");
    let resent = rest.iter().filter(|e| e.op == Op::Read).count();
    assert!(
        resent <= 1000 - 300,
        "the restart resumes after the saved chunk, it does not start over: {resent}"
    );
    // Once finished, checkpoints are plain again.
    db.execute(&format!("INSERT INTO {table} VALUES (5000, 'x')"), &[])
        .await
        .unwrap();
    let tail = collect(&mut s, Duration::from_secs(5), |e| !e.is_empty()).await;
    let Some(Position::Text(t)) = &tail[0].checkpoint else {
        panic!("text checkpoint")
    };
    assert!(!t.contains("snap"), "{t}");
    drop(s);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}

/// With `snapshot = "never"` nothing is copied and no watermark is written.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn never_copies_nothing() {
    let db = connect().await;
    let table = unique("snap_never");
    let publication = unique("snap_never_pub");
    let slot = unique("snap_never");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY);
         INSERT INTO {table} SELECT generate_series(1, 10);
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();
    let mut s = source(&slot, &publication, SnapshotMode::Never, 4);
    s.start(None).await.unwrap();
    db.execute(&format!("INSERT INTO {table} VALUES (11)"), &[])
        .await
        .unwrap();
    let events = collect(&mut s, Duration::from_secs(2), |_| false).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].op, Op::Insert);
    drop(s);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}
