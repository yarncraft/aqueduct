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
mod common;
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
    cfg.transport = common::transport();
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

/// A `TRUNCATE` and re-inserts while the snapshot runs: the consumer still ends up with the table
/// (#76).
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn a_truncate_during_the_snapshot_converges() {
    let db = connect().await;
    let table = unique("snap_trunc");
    let publication = unique("snap_trunc_pub");
    let slot = unique("snap_trunc");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, v text NOT NULL);
         INSERT INTO {table} SELECT g, 'v' FROM generate_series(1, 2000) g;
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();
    let mut s = source(&slot, &publication, SnapshotMode::Initial, 100);
    s.start(None).await.unwrap();
    let mut events = collect(&mut s, Duration::from_secs(5), |e| e.len() >= 500).await;
    db.batch_execute(&format!(
        "TRUNCATE {table}; INSERT INTO {table} SELECT g, 'again' FROM generate_series(1500, 1600) g;"
    ))
    .await
    .unwrap();
    events.extend(collect(&mut s, Duration::from_secs(3), |_| false).await);
    assert!(events.iter().any(|e| e.op == Op::Truncate));
    assert_eq!(fold(&events, &table), table_rows(&db, &table).await);
    drop(s);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}

/// Kill points of ADR 0001 §4 that the restart test does not cover (#76): before the first
/// checkpoint (the initial snapshot starts again) and after the last row (nothing is copied
/// again and the snapshot ends).
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn crashes_before_the_first_and_after_the_last_checkpoint() {
    let db = connect().await;
    let table = unique("snap_kill");
    let publication = unique("snap_kill_pub");
    let slot = unique("snap_kill");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY);
         INSERT INTO {table} SELECT generate_series(1, 300);
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();

    // Rows delivered but never checkpointed: the next start has no checkpoint and copies again.
    let mut s = source(&slot, &publication, SnapshotMode::Initial, 100);
    s.start(None).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(10), s.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.op, Op::Read);
    drop(s);
    let mut s = source(&slot, &publication, SnapshotMode::Initial, 100);
    s.start(None).await.unwrap();
    let all = collect(&mut s, Duration::from_secs(3), |_| false).await;
    assert_eq!(fold(&all, &table).len(), 300);
    let last_read = all
        .iter()
        .rev()
        .find(|e| e.op == Op::Read)
        .unwrap()
        .checkpoint
        .clone()
        .unwrap();
    drop(s);

    // Restart at the last row: the snapshot reads one empty chunk and ends, copying nothing.
    let mut s = source(&slot, &publication, SnapshotMode::Initial, 100);
    s.start(Some(last_read)).await.unwrap();
    let again = collect(&mut s, Duration::from_secs(3), |_| false).await;
    assert!(
        again.iter().all(|e| e.op != Op::Read),
        "nothing is copied again"
    );
    db.execute(&format!("INSERT INTO {table} VALUES (1000)"), &[])
        .await
        .unwrap();
    let tail = collect(&mut s, Duration::from_secs(5), |e| {
        e.iter().any(|e| e.op == Op::Insert)
    })
    .await;
    let Some(Position::Text(t)) = &tail.last().unwrap().checkpoint else {
        panic!("text checkpoint")
    };
    assert!(!t.contains("snap"), "the snapshot ended: {t}");
    drop(s);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}

async fn flushed(db: &Client, slot: &str) -> i64 {
    db.query_one(
        "SELECT (confirmed_flush_lsn - '0/0')::int8 FROM pg_replication_slots WHERE slot_name = $1",
        &[&slot],
    )
    .await
    .unwrap()
    .get(0)
}

/// The slot's confirmed position moves while a snapshot runs, so WAL is not pinned (#76).
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn the_slot_advances_during_the_snapshot() {
    let db = connect().await;
    let table = unique("snap_slot");
    let publication = unique("snap_slot_pub");
    let slot = unique("snap_slot");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY);
         INSERT INTO {table} SELECT generate_series(1, 3000);
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();
    let mut s = source(&slot, &publication, SnapshotMode::Initial, 100);
    s.start(None).await.unwrap();
    let at_start = flushed(&db, &slot).await;
    let events = collect(&mut s, Duration::from_secs(5), |e| e.len() >= 1000).await;
    assert!(events.len() >= 1000);
    let mid = flushed(&db, &slot).await;
    assert!(
        mid > at_start,
        "the slot advanced mid-snapshot: {at_start} -> {mid}"
    );
    drop(s);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}

/// A table without a primary key is skipped, unless `snapshot_key_columns` names a key (#91).
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn key_columns_let_a_table_without_primary_key_be_copied() {
    let db = connect().await;
    let skipped = unique("snap_nopk_skip");
    let keyed = unique("snap_nopk_key");
    let publication = unique("snap_nopk_pub");
    let slot = unique("snap_nopk");
    db.batch_execute(&format!(
        "CREATE TABLE {skipped} (id int);
         CREATE TABLE {keyed} (day date NOT NULL, seq int NOT NULL, id int NOT NULL, UNIQUE (day, seq));
         ALTER TABLE {keyed} REPLICA IDENTITY FULL;
         INSERT INTO {skipped} SELECT generate_series(1, 5);
         INSERT INTO {keyed} SELECT DATE '2024-01-01' + g / 10, g % 10, g FROM generate_series(1, 95) g;
         CREATE PUBLICATION {publication} FOR TABLE {skipped}, {keyed};"
    ))
    .await
    .unwrap();
    let mut cfg = PostgresSourceConfig::new(url(), &slot, &publication);
    cfg.transport = common::transport();
    cfg.ssl_mode = Some(SslMode::Disable);
    cfg.poll_interval = Duration::from_millis(50);
    cfg.snapshot = SnapshotConfig {
        mode: SnapshotMode::Initial,
        chunk_size: 7,
        key_columns: [(format!("public.{keyed}"), vec!["day".into(), "seq".into()])]
            .into_iter()
            .collect(),
        ..SnapshotConfig::default()
    };
    let mut s = PostgresSource::new(cfg);
    s.start(None).await.unwrap();
    let events = collect(&mut s, Duration::from_secs(3), |_| false).await;
    assert!(events.iter().all(|e| e.table != skipped), "no key: skipped");
    let mut ids: Vec<i64> = events
        .iter()
        .filter(|e| e.op == Op::Read && e.table == keyed)
        .map(|e| after(e)["id"].as_i64().unwrap())
        .collect();
    ids.sort();
    assert_eq!(ids, (1..=95).collect::<Vec<_>>());
    drop(s);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {skipped}, {keyed};"
    ))
    .await
    .unwrap();
}

/// Sends one admin request to the source and waits for its answer while driving it.
async fn request(
    s: &mut PostgresSource,
    tx: &aqueduct_core::SnapshotSender,
    command: aqueduct_core::SnapshotCommand,
) -> Result<String, String> {
    let (reply, mut answer) = tokio::sync::oneshot::channel();
    tx.send(aqueduct_core::SnapshotRequest { command, reply })
        .await
        .unwrap();
    // The source answers between batches: keep taking (and acking) events until it does.
    loop {
        if let Ok(a) = answer.try_recv() {
            return a;
        }
        if let Ok(Ok(Some(ev))) = tokio::time::timeout(Duration::from_millis(200), s.next()).await {
            if let Some(c) = ev.checkpoint.clone() {
                s.ack(c).await.unwrap();
            }
        }
    }
}

/// `on_demand`: nothing is copied until requested; a request names published tables; a cancel
/// stops after the chunk in flight and returns the checkpoint to the plain form (#75).
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn on_demand_requests_start_and_cancel() {
    use aqueduct_core::SnapshotCommand;
    let db = connect().await;
    let table = unique("snap_req");
    let publication = unique("snap_req_pub");
    let slot = unique("snap_req");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY);
         INSERT INTO {table} SELECT generate_series(1, 5000);
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();
    let (tx, rx) = aqueduct_core::snapshot_channel();
    let mut s = source(&slot, &publication, SnapshotMode::OnDemand, 50).with_snapshot_control(rx);
    s.start(None).await.unwrap();
    assert!(
        collect(&mut s, Duration::from_secs(1), |_| false)
            .await
            .is_empty(),
        "on_demand copies nothing by itself"
    );

    let err = request(
        &mut s,
        &tx,
        SnapshotCommand::Start(vec!["public.nope".into()]),
    )
    .await
    .unwrap_err();
    assert!(err.contains("not in publication"), "{err}");
    let err = request(&mut s, &tx, SnapshotCommand::Cancel)
        .await
        .unwrap_err();
    assert!(err.contains("no snapshot is running"), "{err}");

    let id = request(
        &mut s,
        &tx,
        SnapshotCommand::Start(vec![format!("public.{table}")]),
    )
    .await
    .unwrap();
    assert!(id.starts_with('s'), "{id}");
    let some = collect(&mut s, Duration::from_secs(5), |e| e.len() >= 200).await;
    assert!(some.iter().all(|e| e.op == Op::Read));
    assert_eq!(
        request(&mut s, &tx, SnapshotCommand::Cancel).await,
        Ok(id.clone())
    );
    // At most the chunk in flight still arrives, then nothing more is copied.
    let rest = collect(&mut s, Duration::from_secs(2), |_| false).await;
    assert!(rest.len() <= 50, "{} rows after the cancel", rest.len());
    db.execute(&format!("INSERT INTO {table} VALUES (9999)"), &[])
        .await
        .unwrap();
    let tail = collect(&mut s, Duration::from_secs(5), |e| {
        e.iter().any(|e| e.op == Op::Insert)
    })
    .await;
    let Some(Position::Text(t)) = &tail.last().unwrap().checkpoint else {
        panic!("text checkpoint")
    };
    assert!(
        !t.contains("snap"),
        "the cancelled snapshot left the checkpoint: {t}"
    );
    drop(s);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}

/// #92: with `snapshot_watermark_table` the watermarks are rows of a published table, so
/// snapshots work without logical messages (Postgres 10 to 13). The table's rows never reach the
/// sink, and the snapshot is complete under live writes.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn a_watermark_table_replaces_logical_messages() {
    let db = connect().await;
    let table = unique("snap_wmt");
    let marks = unique("snap_wmt_marks");
    let publication = unique("snap_wmt_pub");
    let slot = unique("snap_wmt");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, v text NOT NULL);
         INSERT INTO {table} SELECT g, 'v' FROM generate_series(1, 1500) g;
         CREATE TABLE {marks} {};
         CREATE PUBLICATION {publication} FOR TABLE {table}, {marks};",
        aqueduct_source_postgres::snapshot::WATERMARK_TABLE_DDL
    ))
    .await
    .unwrap();
    let mut cfg = PostgresSourceConfig::new(url(), &slot, &publication);
    cfg.transport = common::transport();
    cfg.ssl_mode = Some(SslMode::Disable);
    cfg.poll_interval = Duration::from_millis(50);
    cfg.snapshot = SnapshotConfig {
        mode: SnapshotMode::Initial,
        chunk_size: 100,
        watermark_table: Some(format!("public.{marks}")),
        ..SnapshotConfig::default()
    };
    let mut s = PostgresSource::new(cfg);
    s.start(None).await.unwrap();
    let writer = {
        let table = table.clone();
        tokio::spawn(async move {
            let db = connect().await;
            for i in 0..30 {
                db.batch_execute(&format!(
                    "UPDATE {table} SET v = 'new' WHERE id = {}; DELETE FROM {table} WHERE id = {};",
                    1 + i * 47,
                    2 + i * 47
                ))
                .await
                .unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };
    let mut events = collect(&mut s, Duration::from_secs(5), |_| false).await;
    writer.await.unwrap();
    events.extend(collect(&mut s, Duration::from_secs(3), |_| false).await);
    assert!(
        events.iter().all(|e| e.table != marks),
        "watermark rows are never events"
    );
    assert!(events.iter().filter(|e| e.op == Op::Read).count() >= 1400);
    assert_eq!(fold(&events, &table), table_rows(&db, &table).await);
    drop(s);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table}, {marks};"
    ))
    .await
    .unwrap();
}
