//! Interleaved-transaction check for the Postgres source. Needs the plaintext server the CI
//! `integration` job starts (`wal_level=logical`, user postgres / password aq):
//!
//! ```text
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434
//! cargo test -p aqueduct-source-postgres --test interleave_it -- --ignored
//! ```
//!
//! Transactions are decoded in commit order, but their changes carry the WAL position of the
//! change record, so positions are not monotonic across transactions. `queue()` drops every
//! change at or below the last emitted position; this test overlaps two writers to see whether
//! that drops a row. Every other integration test runs one transaction at a time.
use std::collections::BTreeSet;
use std::time::Duration;

use aqueduct_core::{ChangeEvent, Op, Source};
use aqueduct_source_postgres::{PostgresSource, PostgresSourceConfig, SslMode};
use tokio_postgres::NoTls;

fn addr() -> String {
    std::env::var("AQUEDUCT_TEST_PG_PLAIN")
        .unwrap_or_else(|_| panic!("set AQUEDUCT_TEST_PG_PLAIN, see the module docs"))
}

/// The `k` column of an insert's row image.
fn key_of(e: &ChangeEvent) -> i64 {
    let row: serde_json::Value = serde_json::from_str(e.after.as_ref().unwrap().get()).unwrap();
    row["k"].as_i64().expect("integer key k")
}

async fn next_event(s: &mut PostgresSource) -> Option<ChangeEvent> {
    match tokio::time::timeout(Duration::from_secs(5), s.next()).await {
        Ok(r) => r.unwrap(),
        Err(_) => None,
    }
}

#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn overlapping_transactions_lose_no_rows() {
    let url = format!("postgres://postgres:aq@{}/postgres", addr());
    let (db, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn);
    let (t1, conn1) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn1);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = format!("{}_{}", std::process::id(), nanos % 1_000_000_000);
    let table = format!("interleave_{id}");
    let publication = format!("aq_il_pub_{id}");
    let slot = format!("aq_il_{id}");

    db.batch_execute(&format!(
        "CREATE TABLE {table} (k int PRIMARY KEY, v text NOT NULL);
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();

    let mut cfg = PostgresSourceConfig::new(url, slot.clone(), publication.clone());
    cfg.ssl_mode = Some(SslMode::Disable);
    cfg.poll_interval = Duration::from_millis(100);
    let mut s = PostgresSource::new(cfg);
    s.start(None).await.unwrap();

    // T1 writes first but commits last; T2 starts, writes and commits in between:
    //   T1 insert (lowest WAL position), T2 insert + commit, T1 insert (highest) + commit.
    // The source decodes T2 first, then T1, so T1's first change is below T2's position.
    t1.batch_execute(&format!("BEGIN; INSERT INTO {table} VALUES (1, 't1-first')"))
        .await
        .unwrap();
    db.execute(&format!("INSERT INTO {table} VALUES (2, 't2')"), &[])
        .await
        .unwrap();
    t1.batch_execute(&format!(
        "INSERT INTO {table} VALUES (3, 't1-second'); COMMIT;"
    ))
    .await
    .unwrap();

    let mut got = Vec::new();
    while let Some(e) = next_event(&mut s).await {
        assert_eq!(e.op, Op::Insert);
        got.push(key_of(&e));
    }
    let got: BTreeSet<i64> = got.into_iter().collect();

    drop(s);
    db.execute("SELECT pg_drop_replication_slot($1::text)", &[&slot])
        .await
        .unwrap();
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();

    assert_eq!(
        got.len(),
        3,
        "all three inserts must arrive, got {got:?} (a missing t1-first means the position \
         dedupe dropped it)"
    );
}

/// Crash variant: deliver and ack only the transaction that committed first, then restart from
/// that checkpoint. The slot has moved past T2 only, so the server re-sends T1 (committed after
/// T2). Together with what was delivered before the restart, all three rows must be seen: a
/// restart must not drop T1's earlier change because its position is below the checkpoint.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn restart_after_acking_the_first_committed_transaction_loses_no_rows() {
    let url = format!("postgres://postgres:aq@{}/postgres", addr());
    let (db, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn);
    let (t1, conn1) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn1);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = format!("{}_{}", std::process::id(), nanos % 1_000_000_000);
    let table = format!("interleave_r_{id}");
    let publication = format!("aq_ilr_pub_{id}");
    let slot = format!("aq_ilr_{id}");

    db.batch_execute(&format!(
        "CREATE TABLE {table} (k int PRIMARY KEY, v text NOT NULL);
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();

    let new_source = || {
        let mut cfg = PostgresSourceConfig::new(url.clone(), slot.clone(), publication.clone());
        cfg.ssl_mode = Some(SslMode::Disable);
        cfg.poll_interval = Duration::from_millis(100);
        PostgresSource::new(cfg)
    };
    let mut s = new_source();
    s.start(None).await.unwrap();

    t1.batch_execute(&format!("BEGIN; INSERT INTO {table} VALUES (1, 't1-first')"))
        .await
        .unwrap();
    db.execute(&format!("INSERT INTO {table} VALUES (2, 't2')"), &[])
        .await
        .unwrap();
    t1.batch_execute(&format!(
        "INSERT INTO {table} VALUES (3, 't1-second'); COMMIT;"
    ))
    .await
    .unwrap();

    // Commit order: T2's row arrives first. Ack it alone, as the pipeline would after a batch
    // that ended there, then "crash".
    let first = next_event(&mut s).await.expect("the first committed row");
    let mut seen = vec![key_of(&first)];
    let checkpoint = first.position;
    assert!(
        seen[0] == 2,
        "commit order puts T2 first, got {seen:?}"
    );
    s.ack(checkpoint).await.unwrap();
    drop(s);

    let mut s = new_source();
    s.start(Some(checkpoint)).await.unwrap();
    while let Some(e) = next_event(&mut s).await {
        seen.push(key_of(&e));
    }
    let seen: BTreeSet<i64> = seen.into_iter().collect();

    drop(s);
    db.execute("SELECT pg_drop_replication_slot($1::text)", &[&slot])
        .await
        .unwrap();
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();

    assert_eq!(
        seen,
        BTreeSet::from([1, 2, 3]),
        "after the restart all three rows must have been seen"
    );
}

/// `ack_target` variant. Tb inserts first and commits last; Ta inserts twice and commits in
/// between. Decoding order is Ta, Tb, but Tb's only change has a position below Ta's last one.
/// Acking Ta's last change must not advance the slot past Tb, which was never delivered: a
/// crash right after would lose Tb on the server side, and no local dedupe could recover it.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn acking_the_first_committed_transaction_does_not_ack_an_undelivered_one() {
    let url = format!("postgres://postgres:aq@{}/postgres", addr());
    let (db, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn);
    let (tb, conn1) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn1);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = format!("{}_{}", std::process::id(), nanos % 1_000_000_000);
    let table = format!("interleave_a_{id}");
    let publication = format!("aq_ila_pub_{id}");
    let slot = format!("aq_ila_{id}");

    db.batch_execute(&format!(
        "CREATE TABLE {table} (k int PRIMARY KEY, v text NOT NULL);
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();

    let new_source = || {
        let mut cfg = PostgresSourceConfig::new(url.clone(), slot.clone(), publication.clone());
        cfg.ssl_mode = Some(SslMode::Disable);
        cfg.poll_interval = Duration::from_millis(100);
        PostgresSource::new(cfg)
    };
    let mut s = new_source();
    s.start(None).await.unwrap();

    // Tb: first change, commits last. Ta: two changes, commits in between.
    tb.batch_execute(&format!("BEGIN; INSERT INTO {table} VALUES (1, 'tb')"))
        .await
        .unwrap();
    db.batch_execute(&format!(
        "BEGIN; INSERT INTO {table} VALUES (2, 'ta-1'); INSERT INTO {table} VALUES (3, 'ta-2'); COMMIT;"
    ))
    .await
    .unwrap();
    tb.batch_execute("COMMIT").await.unwrap();

    // Deliver exactly Ta (two events), ack at its last change as the pipeline would after a
    // batch ending there, then "crash".
    let mut seen = Vec::new();
    let mut last = None;
    for _ in 0..2 {
        let e = next_event(&mut s).await.expect("a Ta row");
        seen.push(key_of(&e));
        last = Some(e.position);
    }
    assert!(
        seen.iter().all(|k| *k == 2 || *k == 3),
        "commit order puts Ta first, got {seen:?}"
    );
    let checkpoint = last.unwrap();
    s.ack(checkpoint).await.unwrap();
    drop(s);

    let mut s = new_source();
    s.start(Some(checkpoint)).await.unwrap();
    while let Some(e) = next_event(&mut s).await {
        seen.push(key_of(&e));
    }
    let seen: BTreeSet<i64> = seen.into_iter().collect();

    drop(s);
    db.execute("SELECT pg_drop_replication_slot($1::text)", &[&slot])
        .await
        .unwrap();
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();

    assert_eq!(
        seen,
        BTreeSet::from([1, 2, 3]),
        "Tb (k=1) was never delivered after the restart (the ack moved the slot past it)"
    );
}
