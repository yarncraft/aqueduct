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
use std::time::Duration;

use aqueduct_core::{ChangeEvent, Op, Source};
use aqueduct_source_postgres::{PostgresSource, PostgresSourceConfig, SslMode};
use tokio_postgres::NoTls;

fn addr() -> String {
    std::env::var("AQUEDUCT_TEST_PG_PLAIN")
        .unwrap_or_else(|_| panic!("set AQUEDUCT_TEST_PG_PLAIN, see the module docs"))
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
        got.push(e.after.expect("insert has a row").get().to_owned());
    }
    got.sort();

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
