//! Checkpoint origin check (#63) against a real server. Needs the plaintext server the integration
//! suite uses (`wal_level=logical`, user postgres / password aq):
//!
//! ```text
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434
//! cargo test -p aqueduct-source-postgres --test origin_it -- --ignored
//! ```
//!
//! A checkpoint records the cluster's `system_identifier` and the database OID. Startup must refuse
//! a checkpoint from another cluster or another database of the same cluster, before it looks up or
//! creates the slot, and must accept a checkpoint written before the check existed.
use std::time::Duration;

use aqueduct_core::{ChangeEvent, Error, Position, Source};
use aqueduct_source_postgres::{PostgresSource, PostgresSourceConfig, SslMode};
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls};

fn addr() -> String {
    std::env::var("AQUEDUCT_TEST_PG_PLAIN")
        .unwrap_or_else(|_| panic!("set AQUEDUCT_TEST_PG_PLAIN, see the module docs"))
}

fn url(db: &str) -> String {
    format!("postgres://postgres:aq@{}/{db}", addr())
}

async fn connect(db: &str) -> Client {
    let (c, conn) = tokio_postgres::connect(&url(db), NoTls).await.unwrap();
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

fn source(db: &str, slot: &str, publication: &str, recreate: bool) -> PostgresSource {
    let mut cfg = PostgresSourceConfig::new(url(db), slot, publication);
    cfg.ssl_mode = Some(SslMode::Disable);
    cfg.poll_interval = Duration::from_millis(100);
    cfg.allow_slot_recreate = recreate;
    PostgresSource::new(cfg)
}

async fn next_event(s: &mut PostgresSource) -> Option<ChangeEvent> {
    match tokio::time::timeout(Duration::from_secs(5), s.next()).await {
        Ok(r) => r.unwrap(),
        Err(_) => None,
    }
}

/// Drops a slot. Dropping a source does not close its connection at once, so the slot can still
/// be active (55006) for a moment; retry instead of failing the test.
async fn drop_slot(db: &Client, slot: &str) {
    let mut tries = 0;
    loop {
        match db
            .execute("SELECT pg_drop_replication_slot($1::text)", &[&slot])
            .await
        {
            Ok(_) => return,
            Err(e) if e.code() == Some(&SqlState::OBJECT_IN_USE) && tries < 50 => {
                tries += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("dropping slot {slot}: {e}"),
        }
    }
}

async fn slot_exists(db: &Client, slot: &str) -> bool {
    db.query_opt(
        "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
        &[&slot],
    )
    .await
    .unwrap()
    .is_some()
}

/// Rewrites one `name=value` field of a checkpoint text.
fn with_field(text: &str, name: &str, value: &str) -> String {
    text.split(';')
        .map(|f| match f.split_once('=') {
            Some((n, _)) if n == name => format!("{n}={value}"),
            _ => f.to_string(),
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn field<'a>(text: &'a str, name: &str) -> &'a str {
    text.split(';')
        .find_map(|f| f.strip_prefix(name)?.strip_prefix('='))
        .unwrap_or_else(|| panic!("checkpoint {text:?} has no {name}="))
}

fn text(p: &Position) -> &str {
    match p {
        Position::Text(t) => t,
        other => panic!("expected a text checkpoint, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn checkpoint_from_another_cluster_or_database_is_refused_before_the_slot() {
    let db = connect("postgres").await;
    let table = unique("origin");
    let publication = unique("aq_or_pub");
    let slot = unique("aq_or");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (k int PRIMARY KEY);
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();

    // A checkpoint written by this version carries the cluster and database.
    let mut s = source("postgres", &slot, &publication, false);
    s.start(None).await.unwrap();
    db.execute(&format!("INSERT INTO {table} VALUES (1)"), &[])
        .await
        .unwrap();
    let ev = next_event(&mut s).await.expect("the insert");
    let ckpt = ev.checkpoint.expect("postgres events carry a checkpoint");
    let ckpt_text = text(&ckpt).to_string();
    let (sys, oid): (String, i64) = {
        let row = db
            .query_one(
                "SELECT s.system_identifier::text, d.oid::int8 FROM pg_control_system() s, \
                 pg_database d WHERE d.datname = current_database()",
                &[],
            )
            .await
            .unwrap();
        (row.get(0), row.get(1))
    };
    assert_eq!(field(&ckpt_text, "sys"), sys, "{ckpt_text}");
    assert_eq!(field(&ckpt_text, "db"), oid.to_string(), "{ckpt_text}");
    s.ack(ckpt.clone()).await.unwrap();
    drop(s);

    // The same checkpoint starts again.
    let mut s = source("postgres", &slot, &publication, false);
    s.start(Some(ckpt.clone())).await.unwrap();
    drop(s);

    // Another cluster: refused, also with allow_slot_recreate, and before any slot is created.
    let fresh_slot = unique("aq_or_fresh");
    let foreign = Position::Text(with_field(&ckpt_text, "sys", "1"));
    for recreate in [false, true] {
        let mut s = source("postgres", &fresh_slot, &publication, recreate);
        let err = s.start(Some(foreign.clone())).await.unwrap_err();
        assert!(
            matches!(&err, Error::Checkpoint(m) if m.contains("different or rebuilt cluster")),
            "recreate={recreate}: {err}"
        );
        assert!(
            !slot_exists(&db, &fresh_slot).await,
            "recreate={recreate}: a slot was created on the wrong cluster"
        );
    }

    // Another database of the same cluster: refused.
    let other_db = unique("aq_or_db");
    db.batch_execute(&format!("CREATE DATABASE {other_db}"))
        .await
        .unwrap();
    let mut s = source(&other_db, &fresh_slot, &publication, true);
    let err = s.start(Some(ckpt.clone())).await.unwrap_err();
    assert!(
        matches!(&err, Error::Checkpoint(m) if m.contains("same cluster")),
        "{err}"
    );
    drop(s);
    assert!(!slot_exists(&db, &fresh_slot).await);

    // A checkpoint written before this check (bare key) starts, and the next one is stamped.
    let legacy = ckpt_text.split(';').next().unwrap().to_string();
    let mut s = source("postgres", &slot, &publication, false);
    s.start(Some(Position::Text(legacy))).await.unwrap();
    db.execute(&format!("INSERT INTO {table} VALUES (2)"), &[])
        .await
        .unwrap();
    let ev = next_event(&mut s).await.expect("the second insert");
    let next = ev.checkpoint.expect("checkpoint");
    assert_eq!(field(text(&next), "sys"), sys);
    drop(s);

    drop_slot(&db, &slot).await;
    // DROP DATABASE cannot share a multi-statement batch (it refuses a transaction block).
    db.batch_execute(&format!("DROP DATABASE {other_db}"))
        .await
        .unwrap();
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}
