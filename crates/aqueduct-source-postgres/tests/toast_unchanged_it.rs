//! The decoder's signal for unchanged TOAST columns (ADR 0002 §5 "TOAST", routing slice R7):
//! an update that leaves a large out-of-line value alone does not send it, so `after` lacks the
//! column and `ChangeEvent::unchanged` names it. Needs the plaintext server of the other
//! integration tests (`wal_level=logical`, user postgres / password aq):
//!
//! ```text
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434
//! cargo test -p aqueduct-source-postgres --test toast_unchanged_it -- --ignored
//! ```
use std::time::{Duration, Instant};

use aqueduct_core::{ChangeEvent, Op, Source};
use aqueduct_source_postgres::{PostgresSource, PostgresSourceConfig, SslMode};
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;

fn addr() -> String {
    std::env::var("AQUEDUCT_TEST_PG_PLAIN")
        .unwrap_or_else(|_| panic!("set AQUEDUCT_TEST_PG_PLAIN, see the module docs"))
}

/// The next row event (heartbeats skipped), or a panic after 20 s.
async fn next_change(s: &mut PostgresSource) -> ChangeEvent {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(Instant::now() < deadline, "no change event in time");
        if let Ok(r) = tokio::time::timeout(Duration::from_millis(500), s.next()).await {
            let ev = r.unwrap().expect("stream ended");
            if ev.op != Op::Heartbeat {
                return ev;
            }
        }
    }
}

#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn an_unchanged_toast_column_is_named_and_left_out_of_after() {
    let url = format!("postgres://postgres:aq@{}/postgres", addr());
    let (db, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = format!("{}_{}", std::process::id(), nanos % 1_000_000_000);
    let (table, publication, slot) = (
        format!("toast_{id}"),
        format!("aq_toast_pub_{id}"),
        format!("aq_toast_{id}"),
    );
    // EXTERNAL storage keeps the 100 kB value out of line and uncompressed: it is TOASTed.
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, title text NOT NULL, doc text NOT NULL);
         ALTER TABLE {table} ALTER COLUMN doc SET STORAGE EXTERNAL;
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();

    let mut cfg = PostgresSourceConfig::new(url.clone(), slot.clone(), publication.clone());
    cfg.ssl_mode = Some(SslMode::Disable);
    cfg.poll_interval = Duration::from_millis(100);
    let mut s = PostgresSource::new(cfg);
    s.start(None).await.unwrap();

    db.batch_execute(&format!(
        "INSERT INTO {table} VALUES (1, 'a', repeat('x', 100000));
         UPDATE {table} SET title = 'b' WHERE id = 1;
         UPDATE {table} SET doc = repeat('y', 100000) WHERE id = 1;"
    ))
    .await
    .unwrap();

    let ins = next_change(&mut s).await;
    assert_eq!(ins.op, Op::Insert);
    assert!(ins.unchanged.is_empty(), "an insert sends every column");
    assert!(ins.after.as_ref().unwrap().get().contains("\"doc\""));

    let upd = next_change(&mut s).await;
    assert_eq!(upd.op, Op::Update);
    assert_eq!(upd.unchanged, ["doc"], "the TOASTed column was not sent");
    let after: serde_json::Value = serde_json::from_str(upd.after.as_ref().unwrap().get()).unwrap();
    assert_eq!(after["title"], "b");
    assert!(after.get("doc").is_none(), "left out of after: {after}");
    // The signal never reaches the serialised event.
    assert!(!serde_json::to_string(&upd).unwrap().contains("unchanged"));

    let changed = next_change(&mut s).await;
    assert_eq!(changed.op, Op::Update);
    assert!(
        changed.unchanged.is_empty(),
        "the column changed, so it was sent"
    );
    assert!(changed.after.as_ref().unwrap().get().contains("\"doc\""));

    drop(s);
    let mut tries = 0;
    loop {
        match db
            .execute("SELECT pg_drop_replication_slot($1::text)", &[&slot])
            .await
        {
            Ok(_) => break,
            Err(e) if e.code() == Some(&SqlState::OBJECT_IN_USE) && tries < 50 => {
                tries += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("dropping slot {slot}: {e}"),
        }
    }
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}
