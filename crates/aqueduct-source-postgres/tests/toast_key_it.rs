//! Unchanged-TOAST key column check for the Postgres source (#50). Needs the plaintext server the
//! CI `integration` job starts (`wal_level=logical`, user postgres / password aq):
//!
//! ```text
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434
//! cargo test -p aqueduct-source-postgres --test toast_key_it -- --ignored
//! ```
//!
//! A primary-key column stored out of line (TOAST) is not sent by an update that leaves it alone.
//! `RelationCache::key_with_old` takes it from the old key tuple, so the update keeps the key its
//! insert had, and the row stays on one Kafka partition. The unit tests pin that logic on
//! hand-built tuples; this test checks that a real server actually sends the old key.
mod common;
use std::time::Duration;

use aqueduct_core::{ChangeEvent, Op, Source};
use aqueduct_source_postgres::{PostgresSource, PostgresSourceConfig, SslMode};
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;

fn addr() -> String {
    std::env::var("AQUEDUCT_TEST_PG_PLAIN")
        .unwrap_or_else(|_| panic!("set AQUEDUCT_TEST_PG_PLAIN, see the module docs"))
}

async fn next_event(s: &mut PostgresSource) -> ChangeEvent {
    tokio::time::timeout(Duration::from_secs(20), s.next())
        .await
        .expect("no event within 20s")
        .unwrap()
        .expect("stream ended")
}

#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn unsent_toast_key_column_keeps_the_insert_key() {
    let url = format!("postgres://postgres:aq@{}/postgres", addr());
    let (db, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = format!("{}_{}", std::process::id(), nanos % 1_000_000_000);
    let table = format!("toast_key_{id}");
    let publication = format!("aq_toast_pub_{id}");
    let slot = format!("aq_toast_{id}");

    // `EXTERNAL` stores the value out of line without compressing it, so a 2400-byte key is
    // TOASTed (the threshold is about 2 kB) while still fitting under the ~2.7 kB btree limit.
    db.batch_execute(&format!(
        "CREATE TABLE {table} (k text PRIMARY KEY, v text NOT NULL);
         ALTER TABLE {table} ALTER COLUMN k SET STORAGE EXTERNAL;
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();

    let mut cfg = PostgresSourceConfig::new(url, slot.clone(), publication.clone());
    cfg.ssl_mode = Some(SslMode::Disable);
    cfg.poll_interval = Duration::from_millis(100);
    cfg.transport = common::transport();
    let mut s = PostgresSource::new(cfg);
    s.start(None).await.unwrap();

    let long_key = "k".repeat(2400);
    db.execute(
        &format!("INSERT INTO {table} (k, v) VALUES ($1, 'one')"),
        &[&long_key],
    )
    .await
    .unwrap();
    // Leaves the key column alone, so the new tuple carries it as "unchanged TOAST".
    db.execute(&format!("UPDATE {table} SET v = 'two'"), &[])
        .await
        .unwrap();

    let insert = next_event(&mut s).await;
    assert_eq!(insert.op, Op::Insert);
    let update = next_event(&mut s).await;
    assert_eq!(update.op, Op::Update);

    let insert_key = insert.key.clone().expect("the insert has a key");
    assert!(
        insert_key.contains(&long_key),
        "the key must be the primary-key value"
    );
    // On postgres:16 the server writes the old key for an externally stored identity column.
    // If a server version ever stops doing that, this becomes `None` (the table-key fallback);
    // that is the documented behaviour on older minor releases, so say so before relaxing it.
    assert_eq!(
        update.key.as_deref(),
        Some(insert_key.as_str()),
        "an update that leaves a TOASTed key column alone must keep the insert's key"
    );

    // Dropping the source does not close its connection at once, so the slot can still be
    // active (55006) for a moment; retry instead of failing the test.
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
