//! Poll mode while another connection still holds the slot (#163). After a restart the previous
//! process's backend can hold the slot for a moment; the poll source's peek and slot advance
//! must wait for it instead of failing the pipeline. Needs the plaintext server the integration
//! suite uses (`wal_level=logical`, user postgres / password aq):
//!
//! ```text
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434
//! cargo test -p aqueduct-source-postgres --test slot_busy_it -- --ignored
//! ```
use std::time::{Duration, Instant};

use aqueduct_core::{Position, Source};
use aqueduct_source_postgres::{
    parse_lsn, PostgresSource, PostgresSourceConfig, SslMode, TransportKind,
};
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls};

fn addr() -> String {
    std::env::var("AQUEDUCT_TEST_PG_PLAIN")
        .unwrap_or_else(|_| panic!("set AQUEDUCT_TEST_PG_PLAIN, see the module docs"))
}

fn url() -> String {
    format!("postgres://postgres:aq@{}/postgres", addr())
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

fn source(slot: &str, publication: &str, transport: TransportKind) -> PostgresSource {
    let mut cfg = PostgresSourceConfig::new(url(), slot, publication);
    cfg.ssl_mode = Some(SslMode::Disable);
    cfg.poll_interval = Duration::from_millis(50);
    cfg.transport = transport;
    PostgresSource::new(cfg)
}

async fn slot_state(db: &Client, slot: &str) -> (bool, u64) {
    let row = db
        .query_one(
            "SELECT active, confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap();
    (row.get(0), parse_lsn(&row.get::<_, String>(1)).unwrap())
}

async fn drop_slot(db: &Client, slot: &str) {
    for _ in 0..100 {
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

#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn poll_ack_waits_for_a_slot_another_connection_holds() {
    let db = connect().await;
    let (table, slot, publication) = (unique("busy_t"), unique("aq_busy"), unique("aq_busy_pub"));
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id serial PRIMARY KEY, v text NOT NULL);
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();

    let mut poll = source(&slot, &publication, TransportKind::Poll);
    poll.start(None).await.unwrap();
    db.execute(&format!("INSERT INTO {table} (v) VALUES ('x')"), &[])
        .await
        .unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(10), poll.next())
        .await
        .expect("the insert must arrive")
        .unwrap()
        .expect("stream ended");
    let Position::Lsn(change) = ev.position else {
        panic!("expected an LSN, got {:?}", ev.position)
    };

    // A stream source holds the slot for as long as its replication connection is open, like
    // a previous process's backend that has not gone away yet. It is closed after 2 s.
    let mut holder = source(&slot, &publication, TransportKind::Stream);
    holder.start(None).await.unwrap();
    assert!(
        slot_state(&db, &slot).await.0,
        "the holder must hold the slot"
    );
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        drop(holder);
    });

    let t = Instant::now();
    poll.ack(ev.checkpoint.clone().unwrap())
        .await
        .expect("the ack must wait for the slot instead of failing");
    assert!(
        t.elapsed() >= Duration::from_secs(1),
        "the ack returned after {:?}, so the slot was never busy and nothing was tested",
        t.elapsed()
    );
    release.await.unwrap();
    let (_, confirmed) = slot_state(&db, &slot).await;
    assert!(
        confirmed > change,
        "the slot must move past the acked transaction ({confirmed:x} <= {change:x})"
    );

    drop(poll);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}
