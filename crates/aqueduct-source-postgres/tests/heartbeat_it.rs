//! Idle-heartbeat checks for the Postgres source (#47). Needs the plaintext server the CI
//! `integration` job starts (`wal_level=logical`, user postgres / password aq):
//!
//! ```text
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434
//! cargo test -p aqueduct-source-postgres --test heartbeat_it -- --ignored
//! ```
//!
//! Every test makes its own tables, publication and slot, because tests share one server.
use std::time::{Duration, Instant};

use aqueduct_core::{ChangeEvent, Op, Position, Source};
use aqueduct_source_postgres::{parse_lsn, PostgresSource, PostgresSourceConfig, SslMode};
use tokio_postgres::{Client, NoTls};

fn addr() -> String {
    std::env::var("AQUEDUCT_TEST_PG_PLAIN")
        .unwrap_or_else(|_| panic!("set AQUEDUCT_TEST_PG_PLAIN, see the module docs"))
}

struct Fixture {
    db: Client,
    slot: String,
    publication: String,
    unpub: String,
    publ: String,
}

impl Fixture {
    async fn new(tag: &str) -> Self {
        let url = format!("postgres://postgres:aq@{}/postgres", addr());
        let (db, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(conn);
        // The clock keeps names unique across reruns against a long-lived server.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let id = format!("{}_{tag}_{}", std::process::id(), nanos % 1_000_000_000);
        let f = Fixture {
            db,
            slot: format!("aq_hb_{id}"),
            publication: format!("aq_hb_pub_{id}"),
            unpub: format!("hb_unpub_{id}"),
            publ: format!("hb_pub_{id}"),
        };
        f.db.batch_execute(&format!(
            "CREATE TABLE {u} (id serial PRIMARY KEY, pad text NOT NULL);
             CREATE TABLE {p} (id serial PRIMARY KEY, v text NOT NULL);
             CREATE PUBLICATION {pubn} FOR TABLE {p};",
            u = f.unpub,
            p = f.publ,
            pubn = f.publication
        ))
        .await
        .unwrap();
        f
    }

    fn source(&self, heartbeat: Duration) -> PostgresSource {
        self.source_with(heartbeat, None)
    }

    fn source_with(&self, heartbeat: Duration, action: Option<String>) -> PostgresSource {
        let url = format!("postgres://postgres:aq@{}/postgres", addr());
        let mut cfg = PostgresSourceConfig::new(url, self.slot.clone(), self.publication.clone());
        cfg.ssl_mode = Some(SslMode::Disable);
        cfg.poll_interval = Duration::from_millis(100);
        cfg.heartbeat_interval = Some(heartbeat);
        cfg.heartbeat_action_query = action;
        PostgresSource::new(cfg)
    }

    /// About 2 MiB of WAL in a table the publication does not cover.
    async fn write_unpublished(&self) {
        self.db
            .batch_execute(&format!(
                "INSERT INTO {} (pad) SELECT repeat('x', 200) FROM generate_series(1, 10000)",
                self.unpub
            ))
            .await
            .unwrap();
    }

    async fn insert_published(&self, v: &str) {
        self.db
            .execute(&format!("INSERT INTO {} (v) VALUES ($1)", self.publ), &[&v])
            .await
            .unwrap();
    }

    /// Frees the slot so a long-lived server does not run out of `max_replication_slots`.
    async fn cleanup(self, source: PostgresSource) {
        drop(source);
        self.db
            .execute("SELECT pg_drop_replication_slot($1::text)", &[&self.slot])
            .await
            .unwrap();
    }

    async fn confirmed(&self) -> u64 {
        let row = self
            .db
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&self.slot],
            )
            .await
            .unwrap();
        parse_lsn(&row.get::<_, String>(0)).unwrap()
    }

    async fn wal(&self) -> u64 {
        let row = self
            .db
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await
            .unwrap();
        parse_lsn(&row.get::<_, String>(0)).unwrap()
    }

    /// Drives the source (heartbeats run inside `next`) until the slot has confirmed `target`.
    async fn drive_until_confirmed(&self, s: &mut PostgresSource, target: u64) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while self.confirmed().await < target {
            assert!(
                Instant::now() < deadline,
                "slot never caught up to {target:x}"
            );
            if let Some(ev) = drive(s, Duration::from_millis(500)).await {
                panic!("unpublished writes must not produce events, got {ev:?}");
            }
        }
    }
}

/// Runs the source for up to `dur`, returning an event if one arrives. A cancelled poll only
/// leaves the slot where it was, so cutting `next` short is safe here.
async fn drive(s: &mut PostgresSource, dur: Duration) -> Option<ChangeEvent> {
    tokio::time::timeout(dur, s.next())
        .await
        .ok()
        .map(|r| r.unwrap().expect("stream ended"))
}

fn lsn(ev: &ChangeEvent) -> u64 {
    match ev.position {
        Position::Lsn(l) => l,
        ref other => panic!("expected an LSN, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn slot_advances_when_only_unpublished_tables_change() {
    let f = Fixture::new("unpub").await;
    let mut s = f.source(Duration::from_secs(1));
    s.start(None).await.unwrap();
    let baseline = f.confirmed().await;

    f.write_unpublished().await;
    let target = f.wal().await;
    assert!(
        target - baseline > 1_000_000,
        "the writes should produce WAL"
    );
    f.drive_until_confirmed(&mut s, target).await;
    f.cleanup(s).await;
}

#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn heartbeat_never_confirms_past_an_unacked_event() {
    let f = Fixture::new("hold").await;
    let mut s = f.source(Duration::from_secs(1));
    s.start(None).await.unwrap();

    f.insert_published("held").await;
    let ev = drive(&mut s, Duration::from_secs(10))
        .await
        .expect("the published insert must arrive");
    assert_eq!(ev.op, Op::Insert);

    // Several heartbeat intervals pass with more unpublished writes and no ack.
    f.write_unpublished().await;
    let until = Instant::now() + Duration::from_secs(4);
    while Instant::now() < until {
        if let Some(extra) = drive(&mut s, Duration::from_millis(500)).await {
            panic!("the event was already delivered once, got {extra:?}");
        }
        assert!(
            f.confirmed().await <= lsn(&ev),
            "slot confirmed past an event that was never acked"
        );
    }

    // After the ack the slot is free to move on.
    s.ack(ev.position.clone()).await.unwrap();
    let target = f.wal().await;
    f.drive_until_confirmed(&mut s, target).await;
    f.cleanup(s).await;
}

#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn published_change_after_a_quiet_period_is_delivered() {
    let f = Fixture::new("quiet").await;
    let mut s = f.source(Duration::from_secs(1));
    s.start(None).await.unwrap();

    f.write_unpublished().await;
    let target = f.wal().await;
    f.drive_until_confirmed(&mut s, target).await;

    f.insert_published("after").await;
    let ev = drive(&mut s, Duration::from_secs(10))
        .await
        .expect("the published insert must still arrive");
    assert_eq!(ev.table, f.publ);
    assert_eq!(ev.op, Op::Insert);
    assert!(ev.after.as_ref().unwrap().get().contains("after"));
    assert!(
        drive(&mut s, Duration::from_secs(2)).await.is_none(),
        "no duplicate while the source stays up"
    );
    f.cleanup(s).await;
}

#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn action_query_runs_before_each_heartbeat() {
    let f = Fixture::new("action").await;
    let query = format!("INSERT INTO {} (v) VALUES ('beat')", f.publ);
    let mut s = f.source_with(Duration::from_secs(1), Some(query));
    s.start(None).await.unwrap();
    let ev = drive(&mut s, Duration::from_secs(10))
        .await
        .expect("the action query's published write must arrive");
    assert_eq!(ev.table, f.publ);
    assert!(ev.after.as_ref().unwrap().get().contains("beat"));
    f.cleanup(s).await;
}
