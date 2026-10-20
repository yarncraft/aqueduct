//! Idle-heartbeat checks for the Postgres source (#47). Needs the plaintext server the CI
//! `integration` job starts (`wal_level=logical`, user postgres / password aq):
//!
//! ```text
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434
//! cargo test -p aqueduct-source-postgres --test heartbeat_it -- --ignored
//! ```
//!
//! Every test makes its own tables, publication and slot, because tests share one server.
mod common;
use std::time::{Duration, Instant};

use aqueduct_core::{ChangeEvent, Op, Position, Source};
use aqueduct_source_postgres::{parse_lsn, PostgresSource, PostgresSourceConfig, SslMode};
use tokio_postgres::error::SqlState;
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
        sweep_leftovers(&db).await;
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

    fn source_capped(&self, batch_size: i32) -> PostgresSource {
        let mut cfg = self.config(Duration::from_secs(1), None);
        cfg.batch_size = batch_size;
        PostgresSource::new(cfg)
    }

    fn source_off(&self) -> PostgresSource {
        let mut cfg = self.config(Duration::from_secs(1), None);
        cfg.heartbeat_interval = None;
        PostgresSource::new(cfg)
    }

    fn source_with(&self, heartbeat: Duration, action: Option<String>) -> PostgresSource {
        PostgresSource::new(self.config(heartbeat, action))
    }

    fn config(&self, heartbeat: Duration, action: Option<String>) -> PostgresSourceConfig {
        let url = format!("postgres://postgres:aq@{}/postgres", addr());
        let mut cfg = PostgresSourceConfig::new(url, self.slot.clone(), self.publication.clone());
        cfg.transport = common::transport();
        cfg.ssl_mode = Some(SslMode::Disable);
        cfg.poll_interval = Duration::from_millis(100);
        cfg.heartbeat_interval = Some(heartbeat);
        cfg.heartbeat_action_query = action;
        cfg
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
    /// A failing test never gets here; the next run's [`sweep_leftovers`] covers that.
    async fn cleanup(self, source: PostgresSource) {
        drop(source);
        // Dropping the source does not close its connection at once, so the slot can still be
        // active (55006) for a moment. Retry instead of failing the test.
        let mut tries = 0;
        loop {
            match self
                .db
                .execute("SELECT pg_drop_replication_slot($1::text)", &[&self.slot])
                .await
            {
                Ok(_) => break,
                Err(e) if e.code() == Some(&SqlState::OBJECT_IN_USE) && tries < 50 => {
                    tries += 1;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => panic!("dropping slot {}: {e}", self.slot),
            }
        }
        self.db
            .batch_execute(&format!(
                "DROP PUBLICATION {}; DROP TABLE {}, {};",
                self.publication, self.unpub, self.publ
            ))
            .await
            .unwrap();
    }

    /// Waits until no backend holds the slot. Dropping a source does not stop a peek its
    /// connection already sent, and a new source's peek fails with 55006 while that one runs.
    async fn released(&self) {
        for _ in 0..100 {
            let active: bool = self
                .db
                .query_one(
                    "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
                    &[&self.slot],
                )
                .await
                .unwrap()
                .get(0);
            if !active {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("slot {} stayed active", self.slot);
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

/// Best-effort removal of what earlier runs left behind when a test failed before `cleanup`
/// (a long-lived CI server would otherwise run out of `max_replication_slots`). Objects of
/// this process are skipped because the other tests are still using them; names carry the pid.
async fn sweep_leftovers(db: &Client) {
    let mine = format!("_{}_", std::process::id());
    let names = |sql: &'static str| async move {
        db.query(sql, &[])
            .await
            .map(|rows| {
                rows.iter()
                    .map(|r| r.get::<_, String>(0))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };
    let others = |all: Vec<String>| {
        all.into_iter()
            .filter(|n| !n.contains(&mine))
            .collect::<Vec<_>>()
    };
    // Errors are ignored: a slot that is still active in a concurrent run cannot be dropped.
    for slot in others(
        names(r"SELECT slot_name::text FROM pg_replication_slots WHERE slot_name LIKE 'aq\_hb\_%'")
            .await,
    ) {
        let _ = db
            .execute("SELECT pg_drop_replication_slot($1::text)", &[&slot])
            .await;
    }
    for p in others(
        names(r"SELECT pubname::text FROM pg_publication WHERE pubname LIKE 'aq\_hb\_pub\_%'")
            .await,
    ) {
        let _ = db.batch_execute(&format!("DROP PUBLICATION {p}")).await;
    }
    for t in others(names(r"SELECT tablename::text FROM pg_tables WHERE tablename LIKE 'hb\_pub\_%' OR tablename LIKE 'hb\_unpub\_%'").await) {
        let _ = db.batch_execute(&format!("DROP TABLE {t}")).await;
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

    // `ev.position` is the change's own LSN, below its transaction's commit-end LSN. While the
    // event is unacked the slot must not move past the start of that transaction at all.
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
    s.ack(
        ev.checkpoint
            .clone()
            .expect("postgres events carry a checkpoint"),
    )
    .await
    .unwrap();
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

#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn unacked_event_is_redelivered_after_a_restart() {
    let f = Fixture::new("restart").await;
    let mut s = f.source(Duration::from_secs(1));
    s.start(None).await.unwrap();

    f.insert_published("survives").await;
    let ev = drive(&mut s, Duration::from_secs(10))
        .await
        .expect("the published insert must arrive");

    // Heartbeats and unpublished writes pass without an ack, then the process "crashes".
    f.write_unpublished().await;
    let until = Instant::now() + Duration::from_secs(3);
    while Instant::now() < until {
        assert!(drive(&mut s, Duration::from_millis(500)).await.is_none());
    }
    drop(s);
    f.released().await;

    let mut again = f.source(Duration::from_secs(1));
    again.start(None).await.unwrap();
    let redelivered = drive(&mut again, Duration::from_secs(10))
        .await
        .expect("the unacked event must come back after a restart");
    assert_eq!(redelivered.position, ev.position);
    assert_eq!(redelivered.table, f.publ);
    assert!(redelivered
        .after
        .as_ref()
        .unwrap()
        .get()
        .contains("survives"));
    f.cleanup(again).await;
}

/// Negative control for the tests above: with heartbeats off, nothing moves the slot past
/// writes the publication does not cover, so those tests really exercise the heartbeat.
/// (Only the source advances the slot here; `peek` never does, on any server version.)
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn slot_stays_behind_without_a_heartbeat() {
    let f = Fixture::new("off").await;
    let mut s = f.source_off();
    s.start(None).await.unwrap();
    let baseline = f.confirmed().await;

    f.write_unpublished().await;
    let until = Instant::now() + Duration::from_secs(4);
    while Instant::now() < until {
        assert!(drive(&mut s, Duration::from_millis(500)).await.is_none());
    }
    assert_eq!(
        f.confirmed().await,
        baseline,
        "the slot moved without a heartbeat"
    );
    assert!(f.wal().await > baseline);
    f.cleanup(s).await;
}

/// A capped peek ends at a commit boundary that can lie well below the WAL position the
/// heartbeat captured. After a crash between `store.save` and `ack` the checkpoint is ahead of
/// the slot, so every peek of the replay window returns only already-delivered rows. The
/// heartbeat must walk the slot through that window one capped peek at a time (step 1) and must
/// not jump to the captured WAL position (step 2) while the peek is capped: that would skip the
/// published row committed behind the window.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn capped_replay_window_does_not_skip_later_rows() {
    let f = Fixture::new("capped").await;
    // The first source (default batch size) delivers the old events and never acks them: the
    // slot stays behind.
    let mut first = f.source(Duration::from_secs(1));
    first.start(None).await.unwrap();
    for i in 0..10 {
        f.insert_published(&format!("old{i}")).await;
    }
    let mut last_old = None;
    for _ in 0..10 {
        last_old = Some(
            drive(&mut first, Duration::from_secs(10))
                .await
                .expect("an old event never arrived"),
        );
    }
    // Pretend the checkpoint for the last old event was saved just before a crash.
    let last_old = last_old.unwrap();
    let checkpoint = lsn(&last_old);
    let checkpoint_key = last_old
        .checkpoint
        .clone()
        .expect("postgres events carry a checkpoint");
    drop(first);
    let behind = f.confirmed().await;
    assert!(
        behind < checkpoint,
        "the slot must still be behind the checkpoint"
    );

    // `X` sits in the committed WAL behind the replay window.
    f.insert_published("after-window").await;

    // Restart with the checkpoint ahead of the slot. With 3 rows per transaction and
    // `batch_size = 4`, one peek covers about one transaction, so the window spans many peeks.
    let mut s = f.source_capped(4);
    s.start(Some(checkpoint_key)).await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(30);
    let ev = loop {
        assert!(
            Instant::now() < deadline,
            "the row behind the replay window never arrived"
        );
        if let Some(ev) = drive(&mut s, Duration::from_millis(500)).await {
            break ev;
        }
    };
    assert!(
        lsn(&ev) > checkpoint,
        "an event at or below the checkpoint was redelivered: {ev:?}"
    );
    assert!(ev.after.as_ref().unwrap().get().contains("after-window"));

    // The slot kept moving through the replay window although nothing was acked, but not past
    // the unacked event. The stream transport confirms with its next status update, so allow
    // a moment.
    let until = Instant::now() + Duration::from_secs(5);
    while f.confirmed().await <= checkpoint && Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        f.confirmed().await > checkpoint,
        "the slot did not move through the replay window"
    );
    assert!(f.confirmed().await <= lsn(&ev));

    // Nothing else arrives, in particular no second copy of X, over more than one interval.
    assert!(
        drive(&mut s, Duration::from_millis(2500)).await.is_none(),
        "an extra event arrived after X"
    );

    // After the ack the slot moves past the delivered row and catches up to the end of the WAL.
    s.ack(
        ev.checkpoint
            .clone()
            .expect("postgres events carry a checkpoint"),
    )
    .await
    .unwrap();
    f.drive_until_confirmed(&mut s, lsn(&ev) + 1).await;
    let target = f.wal().await;
    f.drive_until_confirmed(&mut s, target).await;
    f.cleanup(s).await;
}

/// A checkpoint ahead of the server's WAL belongs to another cluster; accepting it would let the
/// heartbeat acknowledge (and skip) data that was never delivered.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn checkpoint_ahead_of_the_wal_is_rejected() {
    let f = Fixture::new("foreign").await;
    let mut s = f.source(Duration::from_secs(1));
    s.start(None).await.unwrap();
    drop(s);
    f.released().await;
    let mut s = f.source(Duration::from_secs(1));
    let err = s
        .start(Some(Position::Lsn(f.wal().await + (1 << 32))))
        .await
        .unwrap_err();
    assert!(
        matches!(err, aqueduct_core::Error::Checkpoint(_)),
        "unexpected error: {err:?}"
    );
    f.cleanup(s).await;
}

/// A failing action query must not stop the stream.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn failing_action_query_does_not_stop_the_stream() {
    let f = Fixture::new("badaction").await;
    let mut s = f.source_with(
        Duration::from_secs(1),
        Some("SELECT * FROM no_such_table".into()),
    );
    s.start(None).await.unwrap();
    f.insert_published("still-flows").await;
    let ev = drive(&mut s, Duration::from_secs(5))
        .await
        .expect("the event never arrived");
    assert!(ev.after.as_ref().unwrap().get().contains("still-flows"));
    // Let at least one failing heartbeat run, then check the stream is still alive.
    assert!(drive(&mut s, Duration::from_millis(2500)).await.is_none());
    f.insert_published("second").await;
    assert!(drive(&mut s, Duration::from_secs(5)).await.is_some());
    f.cleanup(s).await;
}
