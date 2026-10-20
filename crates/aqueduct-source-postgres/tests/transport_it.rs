//! Stream transport against poll (ADR 0003 T4, #109). Needs the plaintext server the integration
//! suite uses (`wal_level=logical`, user postgres / password aq):
//!
//! ```text
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434
//! cargo test -p aqueduct-source-postgres --test transport_it -- --ignored
//! ```
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use aqueduct_core::{ChangeEvent, Op, Position, Source};
use aqueduct_source_postgres::{PostgresSource, PostgresSourceConfig, SslMode, TransportKind};
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

fn source(
    url: String,
    slot: &str,
    publication: &str,
    transport: TransportKind,
    batch: i32,
) -> PostgresSource {
    let mut cfg = PostgresSourceConfig::new(url, slot, publication);
    cfg.ssl_mode = Some(SslMode::Disable);
    cfg.poll_interval = Duration::from_millis(50);
    cfg.batch_size = batch;
    cfg.transport = transport;
    PostgresSource::new(cfg)
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

/// What an event delivers, keyed by its resume key: equal maps mean equal events with equal
/// keys, duplicates from a restart folded together.
type Delivered = BTreeMap<String, (String, Op, Option<String>, Option<String>, Option<String>)>;

fn key_text(ev: &ChangeEvent) -> String {
    match ev
        .checkpoint
        .as_ref()
        .expect("postgres events carry a checkpoint")
    {
        Position::Text(t) => t.split(';').next().unwrap().to_string(),
        other => panic!("text checkpoint expected, got {other:?}"),
    }
}

/// Takes events until nothing arrives for `idle`, acking the last one of every batch.
async fn drain(s: &mut PostgresSource, idle: Duration, out: &mut Delivered) -> Option<Position> {
    let mut last = None;
    loop {
        match tokio::time::timeout(idle, s.next()).await {
            Ok(r) => {
                let ev = r.unwrap().expect("the source never ends");
                if ev.op == Op::Heartbeat {
                    continue;
                }
                let k = key_text(&ev);
                let v = (
                    format!("{}.{}", ev.schema, ev.table),
                    ev.op,
                    ev.before.as_ref().map(|r| r.get().to_string()),
                    ev.after.as_ref().map(|r| r.get().to_string()),
                    ev.key.clone(),
                );
                if let Some(prev) = out.insert(k.clone(), v.clone()) {
                    assert_eq!(prev, v, "a redelivered key {k} carried another event");
                }
                last = ev.checkpoint.clone();
                if let Some(c) = &last {
                    s.ack(c.clone()).await.unwrap();
                }
            }
            Err(_) => return last,
        }
    }
}

/// Poll and stream deliver the same events with the same keys: overlapping transactions across
/// batch boundaries, a COPY cut by a small batch, a multi-table TRUNCATE, updates and deletes,
/// and a restart of the stream source in the middle.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn poll_and_stream_deliver_the_same_events_with_the_same_keys() {
    let db = connect().await;
    let a = unique("tr_a");
    let b = unique("tr_b");
    let publication = unique("tr_pub");
    let (poll_slot, stream_slot) = (unique("tr_poll"), unique("tr_stream"));
    db.batch_execute(&format!(
        "CREATE TABLE {a} (id int PRIMARY KEY, v text);
         CREATE TABLE {b} (id int PRIMARY KEY, v text);
         CREATE PUBLICATION {publication} FOR TABLE {a}, {b};"
    ))
    .await
    .unwrap();
    for slot in [&poll_slot, &stream_slot] {
        db.execute(
            "SELECT pg_create_logical_replication_slot($1::text, 'pgoutput')",
            &[slot],
        )
        .await
        .unwrap();
    }

    // Overlapping transactions: t1 begins first and commits last.
    let other = connect().await;
    db.batch_execute(&format!(
        "BEGIN; INSERT INTO {a} SELECT g, 't1' FROM generate_series(1, 5) g;"
    ))
    .await
    .unwrap();
    other
        .batch_execute(&format!(
            "INSERT INTO {a} SELECT g, 't2' FROM generate_series(100, 104) g;"
        ))
        .await
        .unwrap();
    db.batch_execute(&format!("INSERT INTO {b} VALUES (1, 't1'); COMMIT;"))
        .await
        .unwrap();
    // A multi-row COPY (rows share WAL records), updates, deletes.
    db.batch_execute(&format!(
        "COPY {b} (id, v) FROM PROGRAM 'seq 10 39 | sed s/$/,copied/' WITH (FORMAT csv)"
    ))
    .await
    .unwrap();
    db.batch_execute(&format!(
        "UPDATE {a} SET v = 'upd' WHERE id <= 3; DELETE FROM {b} WHERE id BETWEEN 10 AND 14;"
    ))
    .await
    .unwrap();

    // Poll reads everything in one go.
    let mut polled = Delivered::new();
    let mut p = source(url(), &poll_slot, &publication, TransportKind::Poll, 7);
    p.start(None).await.unwrap();
    drain(&mut p, Duration::from_secs(3), &mut polled).await;

    // Stream: read part, restart from the last acked checkpoint, read the rest.
    let mut streamed = Delivered::new();
    let mut s = source(url(), &stream_slot, &publication, TransportKind::Stream, 7);
    s.start(None).await.unwrap();
    let mut first = Delivered::new();
    let mut last = None;
    while first.len() < 20 {
        let ev = tokio::time::timeout(Duration::from_secs(10), s.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if ev.op == Op::Heartbeat {
            continue;
        }
        first.insert(
            key_text(&ev),
            (
                format!("{}.{}", ev.schema, ev.table),
                ev.op,
                ev.before.as_ref().map(|r| r.get().to_string()),
                ev.after.as_ref().map(|r| r.get().to_string()),
                ev.key.clone(),
            ),
        );
        last = ev.checkpoint.clone();
    }
    // Ack a mid-stream key (inside the COPY), then "crash".
    s.ack(last.clone().unwrap()).await.unwrap();
    drop(s);
    streamed.extend(first);
    let mut s = source(url(), &stream_slot, &publication, TransportKind::Stream, 7);
    s.start(last).await.unwrap();
    // The multi-table TRUNCATE happens while the restarted stream runs.
    db.batch_execute(&format!("TRUNCATE {a}, {b};"))
        .await
        .unwrap();
    drain(&mut s, Duration::from_secs(3), &mut streamed).await;
    drop(s);
    let mut p = p;
    drain(&mut p, Duration::from_secs(3), &mut polled).await;
    drop(p);

    assert!(polled.len() >= 50, "{} events", polled.len());
    assert!(polled.values().filter(|v| v.1 == Op::Truncate).count() == 2);
    assert_eq!(polled, streamed, "poll and stream disagree");

    drop_slot(&db, &poll_slot).await;
    drop_slot(&db, &stream_slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {a}, {b};"
    ))
    .await
    .unwrap();
}

/// A sink that holds the source up for 20 s does not lose the replication connection although
/// the server's `wal_sender_timeout` is 5 s: status updates go out on their own.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn a_slow_sink_does_not_lose_the_connection() {
    let db = connect().await;
    let table = unique("tr_slow");
    let publication = unique("tr_slow_pub");
    let slot = unique("tr_slow");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY);
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();
    let slow_url = format!("{}?options=-c%20wal_sender_timeout%3D5s", url());
    let mut s = source(slow_url, &slot, &publication, TransportKind::Stream, 100);
    s.start(None).await.unwrap();
    db.execute(&format!("INSERT INTO {table} VALUES (1)"), &[])
        .await
        .unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(10), s.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    s.ack(ev.checkpoint.unwrap()).await.unwrap();
    let pid_before: i32 = db
        .query_one(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap()
        .get(0);

    // The "sink" is busy: nothing calls next() for 20 s while more data arrives.
    for i in 2..50 {
        db.execute(&format!("INSERT INTO {table} VALUES ({i})"), &[])
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_secs(20)).await;

    let pid_after: Option<i32> = db
        .query_one(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        pid_after,
        Some(pid_before),
        "the walsender was replaced or gone"
    );
    let mut n = 0;
    while n < 48 {
        let ev = tokio::time::timeout(Duration::from_secs(10), s.next())
            .await
            .expect("the stream stalled")
            .expect("the connection failed")
            .unwrap();
        if ev.op == Op::Insert {
            n += 1;
        }
    }
    drop(s);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}

fn rss_bytes() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.trim().trim_end_matches(" kB").trim().parse::<u64>().ok())
        .map_or(0, |kb| kb * 1024)
}

/// A one-million-row transaction is streamed with bounded memory and every row delivered.
#[tokio::test]
#[ignore = "needs a Postgres server, see the module docs"]
async fn a_million_row_transaction_streams_in_bounded_memory() {
    let db = connect().await;
    let table = unique("tr_big");
    let publication = unique("tr_big_pub");
    let slot = unique("tr_big");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, v text);
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();
    let mut s = source(url(), &slot, &publication, TransportKind::Stream, 1000);
    s.start(None).await.unwrap();
    db.batch_execute(&format!(
        "INSERT INTO {table} SELECT g, repeat('x', 40) FROM generate_series(1, 1000000) g;"
    ))
    .await
    .unwrap();
    let baseline = rss_bytes();
    assert!(baseline > 0, "VmRSS not readable");
    let mut peak = baseline;
    let mut n = 0u64;
    let mut last = None;
    let deadline = Instant::now() + Duration::from_secs(600);
    while n < 1_000_000 {
        assert!(Instant::now() < deadline, "only {n} rows in 10 minutes");
        let ev = tokio::time::timeout(Duration::from_secs(30), s.next())
            .await
            .expect("the stream stalled")
            .unwrap()
            .unwrap();
        if ev.op == Op::Insert {
            n += 1;
            last = ev.checkpoint;
            if n.is_multiple_of(1000) {
                s.ack(last.clone().unwrap()).await.unwrap();
                peak = peak.max(rss_bytes());
            }
        }
    }
    s.ack(last.unwrap()).await.unwrap();
    let grown = peak.saturating_sub(baseline);
    eprintln!(
        "RSS {} MiB at the start, peak {} MiB over 1M rows",
        baseline >> 20,
        peak >> 20
    );
    assert!(grown < 256 << 20, "RSS grew by {} MiB", grown >> 20);
    drop(s);
    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}
