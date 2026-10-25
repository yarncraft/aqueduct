//! Postgres source benchmark (ADR 0003 §8, #110). Needs a local Postgres with
//! `wal_level=logical`.
//!
//!   cargo run --release -p aqueduct-bench -- --source postgres --pg-url URL
//!       [--rows N] [--latency-samples N] [--big-rows N]
//!
//! - throughput: N rows are committed first (1000 per transaction), then the pipeline (source,
//!   null sink that encodes every event, file checkpoint) runs until all N are delivered;
//! - latency: on an idle stream, one row is inserted at a time and timed from its commit to the
//!   moment the source hands it out; p50 and p99 are reported;
//! - memory: peak RSS while one transaction of `--big-rows` rows streams through the pipeline.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aqueduct_core::{
    run_pipeline_with_metrics, ChangeEvent, Error, Metrics, Op, Result, Sink, Source,
};
use aqueduct_source_postgres::{PostgresSource, PostgresSourceConfig, SslMode};
use async_trait::async_trait;
use tokio::sync::watch;
use tokio_postgres::NoTls;

/// Encodes every event (the work a real sink does before I/O), counts the deliverable ones and
/// stops the pipeline once `target` arrived.
struct StopAt {
    target: u64,
    seen: Arc<AtomicU64>,
    stop: watch::Sender<bool>,
}

#[async_trait]
impl Sink for StopAt {
    async fn send(&mut self, e: &ChangeEvent) -> Result<()> {
        self.send_batch(std::slice::from_ref(e)).await
    }
    async fn send_batch(&mut self, events: &[ChangeEvent]) -> Result<()> {
        for e in events {
            serde_json::to_vec(e).map_err(|e| Error::Sink(e.to_string()))?;
            if e.op == Op::Insert {
                let n = self.seen.fetch_add(1, Ordering::Relaxed) + 1;
                if n >= self.target {
                    let _ = self.stop.send(true);
                }
            }
        }
        Ok(())
    }
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("VmRSS:"))
                .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        })
        .map_or(0, |kb| kb * 1024)
}

fn source(url: &str, slot: &str, publication: &str) -> PostgresSource {
    let mut cfg = PostgresSourceConfig::new(url, slot, publication);
    cfg.ssl_mode = Some(SslMode::Disable);
    PostgresSource::new(cfg)
}

async fn sql(url: &str) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("connect to --pg-url");
    tokio::spawn(conn);
    c
}

/// Runs the pipeline over `src` until `target` inserts were delivered; returns the time from the
/// pipeline's start (connecting, and for poll the first peek, included) to the last event, and
/// the peak RSS seen meanwhile.
async fn pipeline_until(src: &mut PostgresSource, target: u64, dir: &Path) -> (Duration, u64) {
    let seen = Arc::new(AtomicU64::new(0));
    let (stop, rx) = watch::channel(false);
    let mut sink = StopAt {
        target,
        seen: seen.clone(),
        stop,
    };
    let mut store = crate::FileCkpt(dir.join("pg-ckpt"));
    let metrics = Metrics::new();
    let peak = Arc::new(AtomicU64::new(rss_bytes()));
    let sampler = {
        let peak = peak.clone();
        tokio::spawn(async move {
            loop {
                peak.fetch_max(rss_bytes(), Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };
    let began = Instant::now();
    run_pipeline_with_metrics(src, &mut sink, &mut store, rx, &metrics)
        .await
        .expect("pipeline");
    let end = Instant::now();
    sampler.abort();
    (end - began, peak.load(Ordering::Relaxed))
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
}

pub async fn run(url: &str, dir: &Path) {
    let rows: u64 = crate::arg("--rows").map_or(200_000, |v| v.parse().unwrap());
    let samples: usize = crate::arg("--latency-samples").map_or(200, |v| v.parse().unwrap());
    let big: u64 = crate::arg("--big-rows").map_or(1_000_000, |v| v.parse().unwrap());
    let name = format!("bench_stream_{}", std::process::id());
    let db = sql(url).await;
    db.batch_execute(&format!(
        "CREATE TABLE {name} (id bigint PRIMARY KEY, v text);
         CREATE PUBLICATION {name} FOR TABLE {name};"
    ))
    .await
    .expect("set up the bench table");
    // Its own statement: a slot cannot be created in a transaction that wrote.
    db.batch_execute(&format!(
        "SELECT pg_create_logical_replication_slot('{name}', 'pgoutput')"
    ))
    .await
    .expect("create the bench slot");

    // Throughput.
    let mut id = 0u64;
    while id < rows {
        let n = 1000.min(rows - id);
        db.batch_execute(&format!(
            "INSERT INTO {name} SELECT g, repeat('x', 100) FROM generate_series({}, {}) g",
            id + 1,
            id + n
        ))
        .await
        .unwrap();
        id += n;
    }
    let mut src = source(url, &name, &name);
    let (took, _) = pipeline_until(&mut src, rows, dir).await;
    let throughput = rows as f64 / took.as_secs_f64();
    // Shadowing would keep it (and its replication connection) alive.
    drop(src);

    // Latency on an idle stream: the source is driven directly so nothing batches the wait.
    let mut src = source(url, &name, &name);
    src.start(None).await.unwrap();
    // Drain whatever is left from the throughput run.
    while tokio::time::timeout(Duration::from_secs(2), src.next())
        .await
        .is_ok()
    {}
    let mut lat = Vec::with_capacity(samples);
    for _ in 0..samples {
        id += 1;
        db.execute(&format!("INSERT INTO {name} VALUES ({id}, 'l')"), &[])
            .await
            .unwrap();
        let committed = Instant::now();
        loop {
            let ev = src.next().await.unwrap().unwrap();
            if let Some(c) = ev.checkpoint.clone() {
                src.ack(c).await.unwrap();
            }
            if ev.op == Op::Insert {
                break;
            }
        }
        lat.push(committed.elapsed());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(src);
    lat.sort();

    // Memory for one big transaction.
    db.batch_execute(&format!(
        "INSERT INTO {name} SELECT g, repeat('y', 40) FROM generate_series({}, {}) g",
        id + 1,
        id + big
    ))
    .await
    .unwrap();
    let mut src = source(url, &name, &name);
    let before = rss_bytes();
    let (big_took, peak) = pipeline_until(&mut src, big, dir).await;
    drop(src);

    println!(
        "throughput: {rows} rows in {:.2}s = {throughput:.0} events/s | \
         idle latency over {samples} single-row inserts: p50={:.1}ms p99={:.1}ms max={:.1}ms | \
         {big}-row transaction: {:.2}s, RSS {} MiB before, peak {} MiB",
        took.as_secs_f64(),
        percentile(&lat, 0.5).as_secs_f64() * 1000.0,
        percentile(&lat, 0.99).as_secs_f64() * 1000.0,
        lat.last().unwrap().as_secs_f64() * 1000.0,
        big_took.as_secs_f64(),
        before >> 20,
        peak >> 20,
    );

    for _ in 0..50 {
        if db
            .batch_execute(&format!(
                "SELECT pg_drop_replication_slot('{name}'); DROP PUBLICATION {name}; DROP TABLE {name};"
            ))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
