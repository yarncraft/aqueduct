//! Pipeline throughput benchmark (no Postgres needed).
//!
//! A synthetic source feeds `run_pipeline_with_metrics` -> sink -> file checkpoint, so this
//! measures Aqueduct's own overhead: JSON encoding, sink I/O, fsync and checkpoint writes.
//! MB/s = bytes of encoded JSON per event * events / time.
//!
//!   cargo run --release -p aqueduct-bench -- [--events N] [--payload BYTES]
//!       [--sink null|render|kafka] [--profile simple|industrial] [--dir PATH]
//!       [--brokers HOST:PORT] [--partitions N] [--in-flight K]
//!
//! Events carry a primary key (`id`, from 10 000 distinct values; `asset_id` for the industrial
//! profile), so `--partitions` spreads them like a real table. `--in-flight` is
//! `sink.in_flight_batches` (default 4).
//!
//! `--sink render` runs the route plan (topic, key, JSON value, headers) on every event without a
//! broker: the cost of everything the Kafka sink does before the network.
//!
//! With `--source postgres --pg-url URL` it instead measures the
//! Postgres source against a real server; see `postgres.rs`.
//!
//! The pipeline drains every ready event into one batch (up to `MAX_BATCH`), submits it, and
//! checkpoints and acks once per batch as batches are delivered, so the source hands out its
//! events through `try_next` the way the real sources do.
// A benchmark harness: a failed setup panics with its message.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod industrial;
mod postgres;

use std::path::PathBuf;
use std::time::Instant;

use aqueduct_core::{
    ChangeEvent, CheckpointStore, Delivery, Error, Metrics, Op, Position, Result, Sink, Source,
    run_pipeline_with_metrics,
};
use aqueduct_sinks::{DEFAULT_IN_FLIGHT_BATCHES, KafkaSecurity, KafkaSink, OutRecord, RoutePlan};
use async_trait::async_trait;
use serde_json::value::to_raw_value;
use tokio::sync::watch;

/// Renders every event through the route plan and drops the records.
struct RenderSink {
    plan: RoutePlan,
    out: Vec<OutRecord>,
    bytes: u64,
}

#[async_trait]
impl Sink for RenderSink {
    async fn send(&mut self, event: &ChangeEvent) -> Result<()> {
        self.out.clear();
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as i64);
        self.plan.render_into(event, now_us, &mut self.out)?;
        // Delivered, as far as this sink goes: count it, as the Kafka sink does after the ack.
        self.plan.commit_counts();
        self.bytes += self
            .out
            .iter()
            .map(|r| r.value.as_ref().map_or(0, String::len) as u64 + 1)
            .sum::<u64>();
        Ok(())
    }
}

struct SynthSource {
    next: u64,
    total: u64,
    pool: Vec<ChangeEvent>,
}

impl SynthSource {
    fn make(&mut self) -> Option<ChangeEvent> {
        if self.next >= self.total {
            return None;
        }
        self.next += 1;
        let mut e = self.pool[(self.next as usize) % self.pool.len()].clone();
        e.position = Position::Lsn(self.next * 64);
        Some(e)
    }
}

#[async_trait]
impl Source for SynthSource {
    async fn start(&mut self, _: Option<Position>) -> Result<()> {
        Ok(())
    }
    async fn next(&mut self) -> Result<Option<ChangeEvent>> {
        Ok(self.make())
    }
    fn try_next(&mut self) -> Option<ChangeEvent> {
        self.make()
    }
    async fn ack(&mut self, _: Position) -> Result<()> {
        Ok(())
    }
}

/// Wraps a sink and counts the encoded size of what it is given, from sizes precomputed so
/// that byte counting does not add an encode to the timing. With no inner sink it only
/// encodes (isolates CPU cost).
struct Counting<S> {
    sizes: Vec<u64>,
    inner: Option<S>,
    bytes: u64,
}

impl<S> Counting<S> {
    fn count(&mut self, e: &ChangeEvent) -> Result<()> {
        let Position::Lsn(lsn) = e.position else {
            return Err(Error::Sink("bench events carry Lsn positions".into()));
        };
        self.bytes += self.sizes[(lsn / 64) as usize % self.sizes.len()];
        Ok(())
    }
}

#[async_trait]
impl<S: Sink> Sink for Counting<S> {
    async fn send(&mut self, e: &ChangeEvent) -> Result<()> {
        self.count(e)?;
        match &mut self.inner {
            Some(inner) => inner.send(e).await,
            None => serde_json::to_vec(e)
                .map(drop)
                .map_err(|e| Error::Sink(e.to_string())),
        }
    }
    async fn send_batch(&mut self, events: &[ChangeEvent]) -> Result<()> {
        for e in events {
            self.count(e)?;
        }
        match &mut self.inner {
            Some(inner) => inner.send_batch(events).await,
            None => {
                for e in events {
                    serde_json::to_vec(e).map_err(|e| Error::Sink(e.to_string()))?;
                }
                Ok(())
            }
        }
    }
    async fn flush(&mut self) -> Result<()> {
        match &mut self.inner {
            Some(inner) => inner.flush().await,
            None => Ok(()),
        }
    }
    fn in_flight(&self) -> usize {
        self.inner.as_ref().map_or(1, Sink::in_flight)
    }
    async fn submit(&mut self, events: &[ChangeEvent]) -> Result<Delivery> {
        if self.inner.is_none() {
            self.send_batch(events).await?;
            return Ok(Box::pin(std::future::ready(Ok(()))));
        }
        for e in events {
            self.count(e)?;
        }
        // invariant: checked above.
        self.inner
            .as_mut()
            .expect("inner sink")
            .submit(events)
            .await
    }
}

/// Same strategy as the binary's FileCheckpoint: tmp file + rename.
struct FileCkpt(PathBuf);
#[async_trait]
impl CheckpointStore for FileCkpt {
    async fn load(&mut self) -> Result<Option<Position>> {
        Ok(None)
    }
    async fn save(&mut self, position: &Position) -> Result<()> {
        let m = |e: std::io::Error| Error::Checkpoint(e.to_string());
        let tmp = self.0.with_extension("tmp");
        tokio::fs::write(&tmp, position.to_checkpoint())
            .await
            .map_err(m)?;
        tokio::fs::rename(&tmp, &self.0).await.map_err(m)
    }
}

fn arg(name: &str) -> Option<String> {
    let a: Vec<String> = std::env::args().collect();
    a.iter()
        .position(|x| x == name)
        .and_then(|i| a.get(i + 1).cloned())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let events: u64 = arg("--events").map_or(100_000, |v| v.parse().unwrap());
    let payload: usize = arg("--payload").map_or(256, |v| v.parse().unwrap());
    let sink_kind = arg("--sink").unwrap_or_else(|| "null".into());
    let dir = PathBuf::from(arg("--dir").unwrap_or_else(|| ".bench-out".into()));
    std::fs::create_dir_all(&dir).unwrap();
    if arg("--source").as_deref() == Some("postgres") {
        let url = arg("--pg-url").expect("--pg-url is required with --source postgres");
        return postgres::run(&url, &dir).await;
    }

    let template = ChangeEvent {
        position: Position::Lsn(0),
        xid: 1,
        commit_ts_micros: 1_700_000_000_000_000,
        schema: "public".into(),
        table: "bench".into(),
        op: Op::Insert,
        before: None,
        after: Some(
            to_raw_value(&serde_json::json!({"id": 1, "data": "x".repeat(payload)})).unwrap(),
        ),
        key: None,
        checkpoint: None,
        unchanged: Vec::new(),
        after_is_key: false,
    };
    let profile = arg("--profile").unwrap_or_else(|| "simple".into());
    let pool = if profile == "industrial" {
        industrial::pool(20_000)
    } else {
        // 10 000 rows, so the key spreads them over the partitions.
        (0..10_000)
            .map(|id| {
                let row = serde_json::json!({"id": id, "data": "x".repeat(payload)});
                ChangeEvent {
                    after: Some(to_raw_value(&row).unwrap()),
                    key: Some(format!(r#"{{"id":{id}}}"#)),
                    ..template.clone()
                }
            })
            .collect()
    };
    let sizes: Vec<u64> = pool
        .iter()
        .map(|e| serde_json::to_string(e).unwrap().len() as u64 + 1)
        .collect();
    let per_event = sizes.iter().sum::<u64>() / sizes.len() as u64;

    let mut src = SynthSource {
        next: 0,
        total: events,
        pool,
    };
    let mut store = FileCkpt(dir.join("ckpt"));
    let (_tx, rx) = watch::channel(false);
    let metrics = Metrics::new();

    let start = Instant::now();
    let bytes = match sink_kind.as_str() {
        "kafka" => {
            let brokers = arg("--brokers").unwrap_or_else(|| "localhost:9092".into());
            let parts: i32 = arg("--partitions").map_or(3, |v| v.parse().unwrap());
            let in_flight: usize =
                arg("--in-flight").map_or(DEFAULT_IN_FLIGHT_BATCHES, |v| v.parse().unwrap());
            let topic = format!("aqueduct-bench-{}", std::process::id());
            let router = RoutePlan::new(vec![], Some(topic)).unwrap();
            let inner = KafkaSink::connect(
                vec![brokers],
                router,
                parts,
                None,
                &KafkaSecurity::default(),
            )
            .await
            .unwrap()
            .with_in_flight(in_flight);
            let mut sink = Counting {
                sizes,
                inner: Some(inner),
                bytes: 0,
            };
            run_pipeline_with_metrics(&mut src, &mut sink, &mut store, rx, &metrics)
                .await
                .unwrap();
            sink.bytes
        }
        "render" => {
            let topic = format!("aqueduct-bench-{}", std::process::id());
            let mut sink = RenderSink {
                plan: RoutePlan::new(vec![], Some(topic)).unwrap(),
                out: Vec::new(),
                bytes: 0,
            };
            run_pipeline_with_metrics(&mut src, &mut sink, &mut store, rx, &metrics)
                .await
                .unwrap();
            sink.bytes
        }
        _ => {
            let mut sink: Counting<KafkaSink> = Counting {
                sizes,
                inner: None,
                bytes: 0,
            };
            run_pipeline_with_metrics(&mut src, &mut sink, &mut store, rx, &metrics)
                .await
                .unwrap();
            sink.bytes
        }
    };
    let secs = start.elapsed().as_secs_f64();
    println!(
        "profile={profile} sink={sink_kind} events={events} payload={payload}B avg_event_json={per_event}B \
         time={secs:.2}s events/s={:.0} MB/s={:.2}",
        events as f64 / secs,
        bytes as f64 / secs / 1_000_000.0
    );
}
