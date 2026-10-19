use async_trait::async_trait;
use tokio::sync::watch;

use std::time::{Duration, Instant};

use crate::{ChangeEvent, Metrics, Op, Position, State};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("source error: {0}")]
    Source(String),
    #[error("sink error: {0}")]
    Sink(String),
    #[error("checkpoint error: {0}")]
    Checkpoint(String),
    #[error("config error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Produces change events, e.g. Postgres logical replication.
#[async_trait]
pub trait Source: Send {
    /// Start streaming from `start` (the last durable checkpoint, if any).
    async fn start(&mut self, start: Option<Position>) -> Result<()>;
    /// Next event; `Ok(None)` means the stream ended cleanly.
    async fn next(&mut self) -> Result<Option<ChangeEvent>>;
    /// An already-buffered event, if any, without waiting. The pipeline uses it to gather a
    /// batch after `next` returns; sources that cannot tell may keep the default (batches of 1).
    fn try_next(&mut self) -> Option<ChangeEvent> {
        None
    }
    /// Tell the source everything up to `position` is durably delivered.
    async fn ack(&mut self, position: Position) -> Result<()>;
}

/// Delivers change events somewhere. Events may be redelivered after a crash,
/// so sinks should tolerate duplicates.
#[async_trait]
pub trait Sink: Send {
    async fn send(&mut self, event: &ChangeEvent) -> Result<()>;
    /// Deliver `events` in order. Sinks that can pipeline (Kafka) override this; the default
    /// sends one by one.
    async fn send_batch(&mut self, events: &[ChangeEvent]) -> Result<()> {
        for event in events {
            self.send(event).await?;
        }
        Ok(())
    }
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Persists the last acknowledged LSN.
#[async_trait]
pub trait CheckpointStore: Send {
    async fn load(&mut self) -> Result<Option<Position>>;
    async fn save(&mut self, position: &Position) -> Result<()>;
}

/// A sink call that takes longer than this is treated as a failure, so a hung broker or
/// endpoint cannot stall the pipeline silently. The process exits and is restarted by the
/// orchestrator; the event is redelivered (at-least-once).
const SINK_TIMEOUT: Duration = Duration::from_secs(60);

/// Most events delivered, checkpointed and acked together. Matches the Postgres source's
/// default poll size, so one poll is normally one batch.
pub const MAX_BATCH: usize = 1000;

/// Drive `source` -> `sink` until the stream ends or `shutdown` flips to true.
/// The position is checkpointed and acked only after the sink succeeds.
pub async fn run_pipeline(
    source: &mut dyn Source,
    sink: &mut dyn Sink,
    store: &mut dyn CheckpointStore,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    run_pipeline_with_metrics(source, sink, store, shutdown, &Metrics::new()).await
}

/// Like [`run_pipeline`], reporting progress and failures to `metrics`.
pub async fn run_pipeline_with_metrics(
    source: &mut dyn Source,
    sink: &mut dyn Sink,
    store: &mut dyn CheckpointStore,
    shutdown: watch::Receiver<bool>,
    metrics: &Metrics,
) -> Result<()> {
    let res = drive(source, sink, store, shutdown, metrics).await;
    match &res {
        Ok(()) => metrics.set_state(State::Stopped),
        Err(e) => {
            metrics.record_error(e.to_string());
            metrics.set_state(State::Failed);
        }
    }
    res
}

async fn drive(
    source: &mut dyn Source,
    sink: &mut dyn Sink,
    store: &mut dyn CheckpointStore,
    mut shutdown: watch::Receiver<bool>,
    metrics: &Metrics,
) -> Result<()> {
    let start = store.load().await?;
    source.start(start).await?;
    metrics.set_state(State::Streaming);
    let mut batch: Vec<ChangeEvent> = Vec::with_capacity(MAX_BATCH);
    loop {
        batch.clear();
        let first = tokio::select! {
            _ = shutdown.changed() => break,
            ev = source.next() => match ev? {
                Some(ev) => ev,
                None => break,
            },
        };
        batch.push(first);
        while batch.len() < MAX_BATCH {
            match source.try_next() {
                Some(ev) => batch.push(ev),
                None => break,
            }
        }
        // Heartbeats only move the checkpoint: the last event's position is saved below,
        // but sinks and metrics never see them.
        let last = batch[batch.len() - 1].position.clone();
        batch.retain(|e| e.op != Op::Heartbeat);
        if !batch.is_empty() {
            let began = Instant::now();
            let delivered = tokio::time::timeout(SINK_TIMEOUT, async {
                sink.send_batch(&batch).await?;
                sink.flush().await
            })
            .await
            .map_err(|_| {
                Error::Sink(format!("sink timed out after {}s", SINK_TIMEOUT.as_secs()))
            })?;
            delivered?;
            // The batch is delivered as a unit; report its average per-event sink time.
            let per_event = began.elapsed() / batch.len() as u32;
            for event in &batch {
                metrics.record_event(event, per_event);
            }
        }
        // One checkpoint write and one ack for the whole batch, after the sink succeeded.
        store.save(&last).await?;
        source.ack(last).await?;
    }
    sink.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct VecSource(VecDeque<ChangeEvent>, Vec<Position>);
    #[async_trait]
    impl Source for VecSource {
        async fn start(&mut self, _: Option<Position>) -> Result<()> {
            Ok(())
        }
        async fn next(&mut self) -> Result<Option<ChangeEvent>> {
            Ok(self.0.pop_front())
        }
        fn try_next(&mut self) -> Option<ChangeEvent> {
            self.0.pop_front()
        }
        async fn ack(&mut self, position: Position) -> Result<()> {
            self.1.push(position);
            Ok(())
        }
    }
    struct VecSink(Vec<Position>, usize);
    #[async_trait]
    impl Sink for VecSink {
        async fn send(&mut self, e: &ChangeEvent) -> Result<()> {
            self.0.push(e.position.clone());
            Ok(())
        }
        async fn send_batch(&mut self, events: &[ChangeEvent]) -> Result<()> {
            self.1 += 1;
            for e in events {
                self.send(e).await?;
            }
            Ok(())
        }
    }
    struct MemStore(Option<Position>);
    #[async_trait]
    impl CheckpointStore for MemStore {
        async fn load(&mut self) -> Result<Option<Position>> {
            Ok(self.0.clone())
        }
        async fn save(&mut self, l: &Position) -> Result<()> {
            self.0 = Some(l.clone());
            Ok(())
        }
    }

    fn ev(n: u64) -> ChangeEvent {
        ChangeEvent {
            position: Position::Lsn(n),
            xid: 1,
            commit_ts_micros: 0,
            schema: "public".into(),
            table: "t".into(),
            op: Op::Insert,
            before: None,
            after: None,
            key: None,
        }
    }

    #[tokio::test]
    async fn acks_after_sink() {
        let mut src = VecSource((1..=3).map(ev).collect(), vec![]);
        let mut sink = VecSink(vec![], 0);
        let mut store = MemStore(None);
        let (_tx, rx) = watch::channel(false);
        run_pipeline(&mut src, &mut sink, &mut store, rx)
            .await
            .unwrap();
        assert_eq!(
            sink.0,
            vec![Position::Lsn(1), Position::Lsn(2), Position::Lsn(3)]
        );
        // One batch: a single sink call and a single ack for the last LSN.
        assert_eq!(sink.1, 1);
        assert_eq!(src.1, vec![Position::Lsn(3)]);
        assert_eq!(store.0, Some(Position::Lsn(3)));
    }

    #[tokio::test]
    async fn heartbeats_move_the_checkpoint_but_never_reach_the_sink() {
        let hb = |n| ChangeEvent::heartbeat(Position::Lsn(n), 0);
        // Mixed batch: heartbeat in the middle and at the end.
        let mut src = VecSource(VecDeque::from([ev(1), hb(2), ev(3), hb(4)]), vec![]);
        let mut sink = VecSink(vec![], 0);
        let mut store = MemStore(None);
        let metrics = Metrics::new();
        let (_tx, rx) = watch::channel(false);
        run_pipeline_with_metrics(&mut src, &mut sink, &mut store, rx, &metrics)
            .await
            .unwrap();
        assert_eq!(sink.0, vec![Position::Lsn(1), Position::Lsn(3)]);
        assert_eq!(metrics.events_total(), 2);
        assert_eq!(store.0, Some(Position::Lsn(4)));
        assert_eq!(src.1, vec![Position::Lsn(4)]);

        // Heartbeat-only batch: no sink call at all, checkpoint still moves.
        let mut src = VecSource(VecDeque::from([hb(9)]), vec![]);
        let mut sink = VecSink(vec![], 0);
        let (_tx, rx) = watch::channel(false);
        run_pipeline(&mut src, &mut sink, &mut store, rx)
            .await
            .unwrap();
        assert_eq!(sink.1, 0);
        assert_eq!(store.0, Some(Position::Lsn(9)));
    }
}
