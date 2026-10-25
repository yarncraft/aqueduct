use async_trait::async_trait;
use tokio::sync::watch;

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
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

/// A submitted batch's delivery: resolves once every event of the batch is durably delivered.
/// See [`Sink::submit`].
pub type Delivery = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

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
    /// Fill the buffer `try_next` pops, while earlier batches are still being delivered. It must
    /// return promptly when there is nothing to read (one poll, no idle sleep), since the pipeline
    /// waits for it before it looks at those deliveries again. The default reads nothing, so a
    /// source that keeps it overlaps deliveries only while its buffer lasts.
    async fn read_ahead(&mut self) -> Result<()> {
        Ok(())
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
    /// How many submitted batches may be undelivered at once. 1 (the default) delivers one batch
    /// at a time.
    fn in_flight(&self) -> usize {
        1
    }
    /// Start delivering `events` and return a future that resolves when they are delivered.
    /// Batches submitted later must not overtake this one where order matters (Kafka: within a
    /// partition), and a failure must fail every later batch that depends on it. The default
    /// delivers the whole batch before it returns.
    async fn submit(&mut self, events: &[ChangeEvent]) -> Result<Delivery> {
        self.send_batch(events).await?;
        self.flush().await?;
        Ok(Box::pin(std::future::ready(Ok(()))))
    }
}

/// Persists the last acknowledged LSN.
#[async_trait]
pub trait CheckpointStore: Send {
    async fn load(&mut self) -> Result<Option<Position>>;
    async fn save(&mut self, position: &Position) -> Result<()>;
}

/// A batch whose delivery takes longer than this, from submit to ack, is treated as a failure, so
/// a hung broker or endpoint cannot stall the pipeline silently. The process exits and is
/// restarted by the orchestrator; the event is redelivered (at-least-once).
const SINK_TIMEOUT: Duration = Duration::from_secs(60);

/// Most events delivered, checkpointed and acked together. Matches the Postgres source's
/// default poll size, so one poll is normally one batch.
pub const MAX_BATCH: usize = 1000;

/// Drive `source` -> `sink` until the stream ends or `shutdown` flips to true.
/// A position is checkpointed and acked only after the sink delivered it and everything before
/// it, so with several batches in flight the acks still never go backwards.
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

/// Debug check of the contract that lets a batch end anywhere: two events in a row with the same
/// `position` could not be told apart by an ack of that position, so both must carry a unique
/// resume key in `checkpoint`. Heartbeats only move the checkpoint and are exempt.
fn check_resume_keys(prev: &mut Option<(Position, bool)>, ev: &ChangeEvent) {
    if ev.op == Op::Heartbeat {
        return;
    }
    let has_key = ev.checkpoint.is_some();
    if let Some((pos, prev_key)) = prev {
        debug_assert!(
            *pos != ev.position || (*prev_key && has_key),
            "events sharing position {pos} need a `checkpoint` each, or an ack could skip one"
        );
    }
    *prev = Some((ev.position.clone(), has_key));
}

/// A submitted batch, waiting for its delivery.
struct InFlight {
    delivery: Delivery,
    landed: Landed,
}

/// What is checkpointed and reported once a batch is delivered.
struct Landed {
    began: Instant,
    events: Vec<ChangeEvent>,
    last: Position,
}

fn timed_out() -> Error {
    Error::Sink(format!("sink timed out after {}s", SINK_TIMEOUT.as_secs()))
}

/// The oldest batch is delivered: report it, then one checkpoint write and one ack for it.
async fn land(
    l: Landed,
    source: &mut dyn Source,
    store: &mut dyn CheckpointStore,
    metrics: &Metrics,
) -> Result<()> {
    if !l.events.is_empty() {
        // The batch is delivered as a unit; report its average per-event time, submit to ack.
        let per_event = l.began.elapsed() / l.events.len() as u32;
        metrics.record_events(&l.events, per_event);
    }
    // A heartbeat position may end up here, but a source that advances its slot on idle
    // heartbeats (Postgres) keeps that position on the server, not in this file. On restart
    // the slot's own position wins over an older checkpoint, so nothing is lost; at worst
    // the stream re-reads from the last delivered event, which at-least-once allows.
    store.save(&l.last).await?;
    metrics.record_checkpoint(&l.last);
    source.ack(l.last).await
}

/// Wait for the oldest batch and land it.
async fn settle(
    queue: &mut VecDeque<InFlight>,
    source: &mut dyn Source,
    store: &mut dyn CheckpointStore,
    metrics: &Metrics,
) -> Result<()> {
    let Some(f) = queue.pop_front() else {
        return Ok(());
    };
    tokio::time::timeout_at((f.landed.began + SINK_TIMEOUT).into(), f.delivery)
        .await
        .map_err(|_| timed_out())??;
    land(f.landed, source, store, metrics).await
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
    let max = sink.in_flight().max(1);
    // Submitted batches in submit order. Only the front may land, so a later batch that is
    // delivered first waits for every earlier one before its position is checkpointed.
    let mut queue: VecDeque<InFlight> = VecDeque::with_capacity(max);
    let mut prev: Option<(Position, bool)> = None;
    loop {
        // Land every front batch that is already delivered, without waiting.
        while let Some(f) = queue.front_mut() {
            let Poll::Ready(res) =
                std::future::poll_fn(|cx| Poll::Ready(f.delivery.as_mut().poll(cx))).await
            else {
                break;
            };
            res?;
            if let Some(f) = queue.pop_front() {
                land(f.landed, source, store, metrics).await?;
            }
        }
        if queue.len() >= max {
            settle(&mut queue, source, store, metrics).await?;
            continue;
        }
        let first = if queue.is_empty() {
            tokio::select! {
                _ = shutdown.changed() => break,
                ev = source.next() => match ev? {
                    Some(ev) => ev,
                    None => break,
                },
            }
        } else {
            // Batches are in flight: read on only what is there now, else wait for the oldest.
            if shutdown.has_changed().unwrap_or(true) {
                break;
            }
            let ev = match source.try_next() {
                Some(ev) => Some(ev),
                None => {
                    source.read_ahead().await?;
                    source.try_next()
                }
            };
            let Some(ev) = ev else {
                settle(&mut queue, source, store, metrics).await?;
                continue;
            };
            ev
        };
        check_resume_keys(&mut prev, &first);
        let mut batch: Vec<ChangeEvent> = Vec::with_capacity(MAX_BATCH);
        batch.push(first);
        // A batch may end anywhere, even inside events that share a position: sources whose
        // positions are not unique put a unique resume key in `checkpoint`, and that is what is
        // saved and acked.
        while batch.len() < MAX_BATCH {
            let Some(ev) = source.try_next() else { break };
            check_resume_keys(&mut prev, &ev);
            batch.push(ev);
        }
        // Heartbeats only move the checkpoint: the last event's position is saved when the
        // batch lands, but sinks and metrics never see them.
        // invariant: `first` was pushed above, so the batch is never empty here.
        let last = {
            let e = &batch[batch.len() - 1];
            e.checkpoint.clone().unwrap_or_else(|| e.position.clone())
        };
        batch.retain(|e| e.op != Op::Heartbeat);
        let began = Instant::now();
        let delivery: Delivery = if batch.is_empty() {
            Box::pin(std::future::ready(Ok(())))
        } else {
            tokio::time::timeout(SINK_TIMEOUT, sink.submit(&batch))
                .await
                .map_err(|_| timed_out())??
        };
        queue.push_back(InFlight {
            delivery,
            landed: Landed {
                began,
                events: batch,
                last,
            },
        });
    }
    // Stopping: read nothing more, but land everything already submitted.
    while !queue.is_empty() {
        settle(&mut queue, source, store, metrics).await?;
    }
    sink.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

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
            checkpoint: None,
            unchanged: Vec::new(),
            after_is_key: false,
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

    #[tokio::test]
    async fn checkpoint_not_position_is_saved_and_acked_across_a_batch_split() {
        // One same-LSN group larger than MAX_BATCH: every event has the same `position`, only
        // `checkpoint` tells them apart. A batch cut inside the group saves the cut event's key.
        let n = MAX_BATCH + 500;
        let key = |i: usize| Position::Text(format!("9@5#{i}"));
        let group = (0..n).map(|i| {
            let mut e = ev(5);
            e.checkpoint = Some(key(i));
            e
        });
        let mut src = VecSource(group.collect(), vec![]);
        let mut sink = VecSink(vec![], 0);
        let mut store = MemStore(None);
        let (_tx, rx) = watch::channel(false);
        run_pipeline(&mut src, &mut sink, &mut store, rx)
            .await
            .unwrap();
        assert_eq!(sink.0.len(), n, "every event of the group reaches the sink");
        assert_eq!(sink.1, 2);
        assert_eq!(src.1, vec![key(MAX_BATCH - 1), key(n - 1)]);
        assert_eq!(store.0, Some(key(n - 1)));
    }

    /// Hands out `per` events per batch: `try_next` runs dry after that until `read_ahead` "reads"
    /// the next batch, like a source whose buffer holds one poll.
    struct Polled {
        events: VecDeque<ChangeEvent>,
        per: usize,
        taken: usize,
        acks: Vec<Position>,
    }
    impl Polled {
        fn new(events: Vec<ChangeEvent>, per: usize) -> Self {
            Polled {
                events: events.into(),
                per,
                taken: 0,
                acks: vec![],
            }
        }
    }
    #[async_trait]
    impl Source for Polled {
        async fn start(&mut self, _: Option<Position>) -> Result<()> {
            Ok(())
        }
        async fn next(&mut self) -> Result<Option<ChangeEvent>> {
            self.taken = 1;
            Ok(self.events.pop_front())
        }
        fn try_next(&mut self) -> Option<ChangeEvent> {
            if self.taken >= self.per {
                return None;
            }
            self.taken += 1;
            self.events.pop_front()
        }
        async fn read_ahead(&mut self) -> Result<()> {
            self.taken = 0;
            Ok(())
        }
        async fn ack(&mut self, position: Position) -> Result<()> {
            if let Some(Position::Lsn(prev)) = self.acks.last() {
                let Position::Lsn(n) = &position else {
                    unreachable!()
                };
                assert!(n > prev, "ack {n} after {prev}: acks went backwards");
            }
            self.acks.push(position);
            Ok(())
        }
    }

    /// Submits up to `k` batches at once. The n-th batch's delivery takes `delay(n)` and fails
    /// when `fail(n)`; `busy`/`peak` count how many deliveries overlapped.
    struct Overlapping {
        k: usize,
        n: usize,
        delay: fn(usize) -> u64,
        fail: fn(usize) -> bool,
        busy: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }
    impl Overlapping {
        fn new(k: usize, delay: fn(usize) -> u64, fail: fn(usize) -> bool) -> Self {
            Overlapping {
                k,
                n: 0,
                delay,
                fail,
                busy: Arc::default(),
                peak: Arc::default(),
            }
        }
    }
    #[async_trait]
    impl Sink for Overlapping {
        async fn send(&mut self, _: &ChangeEvent) -> Result<()> {
            unreachable!("the pipeline submits")
        }
        fn in_flight(&self) -> usize {
            self.k
        }
        async fn submit(&mut self, _: &[ChangeEvent]) -> Result<Delivery> {
            let n = self.n;
            self.n += 1;
            let busy = self.busy.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(busy, Ordering::SeqCst);
            let (wait, fail, busy) = ((self.delay)(n), (self.fail)(n), self.busy.clone());
            Ok(Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(wait)).await;
                busy.fetch_sub(1, Ordering::SeqCst);
                if fail {
                    return Err(Error::Sink(format!("batch {n} failed")));
                }
                Ok(())
            }))
        }
    }

    #[tokio::test]
    async fn batches_delivered_out_of_order_land_in_submit_order() {
        // Five batches of two; each delivers faster than the one before it.
        let mut src = Polled::new((1..=10).map(ev).collect(), 2);
        let mut sink = Overlapping::new(4, |n| 50 - 10 * n as u64, |_| false);
        let mut store = MemStore(None);
        let metrics = Metrics::new();
        let (_tx, rx) = watch::channel(false);
        run_pipeline_with_metrics(&mut src, &mut sink, &mut store, rx, &metrics)
            .await
            .unwrap();
        let lsns = [2, 4, 6, 8, 10].map(Position::Lsn).to_vec();
        assert_eq!(src.acks, lsns, "one ack per batch, in submit order");
        assert_eq!(store.0, Some(Position::Lsn(10)));
        assert_eq!(metrics.events_total(), 10);
        assert_eq!(
            sink.peak.load(Ordering::SeqCst),
            4,
            "deliveries overlapped up to the limit"
        );
    }

    #[tokio::test]
    async fn a_failed_batch_checkpoints_nothing_past_the_last_good_one() {
        // Batch 2 (events 5-6) fails; batch 3 is delivered before it but must not land.
        let mut src = Polled::new((1..=8).map(ev).collect(), 2);
        let mut sink = Overlapping::new(4, |n| if n == 2 { 30 } else { 5 }, |n| n == 2);
        let mut store = MemStore(None);
        let (_tx, rx) = watch::channel(false);
        let err = run_pipeline(&mut src, &mut sink, &mut store, rx)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("batch 2 failed"), "{err}");
        assert_eq!(src.acks, vec![Position::Lsn(2), Position::Lsn(4)]);
        assert_eq!(store.0, Some(Position::Lsn(4)));
    }

    #[tokio::test]
    async fn a_heartbeat_only_batch_waits_its_turn() {
        // The heartbeat batch is delivered at once, but lands only after the slow batch before it
        // (`Polled::ack` panics on an ack that goes backwards).
        let hb = ChangeEvent::heartbeat(Position::Lsn(2), 0);
        let mut src = Polled::new(vec![ev(1), hb, ev(3)], 1);
        let mut sink = Overlapping::new(4, |n| if n == 0 { 30 } else { 1 }, |_| false);
        let mut store = MemStore(None);
        let (_tx, rx) = watch::channel(false);
        run_pipeline(&mut src, &mut sink, &mut store, rx)
            .await
            .unwrap();
        assert_eq!(src.acks, [1, 2, 3].map(Position::Lsn).to_vec());
        assert_eq!(sink.n, 2, "the heartbeat-only batch never reaches the sink");
    }

    #[tokio::test]
    async fn shutdown_lands_every_submitted_batch() {
        let mut src = Polled::new((1..=6).map(ev).collect(), 2);
        let mut sink = Overlapping::new(4, |_| 20, |_| false);
        let mut store = MemStore(None);
        let (tx, rx) = watch::channel(false);
        // Stop once the first batch is submitted: reading stops, but what is in flight lands.
        let stop = async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            tx.send(true).unwrap();
        };
        let (res, ()) = tokio::join!(run_pipeline(&mut src, &mut sink, &mut store, rx), stop);
        res.unwrap();
        let landed = src.acks.len();
        assert!(
            landed >= 1 && landed == sink.n,
            "{landed} landed, {} submitted",
            sink.n
        );
        assert_eq!(store.0, src.acks.last().cloned());
    }

    /// A source whose positions repeat and that sets no `checkpoint` could ack past events it has
    /// not delivered; the debug check turns that into a failing test.
    #[tokio::test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "need a `checkpoint` each")]
    async fn same_position_without_checkpoints_trips_the_debug_assert() {
        let mut src = VecSource(VecDeque::from([ev(5), ev(5)]), vec![]);
        let mut sink = VecSink(vec![], 0);
        let mut store = MemStore(None);
        let (_tx, rx) = watch::channel(false);
        let _ = run_pipeline(&mut src, &mut sink, &mut store, rx).await;
    }
}
