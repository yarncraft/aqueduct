use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicU8, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::{ChangeEvent, Lsn, Op};

/// Pipeline lifecycle, shown on the admin cockpit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Starting = 0,
    Streaming = 1,
    Stopped = 2,
    Failed = 3,
}

impl State {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => State::Streaming,
            2 => State::Stopped,
            3 => State::Failed,
            _ => State::Starting,
        }
    }
}

/// Default number of recent events kept for the admin event inspector.
pub const DEFAULT_RECENT_EVENTS: usize = 500;

/// An event kept in the inspector buffer. `seq` increases by one per event.
#[derive(Debug, Clone, Serialize)]
pub struct RecordedEvent {
    pub seq: u64,
    /// When the sink accepted the event (Unix ms).
    pub delivered_unix_ms: u64,
    pub event: ChangeEvent,
}

/// Live counters updated by the pipeline and read by the admin server.
pub struct Metrics {
    started: Instant,
    state: AtomicU8,
    events: AtomicU64,
    inserts: AtomicU64,
    updates: AtomicU64,
    deletes: AtomicU64,
    truncates: AtomicU64,
    errors: AtomicU64,
    last_lsn: AtomicU64,
    last_commit_ts_micros: AtomicI64,
    last_event_unix_ms: AtomicU64,
    sink_us_total: AtomicU64,
    sink_us_last: AtomicU64,
    sink_us_max: AtomicU64,
    tables: Mutex<BTreeMap<String, u64>>,
    last_error: Mutex<Option<String>>,
    recent: Mutex<VecDeque<RecordedEvent>>,
    recent_cap: usize,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    pub state: State,
    pub uptime_secs: u64,
    pub events_total: u64,
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub truncates: u64,
    pub errors: u64,
    pub last_lsn: Option<String>,
    /// Commit-time to delivery-time of the newest event, in ms.
    pub lag_ms: Option<i64>,
    pub last_event_unix_ms: Option<u64>,
    pub sink_avg_ms: f64,
    pub sink_last_ms: f64,
    pub sink_max_ms: f64,
    pub tables: BTreeMap<String, u64>,
    pub last_error: Option<String>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Metrics {
    pub fn new() -> Self {
        Self::with_recent_capacity(DEFAULT_RECENT_EVENTS)
    }

    /// Keep the last `cap` events for inspection (`0` disables the buffer).
    pub fn with_recent_capacity(cap: usize) -> Self {
        Self {
            started: Instant::now(),
            state: AtomicU8::new(State::Starting as u8),
            events: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            truncates: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last_lsn: AtomicU64::new(0),
            last_commit_ts_micros: AtomicI64::new(0),
            last_event_unix_ms: AtomicU64::new(0),
            sink_us_total: AtomicU64::new(0),
            sink_us_last: AtomicU64::new(0),
            sink_us_max: AtomicU64::new(0),
            tables: Mutex::new(BTreeMap::new()),
            last_error: Mutex::new(None),
            recent: Mutex::new(VecDeque::new()),
            recent_cap: cap,
        }
    }

    pub fn set_state(&self, s: State) {
        self.state.store(s as u8, Relaxed);
    }

    pub fn events_total(&self) -> u64 {
        self.events.load(Relaxed)
    }

    /// Record one event that the sink accepted, taking `sink_time` to deliver.
    pub fn record_event(&self, ev: &ChangeEvent, sink_time: Duration) {
        self.events.fetch_add(1, Relaxed);
        match ev.op {
            Op::Insert => &self.inserts,
            Op::Update => &self.updates,
            Op::Delete => &self.deletes,
            Op::Truncate => &self.truncates,
        }
        .fetch_add(1, Relaxed);
        self.last_lsn.store(ev.lsn.0, Relaxed);
        self.last_commit_ts_micros
            .store(ev.commit_ts_micros, Relaxed);
        self.last_event_unix_ms.store(now_ms(), Relaxed);
        let us = sink_time.as_micros() as u64;
        self.sink_us_total.fetch_add(us, Relaxed);
        self.sink_us_last.store(us, Relaxed);
        self.sink_us_max.fetch_max(us, Relaxed);
        {
            let mut t = self.tables.lock().unwrap();
            *t.entry(format!("{}.{}", ev.schema, ev.table)).or_default() += 1;
        }
        if self.recent_cap > 0 {
            let mut r = self.recent.lock().unwrap();
            if r.len() == self.recent_cap {
                r.pop_front();
            }
            r.push_back(RecordedEvent {
                // `events` was already incremented, so it is this event's 1-based sequence.
                seq: self.events.load(Relaxed),
                delivered_unix_ms: now_ms(),
                event: ev.clone(),
            });
        }
    }

    /// Up to `limit` newest buffered events with `seq > after`, oldest first.
    pub fn recent(&self, after: u64, limit: usize) -> Vec<RecordedEvent> {
        let r = self.recent.lock().unwrap();
        let mut out: Vec<RecordedEvent> = r
            .iter()
            .rev()
            .take_while(|e| e.seq > after)
            .take(limit)
            .cloned()
            .collect();
        out.reverse();
        out
    }

    pub fn record_error(&self, msg: impl Into<String>) {
        self.errors.fetch_add(1, Relaxed);
        *self.last_error.lock().unwrap() = Some(msg.into());
    }

    pub fn snapshot(&self) -> Snapshot {
        let events = self.events.load(Relaxed);
        let last_lsn = self.last_lsn.load(Relaxed);
        let ts = self.last_commit_ts_micros.load(Relaxed);
        let last_ms = self.last_event_unix_ms.load(Relaxed);
        Snapshot {
            state: State::from_u8(self.state.load(Relaxed)),
            uptime_secs: self.started.elapsed().as_secs(),
            events_total: events,
            inserts: self.inserts.load(Relaxed),
            updates: self.updates.load(Relaxed),
            deletes: self.deletes.load(Relaxed),
            truncates: self.truncates.load(Relaxed),
            errors: self.errors.load(Relaxed),
            last_lsn: (events > 0).then(|| Lsn(last_lsn).to_string()),
            lag_ms: (events > 0 && ts > 0).then(|| now_ms() as i64 - ts / 1000),
            last_event_unix_ms: (last_ms > 0).then_some(last_ms),
            sink_avg_ms: if events > 0 {
                self.sink_us_total.load(Relaxed) as f64 / events as f64 / 1000.0
            } else {
                0.0
            },
            sink_last_ms: self.sink_us_last.load(Relaxed) as f64 / 1000.0,
            sink_max_ms: self.sink_us_max.load(Relaxed) as f64 / 1000.0,
            tables: self.tables.lock().unwrap().clone(),
            last_error: self.last_error.lock().unwrap().clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(op: Op, table: &str, lsn: u64) -> ChangeEvent {
        ChangeEvent {
            lsn: Lsn(lsn),
            xid: 1,
            commit_ts_micros: 1,
            schema: "public".into(),
            table: table.into(),
            op,
            before: None,
            after: None,
        }
    }

    #[test]
    fn counts_by_op_and_table() {
        let m = Metrics::new();
        assert!(m.snapshot().last_lsn.is_none());
        m.record_event(&ev(Op::Insert, "a", 10), Duration::from_millis(2));
        m.record_event(&ev(Op::Insert, "a", 11), Duration::from_millis(4));
        m.record_event(&ev(Op::Delete, "b", 12), Duration::from_millis(6));
        m.record_error("boom");
        let s = m.snapshot();
        assert_eq!(
            (s.events_total, s.inserts, s.deletes, s.errors),
            (3, 2, 1, 1)
        );
        assert_eq!(s.tables["public.a"], 2);
        assert_eq!(s.last_lsn.as_deref(), Some("0/C"));
        assert!((s.sink_avg_ms - 4.0).abs() < 0.01);
        assert!((s.sink_max_ms - 6.0).abs() < 0.01);
        assert_eq!(s.last_error.as_deref(), Some("boom"));
    }

    #[test]
    fn recent_buffer_is_bounded_and_resumable() {
        let m = Metrics::with_recent_capacity(3);
        for i in 1..=5 {
            m.record_event(&ev(Op::Insert, "a", i), Duration::ZERO);
        }
        let seqs = |v: Vec<RecordedEvent>| v.into_iter().map(|e| e.seq).collect::<Vec<_>>();
        assert_eq!(seqs(m.recent(0, 10)), vec![3, 4, 5]);
        assert_eq!(seqs(m.recent(3, 10)), vec![4, 5]);
        assert_eq!(seqs(m.recent(0, 2)), vec![4, 5]);
        assert!(m.recent(5, 10).is_empty());
        let off = Metrics::with_recent_capacity(0);
        off.record_event(&ev(Op::Insert, "a", 1), Duration::ZERO);
        assert!(off.recent(0, 10).is_empty());
    }
}
