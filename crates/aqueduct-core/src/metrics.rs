use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::{ChangeEvent, Op};

/// Metrics are best-effort telemetry: a panic elsewhere that poisoned a lock must not
/// cascade into the pipeline, so recover the guard instead of unwrapping.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

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
    last_position: Mutex<Option<String>>,
    last_commit_ts_micros: AtomicI64,
    last_event_unix_ms: AtomicU64,
    sink_us_total: AtomicU64,
    sink_us_last: AtomicU64,
    sink_us_max: AtomicU64,
    tables: Mutex<BTreeMap<String, u64>>,
    table_ops: Mutex<BTreeMap<String, [u64; 4]>>,
    last_error: Mutex<Option<String>>,
    /// Slot lag in bytes, or `-1` when unknown (no slot, or the source has none).
    slot_lag_bytes: AtomicI64,
    confirmed_flush_lsn: Mutex<Option<String>>,
    /// Set once a source reports heartbeat state; until then the snapshot omits `heartbeat`.
    hb_reported: AtomicBool,
    /// Heartbeat interval in ms, `0` when heartbeats are off.
    hb_interval_ms: AtomicU64,
    /// Unix ms of the last completed heartbeat, `0` before the first.
    hb_last_ms: AtomicU64,
    hb_skipped: AtomicU64,
    hb_skip_reason: Mutex<Option<String>>,
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
    /// Per table counts as `[insert, update, delete, truncate]`.
    pub table_ops: BTreeMap<String, [u64; 4]>,
    pub last_error: Option<String>,
    /// Replication slot lag in bytes (WAL head minus `confirmed_flush_lsn`); `None` when unknown
    /// or the source has no slot.
    pub slot_lag_bytes: Option<u64>,
    pub confirmed_flush_lsn: Option<String>,
    /// Omitted when the source does not report heartbeat state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat: Option<HeartbeatSnapshot>,
}

/// Idle-heartbeat state of a source, see [`Metrics::heartbeat_configured`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatSnapshot {
    /// `None` when heartbeats are off.
    pub interval_ms: Option<u64>,
    /// Unix ms of the last completed heartbeat; `None` until the first.
    pub last_ms: Option<u64>,
    pub skipped_total: u64,
    /// Why the most recent heartbeat was skipped; `None` until one is.
    pub last_skip_reason: Option<String>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            state: AtomicU8::new(State::Starting as u8),
            events: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            truncates: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last_position: Mutex::new(None),
            last_commit_ts_micros: AtomicI64::new(0),
            last_event_unix_ms: AtomicU64::new(0),
            sink_us_total: AtomicU64::new(0),
            sink_us_last: AtomicU64::new(0),
            sink_us_max: AtomicU64::new(0),
            tables: Mutex::new(BTreeMap::new()),
            table_ops: Mutex::new(BTreeMap::new()),
            last_error: Mutex::new(None),
            slot_lag_bytes: AtomicI64::new(-1),
            confirmed_flush_lsn: Mutex::new(None),
            hb_reported: AtomicBool::new(false),
            hb_interval_ms: AtomicU64::new(0),
            hb_last_ms: AtomicU64::new(0),
            hb_skipped: AtomicU64::new(0),
            hb_skip_reason: Mutex::new(None),
        }
    }

    /// Record the replication slot's lag; `None` clears it.
    pub fn set_slot(&self, lag_bytes: Option<u64>, confirmed_flush_lsn: Option<String>) {
        self.slot_lag_bytes.store(
            lag_bytes.map_or(-1, |n| i64::try_from(n).unwrap_or(i64::MAX)),
            Relaxed,
        );
        *lock(&self.confirmed_flush_lsn) = confirmed_flush_lsn;
    }

    /// A source with idle heartbeats calls this once at start. `None` means they are off.
    pub fn heartbeat_configured(&self, interval: Option<Duration>) {
        let ms = interval.map_or(0, |d| d.as_millis() as u64);
        self.hb_interval_ms.store(ms, Relaxed);
        self.hb_reported.store(true, Relaxed);
    }

    /// A heartbeat ran to completion. Heartbeats never reach the sink or the event counters.
    pub fn record_heartbeat(&self) {
        self.hb_last_ms.store(now_ms(), Relaxed);
    }

    /// A heartbeat was skipped because it failed; the stream carries on.
    pub fn record_heartbeat_skip(&self, reason: impl Into<String>) {
        self.hb_skipped.fetch_add(1, Relaxed);
        *lock(&self.hb_skip_reason) = Some(reason.into());
    }

    pub fn set_state(&self, s: State) {
        self.state.store(s as u8, Relaxed);
    }

    pub fn events_total(&self) -> u64 {
        self.events.load(Relaxed)
    }

    /// Record one event that the sink accepted, taking `sink_time` to deliver.
    pub fn record_event(&self, ev: &ChangeEvent, sink_time: Duration) {
        if ev.op == Op::Heartbeat {
            return;
        }
        self.events.fetch_add(1, Relaxed);
        match ev.op {
            Op::Insert => &self.inserts,
            Op::Update => &self.updates,
            Op::Delete => &self.deletes,
            Op::Truncate => &self.truncates,
            // Unreachable: heartbeats return at the top; the arm keeps the match exhaustive.
            Op::Heartbeat => return,
        }
        .fetch_add(1, Relaxed);
        *lock(&self.last_position) = Some(ev.position.to_string());
        self.last_commit_ts_micros
            .store(ev.commit_ts_micros, Relaxed);
        self.last_event_unix_ms.store(now_ms(), Relaxed);
        let us = sink_time.as_micros() as u64;
        self.sink_us_total.fetch_add(us, Relaxed);
        self.sink_us_last.store(us, Relaxed);
        self.sink_us_max.fetch_max(us, Relaxed);
        {
            let mut t = lock(&self.tables);
            *t.entry(format!("{}.{}", ev.schema, ev.table)).or_default() += 1;
        }
        {
            let slot = match ev.op {
                Op::Insert => 0,
                Op::Update => 1,
                Op::Delete => 2,
                Op::Truncate => 3,
                // Unreachable: heartbeats return at the top of `record_event`; the arm only
                // keeps the match exhaustive.
                Op::Heartbeat => return,
            };
            let mut t = lock(&self.table_ops);
            t.entry(format!("{}.{}", ev.schema, ev.table)).or_default()[slot] += 1;
        }
    }

    pub fn record_error(&self, msg: impl Into<String>) {
        self.errors.fetch_add(1, Relaxed);
        *lock(&self.last_error) = Some(msg.into());
    }

    pub fn snapshot(&self) -> Snapshot {
        let events = self.events.load(Relaxed);

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
            last_lsn: lock(&self.last_position).clone(),
            lag_ms: (events > 0 && ts > 0).then(|| now_ms() as i64 - ts / 1000),
            last_event_unix_ms: (last_ms > 0).then_some(last_ms),
            sink_avg_ms: if events > 0 {
                self.sink_us_total.load(Relaxed) as f64 / events as f64 / 1000.0
            } else {
                0.0
            },
            sink_last_ms: self.sink_us_last.load(Relaxed) as f64 / 1000.0,
            sink_max_ms: self.sink_us_max.load(Relaxed) as f64 / 1000.0,
            tables: lock(&self.tables).clone(),
            table_ops: lock(&self.table_ops).clone(),
            last_error: lock(&self.last_error).clone(),
            slot_lag_bytes: u64::try_from(self.slot_lag_bytes.load(Relaxed)).ok(),
            confirmed_flush_lsn: lock(&self.confirmed_flush_lsn).clone(),
            heartbeat: self.hb_reported.load(Relaxed).then(|| {
                let interval = self.hb_interval_ms.load(Relaxed);
                let last = self.hb_last_ms.load(Relaxed);
                HeartbeatSnapshot {
                    interval_ms: (interval > 0).then_some(interval),
                    last_ms: (last > 0).then_some(last),
                    skipped_total: self.hb_skipped.load(Relaxed),
                    last_skip_reason: lock(&self.hb_skip_reason).clone(),
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Position;

    fn ev(op: Op, table: &str, lsn: u64) -> ChangeEvent {
        ChangeEvent {
            position: Position::Lsn(lsn),
            xid: 1,
            commit_ts_micros: 1,
            schema: "public".into(),
            table: table.into(),
            op,
            before: None,
            after: None,
            key: None,
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
        assert_eq!(s.table_ops["public.a"], [2, 0, 0, 0]);
        assert_eq!(s.table_ops["public.b"], [0, 0, 1, 0]);
        assert_eq!(s.last_lsn.as_deref(), Some("0/C"));
        assert!((s.sink_avg_ms - 4.0).abs() < 0.01);
        assert!((s.sink_max_ms - 6.0).abs() < 0.01);
        assert_eq!(s.last_error.as_deref(), Some("boom"));
    }

    #[test]
    fn slot_and_heartbeat_are_null_until_reported() {
        let m = Metrics::new();
        let s = m.snapshot();
        assert_eq!(s.slot_lag_bytes, None);
        assert_eq!(s.confirmed_flush_lsn, None);
        assert_eq!(s.heartbeat, None);
        let json = serde_json::to_value(&s).unwrap();
        assert!(json["slot_lag_bytes"].is_null());
        assert!(json["confirmed_flush_lsn"].is_null());
        assert!(json.get("heartbeat").is_none());
    }

    #[test]
    fn heartbeat_off_has_null_interval() {
        let m = Metrics::new();
        m.heartbeat_configured(None);
        let hb = m.snapshot().heartbeat.unwrap();
        assert_eq!(hb.interval_ms, None);
        assert_eq!(hb.last_ms, None);
        assert_eq!(hb.skipped_total, 0);
        assert_eq!(hb.last_skip_reason, None);
    }

    #[test]
    fn heartbeat_last_ms_is_null_until_first_beat() {
        let m = Metrics::new();
        m.heartbeat_configured(Some(Duration::from_secs(10)));
        let hb = m.snapshot().heartbeat.unwrap();
        assert_eq!(hb.interval_ms, Some(10_000));
        assert_eq!(hb.last_ms, None);
        m.record_heartbeat();
        assert!(m.snapshot().heartbeat.unwrap().last_ms.is_some());
        // Heartbeats never count as events.
        assert_eq!(m.snapshot().events_total, 0);
    }

    #[test]
    fn heartbeat_skip_reason_is_null_until_a_skip() {
        let m = Metrics::new();
        m.heartbeat_configured(Some(Duration::from_secs(10)));
        assert_eq!(m.snapshot().heartbeat.unwrap().last_skip_reason, None);
        m.record_heartbeat_skip("action query failed: lock timeout");
        m.record_heartbeat_skip("WAL position query failed");
        let hb = m.snapshot().heartbeat.unwrap();
        assert_eq!(hb.skipped_total, 2);
        assert_eq!(
            hb.last_skip_reason.as_deref(),
            Some("WAL position query failed")
        );
    }

    #[test]
    fn slot_lag_round_trips_and_clears() {
        let m = Metrics::new();
        m.set_slot(Some(1_048_576), Some("0/16B3748".into()));
        let s = m.snapshot();
        assert_eq!(s.slot_lag_bytes, Some(1_048_576));
        assert_eq!(s.confirmed_flush_lsn.as_deref(), Some("0/16B3748"));
        m.set_slot(None, None);
        let s = m.snapshot();
        assert_eq!((s.slot_lag_bytes, s.confirmed_flush_lsn), (None, None));
    }
}
