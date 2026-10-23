use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::lag::{self, Kind, Sample, SourceLag};
use crate::{ChangeEvent, Checkpoint, Op, Position};

/// Per-table op counters: `[insert, update, delete, truncate, read]`.
pub const OP_SLOTS: usize = 5;

/// Metrics are best-effort telemetry: a panic elsewhere that poisoned a lock must not
/// cascade into the pipeline, so recover the guard instead of unwrapping.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A checkpoint as shown outside the checkpoint file: an LSN as `X/Y`, other positions as their
/// text, and a composite snapshot checkpoint as its stream position plus the table being read,
/// without the snapshot cursor. Text that is not a composite checkpoint is shown as is.
pub fn display_checkpoint(position: &Position) -> String {
    match position {
        Position::Text(t) if t.trim_start().starts_with('{') && t.contains("\"snap\"") => {
            match Checkpoint::decode(t) {
                Ok(c) => c.display(),
                // Unreadable but snapshot-shaped: show nothing rather than risk the cursor.
                Err(_) => "(unreadable checkpoint)".into(),
            }
        }
        p => p.to_string(),
    }
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
    reads: AtomicU64,
    errors: AtomicU64,
    last_position: Mutex<Option<String>>,
    last_commit_ts_micros: AtomicI64,
    last_event_unix_ms: AtomicU64,
    sink_us_total: AtomicU64,
    sink_us_last: AtomicU64,
    sink_us_max: AtomicU64,
    /// Per `schema.table`: op counts and the newest delivered change. One lock per event.
    tables: Mutex<BTreeMap<String, TableEntry>>,
    /// The last position the pipeline checkpointed (global, not per table).
    checkpoint: Mutex<Option<String>>,
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
    /// The running or last snapshot, as the source reports it.
    snapshot: Mutex<Option<SnapshotStatus>>,
    /// `snapshot_max_rows_per_sec`, `0` when unthrottled.
    snapshot_limit: AtomicU64,
    /// Source positions for "behind source" (#150), `0` until reported. Postgres reports LSNs,
    /// MongoDB cluster times in Unix ms. Each only moves forward.
    head_lsn: AtomicU64,
    delivered_lsn: AtomicU64,
    head_time_ms: AtomicU64,
    delivered_time_ms: AtomicU64,
    /// When the head was last reported, ms since `started` plus one; `0` before the first.
    head_read_ms: AtomicU64,
    /// One reading of the positions per second, see [`Metrics::sample_source_lag`].
    lag_ring: Mutex<VecDeque<Sample>>,
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
    pub reads: u64,
    pub errors: u64,
    pub last_lsn: Option<String>,
    /// Commit-time to delivery-time of the newest event, in ms.
    pub lag_ms: Option<i64>,
    pub last_event_unix_ms: Option<u64>,
    pub sink_avg_ms: f64,
    pub sink_last_ms: f64,
    pub sink_max_ms: f64,
    pub tables: BTreeMap<String, u64>,
    /// Per table counts as `[insert, update, delete, truncate, read]`.
    pub table_ops: BTreeMap<String, [u64; OP_SLOTS]>,
    /// Per table, the newest delivered change (its high-water mark on the pipeline side).
    pub table_marks: BTreeMap<String, TableMark>,
    /// The last position the pipeline checkpointed and acked; `None` before the first batch.
    pub checkpoint: Option<String>,
    pub last_error: Option<String>,
    /// Replication slot lag in bytes (WAL head minus `confirmed_flush_lsn`); `None` when unknown
    /// or the source has no slot.
    pub slot_lag_bytes: Option<u64>,
    pub confirmed_flush_lsn: Option<String>,
    /// Omitted when the source does not report heartbeat state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat: Option<HeartbeatSnapshot>,
    /// The running or last snapshot (ADR 0001); `None` before the first.
    pub snapshot: Option<SnapshotStatus>,
    /// Postgres: the server's WAL head as `X/Y`, as last read; `None` for other sources.
    pub source_head_lsn: Option<String>,
    /// MongoDB: the newest cluster time the server reported, Unix ms; `None` for other sources.
    pub source_head_time_ms: Option<u64>,
    /// Whether Aqueduct is behind its source (#150).
    pub source_lag: SourceLag,
}

/// The newest change delivered for one table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableMark {
    /// Schema (Postgres) or database (MongoDB); needed to route the table to its topic.
    pub schema: String,
    /// Table or collection.
    pub table: String,
    /// The change's LSN as `X/Y`; `None` for sources without LSNs (MongoDB).
    pub last_lsn: Option<String>,
    /// Commit time of the change, Unix ms; `None` when the source gave none.
    pub last_commit_ms: Option<i64>,
    /// When the sink acknowledged it, Unix ms.
    pub last_delivered_ms: u64,
}

struct TableEntry {
    ops: [u64; OP_SLOTS],
    schema: String,
    table: String,
    last_lsn: Option<u64>,
    last_commit_us: i64,
    last_delivered_ms: u64,
}

/// Progress of a snapshot, see [`Metrics::set_snapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SnapshotStatus {
    pub id: String,
    /// `running`, `finished` or `cancelled`.
    pub state: String,
    /// `schema.table` of every table to copy, in order.
    pub tables: Vec<String>,
    /// Tables fully copied.
    pub tables_done: usize,
    /// The table being copied, `None` when not running.
    pub table: Option<String>,
    /// Rows emitted so far by this process (a restart counts again from 0).
    pub rows: u64,
    /// Planner estimate of all rows to copy; `None` when a table was never analyzed.
    pub rows_estimated: Option<u64>,
    /// Tables left out and why.
    pub skipped: Vec<SkippedTable>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkippedTable {
    pub table: String,
    pub reason: String,
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
            reads: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last_position: Mutex::new(None),
            last_commit_ts_micros: AtomicI64::new(0),
            last_event_unix_ms: AtomicU64::new(0),
            sink_us_total: AtomicU64::new(0),
            sink_us_last: AtomicU64::new(0),
            sink_us_max: AtomicU64::new(0),
            tables: Mutex::new(BTreeMap::new()),
            checkpoint: Mutex::new(None),
            last_error: Mutex::new(None),
            slot_lag_bytes: AtomicI64::new(-1),
            confirmed_flush_lsn: Mutex::new(None),
            hb_reported: AtomicBool::new(false),
            hb_interval_ms: AtomicU64::new(0),
            hb_last_ms: AtomicU64::new(0),
            hb_skipped: AtomicU64::new(0),
            hb_skip_reason: Mutex::new(None),
            snapshot: Mutex::new(None),
            snapshot_limit: AtomicU64::new(0),
            head_lsn: AtomicU64::new(0),
            delivered_lsn: AtomicU64::new(0),
            head_time_ms: AtomicU64::new(0),
            delivered_time_ms: AtomicU64::new(0),
            head_read_ms: AtomicU64::new(0),
            lag_ring: Mutex::new(VecDeque::with_capacity(lag::LAG_HISTORY)),
        }
    }

    /// The configured `snapshot_max_rows_per_sec`, so the cockpit can tell when it sets the pace.
    pub fn set_snapshot_rate_limit(&self, rows_per_sec: Option<u32>) {
        self.snapshot_limit
            .store(rows_per_sec.map_or(0, u64::from), Relaxed);
    }

    /// Postgres: the server's WAL head (or a lower bound of it, such as a keepalive's WAL end).
    /// Only a newer value is kept.
    pub fn report_source_head_lsn(&self, lsn: u64) {
        self.head_lsn.fetch_max(lsn, Relaxed);
        self.head_read();
    }

    /// The head was just read (even if it did not move): it is current.
    fn head_read(&self) {
        let ms = self.started.elapsed().as_millis() as u64;
        self.head_read_ms.fetch_max(ms + 1, Relaxed);
    }

    /// Postgres: an LSN up to which everything has been delivered (or found to hold nothing to
    /// deliver). Only a newer value is kept.
    pub fn report_source_delivered_lsn(&self, lsn: u64) {
        self.delivered_lsn.fetch_max(lsn, Relaxed);
    }

    /// MongoDB: the newest cluster time the server reported, Unix ms.
    pub fn report_source_head_time_ms(&self, ms: u64) {
        self.head_time_ms.fetch_max(ms, Relaxed);
        self.head_read();
    }

    /// MongoDB: the cluster time, Unix ms, up to which everything has been delivered.
    pub fn report_source_delivered_time_ms(&self, ms: u64) {
        self.delivered_time_ms.fetch_max(ms, Relaxed);
    }

    /// The positions as of now; LSNs win over times (a source reports one kind).
    fn lag_sample(&self) -> Sample {
        let pair = |head: &AtomicU64, delivered: &AtomicU64| {
            let (h, d) = (head.load(Relaxed), delivered.load(Relaxed));
            (h > 0 && d > 0).then_some((h.max(d), d))
        };
        let (kind, (head, delivered)) = match (
            pair(&self.head_lsn, &self.delivered_lsn),
            pair(&self.head_time_ms, &self.delivered_time_ms),
        ) {
            (Some(p), _) => (Some(Kind::Lsn), p),
            (None, Some(p)) => (Some(Kind::Time), p),
            (None, None) => (None, (0, 0)),
        };
        Sample {
            t_ms: self.started.elapsed().as_millis() as u64,
            kind,
            head,
            delivered,
            head_read_ms: Some(self.head_read_ms.load(Relaxed))
                .filter(|ms| *ms > 0)
                .map(|ms| ms - 1),
            snapshot: lock(&self.snapshot)
                .as_ref()
                .filter(|s| s.state == "running")
                .map(|s| (s.id.clone(), s.rows)),
        }
    }

    /// Records one reading of the source positions; call once a second. Keeps
    /// [`lag::LAG_HISTORY`] readings.
    pub fn sample_source_lag(&self) {
        let s = self.lag_sample();
        lag::push(&mut lock(&self.lag_ring), s);
    }

    /// Record the replication slot's lag; `None` clears it.
    /// A source with snapshots reports their progress here whenever it changes.
    pub fn set_snapshot(&self, status: Option<SnapshotStatus>) {
        *lock(&self.snapshot) = status;
    }

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
            Op::Read => &self.reads,
            // Unreachable: heartbeats return at the top; the arm keeps the match exhaustive.
            Op::Heartbeat => return,
        }
        .fetch_add(1, Relaxed);
        *lock(&self.last_position) = Some(ev.position.to_string());
        self.last_commit_ts_micros
            .store(ev.commit_ts_micros, Relaxed);
        let now = now_ms();
        self.last_event_unix_ms.store(now, Relaxed);
        let us = sink_time.as_micros() as u64;
        self.sink_us_total.fetch_add(us, Relaxed);
        self.sink_us_last.store(us, Relaxed);
        self.sink_us_max.fetch_max(us, Relaxed);
        let slot = match ev.op {
            Op::Insert => 0,
            Op::Update => 1,
            Op::Delete => 2,
            Op::Truncate => 3,
            Op::Read => 4,
            // Unreachable: heartbeats return at the top of `record_event`; the arm only
            // keeps the match exhaustive.
            Op::Heartbeat => return,
        };
        let mut t = lock(&self.tables);
        let e = t
            .entry(format!("{}.{}", ev.schema, ev.table))
            .or_insert_with(|| TableEntry {
                ops: [0; OP_SLOTS],
                schema: ev.schema.clone(),
                table: ev.table.clone(),
                last_lsn: None,
                last_commit_us: 0,
                last_delivered_ms: 0,
            });
        e.ops[slot] += 1;
        e.last_lsn = match ev.position {
            Position::Lsn(l) => Some(l),
            Position::Text(_) => None,
        };
        e.last_commit_us = ev.commit_ts_micros;
        e.last_delivered_ms = now;
    }

    /// The pipeline saved and acked `position`. Only its display form is kept: while a snapshot
    /// runs the checkpoint also holds the snapshot cursor (the last chunk's primary-key values),
    /// which must not reach the admin API or the cockpit (see [`Checkpoint::display`]).
    pub fn record_checkpoint(&self, position: &Position) {
        *lock(&self.checkpoint) = Some(display_checkpoint(position));
    }

    pub fn record_error(&self, msg: impl Into<String>) {
        self.errors.fetch_add(1, Relaxed);
        *lock(&self.last_error) = Some(msg.into());
    }

    pub fn snapshot(&self) -> Snapshot {
        // First: it takes the snapshot lock, which the struct literal below holds to its end.
        let source_lag = self.source_lag();
        let events = self.events.load(Relaxed);

        let ts = self.last_commit_ts_micros.load(Relaxed);
        let last_ms = self.last_event_unix_ms.load(Relaxed);
        let (mut tables, mut table_ops, mut table_marks) =
            (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
        for (name, e) in lock(&self.tables).iter() {
            tables.insert(name.clone(), e.ops.iter().sum());
            table_ops.insert(name.clone(), e.ops);
            table_marks.insert(
                name.clone(),
                TableMark {
                    schema: e.schema.clone(),
                    table: e.table.clone(),
                    last_lsn: e.last_lsn.map(|l| Position::Lsn(l).to_string()),
                    last_commit_ms: (e.last_commit_us > 0)
                        .then(|| e.last_commit_us.div_euclid(1000)),
                    last_delivered_ms: e.last_delivered_ms,
                },
            );
        }
        Snapshot {
            state: State::from_u8(self.state.load(Relaxed)),
            uptime_secs: self.started.elapsed().as_secs(),
            events_total: events,
            inserts: self.inserts.load(Relaxed),
            updates: self.updates.load(Relaxed),
            deletes: self.deletes.load(Relaxed),
            truncates: self.truncates.load(Relaxed),
            reads: self.reads.load(Relaxed),
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
            tables,
            table_ops,
            table_marks,
            checkpoint: lock(&self.checkpoint).clone(),
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
            snapshot: lock(&self.snapshot).clone(),
            source_head_lsn: (self.head_lsn.load(Relaxed) > 0)
                .then(|| Position::Lsn(self.head_lsn.load(Relaxed)).to_string()),
            source_head_time_ms: Some(self.head_time_ms.load(Relaxed)).filter(|ms| *ms > 0),
            source_lag,
        }
    }

    /// Derives [`SourceLag`] from the sampled window plus the current reading.
    fn source_lag(&self) -> SourceLag {
        let mut samples: Vec<Sample> = lock(&self.lag_ring).iter().cloned().collect();
        samples.push(self.lag_sample());
        let snapshot = lock(&self.snapshot).clone();
        let ts = self.last_commit_ts_micros.load(Relaxed);
        let limit = self.snapshot_limit.load(Relaxed);
        lag::derive(
            &samples,
            &lag::Context {
                running: State::from_u8(self.state.load(Relaxed)) == State::Streaming,
                last_commit_ms: (ts > 0).then(|| ts.div_euclid(1000)),
                now_ms: now_ms(),
                snapshot: snapshot.as_ref(),
                max_rows_per_sec: (limit > 0).then_some(limit),
            },
        )
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
            checkpoint: None,
            unchanged: Vec::new(),
            after_is_key: false,
        }
    }

    #[test]
    fn read_events_count_as_events_and_per_table_reads() {
        let m = Metrics::new();
        m.record_event(&ev(Op::Read, "a", 10), Duration::from_millis(1));
        m.record_event(&ev(Op::Read, "a", 11), Duration::from_millis(1));
        m.record_event(&ev(Op::Insert, "a", 12), Duration::from_millis(1));
        let s = m.snapshot();
        assert_eq!((s.events_total, s.reads, s.inserts), (3, 2, 1));
        assert_eq!(s.table_ops["public.a"], [1, 0, 0, 0, 2]);
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
        assert_eq!(s.table_ops["public.a"], [2, 0, 0, 0, 0]);
        assert_eq!(s.table_ops["public.b"], [0, 0, 1, 0, 0]);
        assert_eq!(s.last_lsn.as_deref(), Some("0/C"));
        assert!((s.sink_avg_ms - 4.0).abs() < 0.01);
        assert!((s.sink_max_ms - 6.0).abs() < 0.01);
        assert_eq!(s.last_error.as_deref(), Some("boom"));
    }

    #[test]
    fn table_marks_follow_the_newest_delivered_change() {
        let m = Metrics::new();
        assert!(m.snapshot().table_marks.is_empty());
        assert_eq!(m.snapshot().checkpoint, None);
        let mut a = ev(Op::Insert, "a", 0x1_0000_0010);
        a.commit_ts_micros = 1_700_000_000_123_456;
        m.record_event(&a, Duration::from_millis(1));
        m.record_event(&ev(Op::Update, "b", 0x20), Duration::from_millis(1));
        let mut mongo = ev(Op::Delete, "c", 0);
        mongo.schema = "shop".into();
        mongo.table = "or.ders".into();
        mongo.position = Position::Text("{\"_data\":\"82\"}".into());
        m.record_event(&mongo, Duration::from_millis(1));
        m.record_checkpoint(&Position::Lsn(0x20));
        let s = m.snapshot();
        let ma = &s.table_marks["public.a"];
        assert_eq!(ma.last_lsn.as_deref(), Some("1/10"));
        assert_eq!(ma.last_commit_ms, Some(1_700_000_000_123));
        assert!(ma.last_delivered_ms > 0);
        assert_eq!(s.table_marks["public.b"].last_lsn.as_deref(), Some("0/20"));
        // MongoDB has no LSN; the collection keeps its dots apart from the database.
        let mc = &s.table_marks["shop.or.ders"];
        assert_eq!((mc.schema.as_str(), mc.table.as_str()), ("shop", "or.ders"));
        assert_eq!(mc.last_lsn, None);
        assert_eq!(s.tables["shop.or.ders"], 1);
        assert_eq!(s.table_ops["shop.or.ders"], [0, 0, 1, 0, 0]);
        assert_eq!(s.checkpoint.as_deref(), Some("0/20"));
    }

    #[test]
    fn snapshot_cursor_values_never_reach_the_checkpoint_shown() {
        use crate::SnapState;
        let m = Metrics::new();
        // Postgres: K text with its origin, wrapped with snapshot progress.
        let pg = Checkpoint {
            pos: "16/B374D848@16/B374D8E0#2;db=5".into(),
            snap: Some(SnapState {
                id: "s1".into(),
                tables: vec!["public.users".into(), "public.orders".into()],
                table: 0,
                after: Some(vec!["ada@example.com".into(), "4111".into()]),
            }),
        };
        m.record_checkpoint(&Position::Text(pg.encode()));
        let shown = m.snapshot().checkpoint.unwrap();
        assert_eq!(
            shown,
            "16/B374D848@16/B374D8E0#2;db=5 (snapshot: public.users 1/2)"
        );
        assert!(
            !shown.contains("ada@") && !shown.contains("4111"),
            "{shown}"
        );
        // MongoDB: a resume token, the cursor an `_id` in extended JSON.
        let mongo = Checkpoint {
            pos: r#"{"_data":"8263"}"#.into(),
            snap: Some(SnapState {
                id: "s2".into(),
                tables: vec!["shop.customers".into()],
                table: 0,
                after: Some(vec![r#"{"$oid":"6530aa"}"#.into()]),
            }),
        };
        m.record_checkpoint(&Position::Text(mongo.encode()));
        let shown = m.snapshot().checkpoint.unwrap();
        assert_eq!(shown, r#"{"_data":"8263"} (snapshot: shop.customers 1/1)"#);
        assert!(!shown.contains("6530aa"), "{shown}");
        // Plain positions are shown as before, a resume token byte for byte.
        m.record_checkpoint(&Position::Text(r#"{"_data":"8263"}"#.into()));
        assert_eq!(
            m.snapshot().checkpoint.as_deref(),
            Some(r#"{"_data":"8263"}"#)
        );
        m.record_checkpoint(&Position::Lsn(0x20));
        assert_eq!(m.snapshot().checkpoint.as_deref(), Some("0/20"));
        // Snapshot-shaped but malformed: nothing of it is shown.
        let bad = r#"{"pos":"1","snap":{"after":["secret"]}}"#;
        assert_eq!(
            display_checkpoint(&Position::Text(bad.into())),
            "(unreadable checkpoint)"
        );
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
