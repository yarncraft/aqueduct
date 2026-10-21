//! "Behind source" (#150): how far the delivered position trails the source's head, the rates
//! on both sides, and when the backlog is gone.
//!
//! A source reports two raw positions to [`crate::Metrics`]: the head (the newest position the
//! server has; Postgres: the WAL head LSN, MongoDB: the newest cluster time) and the delivered
//! position (the newest position Aqueduct has delivered, or verified there is nothing left
//! below). Once a second the admin sampler pushes both into a ring of [`LAG_WINDOW`] samples;
//! everything else is derived from that ring here, so the memory is bounded.

use std::collections::VecDeque;

use serde::Serialize;

use crate::SnapshotStatus;

/// Samples kept, one per second: the window the rates and the sparkline cover.
pub const LAG_WINDOW: usize = 60;

/// Postgres is caught up while at most this much WAL is left. Never exactly 0: heartbeat
/// writes, `pg_current_wal_lsn` running ahead of the flushed WAL and the batch in flight leave a
/// few kB, and a busy cluster's WAL head moves between two reads.
pub const CAUGHT_UP_BYTES: u64 = 1024 * 1024;

/// MongoDB is caught up while the newest delivered cluster time is at most this far behind the
/// server's. Cluster times have second resolution, and an idle replica set only moves its
/// clock with the no-op writer (every 10 s), so less would flap.
pub const CAUGHT_UP_MS: u64 = 2_000;

/// A backlog with no delivered progress for this long, while the pipeline runs, is `stalled`.
pub const STALLED_AFTER_SECS: u64 = 30;

/// Rates need at least this much history; until then they are `null` and a backlog is `unknown`.
const MIN_RATE_MS: u64 = 2_000;

/// A head read longer ago than this is too old to compare with: the state is `unknown`. Sources
/// read it once a second independently of the pipeline loop, so only a source that cannot reach
/// its server (or a Postgres peek that holds the connection) gets here.
pub const HEAD_STALE_MS: u64 = 3_000;

/// A snapshot is `throttled` while it reads at least this share of `snapshot_max_rows_per_sec`.
const THROTTLED_SHARE: f64 = 0.9;

/// What the positions measure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// Postgres WAL LSNs: differences are bytes.
    Lsn,
    /// MongoDB cluster times in Unix ms: differences are milliseconds.
    Time,
}

/// One second's reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Sample {
    /// Milliseconds since the metrics were created (monotonic).
    pub t_ms: u64,
    /// `None` while the source has not reported both positions.
    pub kind: Option<Kind>,
    /// At least `delivered`: the server cannot be behind what was delivered from it.
    pub head: u64,
    pub delivered: u64,
    /// When the head was last read, in the same clock as `t_ms`; `None` before the first read.
    pub head_read_ms: Option<u64>,
    /// The running snapshot's id and rows copied so far.
    pub snapshot: Option<(String, u64)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LagState {
    /// The source has not reported its head or delivered position (yet), its head was last read
    /// more than [`HEAD_STALE_MS`] ago, or a backlog has less than two seconds of samples.
    Unknown,
    /// The backlog is within the threshold.
    CaughtUp,
    /// The backlog shrinks: delivery is faster than the source produces.
    CatchingUp,
    /// The backlog grows, or does not shrink.
    FallingBehind,
    /// A backlog, and nothing delivered for [`STALLED_AFTER_SECS`] while the pipeline runs.
    Stalled,
}

/// When the backlog counts as caught up, and when no progress counts as stalled.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LagThreshold {
    /// Postgres: caught up at or below this many bytes of WAL.
    pub backlog_bytes: Option<u64>,
    /// MongoDB: caught up at or below this many seconds behind the server's cluster time.
    pub behind_secs: Option<f64>,
    pub stalled_after_secs: u64,
}

/// The running snapshot's speed and time left.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SnapshotRate {
    /// Rows copied per second over the window; `null` until two seconds of samples.
    pub rows_per_sec: Option<f64>,
    /// Planner estimate of all tables minus the rows copied by this process; `null` when a table
    /// has no estimate. After a restart the rows copied before it are not subtracted. 0 while the
    /// snapshot still runs means the estimate was exceeded.
    pub rows_left: Option<u64>,
    /// `rows_left` covers every table of the snapshot.
    pub rows_left_scope: &'static str,
    pub eta_secs: Option<f64>,
    /// Reading at 90 % or more of `snapshot_max_rows_per_sec`: the limit sets the pace.
    pub throttled: bool,
    pub max_rows_per_sec: Option<u64>,
}

/// Whether Aqueduct is behind its source, see the module docs.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SourceLag {
    pub state: LagState,
    /// Postgres: WAL bytes between the server's head and the delivered position. `null` for
    /// MongoDB, whose positions are times.
    pub backlog_bytes: Option<u64>,
    /// Postgres: while there is a backlog, now minus the later of the newest delivered change's
    /// commit time and the moment the backlog crossed the threshold (when that is inside the
    /// window), so a burst after a quiet period does not count the quiet time; 0 when caught up.
    /// MongoDB: the server's cluster time minus the newest delivered.
    pub behind_secs: Option<f64>,
    /// How fast the source's head moves over the window, in `rate_unit`.
    pub ingest_rate: Option<f64>,
    /// How fast the delivered position moves over the window, in `rate_unit`.
    pub process_rate: Option<f64>,
    /// `bytes/s` (Postgres WAL) or `s/s` (MongoDB: seconds of source time per second).
    pub rate_unit: Option<&'static str>,
    /// Seconds until the backlog is gone at the current rates; 0 when caught up, `null` when it
    /// does not shrink.
    pub eta_secs: Option<f64>,
    /// Seconds of samples the rates cover.
    pub window_secs: f64,
    /// Seconds since the source's head was last read; `null` before the first read.
    pub head_age_secs: Option<f64>,
    pub threshold: LagThreshold,
    /// The backlog once a second over the window, oldest first, at most [`LAG_WINDOW`] values:
    /// bytes (Postgres) or ms (MongoDB).
    pub history: Vec<u64>,
    /// The running snapshot; `null` when none runs.
    pub snapshot: Option<SnapshotRate>,
}

/// Pushes `s` into `ring`, dropping the oldest beyond [`LAG_WINDOW`].
pub(crate) fn push(ring: &mut VecDeque<Sample>, s: Sample) {
    while ring.len() >= LAG_WINDOW {
        ring.pop_front();
    }
    ring.push_back(s);
}

/// Everything the derivation needs besides the samples.
pub(crate) struct Context<'a> {
    /// The pipeline is streaming (stalled needs it running).
    pub running: bool,
    /// Commit time of the newest delivered change, Unix ms.
    pub last_commit_ms: Option<i64>,
    pub now_ms: u64,
    pub snapshot: Option<&'a SnapshotStatus>,
    pub max_rows_per_sec: Option<u64>,
}

/// Derives the card from `samples` (oldest first; the last one is the current reading).
pub(crate) fn derive(samples: &[Sample], cx: &Context<'_>) -> SourceLag {
    let snapshot = snapshot_rate(samples, cx);
    let cur = samples.last();
    let kind = cur.and_then(|c| c.kind);
    let threshold = LagThreshold {
        backlog_bytes: (kind != Some(Kind::Time)).then_some(CAUGHT_UP_BYTES),
        behind_secs: (kind != Some(Kind::Lsn)).then_some(CAUGHT_UP_MS as f64 / 1000.0),
        stalled_after_secs: STALLED_AFTER_SECS,
    };
    let head_age_ms = cur.and_then(|c| Some(c.t_ms.saturating_sub(c.head_read_ms?)));
    let unknown = |snapshot| SourceLag {
        state: LagState::Unknown,
        backlog_bytes: None,
        behind_secs: None,
        ingest_rate: None,
        process_rate: None,
        rate_unit: None,
        eta_secs: None,
        window_secs: 0.0,
        head_age_secs: head_age_ms.map(|a| a as f64 / 1000.0),
        threshold: threshold.clone(),
        history: Vec::new(),
        snapshot,
    };
    let (Some(cur), Some(kind)) = (cur, kind) else {
        return unknown(snapshot);
    };
    // A head that was not read for a while says nothing about now: the server may have moved on.
    if head_age_ms.is_none_or(|a| a > HEAD_STALE_MS) {
        return unknown(snapshot);
    }
    // Only samples of the current kind (a source never changes kind, but be safe).
    let win: Vec<&Sample> = samples.iter().filter(|s| s.kind == Some(kind)).collect();
    let backlog = |s: &Sample| s.head.saturating_sub(s.delivered);
    let first = win.first().copied().unwrap_or(cur);
    let dt_ms = cur.t_ms.saturating_sub(first.t_ms);
    // Raw units per second: bytes/s, or ms/s for times.
    let rates = (dt_ms >= MIN_RATE_MS).then(|| {
        let dt = dt_ms as f64 / 1000.0;
        (
            cur.head.saturating_sub(first.head) as f64 / dt,
            cur.delivered.saturating_sub(first.delivered) as f64 / dt,
        )
    });
    let b = backlog(cur);
    let caught_up_at = match kind {
        Kind::Lsn => CAUGHT_UP_BYTES,
        Kind::Time => CAUGHT_UP_MS,
    };
    // The current run of samples over the threshold: when it began, and whether it began inside
    // the window (otherwise its start is not known).
    let over: Vec<&&Sample> = win
        .iter()
        .rev()
        .take_while(|s| backlog(s) > caught_up_at)
        .collect();
    let over_since = over.last().map_or(cur.t_ms, |s| s.t_ms);
    let over_start_known = over.len() < win.len();
    // How long nothing was delivered while over the threshold: quiet time before the backlog
    // appeared does not count.
    let unchanged_since = win
        .iter()
        .rev()
        .take_while(|s| s.delivered == cur.delivered)
        .last()
        .map_or(cur.t_ms, |s| s.t_ms);
    let idle_ms = cur.t_ms.saturating_sub(unchanged_since.max(over_since));
    let state = if b <= caught_up_at {
        LagState::CaughtUp
    } else if cx.running && idle_ms >= STALLED_AFTER_SECS * 1000 {
        LagState::Stalled
    } else {
        match rates {
            Some((ingest, process)) if process > ingest => LagState::CatchingUp,
            Some(_) => LagState::FallingBehind,
            None => LagState::Unknown,
        }
    };
    let eta_secs = match (state, rates) {
        (LagState::CaughtUp, _) => Some(0.0),
        (LagState::CatchingUp, Some((ingest, process))) => Some(b as f64 / (process - ingest)),
        _ => None,
    };
    let behind_secs = match kind {
        Kind::Lsn if state == LagState::CaughtUp => Some(0.0),
        Kind::Lsn => {
            // Wall-clock time the backlog crossed the threshold, when that happened in the window.
            let crossed = over_start_known.then(|| {
                cx.now_ms
                    .saturating_sub(cur.t_ms.saturating_sub(over_since)) as i64
            });
            let from = match (cx.last_commit_ms, crossed) {
                (Some(c), Some(x)) => Some(c.max(x)),
                (c, x) => c.or(x),
            };
            from.map(|f| (cx.now_ms as i64 - f).max(0) as f64 / 1000.0)
        }
        Kind::Time => Some(b as f64 / 1000.0),
    };
    let (unit, scale) = match kind {
        Kind::Lsn => ("bytes/s", 1.0),
        Kind::Time => ("s/s", 1000.0),
    };
    let skip = win.len().saturating_sub(LAG_WINDOW);
    SourceLag {
        state,
        backlog_bytes: (kind == Kind::Lsn).then_some(b),
        behind_secs,
        ingest_rate: rates.map(|r| r.0 / scale),
        process_rate: rates.map(|r| r.1 / scale),
        rate_unit: Some(unit),
        eta_secs,
        window_secs: dt_ms as f64 / 1000.0,
        head_age_secs: head_age_ms.map(|a| a as f64 / 1000.0),
        threshold,
        history: win.iter().skip(skip).map(|s| backlog(s)).collect(),
        snapshot,
    }
}

/// Speed and time left of the running snapshot, from the samples that carry its id.
fn snapshot_rate(samples: &[Sample], cx: &Context<'_>) -> Option<SnapshotRate> {
    let st = cx.snapshot.filter(|s| s.state == "running")?;
    let points: Vec<(u64, u64)> = samples
        .iter()
        .filter_map(|s| match &s.snapshot {
            Some((id, rows)) if *id == st.id => Some((s.t_ms, *rows)),
            _ => None,
        })
        .collect();
    let rows_per_sec = match (points.first(), points.last()) {
        (Some(a), Some(z)) if z.0.saturating_sub(a.0) >= MIN_RATE_MS => {
            Some(z.1.saturating_sub(a.1) as f64 / ((z.0 - a.0) as f64 / 1000.0))
        }
        _ => None,
    };
    let rows_left = st.rows_estimated.map(|e| e.saturating_sub(st.rows));
    // At 0 rows left the estimate was too low (it is only an estimate) and the snapshot runs on:
    // no ETA rather than a wrong 0.
    let eta_secs = match (rows_left, rows_per_sec) {
        (Some(left), Some(r)) if left > 0 && r > 0.0 => Some(left as f64 / r),
        _ => None,
    };
    let throttled = match (cx.max_rows_per_sec, rows_per_sec) {
        (Some(limit), Some(r)) => r >= limit as f64 * THROTTLED_SHARE,
        _ => false,
    };
    Some(SnapshotRate {
        rows_per_sec,
        rows_left,
        rows_left_scope: "all_tables",
        eta_secs,
        throttled,
        max_rows_per_sec: cx.max_rows_per_sec,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn cx() -> Context<'static> {
        Context {
            running: true,
            last_commit_ms: Some(1_000_000),
            now_ms: 1_030_000,
            snapshot: None,
            max_rows_per_sec: None,
        }
    }

    /// One sample a second, `head(i)` and `delivered(i)` for second `i`.
    fn series(
        n: u64,
        kind: Kind,
        head: impl Fn(u64) -> u64,
        delivered: impl Fn(u64) -> u64,
    ) -> Vec<Sample> {
        (0..n)
            .map(|i| {
                let d = delivered(i);
                Sample {
                    t_ms: i * 1000,
                    kind: Some(kind),
                    head: head(i).max(d),
                    delivered: d,
                    head_read_ms: Some(i * 1000),
                    snapshot: None,
                }
            })
            .collect()
    }

    #[test]
    fn unknown_until_both_positions_are_reported() {
        let lag = derive(&[], &cx());
        assert_eq!(lag.state, LagState::Unknown);
        let s = Sample {
            t_ms: 0,
            kind: None,
            head: 0,
            delivered: 0,
            head_read_ms: None,
            snapshot: None,
        };
        let lag = derive(&[s], &cx());
        assert_eq!(lag.state, LagState::Unknown);
        assert_eq!((lag.backlog_bytes, lag.eta_secs), (None, None));
        assert!(lag.history.is_empty());
    }

    #[test]
    fn heartbeat_noise_is_caught_up() {
        // The head moves by a few kB of heartbeat WAL; delivery jumps to it every 10 s.
        let s = series(
            60,
            Kind::Lsn,
            |i| 1_000_000 + i * 4096,
            |i| 1_000_000 + (i / 10) * 40960,
        );
        let lag = derive(&s, &cx());
        assert_eq!(lag.state, LagState::CaughtUp);
        assert_eq!(lag.behind_secs, Some(0.0));
        assert_eq!(lag.eta_secs, Some(0.0));
        assert!(lag.backlog_bytes.unwrap() <= CAUGHT_UP_BYTES);
        assert_eq!(lag.rate_unit, Some("bytes/s"));
        assert_eq!(lag.history.len(), 60);
    }

    #[test]
    fn catching_up_with_eta() {
        // 10 MiB behind at the end; WAL grows 1 MiB/s, delivery runs 3 MiB/s.
        let s = series(11, Kind::Lsn, |i| 30 * MIB + i * MIB, |i| i * 3 * MIB);
        let lag = derive(&s, &cx());
        assert_eq!(lag.state, LagState::CatchingUp);
        assert_eq!(lag.backlog_bytes, Some(10 * MIB));
        let (ingest, process) = (lag.ingest_rate.unwrap(), lag.process_rate.unwrap());
        assert!((ingest - MIB as f64).abs() < 1.0, "{ingest}");
        assert!((process - 3.0 * MIB as f64).abs() < 1.0, "{process}");
        // 10 MiB at a net 2 MiB/s.
        assert!((lag.eta_secs.unwrap() - 5.0).abs() < 1e-9);
        // Behind: now minus the newest delivered commit.
        assert_eq!(lag.behind_secs, Some(30.0));
        assert!((lag.window_secs - 10.0).abs() < 1e-9);
    }

    #[test]
    fn falling_behind_has_no_eta() {
        let s = series(11, Kind::Lsn, |i| 2 * MIB + i * 3 * MIB, |i| i * MIB);
        let lag = derive(&s, &cx());
        assert_eq!(lag.state, LagState::FallingBehind);
        assert_eq!(lag.eta_secs, None);
        assert!(lag.ingest_rate.unwrap() > lag.process_rate.unwrap());
    }

    #[test]
    fn stalled_only_while_running_and_after_the_quiet_period() {
        let s = series(40, Kind::Lsn, |i| 10 * MIB + i * MIB, |_| MIB);
        assert_eq!(derive(&s, &cx()).state, LagState::Stalled);
        assert_eq!(derive(&s, &cx()).eta_secs, None);
        let stopped = Context {
            running: false,
            ..cx()
        };
        assert_eq!(derive(&s, &stopped).state, LagState::FallingBehind);
        // 20 s without progress is not stalled yet.
        assert_eq!(derive(&s[..21], &cx()).state, LagState::FallingBehind);
        // Progress in the last seconds ends the stall.
        let moving = series(
            40,
            Kind::Lsn,
            |_| 100 * MIB,
            |i| if i < 38 { MIB } else { 2 * MIB },
        );
        assert_ne!(derive(&moving, &cx()).state, LagState::Stalled);
    }

    #[test]
    fn rates_need_two_seconds_of_samples() {
        let s = series(2, Kind::Lsn, |_| 50 * MIB, |i| i * MIB);
        let lag = derive(&s, &cx());
        assert_eq!((lag.ingest_rate, lag.process_rate), (None, None));
        assert_eq!(lag.state, LagState::Unknown);
        assert_eq!(lag.eta_secs, None);
    }

    #[test]
    fn mongodb_times_are_seconds_behind_without_bytes() {
        // The cluster clock runs 1 s/s; delivery covers 4 s of it per second, 20 s behind.
        let s = series(
            11,
            Kind::Time,
            |i| 100_000 + i * 1000,
            |i| 50_000 + i * 4000,
        );
        let lag = derive(&s, &cx());
        assert_eq!(lag.state, LagState::CatchingUp);
        assert_eq!(lag.backlog_bytes, None);
        assert_eq!(lag.behind_secs, Some(20.0));
        assert_eq!(lag.rate_unit, Some("s/s"));
        assert!((lag.ingest_rate.unwrap() - 1.0).abs() < 1e-9);
        assert!((lag.process_rate.unwrap() - 4.0).abs() < 1e-9);
        // 20 s at a net 3 s/s.
        assert!((lag.eta_secs.unwrap() - 20.0 / 3.0).abs() < 1e-9);
        assert_eq!(lag.threshold.backlog_bytes, None);
        assert_eq!(lag.threshold.behind_secs, Some(2.0));
        // Idle within two seconds is caught up.
        let idle = series(5, Kind::Time, |i| 100_000 + i * 1000, |i| 99_000 + i * 1000);
        assert_eq!(derive(&idle, &cx()).state, LagState::CaughtUp);
    }

    #[test]
    fn the_ring_keeps_one_window() {
        let mut ring = VecDeque::new();
        for i in 0..(LAG_WINDOW as u64 * 3) {
            push(
                &mut ring,
                Sample {
                    t_ms: i * 1000,
                    kind: Some(Kind::Lsn),
                    head: i,
                    delivered: i,
                    head_read_ms: Some(i * 1000),
                    snapshot: None,
                },
            );
        }
        assert_eq!(ring.len(), LAG_WINDOW);
        assert_eq!(ring.front().unwrap().t_ms, (LAG_WINDOW as u64 * 2) * 1000);
        // The ring plus the current reading: the sparkline still has one window.
        let mut samples: Vec<Sample> = ring.into_iter().collect();
        let mut now = samples.last().unwrap().clone();
        now.t_ms += 500;
        samples.push(now);
        assert_eq!(derive(&samples, &cx()).history.len(), LAG_WINDOW);
    }

    fn running(rows: u64, estimated: Option<u64>) -> SnapshotStatus {
        SnapshotStatus {
            id: "s1".into(),
            state: "running".into(),
            tables: vec!["public.a".into()],
            tables_done: 0,
            table: Some("public.a".into()),
            rows,
            rows_estimated: estimated,
            skipped: vec![],
        }
    }

    fn snap_series(rows: impl Fn(u64) -> u64, n: u64) -> Vec<Sample> {
        (0..n)
            .map(|i| Sample {
                t_ms: i * 1000,
                kind: Some(Kind::Lsn),
                head: 0,
                delivered: 0,
                head_read_ms: Some(i * 1000),
                snapshot: Some(("s1".into(), rows(i))),
            })
            .collect()
    }

    #[test]
    fn snapshot_rate_rows_left_and_eta() {
        let s = snap_series(|i| i * 500, 11);
        let st = running(5_000, Some(20_000));
        let c = Context {
            snapshot: Some(&st),
            ..cx()
        };
        let r = derive(&s, &c).snapshot.unwrap();
        assert!((r.rows_per_sec.unwrap() - 500.0).abs() < 1e-9);
        assert_eq!(r.rows_left, Some(15_000));
        assert_eq!(r.rows_left_scope, "all_tables");
        assert!((r.eta_secs.unwrap() - 30.0).abs() < 1e-9);
        assert!(!r.throttled);
        // Without an estimate there is nothing to count down.
        let st = running(5_000, None);
        let c = Context {
            snapshot: Some(&st),
            ..cx()
        };
        let r = derive(&s, &c).snapshot.unwrap();
        assert_eq!((r.rows_left, r.eta_secs), (None, None));
        // Past the estimate the snapshot runs on: no ETA rather than 0.
        let st = running(25_000, Some(20_000));
        let c = Context {
            snapshot: Some(&st),
            ..cx()
        };
        let r = derive(&s, &c).snapshot.unwrap();
        assert_eq!((r.rows_left, r.eta_secs), (Some(0), None));
        // Samples of another snapshot do not count.
        let mut other = s.clone();
        for x in &mut other {
            x.snapshot = Some(("s0".into(), 9));
        }
        let st = running(5_000, Some(20_000));
        let c = Context {
            snapshot: Some(&st),
            ..cx()
        };
        assert_eq!(derive(&other, &c).snapshot.unwrap().rows_per_sec, None);
    }

    #[test]
    fn snapshot_at_its_limit_is_throttled() {
        let s = snap_series(|i| i * 480, 11);
        let st = running(4_800, Some(100_000));
        let limited = |limit| Context {
            snapshot: Some(&st),
            max_rows_per_sec: Some(limit),
            ..cx()
        };
        assert!(derive(&s, &limited(500)).snapshot.unwrap().throttled);
        assert!(!derive(&s, &limited(5_000)).snapshot.unwrap().throttled);
        assert_eq!(
            derive(&s, &limited(500)).snapshot.unwrap().max_rows_per_sec,
            Some(500)
        );
    }

    #[test]
    fn no_snapshot_rate_unless_one_runs() {
        let s = snap_series(|i| i, 5);
        let mut st = running(5, Some(10));
        st.state = "finished".into();
        let c = Context {
            snapshot: Some(&st),
            ..cx()
        };
        assert_eq!(derive(&s, &c).snapshot, None);
        assert_eq!(derive(&s, &cx()).snapshot, None);
    }

    /// The pipeline (and its polls) can stop while WAL piles up; a head that is no longer read
    /// must not keep the card at caught up.
    #[test]
    fn a_head_not_read_for_a_while_is_unknown_not_caught_up() {
        let mut s = series(30, Kind::Lsn, |_| 5 * MIB, |_| 5 * MIB);
        assert_eq!(derive(&s, &cx()).state, LagState::CaughtUp);
        // Head and delivered frozen since second 10: the last read is 19 s old.
        for x in &mut s[10..] {
            x.head_read_ms = Some(10_000);
        }
        let lag = derive(&s, &cx());
        assert_eq!(lag.state, LagState::Unknown);
        assert_eq!(lag.backlog_bytes, None);
        assert_eq!(lag.head_age_secs, Some(19.0));
        // Never read at all.
        for x in &mut s {
            x.head_read_ms = None;
        }
        assert_eq!(derive(&s, &cx()).state, LagState::Unknown);
    }

    /// A long quiet period (nothing delivered because nothing happened), then a burst: the
    /// quiet time is not "no progress".
    #[test]
    fn a_burst_after_a_quiet_minute_is_not_stalled() {
        let s = series(
            62,
            Kind::Lsn,
            |i| if i < 60 { 10 * MIB } else { 12 * MIB },
            |_| 10 * MIB,
        );
        let lag = derive(&s, &cx());
        assert_ne!(lag.state, LagState::Stalled, "{lag:?}");
        assert_eq!(lag.backlog_bytes, Some(2 * MIB));
        // The last commit was an hour ago, but the backlog is 2 s old: that is the time behind.
        let quiet = Context {
            last_commit_ms: Some(1_030_000 - 3_600_000),
            ..cx()
        };
        assert_eq!(derive(&s, &quiet).behind_secs, Some(1.0));
        // 30 s over the threshold without progress still is.
        let s = series(
            95,
            Kind::Lsn,
            |i| if i < 60 { 10 * MIB } else { 12 * MIB },
            |_| 10 * MIB,
        );
        assert_eq!(derive(&s, &cx()).state, LagState::Stalled);
    }
}
