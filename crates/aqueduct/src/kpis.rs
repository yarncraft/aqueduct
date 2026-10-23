//! The day behind the cockpit's top tiles: the per-second samples folded into one mean per
//! wall-clock minute, for each tile, over the last [`DAY_MINUTES`] minutes.
//!
//! - `throughput`: events delivered per second.
//! - `delivered`: the events-delivered counter.
//! - `lag_ms`: commit to delivery of the newest event, over the seconds the tile shows a lag
//!   (not while it reads "idle": caught up with nothing delivered for [`IDLE_AFTER_MS`]).
//! - `sink_ms`: sink time per event delivered in that minute.
//! - `errors`: the error counter.
//!
//! A minute without a value (no lag shown, nothing delivered) is `null`. Bounded: at most
//! [`DAY_MINUTES`] minutes of five numbers, the current one included.

use std::collections::VecDeque;

use aqueduct_core::{LagState, Snapshot};
use serde_json::{json, Value};

/// A day of minutes, the one in progress included.
pub const DAY_MINUTES: usize = 24 * 60;
const MINUTE_MS: u64 = 60_000;
/// Same as the page's `IDLE_AFTER_MS`: the lag tile reads "idle" past this.
const IDLE_AFTER_MS: u64 = 10_000;

/// One closed minute: `[throughput, delivered, lag_ms, sink_ms, errors]`.
type Minute = [Option<f64>; 5];

/// The minute being sampled.
#[derive(Default)]
struct Acc {
    /// Unix minute (ms / 60 000).
    minute: u64,
    samples: u32,
    rate: f64,
    delivered: f64,
    errors: f64,
    lag: f64,
    lag_samples: u32,
    /// Events and total sink ms at the end of the previous minute.
    base: (u64, f64),
    /// Events and total sink ms at the newest sample.
    last: (u64, f64),
}

impl Acc {
    fn close(&self) -> Minute {
        let n = f64::from(self.samples.max(1));
        let events = self.last.0.saturating_sub(self.base.0);
        [
            Some(self.rate / n),
            Some(self.delivered / n),
            (self.lag_samples > 0).then(|| self.lag / f64::from(self.lag_samples)),
            (events > 0).then(|| ((self.last.1 - self.base.1) / events as f64).max(0.0)),
            Some(self.errors / n),
        ]
    }
}

pub struct KpiHistory {
    len: usize,
    /// Closed minutes, oldest first; the newest is the minute before `cur`.
    closed: VecDeque<Minute>,
    cur: Option<Acc>,
}

impl KpiHistory {
    pub fn new(len: usize) -> Self {
        Self {
            len: len.max(1),
            closed: VecDeque::with_capacity(len),
            cur: None,
        }
    }

    /// Adds one second at `now_ms` (Unix): `rate` events/s and the tiles' values from `s`.
    pub fn record(&mut self, now_ms: u64, rate: f64, s: &Snapshot) {
        let minute = now_ms / MINUTE_MS;
        let totals = (s.events_total, s.sink_avg_ms * s.events_total as f64);
        // A new minute closes the current one. A clock that stepped back keeps folding into it.
        let mut c = match self.cur.take() {
            Some(c) if minute <= c.minute => c,
            prev => {
                let base = prev.as_ref().map_or(totals, |c| c.last);
                if let Some(c) = prev {
                    self.push(c.close());
                    // The sampler did not run for whole minutes (a suspended host): leave them empty.
                    for _ in 0..(minute - c.minute - 1).min(self.len as u64) {
                        self.push([None; 5]);
                    }
                }
                Acc {
                    minute,
                    base,
                    ..Acc::default()
                }
            }
        };
        c.samples += 1;
        c.rate += rate;
        c.delivered += s.events_total as f64;
        c.errors += s.errors as f64;
        let idle = s.source_lag.state == LagState::CaughtUp
            && s.last_event_unix_ms
                .is_none_or(|t| now_ms.saturating_sub(t) > IDLE_AFTER_MS);
        if let (Some(lag), false) = (s.lag_ms, idle) {
            c.lag += lag.max(0) as f64;
            c.lag_samples += 1;
        }
        c.last = totals;
        self.cur = Some(c);
    }

    fn push(&mut self, m: Minute) {
        // The current minute takes the last slot of the window.
        while self.closed.len() + 1 >= self.len {
            self.closed.pop_front();
        }
        self.closed.push_back(m);
    }

    /// The window for `/api/kpis/history`: one series per tile, oldest first, each ending with
    /// the minute in progress; `start_ms` is when the first one began.
    pub fn window(&self) -> Value {
        let mut rows: Vec<Minute> = self.closed.iter().copied().collect();
        if let Some(c) = &self.cur {
            rows.push(c.close());
        }
        let start_ms = self
            .cur
            .as_ref()
            .map(|c| (c.minute - (rows.len() as u64 - 1)) * MINUTE_MS);
        let col = |k: usize| rows.iter().map(|r| r[k]).collect::<Vec<_>>();
        json!({
            "interval_ms": MINUTE_MS,
            "len": self.len,
            "start_ms": start_ms,
            "throughput": col(0),
            "delivered": col(1),
            "lag_ms": col(2),
            "sink_ms": col(3),
            "errors": col(4),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aqueduct_core::Metrics;

    const T0: u64 = 1_800_000_000_000; // a minute boundary

    fn snap(events: u64, sink_avg_ms: f64, errors: u64, lag_ms: Option<i64>) -> Snapshot {
        let mut s = Metrics::new().snapshot();
        s.events_total = events;
        s.sink_avg_ms = sink_avg_ms;
        s.errors = errors;
        s.lag_ms = lag_ms;
        s
    }

    #[test]
    fn folds_seconds_into_minute_means() {
        let mut h = KpiHistory::new(DAY_MINUTES);
        // Minute 0: 10 events at 2 ms, then 30 more at 4 ms (total 10*2 + 30*4 = 140 ms over 40).
        h.record(T0, 10.0, &snap(10, 2.0, 0, Some(100)));
        h.record(T0 + 1_000, 30.0, &snap(40, 3.5, 1, Some(300)));
        // Minute 1: nothing new delivered, no lag shown.
        h.record(T0 + MINUTE_MS, 0.0, &snap(40, 3.5, 1, None));
        let w = h.window();
        assert_eq!(w["start_ms"], T0);
        assert_eq!(w["throughput"], json!([20.0, 0.0]));
        assert_eq!(w["delivered"], json!([25.0, 40.0]));
        assert_eq!(w["lag_ms"], json!([200.0, null]));
        assert_eq!(w["errors"], json!([0.5, 1.0]));
        // The first sample is the base: only the 30 events after it count, at (140 - 20) / 30.
        assert_eq!(w["sink_ms"], json!([4.0, null]));
    }

    #[test]
    fn idle_seconds_do_not_count_toward_the_lag() {
        let mut h = KpiHistory::new(DAY_MINUTES);
        let mut s = snap(5, 1.0, 0, Some(50_000));
        s.source_lag.state = LagState::CaughtUp;
        s.last_event_unix_ms = Some(T0 - 20_000);
        h.record(T0, 0.0, &s);
        s.last_event_unix_ms = Some(T0);
        s.lag_ms = Some(80);
        h.record(T0 + 1_000, 5.0, &s);
        assert_eq!(h.window()["lag_ms"], json!([80.0]));
    }

    #[test]
    fn keeps_a_day_and_leaves_skipped_minutes_empty() {
        let mut h = KpiHistory::new(3);
        h.record(T0, 1.0, &snap(1, 0.0, 0, None));
        h.record(T0 + 2 * MINUTE_MS, 3.0, &snap(3, 0.0, 0, None));
        assert_eq!(h.window()["throughput"], json!([1.0, null, 3.0]));
        h.record(T0 + 3 * MINUTE_MS, 4.0, &snap(4, 0.0, 0, None));
        let w = h.window();
        assert_eq!(w["throughput"], json!([null, 3.0, 4.0]));
        assert_eq!(w["start_ms"], T0 + MINUTE_MS);
        // A long outage cannot grow the window past its length.
        h.record(T0 + 10_000 * MINUTE_MS, 5.0, &snap(5, 0.0, 0, None));
        assert_eq!(h.window()["throughput"], json!([null, null, 5.0]));
    }
}
