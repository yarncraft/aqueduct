//! The window behind the cockpit's top tiles: one sample per second for each tile, over the same
//! window as the routing cards (the admin server's `HISTORY_LEN`), not averaged.
//!
//! - `throughput`: events delivered in that second.
//! - `kafka_mb_per_sec`: key, value and header MB (10^6 bytes) Kafka acknowledged in that second.
//! - `lag_ms`: commit to delivery of the newest event, while the tile shows a lag (not while it
//!   reads "idle": caught up with nothing delivered for [`IDLE_AFTER_MS`]).
//! - `sink_ms`: sink time per event delivered in that second.
//! - `errors`: the error counter.
//!
//! A second without a value (no lag shown, nothing delivered) is `null`. Bounded: at most `len`
//! samples of five numbers.

use std::collections::VecDeque;

use aqueduct_core::{LagState, Snapshot};
use serde_json::{json, Value};

const SECOND_MS: u64 = 1_000;
/// Same as the page's `IDLE_AFTER_MS`: the lag tile reads "idle" past this.
const IDLE_AFTER_MS: u64 = 10_000;

/// One second: `[throughput, kafka_mb_per_sec, lag_ms, sink_ms, errors]`.
type Sample = [Option<f64>; 5];

pub struct KpiHistory {
    len: usize,
    /// Oldest first.
    samples: VecDeque<Sample>,
    /// Events and total sink ms at the previous sample.
    last: Option<(u64, f64)>,
}

impl KpiHistory {
    pub fn new(len: usize) -> Self {
        Self {
            len: len.max(1),
            samples: VecDeque::with_capacity(len),
            last: None,
        }
    }

    /// Adds one second at `now_ms` (Unix): `rate` events/s, `bytes_rate` bytes/s acknowledged by
    /// Kafka, and the tiles' values from `s`.
    pub fn record(&mut self, now_ms: u64, rate: f64, bytes_rate: f64, s: &Snapshot) {
        let totals = (s.events_total, s.sink_avg_ms * s.events_total as f64);
        // The first sample is the base: its sink time per event is not known yet.
        let (events, sink) = self.last.map_or((0, 0.0), |(e, ms)| {
            (totals.0.saturating_sub(e), totals.1 - ms)
        });
        self.last = Some(totals);
        let idle = s.source_lag.state == LagState::CaughtUp
            && s.last_event_unix_ms
                .is_none_or(|t| now_ms.saturating_sub(t) > IDLE_AFTER_MS);
        let sample = [
            Some(rate),
            Some(bytes_rate / 1e6),
            s.lag_ms.filter(|_| !idle).map(|l| l.max(0) as f64),
            (events > 0).then(|| (sink / events as f64).max(0.0)),
            Some(s.errors as f64),
        ];
        if self.samples.len() == self.len {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }

    /// The window for `/api/kpis/history` and every stream frame: one series per tile, oldest
    /// first, ending with the newest second.
    pub fn window(&self) -> Value {
        let col = |k: usize| self.samples.iter().map(|r| r[k]).collect::<Vec<_>>();
        json!({
            "interval_ms": SECOND_MS,
            "len": self.len,
            "throughput": col(0),
            "kafka_mb_per_sec": col(1),
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

    const T0: u64 = 1_800_000_000_000;

    fn snap(events: u64, sink_avg_ms: f64, errors: u64, lag_ms: Option<i64>) -> Snapshot {
        let mut s = Metrics::new().snapshot();
        s.events_total = events;
        s.sink_avg_ms = sink_avg_ms;
        s.errors = errors;
        s.lag_ms = lag_ms;
        s
    }

    #[test]
    fn keeps_each_second_as_it_was() {
        let mut h = KpiHistory::new(120);
        h.record(T0, 10.0, 1_500_000.0, &snap(10, 2.0, 0, Some(100)));
        // 30 more events; total sink time 10*2 + 30*4 = 140 ms, so 120 ms over these 30.
        h.record(T0 + 1_000, 30.0, 500_000.0, &snap(40, 3.5, 1, Some(300)));
        // Nothing new delivered, no lag shown.
        h.record(T0 + 2_000, 0.0, 0.0, &snap(40, 3.5, 1, None));
        let w = h.window();
        assert_eq!(w["interval_ms"], 1_000);
        assert_eq!(w["throughput"], json!([10.0, 30.0, 0.0]));
        assert_eq!(w["kafka_mb_per_sec"], json!([1.5, 0.5, 0.0]));
        assert_eq!(w["lag_ms"], json!([100.0, 300.0, null]));
        assert_eq!(w["sink_ms"], json!([null, 4.0, null]));
        assert_eq!(w["errors"], json!([0.0, 1.0, 1.0]));
    }

    #[test]
    fn idle_seconds_show_no_lag() {
        let mut h = KpiHistory::new(120);
        let mut s = snap(5, 1.0, 0, Some(50_000));
        s.source_lag.state = LagState::CaughtUp;
        s.last_event_unix_ms = Some(T0 - 20_000);
        h.record(T0, 0.0, 0.0, &s);
        s.last_event_unix_ms = Some(T0);
        s.lag_ms = Some(80);
        h.record(T0 + 1_000, 5.0, 0.0, &s);
        assert_eq!(h.window()["lag_ms"], json!([null, 80.0]));
    }

    #[test]
    fn keeps_only_the_window() {
        let mut h = KpiHistory::new(3);
        for i in 0..10u64 {
            h.record(T0 + i * 1_000, i as f64, 0.0, &snap(i, 0.0, 0, None));
        }
        let w = h.window();
        assert_eq!(w["len"], 3);
        assert_eq!(w["throughput"], json!([7.0, 8.0, 9.0]));
    }
}
