//! The window behind the cockpit's top tiles: one sample per second for each tile, over the same
//! window as the routing cards (the admin server's `HISTORY_LEN`), not averaged.
//!
//! - `throughput`: events delivered in that second.
//! - `kafka_mb_per_sec`: key, value and header MB (10^6 bytes) Kafka acknowledged in that second.
//! - `lag_ms`: commit to delivery of the newest event, while the tile shows a lag (not while it
//!   reads "idle": caught up with nothing delivered for [`IDLE_AFTER_MS`]).
//! - `cpu_pct`: CPU time the process used in that second, user and system, all threads, as a
//!   percentage of one core (200 = two cores busy).
//! - `mem_mb`: the process's resident memory (RSS) in MB (10^6 bytes) at that second.
//!
//! A second without a value (no lag shown, no process figures on this platform) is `null`.
//! Bounded: at most `len` samples of five numbers.

use std::collections::VecDeque;

use aqueduct_core::{LagState, Snapshot};
use serde_json::{json, Value};

const SECOND_MS: u64 = 1_000;
/// Same as the page's `IDLE_AFTER_MS`: the lag tile reads "idle" past this.
const IDLE_AFTER_MS: u64 = 10_000;

/// One second: `[throughput, kafka_mb_per_sec, lag_ms, cpu_pct, mem_mb]`.
type Sample = [Option<f64>; 5];

/// What the process has used so far: CPU seconds since start and resident bytes now.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Usage {
    pub cpu_secs: f64,
    pub rss_bytes: u64,
}

/// This process's [`Usage`], from `/proc/self`. `None` where there is no procfs (macOS, Windows).
pub fn process_usage() -> Option<Usage> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_usage(&stat, &status)
}

/// `utime` and `stime` (fields 14 and 15 of `/proc/self/stat`, in clock ticks of 1/100 s: the
/// kernel's `USER_HZ`, fixed at 100 for userspace) and `VmRSS` (kB) from `/proc/self/status`.
fn parse_usage(stat: &str, status: &str) -> Option<Usage> {
    // The command name (field 2) is in parentheses and may contain spaces: count from after it.
    let mut rest = stat.get(stat.rfind(')')? + 1..)?.split_whitespace();
    let utime: u64 = rest.nth(11)?.parse().ok()?;
    let stime: u64 = rest.next()?.parse().ok()?;
    let rss_kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some(Usage {
        cpu_secs: (utime + stime) as f64 / 100.0,
        rss_bytes: rss_kb * 1024,
    })
}

pub struct KpiHistory {
    len: usize,
    /// Oldest first.
    samples: VecDeque<Sample>,
    /// Time (Unix ms) and CPU seconds at the previous sample.
    last_cpu: Option<(u64, f64)>,
}

impl KpiHistory {
    pub fn new(len: usize) -> Self {
        Self {
            len: len.max(1),
            samples: VecDeque::with_capacity(len),
            last_cpu: None,
        }
    }

    /// Adds one second at `now_ms` (Unix): `rate` events/s, `bytes_rate` bytes/s acknowledged by
    /// Kafka, the lag from `s`, and the process's `usage` ([`process_usage`]).
    pub fn record(
        &mut self,
        now_ms: u64,
        rate: f64,
        bytes_rate: f64,
        s: &Snapshot,
        usage: Option<Usage>,
    ) {
        let idle = s.source_lag.state == LagState::CaughtUp
            && s.last_event_unix_ms
                .is_none_or(|t| now_ms.saturating_sub(t) > IDLE_AFTER_MS);
        // The first sample is the base: the CPU used in its second is not known yet. Divided by
        // the time that really passed, so a late tick does not read as a spike.
        let cpu = usage.and_then(|u| {
            let (t, secs) = self.last_cpu.replace((now_ms, u.cpu_secs))?;
            let elapsed = now_ms.checked_sub(t).filter(|&d| d > 0)? as f64 / 1e3;
            Some(((u.cpu_secs - secs) / elapsed * 100.0).max(0.0))
        });
        let sample = [
            Some(rate),
            Some(bytes_rate / 1e6),
            s.lag_ms.filter(|_| !idle).map(|l| l.max(0) as f64),
            cpu,
            usage.map(|u| u.rss_bytes as f64 / 1e6),
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
            "cpu_pct": col(3),
            "mem_mb": col(4),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aqueduct_core::Metrics;

    const T0: u64 = 1_800_000_000_000;

    fn snap(lag_ms: Option<i64>) -> Snapshot {
        let mut s = Metrics::new().snapshot();
        s.lag_ms = lag_ms;
        s
    }

    fn usage(cpu_secs: f64, rss_mb: u64) -> Option<Usage> {
        Some(Usage {
            cpu_secs,
            rss_bytes: rss_mb * 1_000_000,
        })
    }

    #[test]
    fn keeps_each_second_as_it_was() {
        let mut h = KpiHistory::new(120);
        h.record(T0, 10.0, 1_500_000.0, &snap(Some(100)), usage(4.0, 30));
        // A quarter of a core over the second.
        h.record(
            T0 + 1_000,
            30.0,
            500_000.0,
            &snap(Some(300)),
            usage(4.25, 32),
        );
        // No lag shown, no process figures.
        h.record(T0 + 2_000, 0.0, 0.0, &snap(None), None);
        let w = h.window();
        assert_eq!(w["interval_ms"], 1_000);
        assert_eq!(w["throughput"], json!([10.0, 30.0, 0.0]));
        assert_eq!(w["kafka_mb_per_sec"], json!([1.5, 0.5, 0.0]));
        assert_eq!(w["lag_ms"], json!([100.0, 300.0, null]));
        assert_eq!(w["cpu_pct"], json!([null, 25.0, null]));
        assert_eq!(w["mem_mb"], json!([30.0, 32.0, null]));
    }

    #[test]
    fn cpu_is_per_elapsed_time_and_spans_cores() {
        let mut h = KpiHistory::new(120);
        h.record(T0, 0.0, 0.0, &snap(None), usage(1.0, 1));
        // A late tick: 3 CPU seconds over 2 s is one and a half cores, not three.
        h.record(T0 + 2_000, 0.0, 0.0, &snap(None), usage(4.0, 1));
        // A second without figures keeps the base, so the next second still has a value.
        h.record(T0 + 3_000, 0.0, 0.0, &snap(None), None);
        h.record(T0 + 4_000, 0.0, 0.0, &snap(None), usage(5.0, 1));
        assert_eq!(h.window()["cpu_pct"], json!([null, 150.0, null, 50.0]));
    }

    #[test]
    fn idle_seconds_show_no_lag() {
        let mut h = KpiHistory::new(120);
        let mut s = snap(Some(50_000));
        s.events_total = 5;
        s.source_lag.state = LagState::CaughtUp;
        s.last_event_unix_ms = Some(T0 - 20_000);
        h.record(T0, 0.0, 0.0, &s, None);
        s.last_event_unix_ms = Some(T0);
        s.lag_ms = Some(80);
        h.record(T0 + 1_000, 5.0, 0.0, &s, None);
        assert_eq!(h.window()["lag_ms"], json!([null, 80.0]));
    }

    #[test]
    fn keeps_only_the_window() {
        let mut h = KpiHistory::new(3);
        for i in 0..10u64 {
            h.record(T0 + i * 1_000, i as f64, 0.0, &snap(None), None);
        }
        let w = h.window();
        assert_eq!(w["len"], 3);
        assert_eq!(w["throughput"], json!([7.0, 8.0, 9.0]));
    }

    #[test]
    fn parses_proc_self() {
        // The command name holds a space and a parenthesis; utime 250 and stime 50 ticks.
        let stat = "4242 (aque) duct) S 1 4242 4242 0 -1 4194560 900 0 0 0 250 50 0 0 20 0 9 0 \
                    100 300000000 7000 18446744073709551615";
        let status = "Name:\taqueduct\nVmPeak:\t  400000 kB\nVmRSS:\t   51200 kB\nThreads:\t9\n";
        assert_eq!(
            parse_usage(stat, status),
            Some(Usage {
                cpu_secs: 3.0,
                rss_bytes: 51200 * 1024
            })
        );
        assert_eq!(parse_usage("garbage", status), None);
        assert_eq!(parse_usage(stat, "Name:\taqueduct\n"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reads_this_process() {
        let u = process_usage().expect("procfs");
        assert!(u.rss_bytes > 0);
        assert!(u.cpu_secs >= 0.0);
    }
}
