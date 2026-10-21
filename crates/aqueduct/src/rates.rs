//! Per-route and per-topic rate history for the cockpit's Routing card: one sample per second
//! of the route outcome counters ([`RouteCount`]) and of the records Kafka acknowledged per topic
//! ([`aqueduct_sinks::TopicStats`]), kept over the same window as the event-rate history.
//!
//! Bounded: the routes are fixed at startup, and the topic counters already fold every topic
//! past [`aqueduct_sinks::MAX_TRACKED_TOPICS`] into one bucket, so at most that many plus one
//! series are kept, each at most `len` samples long.

use std::collections::{BTreeMap, VecDeque};

use aqueduct_sinks::RouteCount;
use serde_json::{json, Value};

/// A route's outcomes per second: `[produced, filtered, fallback, errors]`.
pub type Outcomes = [u64; 4];

fn outcomes(c: &RouteCount) -> Outcomes {
    let c = &c.counts;
    [c.produced, c.filtered, c.fallback, c.errors]
}

pub struct RateHistory {
    len: usize,
    /// Samples recorded since start; the stream sends it so the page can tell a missed sample.
    seq: u64,
    /// Samples in the window so far (at most `len`); every series below has this many.
    filled: usize,
    routes: Vec<(String, VecDeque<Outcomes>)>,
    last_routes: Vec<Outcomes>,
    topics: BTreeMap<String, VecDeque<u64>>,
    last_topics: BTreeMap<String, u64>,
    /// The newest sample, for the stream: every route, and only the topics that moved.
    latest_routes: Vec<Outcomes>,
    latest_topics: BTreeMap<String, u64>,
}

impl RateHistory {
    pub fn new(len: usize) -> Self {
        Self {
            len: len.max(1),
            seq: 0,
            filled: 0,
            routes: Vec::new(),
            last_routes: Vec::new(),
            topics: BTreeMap::new(),
            last_topics: BTreeMap::new(),
            latest_routes: Vec::new(),
            latest_topics: BTreeMap::new(),
        }
    }

    /// Adds one second: the increase of every counter since the previous sample.
    pub fn record(&mut self, routes: &[RouteCount], topics: &BTreeMap<String, u64>) {
        self.seq += 1;
        self.filled = (self.filled + 1).min(self.len);
        let names_changed = self.routes.len() != routes.len()
            || self.routes.iter().zip(routes).any(|(r, c)| r.0 != c.route);
        if names_changed {
            // The plan is fixed at startup; this only runs on the first sample (or a test).
            self.routes = routes
                .iter()
                .map(|c| (c.route.clone(), VecDeque::with_capacity(self.len)))
                .collect();
            self.last_routes = vec![[0; 4]; routes.len()];
        }
        self.latest_routes.clear();
        for (i, c) in routes.iter().enumerate() {
            let now = outcomes(c);
            let prev = self.last_routes[i];
            let delta: Outcomes = std::array::from_fn(|k| now[k].saturating_sub(prev[k]));
            self.last_routes[i] = now;
            self.latest_routes.push(delta);
            push(&mut self.routes[i].1, delta, self.filled, self.len);
        }
        self.latest_topics.clear();
        for (t, &n) in topics {
            let delta = n.saturating_sub(self.last_topics.get(t).copied().unwrap_or(0));
            if delta > 0 {
                self.latest_topics.insert(t.clone(), delta);
            }
            let series = self.topics.entry(t.clone()).or_default();
            push(series, delta, self.filled, self.len);
        }
        // A topic is never forgotten by the counters, so every series got its sample above.
        self.last_topics.clone_from(topics);
    }

    /// The newest sample for each stream frame: `seq`, every route's outcomes in plan order, and
    /// the topics with records in that second.
    pub fn latest(&self) -> Value {
        json!({
            "seq": self.seq,
            "routes": self.latest_routes,
            "topics": self.latest_topics,
        })
    }

    /// The whole window, for `/api/routes/history`. Every series has the same length, oldest
    /// first, and ends with the sample numbered `seq`.
    pub fn window(&self, interval_ms: u64) -> Value {
        let routes: Vec<Value> = self
            .routes
            .iter()
            .map(|(name, s)| {
                let col = |k: usize| s.iter().map(|o| o[k]).collect::<Vec<u64>>();
                json!({
                    "route": name,
                    "produced": col(0),
                    "filtered": col(1),
                    "fallback": col(2),
                    "errors": col(3),
                })
            })
            .collect();
        json!({
            "seq": self.seq,
            "interval_ms": interval_ms,
            "len": self.len,
            "routes": routes,
            "topics": self.topics,
        })
    }
}

/// Appends `v`, zero-filling a series that is new back to the window start, and keeps `len`.
fn push<T: Copy + Default>(series: &mut VecDeque<T>, v: T, filled: usize, len: usize) {
    while series.len() + 1 < filled {
        series.push_back(T::default());
    }
    series.push_back(v);
    while series.len() > len {
        series.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aqueduct_sinks::RouteCounts;

    fn route(name: &str, produced: u64, filtered: u64, fallback: u64, errors: u64) -> RouteCount {
        RouteCount {
            route: name.into(),
            counts: RouteCounts {
                matched: produced + filtered + fallback + errors,
                produced,
                filtered,
                fallback,
                errors,
                tombstones_skipped: 0,
            },
            unseen_fields: vec![],
        }
    }

    fn topics(v: &[(&str, u64)]) -> BTreeMap<String, u64> {
        v.iter().map(|(t, n)| (t.to_string(), *n)).collect()
    }

    #[test]
    fn samples_are_per_second_deltas_aligned_and_bounded() {
        let mut h = RateHistory::new(3);
        h.record(
            &[route("a", 5, 1, 0, 0), route("default", 0, 0, 0, 0)],
            &topics(&[("t", 5)]),
        );
        h.record(
            &[route("a", 7, 1, 0, 2), route("default", 1, 0, 0, 0)],
            &topics(&[("t", 7), ("u", 1)]),
        );
        let l = h.latest();
        assert_eq!(l["seq"], 2);
        assert_eq!(l["routes"], json!([[2, 0, 0, 2], [1, 0, 0, 0]]));
        // Only topics that moved are in the stream frame.
        assert_eq!(l["topics"], json!({"t": 2, "u": 1}));
        h.record(
            &[route("a", 7, 1, 0, 2), route("default", 1, 0, 0, 0)],
            &topics(&[("t", 7), ("u", 1)]),
        );
        assert_eq!(h.latest()["topics"], json!({}));
        h.record(
            &[route("a", 8, 1, 0, 2), route("default", 1, 0, 0, 0)],
            &topics(&[("t", 9), ("u", 1)]),
        );
        let w = h.window(1000);
        assert_eq!((w["seq"].as_u64(), w["len"].as_u64()), (Some(4), Some(3)));
        // The window keeps the newest 3 samples; a topic first seen later is zero-filled.
        assert_eq!(w["routes"][0]["route"], "a");
        assert_eq!(w["routes"][0]["produced"], json!([2, 0, 1]));
        assert_eq!(w["routes"][0]["errors"], json!([2, 0, 0]));
        assert_eq!(w["routes"][1]["produced"], json!([1, 0, 0]));
        assert_eq!(w["topics"]["t"], json!([2, 0, 2]));
        assert_eq!(w["topics"]["u"], json!([1, 0, 0]));
        let mut h = RateHistory::new(3);
        h.record(&[], &topics(&[("t", 1)]));
        h.record(&[], &topics(&[("t", 1), ("late", 4)]));
        assert_eq!(h.window(1000)["topics"]["late"], json!([0, 4]));
    }

    #[test]
    fn memory_stays_bounded_over_a_long_run() {
        let mut h = RateHistory::new(120);
        let mut t = BTreeMap::new();
        for i in 0..1000u64 {
            // Topic names come from the sink's counters, which cap them; 50 here.
            t.insert(format!("topic.{}", i % 50), i);
            h.record(&[route("a", i, 0, 0, 0)], &t);
        }
        assert_eq!(h.topics.len(), 50);
        assert!(h.topics.values().all(|s| s.len() == 120));
        assert!(h.routes.iter().all(|(_, s)| s.len() == 120));
        assert_eq!(h.window(1000)["seq"], 1000);
    }
}
