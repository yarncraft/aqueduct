//! Per-route and per-topic rate history for the cockpit's Routing card: one sample per second
//! of the route outcome counters and produced records per operation ([`RouteCount`]), and of the
//! records and bytes Kafka acknowledged per topic and operation ([`aqueduct_sinks::TopicStats`]),
//! kept over the same window as the event-rate history.
//!
//! Bounded: the routes are fixed at startup, and the topic counters already fold every topic
//! past [`aqueduct_sinks::MAX_TRACKED_TOPICS`] into one bucket, so at most that many plus one
//! series are kept, each at most `len` samples of five counters.

use std::collections::{BTreeMap, VecDeque};

use aqueduct_sinks::{OpCounts, RouteCount};
use serde_json::{json, Value};

/// A route's outcomes per second: `[produced, filtered, fallback, errors]`.
pub type Outcomes = [u64; 4];

/// Records per second per operation: `[insert, update, delete, truncate, read]`.
pub type Ops = [u64; 5];

fn outcomes(c: &RouteCount) -> Outcomes {
    let c = &c.counts;
    [c.produced, c.filtered, c.fallback, c.errors]
}

fn delta<const N: usize>(now: [u64; N], prev: [u64; N]) -> [u64; N] {
    std::array::from_fn(|k| now[k].saturating_sub(prev[k]))
}

pub struct RateHistory {
    len: usize,
    /// Samples recorded since start; the stream sends it so the page can tell a missed sample.
    seq: u64,
    /// Samples in the window so far (at most `len`); every series below has this many.
    filled: usize,
    /// Per route: its outcomes and its produced records per op, each second.
    routes: Vec<(String, VecDeque<(Outcomes, Ops)>)>,
    last_routes: Vec<(Outcomes, Ops)>,
    /// Per topic: acknowledged records per op, each second.
    topics: BTreeMap<String, VecDeque<Ops>>,
    /// The acknowledged totals since start, per topic and op.
    last_topics: BTreeMap<String, OpCounts>,
    /// Per topic: acknowledged key, value and header bytes, each second; and their totals.
    topic_bytes: BTreeMap<String, VecDeque<u64>>,
    last_topic_bytes: BTreeMap<String, u64>,
    /// The newest sample, for the stream: every route, and only the topics that moved.
    latest_routes: Vec<(Outcomes, Ops)>,
    latest_topics: BTreeMap<String, Ops>,
    latest_topic_bytes: BTreeMap<String, u64>,
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
            topic_bytes: BTreeMap::new(),
            last_topic_bytes: BTreeMap::new(),
            latest_routes: Vec::new(),
            latest_topics: BTreeMap::new(),
            latest_topic_bytes: BTreeMap::new(),
        }
    }

    /// Adds one second: the increase of every counter since the previous sample.
    pub fn record(
        &mut self,
        routes: &[RouteCount],
        topics: &BTreeMap<String, OpCounts>,
        bytes: &BTreeMap<String, u64>,
    ) {
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
            self.last_routes = vec![([0; 4], [0; 5]); routes.len()];
        }
        self.latest_routes.clear();
        for (i, c) in routes.iter().enumerate() {
            let now = (outcomes(c), c.counts.ops.slots());
            let prev = self.last_routes[i];
            let d = (delta(now.0, prev.0), delta(now.1, prev.1));
            self.last_routes[i] = now;
            self.latest_routes.push(d);
            push(&mut self.routes[i].1, d, self.filled, self.len);
        }
        self.latest_topics.clear();
        for (t, n) in topics {
            let prev = self.last_topics.get(t).copied().unwrap_or_default();
            let d = delta(n.slots(), prev.slots());
            if d.iter().any(|&v| v > 0) {
                self.latest_topics.insert(t.clone(), d);
            }
            let series = self.topics.entry(t.clone()).or_default();
            push(series, d, self.filled, self.len);
        }
        // A topic is never forgotten by the counters, so every series got its sample above.
        self.last_topics.clone_from(topics);
        self.latest_topic_bytes.clear();
        for (t, &n) in bytes {
            let d = n.saturating_sub(self.last_topic_bytes.get(t).copied().unwrap_or(0));
            if d > 0 {
                self.latest_topic_bytes.insert(t.clone(), d);
            }
            push(
                self.topic_bytes.entry(t.clone()).or_default(),
                d,
                self.filled,
                self.len,
            );
        }
        self.last_topic_bytes.clone_from(bytes);
    }

    /// The newest sample for each stream frame: `seq`, every route's outcomes and produced
    /// records per op in plan order, and the topics with records in that second (their total and
    /// per op, and their bytes).
    pub fn latest(&self) -> Value {
        let topics: BTreeMap<&String, u64> = self
            .latest_topics
            .iter()
            .map(|(t, o)| (t, o.iter().sum()))
            .collect();
        json!({
            "seq": self.seq,
            "routes": self.latest_routes.iter().map(|r| r.0).collect::<Vec<_>>(),
            "route_ops": self.latest_routes.iter().map(|r| r.1).collect::<Vec<_>>(),
            "topics": topics,
            "topic_ops": self.latest_topics,
            "topic_bytes": self.latest_topic_bytes,
        })
    }

    /// The whole window, for `/api/routes/history`. Every series has the same length, oldest
    /// first, and ends with the sample numbered `seq`. Per-op series are rows of
    /// `[insert, update, delete, truncate, read]`; `topic_totals` are the acknowledged records
    /// per topic and op since start, up to and including that sample; `topic_bytes` the bytes
    /// acknowledged per topic and second.
    pub fn window(&self, interval_ms: u64) -> Value {
        let routes: Vec<Value> = self
            .routes
            .iter()
            .map(|(name, s)| {
                let col = |k: usize| s.iter().map(|o| o.0[k]).collect::<Vec<u64>>();
                json!({
                    "route": name,
                    "produced": col(0),
                    "filtered": col(1),
                    "fallback": col(2),
                    "errors": col(3),
                    "ops": s.iter().map(|o| o.1).collect::<Vec<Ops>>(),
                })
            })
            .collect();
        let topics: BTreeMap<&String, Vec<u64>> = self
            .topics
            .iter()
            .map(|(t, s)| (t, s.iter().map(|o| o.iter().sum()).collect()))
            .collect();
        json!({
            "seq": self.seq,
            "interval_ms": interval_ms,
            "len": self.len,
            "routes": routes,
            "topics": topics,
            "topic_ops": self.topics,
            "topic_totals": self.last_topics,
            "topic_bytes": self.topic_bytes,
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

    /// A route whose produced events were all inserts but `deletes` of them.
    fn route_ops(
        name: &str,
        [produced, filtered, fallback, errors]: Outcomes,
        deletes: u64,
    ) -> RouteCount {
        RouteCount {
            route: name.into(),
            counts: RouteCounts {
                matched: produced + filtered + fallback + errors,
                produced,
                filtered,
                fallback,
                errors,
                tombstones_skipped: 0,
                ops: OpCounts {
                    insert: produced - deletes,
                    delete: deletes,
                    ..Default::default()
                },
            },
            unseen_fields: vec![],
        }
    }

    fn route(name: &str, produced: u64, filtered: u64, fallback: u64, errors: u64) -> RouteCount {
        route_ops(name, [produced, filtered, fallback, errors], 0)
    }

    /// Topics whose acknowledged records were all inserts.
    fn topics(v: &[(&str, u64)]) -> BTreeMap<String, OpCounts> {
        v.iter()
            .map(|(t, n)| {
                let ops = OpCounts {
                    insert: *n,
                    ..Default::default()
                };
                (t.to_string(), ops)
            })
            .collect()
    }

    #[test]
    fn samples_are_per_second_deltas_aligned_and_bounded() {
        let mut h = RateHistory::new(3);
        h.record(
            &[route("a", 5, 1, 0, 0), route("default", 0, 0, 0, 0)],
            &topics(&[("t", 5)]),
            &BTreeMap::new(),
        );
        h.record(
            &[route("a", 7, 1, 0, 2), route("default", 1, 0, 0, 0)],
            &topics(&[("t", 7), ("u", 1)]),
            &BTreeMap::new(),
        );
        let l = h.latest();
        assert_eq!(l["seq"], 2);
        assert_eq!(l["routes"], json!([[2, 0, 0, 2], [1, 0, 0, 0]]));
        assert_eq!(l["route_ops"], json!([[2, 0, 0, 0, 0], [1, 0, 0, 0, 0]]));
        // Only topics that moved are in the stream frame.
        assert_eq!(l["topics"], json!({"t": 2, "u": 1}));
        assert_eq!(
            l["topic_ops"],
            json!({"t": [2, 0, 0, 0, 0], "u": [1, 0, 0, 0, 0]})
        );
        h.record(
            &[route("a", 7, 1, 0, 2), route("default", 1, 0, 0, 0)],
            &topics(&[("t", 7), ("u", 1)]),
            &BTreeMap::new(),
        );
        assert_eq!(h.latest()["topics"], json!({}));
        assert_eq!(h.latest()["topic_ops"], json!({}));
        h.record(
            &[route("a", 8, 1, 0, 2), route("default", 1, 0, 0, 0)],
            &topics(&[("t", 9), ("u", 1)]),
            &BTreeMap::new(),
        );
        let w = h.window(1000);
        assert_eq!((w["seq"].as_u64(), w["len"].as_u64()), (Some(4), Some(3)));
        // The window keeps the newest 3 samples; a topic first seen later is zero-filled.
        assert_eq!(w["routes"][0]["route"], "a");
        assert_eq!(w["routes"][0]["produced"], json!([2, 0, 1]));
        assert_eq!(w["routes"][0]["errors"], json!([2, 0, 0]));
        assert_eq!(
            w["routes"][0]["ops"],
            json!([[2, 0, 0, 0, 0], [0, 0, 0, 0, 0], [1, 0, 0, 0, 0]])
        );
        assert_eq!(w["routes"][1]["produced"], json!([1, 0, 0]));
        assert_eq!(w["topics"]["t"], json!([2, 0, 2]));
        assert_eq!(w["topics"]["u"], json!([1, 0, 0]));
        assert_eq!(
            w["topic_ops"]["t"],
            json!([[2, 0, 0, 0, 0], [0, 0, 0, 0, 0], [2, 0, 0, 0, 0]])
        );
        // Totals since start, not just the window.
        assert_eq!(
            w["topic_totals"]["t"],
            json!({"insert": 9, "update": 0, "delete": 0, "truncate": 0, "read": 0})
        );
        let mut h = RateHistory::new(3);
        h.record(&[], &topics(&[("t", 1)]), &BTreeMap::new());
        h.record(&[], &topics(&[("t", 1), ("late", 4)]), &BTreeMap::new());
        assert_eq!(h.window(1000)["topics"]["late"], json!([0, 4]));
        assert_eq!(
            h.window(1000)["topic_ops"]["late"],
            json!([[0, 0, 0, 0, 0], [4, 0, 0, 0, 0]])
        );
    }

    #[test]
    fn route_and_topic_rates_are_split_by_operation() {
        let mut h = RateHistory::new(10);
        let ops = |c: u64, u: u64, d: u64, t: u64, r: u64| OpCounts {
            insert: c,
            update: u,
            delete: d,
            truncate: t,
            read: r,
        };
        let mut t = BTreeMap::new();
        t.insert("orders".to_string(), ops(3, 1, 0, 0, 0));
        h.record(&[route_ops("a", [4, 0, 0, 0], 1)], &t, &BTreeMap::new());
        // Next second: two deletes, each with its tombstone on the topic, and two snapshot reads.
        t.insert("orders".to_string(), ops(3, 1, 4, 0, 2));
        let bytes = BTreeMap::from([("orders".to_string(), 1_000)]);
        h.record(&[route_ops("a", [8, 0, 0, 0], 3)], &t, &bytes);
        let l = h.latest();
        assert_eq!(l["route_ops"], json!([[2, 0, 2, 0, 0]]));
        assert_eq!(l["topics"], json!({"orders": 6}));
        assert_eq!(l["topic_ops"], json!({"orders": [0, 0, 4, 0, 2]}));
        assert_eq!(l["topic_bytes"], json!({"orders": 1_000}));
        let w = h.window(1000);
        assert_eq!(
            w["routes"][0]["ops"],
            json!([[3, 0, 1, 0, 0], [2, 0, 2, 0, 0]])
        );
        assert_eq!(w["topics"]["orders"], json!([4, 6]));
        assert_eq!(w["topic_totals"]["orders"]["delete"], 4);
        // Bytes first seen in the second sample: zero-filled back to the window start.
        assert_eq!(w["topic_bytes"]["orders"], json!([0, 1_000]));
    }

    #[test]
    fn memory_stays_bounded_over_a_long_run() {
        let mut h = RateHistory::new(120);
        let mut t = BTreeMap::new();
        for i in 0..1000u64 {
            // Topic names come from the sink's counters, which cap them; 50 here.
            t.insert(
                format!("topic.{}", i % 50),
                OpCounts {
                    update: i,
                    ..Default::default()
                },
            );
            h.record(&[route("a", i, 0, 0, 0)], &t, &BTreeMap::new());
        }
        assert_eq!(h.topics.len(), 50);
        assert!(h.topics.values().all(|s| s.len() == 120));
        assert!(h.routes.iter().all(|(_, s)| s.len() == 120));
        assert_eq!(h.window(1000)["seq"], 1000);
    }
}
