//! Records the Kafka sink got acknowledged, per topic and operation, and their bytes. Shared (an
//! `Arc`) between the sink, which counts after each produce is acknowledged, and the admin server,
//! which samples the counts into the cockpit's per-topic rates and bandwidth and exports them to
//! Prometheus.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use crate::OpCounts;

/// Topics counted by name; acknowledged records of any further topic count under [`OTHER_TOPICS`].
/// Content routing already caps its topics (`sink.max_dynamic_topics`); this bounds the counters
/// whatever that cap is.
pub const MAX_TRACKED_TOPICS: usize = 500;

/// The bucket for topics beyond [`MAX_TRACKED_TOPICS`]. Not a valid Kafka topic name, so it never
/// collides with a real one.
pub const OTHER_TOPICS: &str = "(other)";

/// Acknowledged records per topic and operation, see the module docs.
#[derive(Debug)]
pub struct TopicStats {
    cap: usize,
    counts: Mutex<Counts>,
    /// Key, value and header bytes of every acknowledged record, all topics together.
    bytes: AtomicU64,
    warned: AtomicBool,
}

#[derive(Debug, Default)]
struct Counts {
    named: BTreeMap<String, OpCounts>,
    other: OpCounts,
    /// Bytes per topic, under the same names (a topic counts its bytes where it counts its records).
    named_bytes: BTreeMap<String, u64>,
    other_bytes: u64,
}

impl Default for TopicStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Counters are telemetry: a panic that poisoned the lock must not stop the sink.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl TopicStats {
    pub fn new() -> Self {
        Self::with_cap(MAX_TRACKED_TOPICS)
    }

    /// Counts at most `cap` topics by name.
    pub fn with_cap(cap: usize) -> Self {
        Self {
            cap,
            counts: Mutex::new(Counts::default()),
            bytes: AtomicU64::new(0),
            warned: AtomicBool::new(false),
        }
    }

    /// The broker acknowledged the records `ops` counts, produced to `topic`. Call only after the
    /// produce succeeded: a failed produce is retried with its batch and must not count.
    pub fn acked(&self, topic: &str, ops: &OpCounts) {
        if ops.total() == 0 {
            return;
        }
        let mut c = lock(&self.counts);
        if let Some(n) = c.named.get_mut(topic) {
            *n = n.plus(ops);
        } else if c.named.len() < self.cap {
            c.named.insert(topic.to_string(), *ops);
        } else {
            c.other = c.other.plus(ops);
            drop(c);
            if !self.warned.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    tracked = self.cap,
                    "more topics than the cockpit counts by name: further topics count as {OTHER_TOPICS}"
                );
            }
        }
    }

    /// The broker acknowledged records carrying `n` bytes of key, value and headers, produced to
    /// `topic`. Same rule as [`Self::acked`], and called after it: only after the produce succeeded.
    pub fn acked_bytes(&self, topic: &str, n: u64) {
        if n == 0 {
            return;
        }
        self.bytes.fetch_add(n, Ordering::Relaxed);
        let mut c = lock(&self.counts);
        if c.named.contains_key(topic) {
            *c.named_bytes.entry(topic.to_string()).or_default() += n;
        } else {
            c.other_bytes += n;
        }
    }

    /// Bytes acknowledged per topic since start, named like [`Self::snapshot`].
    pub fn bytes_snapshot(&self) -> BTreeMap<String, u64> {
        let c = lock(&self.counts);
        let mut out = c.named_bytes.clone();
        if c.other_bytes > 0 {
            out.insert(OTHER_TOPICS.to_string(), c.other_bytes);
        }
        out
    }

    /// Key, value and header bytes acknowledged since start, all topics together.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Records acknowledged per topic and operation since start, plus [`OTHER_TOPICS`] once a
    /// topic overflowed.
    pub fn snapshot(&self) -> BTreeMap<String, OpCounts> {
        let c = lock(&self.counts);
        let mut out = c.named.clone();
        if c.other.total() > 0 {
            out.insert(OTHER_TOPICS.to_string(), c.other);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ops(code: &str, n: u64) -> OpCounts {
        let mut o = OpCounts::default();
        o.add(code, n);
        o
    }

    fn totals(s: &TopicStats) -> Vec<(String, u64)> {
        s.snapshot()
            .into_iter()
            .map(|(t, o)| (t, o.total()))
            .collect()
    }

    #[test]
    fn counts_per_topic_and_folds_the_rest_into_other() {
        let s = TopicStats::with_cap(2);
        assert!(s.snapshot().is_empty());
        s.acked("a", &ops("c", 3));
        s.acked("b", &ops("u", 1));
        s.acked("a", &ops("c", 2));
        s.acked("c", &ops("d", 4));
        s.acked("d", &ops("r", 1));
        // Tracked topics keep counting by name after the cap is reached.
        s.acked("b", &ops("u", 5));
        s.acked("e", &OpCounts::default());
        assert_eq!(
            totals(&s),
            vec![
                (OTHER_TOPICS.to_string(), 5),
                ("a".to_string(), 5),
                ("b".to_string(), 6),
            ]
        );
    }

    #[test]
    fn counts_bytes_per_topic_where_it_counts_the_records() {
        let s = TopicStats::with_cap(1);
        s.acked("a", &ops("c", 1));
        s.acked_bytes("a", 100);
        s.acked("b", &ops("c", 1));
        s.acked_bytes("b", 40);
        s.acked_bytes("a", 0);
        assert_eq!(s.bytes(), 140);
        assert_eq!(
            s.bytes_snapshot().into_iter().collect::<Vec<_>>(),
            vec![(OTHER_TOPICS.to_string(), 40), ("a".to_string(), 100)]
        );
    }

    #[test]
    fn counts_each_operation_separately() {
        let s = TopicStats::new();
        let mut batch = OpCounts::default();
        batch.add("c", 2);
        batch.add("u", 1);
        // A delete and its tombstone: both records carry the delete's letter.
        batch.add("d", 2);
        batch.add("t", 1);
        batch.add("r", 4);
        // Heartbeats never reach the sink; an unknown letter counts nowhere.
        batch.add("h", 9);
        s.acked("t", &batch);
        s.acked("t", &ops("c", 1));
        let snap = s.snapshot();
        assert_eq!(snap["t"].slots(), [3, 1, 2, 1, 4]);
        assert_eq!(snap["t"].total(), 11);
    }

    #[test]
    fn the_default_cap_bounds_the_named_topics() {
        let s = TopicStats::new();
        for i in 0..MAX_TRACKED_TOPICS + 50 {
            s.acked(&format!("t{i}"), &ops("c", 1));
        }
        let snap = s.snapshot();
        assert_eq!(snap.len(), MAX_TRACKED_TOPICS + 1);
        assert_eq!(snap[OTHER_TOPICS].insert, 50);
    }
}
