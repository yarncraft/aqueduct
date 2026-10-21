//! Records the Kafka sink got acknowledged, per topic. Shared (an `Arc`) between the sink, which
//! counts after each produce is acknowledged, and the admin server, which samples the counts into
//! the cockpit's per-topic rates and exports them to Prometheus.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

/// Topics counted by name; acknowledged records of any further topic count under [`OTHER_TOPICS`].
/// Content routing already caps its topics (`sink.max_dynamic_topics`); this bounds the counters
/// whatever that cap is.
pub const MAX_TRACKED_TOPICS: usize = 500;

/// The bucket for topics beyond [`MAX_TRACKED_TOPICS`]. Not a valid Kafka topic name, so it never
/// collides with a real one.
pub const OTHER_TOPICS: &str = "(other)";

/// Acknowledged records per topic, see the module docs.
#[derive(Debug)]
pub struct TopicStats {
    cap: usize,
    counts: Mutex<BTreeMap<String, u64>>,
    other: AtomicU64,
    warned: AtomicBool,
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
            counts: Mutex::new(BTreeMap::new()),
            other: AtomicU64::new(0),
            warned: AtomicBool::new(false),
        }
    }

    /// The broker acknowledged `n` records produced to `topic`. Call only after the produce
    /// succeeded: a failed produce is retried with its batch and must not count.
    pub fn acked(&self, topic: &str, n: u64) {
        if n == 0 {
            return;
        }
        let mut counts = lock(&self.counts);
        if let Some(c) = counts.get_mut(topic) {
            *c += n;
        } else if counts.len() < self.cap {
            counts.insert(topic.to_string(), n);
        } else {
            drop(counts);
            self.other.fetch_add(n, Ordering::Relaxed);
            if !self.warned.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    tracked = self.cap,
                    "more topics than the cockpit counts by name: further topics count as {OTHER_TOPICS}"
                );
            }
        }
    }

    /// Records acknowledged per topic since start, plus [`OTHER_TOPICS`] once a topic overflowed.
    pub fn snapshot(&self) -> BTreeMap<String, u64> {
        let mut out = lock(&self.counts).clone();
        let other = self.other.load(Ordering::Relaxed);
        if other > 0 {
            out.insert(OTHER_TOPICS.to_string(), other);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_per_topic_and_folds_the_rest_into_other() {
        let s = TopicStats::with_cap(2);
        assert!(s.snapshot().is_empty());
        s.acked("a", 3);
        s.acked("b", 1);
        s.acked("a", 2);
        s.acked("c", 4);
        s.acked("d", 1);
        // Tracked topics keep counting by name after the cap is reached.
        s.acked("b", 5);
        s.acked("e", 0);
        let snap = s.snapshot();
        assert_eq!(
            snap.into_iter().collect::<Vec<_>>(),
            vec![
                (OTHER_TOPICS.to_string(), 5),
                ("a".to_string(), 5),
                ("b".to_string(), 6),
            ]
        );
    }

    #[test]
    fn the_default_cap_bounds_the_named_topics() {
        let s = TopicStats::new();
        for i in 0..MAX_TRACKED_TOPICS + 50 {
            s.acked(&format!("t{i}"), 1);
        }
        let snap = s.snapshot();
        assert_eq!(snap.len(), MAX_TRACKED_TOPICS + 1);
        assert_eq!(snap[OTHER_TOPICS], 50);
    }
}
