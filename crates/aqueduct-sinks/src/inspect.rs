use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

use aqueduct_core::{Error, Result};
use rskafka::client::partition::{OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::kafka::{glob_match, KafkaSecurity};
use crate::route::RoutePlan;

/// Most events one inspection returns, however large `limit` is.
pub const MAX_INSPECT_EVENTS: usize = 500;
/// Byte window requested per Kafka fetch.
const FETCH_BYTES: std::ops::Range<i32> = 1..4 * 1024 * 1024;
const FETCH_WAIT_MS: i32 = 200;

/// One record read back from Kafka.
#[derive(Debug, Clone, Serialize)]
pub struct InspectedEvent {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// Kafka record timestamp (the produce time; the commit time with `record_timestamp = "commit"`), Unix ms.
    pub timestamp_ms: i64,
    pub key: Option<String>,
    /// The record value parsed as JSON, or the raw text if it is not JSON. Usually the change
    /// event envelope; an outbox payload (a route's `outbox`) is whatever the row held, and a
    /// tombstone is `null`.
    pub event: serde_json::Value,
    /// The record headers Aqueduct writes, as text: `op`, `lsn`, `aqueduct-error`,
    /// `aqueduct-hash-key-id`, and `id` and `type` when a route uses `outbox`. Other headers are
    /// left out. They tell the op and position of a value that is not an envelope.
    pub headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Inspection {
    /// Topics the sink's routes can write to that currently exist.
    pub topics: Vec<String>,
    /// Newest first.
    pub events: Vec<InspectedEvent>,
}

/// Reads recent events back from the Kafka topics the sink writes to, so the
/// admin inspector shows what is actually in Kafka rather than what the
/// pipeline kept in memory. Connects lazily, so a broker outage never blocks
/// startup and the pipeline never waits on the inspector.
pub struct KafkaInspector {
    brokers: Vec<String>,
    router: RoutePlan,
    globs: Vec<String>,
    security: KafkaSecurity,
    client: Mutex<Option<Arc<Client>>>,
}

impl KafkaInspector {
    pub fn new(brokers: Vec<String>, plan: &RoutePlan, security: KafkaSecurity) -> Self {
        Self {
            brokers,
            router: plan.clone(),
            globs: plan.topic_globs(),
            security,
            client: Mutex::new(None),
        }
    }

    async fn client(&self) -> Result<Arc<Client>> {
        let mut slot = self.client.lock().await;
        if let Some(c) = slot.as_ref() {
            return Ok(c.clone());
        }
        let c = Arc::new(
            self.security
                .apply(ClientBuilder::new(self.brokers.clone()))?
                .build()
                .await
                .map_err(|e| Error::Sink(format!("kafka inspector connect: {e}")))?,
        );
        *slot = Some(c.clone());
        Ok(c)
    }

    /// The sink's routes, to tell which topic a table goes to.
    pub fn router(&self) -> &RoutePlan {
        &self.router
    }

    /// The high-water mark (the offset the next record will get) of every partition of each of
    /// `topics`, sorted by partition. Topics the sink's routes cannot write to, and topics that
    /// do not exist yet, are left out, so this cannot be used to probe unrelated topics.
    pub async fn high_water_marks(
        &self,
        topics: &[String],
    ) -> Result<BTreeMap<String, Vec<(i32, i64)>>> {
        let client = self.client().await?;
        let listed = client
            .list_topics()
            .await
            .map_err(|e| Error::Sink(format!("kafka list topics: {e}")))?;
        let wanted: Vec<(String, Vec<i32>)> = listed
            .into_iter()
            .filter(|t| topics.contains(&t.name))
            .filter(|t| self.globs.iter().any(|g| glob_match(g, &t.name)))
            .map(|t| (t.name, t.partitions.into_iter().collect()))
            .collect();
        let reads = wanted.iter().flat_map(|(name, parts)| {
            let client = &client;
            parts.iter().map(move |&p| async move {
                let pc = client
                    .partition_client(name.as_str(), p, UnknownTopicHandling::Error)
                    .await
                    .map_err(|e| Error::Sink(format!("kafka partition client {name}/{p}: {e}")))?;
                let hwm = pc
                    .get_offset(OffsetAt::Latest)
                    .await
                    .map_err(|e| Error::Sink(format!("kafka offsets {name}/{p}: {e}")))?;
                Ok::<_, Error>((name.clone(), p, hwm))
            })
        });
        let mut out: BTreeMap<String, Vec<(i32, i64)>> = BTreeMap::new();
        for (name, p, hwm) in futures::future::try_join_all(reads).await? {
            out.entry(name).or_default().push((p, hwm));
        }
        for parts in out.values_mut() {
            parts.sort_unstable();
        }
        Ok(out)
    }

    /// The newest `limit` events (capped at [`MAX_INSPECT_EVENTS`]) across the
    /// partitions of `topic`, or of every routed topic when `topic` is `None`.
    /// A `topic` that is not one of the sink's topics is ignored (empty result),
    /// so the inspector cannot be used to read unrelated topics.
    pub async fn inspect(&self, topic: Option<&str>, limit: usize) -> Result<Inspection> {
        let limit = limit.clamp(1, MAX_INSPECT_EVENTS);
        let client = self.client().await?;
        let listed = client
            .list_topics()
            .await
            .map_err(|e| Error::Sink(format!("kafka list topics: {e}")))?;
        let mut topics: Vec<(String, Vec<i32>)> = listed
            .into_iter()
            .filter(|t| self.globs.iter().any(|g| glob_match(g, &t.name)))
            .map(|t| (t.name, t.partitions.into_iter().collect()))
            .collect();
        topics.sort();
        let names = topics.iter().map(|(n, _)| n.clone()).collect();
        topics.retain(|(n, _)| topic.is_none_or(|t| t == n));

        let mut newest = Newest::new(limit);
        for (name, parts) in &topics {
            for &p in parts {
                let pc = client
                    .partition_client(name, p, UnknownTopicHandling::Error)
                    .await
                    .map_err(|e| Error::Sink(format!("kafka partition client: {e}")))?;
                tail(&pc, name, p, limit, self.router.has_outbox(), &mut newest).await?;
            }
        }
        Ok(Inspection {
            topics: names,
            events: newest.into_sorted(),
        })
    }
}

/// Headers Aqueduct writes that the inspector shows: `op`, `lsn`, the dead-letter reason and
/// the hash key id.
const SHOWN_HEADERS: [&str; 4] = ["op", "lsn", "aqueduct-error", "aqueduct-hash-key-id"];
/// The headers an outbox route adds, shown only when the plan has one.
const OUTBOX_HEADERS: [&str; 2] = ["id", "type"];

/// The headers of a record the inspector shows, as text. Only the allow-list: the inspected
/// topics can also hold records of other producers, whose headers (tracing, auth) must not reach
/// the cockpit.
fn shown_headers(headers: BTreeMap<String, Vec<u8>>, outbox: bool) -> BTreeMap<String, String> {
    headers
        .into_iter()
        .filter(|(k, _)| {
            SHOWN_HEADERS.contains(&k.as_str()) || (outbox && OUTBOX_HEADERS.contains(&k.as_str()))
        })
        .map(|(k, v)| (k, String::from_utf8_lossy(&v).into_owned()))
        .collect()
}

/// Where an event sorts in an inspection, newest first: by timestamp, then topic, partition
/// and offset, all descending.
type Rank<'a> = (i64, &'a str, i32, i64);

fn rank(e: &InspectedEvent) -> Rank<'_> {
    (e.timestamp_ms, &e.topic, e.partition, e.offset)
}

/// An [`InspectedEvent`] ordered by [`rank`] alone.
struct Ranked(InspectedEvent);

impl PartialEq for Ranked {
    fn eq(&self, o: &Self) -> bool {
        rank(&self.0) == rank(&o.0)
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Ranked {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        rank(&self.0).cmp(&rank(&o.0))
    }
}

/// The newest `limit` events seen so far, across every partition read: memory stays at
/// `limit` events however many topics and partitions there are, and a record that cannot
/// make the cut is not parsed.
struct Newest {
    limit: usize,
    /// A min-heap: the top is the oldest event kept, the first to go.
    heap: BinaryHeap<Reverse<Ranked>>,
}

impl Newest {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit.min(MAX_INSPECT_EVENTS) + 1),
        }
    }

    /// Whether an event ranked `at` would be kept.
    fn admits(&self, at: Rank<'_>) -> bool {
        self.limit > 0
            && (self.heap.len() < self.limit
                || self
                    .heap
                    .peek()
                    .is_some_and(|Reverse(min)| at > rank(&min.0)))
    }

    fn push(&mut self, e: InspectedEvent) {
        if !self.admits(rank(&e)) {
            return;
        }
        self.heap.push(Reverse(Ranked(e)));
        if self.heap.len() > self.limit {
            self.heap.pop();
        }
    }

    /// The events kept, newest first.
    fn into_sorted(self) -> Vec<InspectedEvent> {
        // Ascending by `Reverse` is descending by rank.
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(Ranked(e))| e)
            .collect()
    }
}

/// Offer the last `limit` records of one partition to `out`. `outbox`: also show the outbox
/// headers.
async fn tail(
    pc: &PartitionClient,
    topic: &str,
    partition: i32,
    limit: usize,
    outbox: bool,
    out: &mut Newest,
) -> Result<()> {
    let e = |what: &str, e: rskafka::client::error::Error| {
        Error::Sink(format!("kafka {what} {topic}/{partition}: {e}"))
    };
    let earliest = pc
        .get_offset(OffsetAt::Earliest)
        .await
        .map_err(|x| e("offsets", x))?;
    let latest = pc
        .get_offset(OffsetAt::Latest)
        .await
        .map_err(|x| e("offsets", x))?;
    let mut next = earliest.max(latest - limit as i64);
    while next < latest {
        let (records, _) = pc
            .fetch_records(next, FETCH_BYTES, FETCH_WAIT_MS)
            .await
            .map_err(|x| e("fetch", x))?;
        let Some(last) = records.last().map(|r| r.offset) else {
            break;
        };
        for r in records.into_iter().filter(|r| r.offset < latest) {
            let at = r.record.timestamp.timestamp_millis();
            if !out.admits((at, topic, partition, r.offset)) {
                continue;
            }
            let event = match r.record.value {
                None => serde_json::Value::Null,
                Some(v) => serde_json::from_slice(&v)
                    .unwrap_or_else(|_| String::from_utf8_lossy(&v).into_owned().into()),
            };
            out.push(InspectedEvent {
                topic: topic.to_string(),
                partition,
                offset: r.offset,
                timestamp_ms: at,
                key: r
                    .record
                    .key
                    .map(|k| String::from_utf8_lossy(&k).into_owned()),
                event,
                headers: shown_headers(r.record.headers, outbox),
            });
        }
        next = last + 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_allow_listed_headers_are_shown() {
        let headers = || -> BTreeMap<String, Vec<u8>> {
            [
                ("op", "c"),
                ("lsn", "7"),
                ("aqueduct-error", "route `r`: ..."),
                ("aqueduct-hash-key-id", "k1"),
                ("id", "e-1"),
                ("type", "OrderCreated"),
                ("traceparent", "00-abc-def-01"),
                ("authorization", "Bearer secret"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
            .collect()
        };
        let keys = |outbox| {
            shown_headers(headers(), outbox)
                .into_keys()
                .collect::<Vec<_>>()
        };
        assert_eq!(
            keys(false),
            ["aqueduct-error", "aqueduct-hash-key-id", "lsn", "op"]
        );
        assert_eq!(
            keys(true),
            [
                "aqueduct-error",
                "aqueduct-hash-key-id",
                "id",
                "lsn",
                "op",
                "type"
            ]
        );
        assert_eq!(shown_headers(headers(), true)["type"], "OrderCreated");
    }

    fn ev(topic: &str, partition: i32, offset: i64, timestamp_ms: i64) -> InspectedEvent {
        InspectedEvent {
            topic: topic.into(),
            partition,
            offset,
            timestamp_ms,
            key: None,
            event: serde_json::Value::Null,
            headers: BTreeMap::new(),
        }
    }

    /// The bounded selection keeps what sorting everything and truncating kept, in the same
    /// order, and refuses an event older than all it holds once full.
    #[test]
    fn newest_keeps_the_newest_limit_events_newest_first() {
        let all: Vec<InspectedEvent> = (0..40)
            .map(|i| {
                // Timestamps collide across topics and partitions: ties sort by topic,
                // partition, then offset.
                ev(
                    ["a", "b", "c"][i % 3],
                    (i % 2) as i32,
                    i as i64,
                    (i as i64 * 7) % 11,
                )
            })
            .collect();
        let key = |v: &[InspectedEvent]| {
            v.iter()
                .map(|e| (e.timestamp_ms, e.topic.clone(), e.partition, e.offset))
                .collect::<Vec<_>>()
        };
        for limit in [0, 1, 3, 10, 40, 100] {
            let mut want = all.clone();
            want.sort_by(|a, b| {
                (b.timestamp_ms, &b.topic, b.partition, b.offset).cmp(&(
                    a.timestamp_ms,
                    &a.topic,
                    a.partition,
                    a.offset,
                ))
            });
            want.truncate(limit);
            let mut newest = Newest::new(limit);
            for e in all.iter().cloned() {
                newest.push(e);
            }
            assert_eq!(key(&newest.into_sorted()), key(&want), "limit {limit}");
        }
        let mut newest = Newest::new(2);
        newest.push(ev("a", 0, 5, 100));
        newest.push(ev("a", 0, 6, 200));
        assert!(!newest.admits((50, "a", 0, 1)), "older than both");
        assert!(!newest.admits((100, "a", 0, 5)), "the oldest kept");
        assert!(newest.admits((100, "a", 0, 7)));
        assert!(newest.admits((150, "a", 0, 1)));
    }
}
