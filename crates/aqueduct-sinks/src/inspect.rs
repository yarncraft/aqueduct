use std::sync::Arc;

use aqueduct_core::{Error, Result};
use rskafka::client::partition::{OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::kafka::{glob_match, KafkaSecurity, TopicRouter};

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
    /// Kafka record timestamp (the source commit time), Unix ms.
    pub timestamp_ms: i64,
    pub key: Option<String>,
    /// The record value parsed as JSON (a `ChangeEvent`), or the raw text if it is not JSON.
    pub event: serde_json::Value,
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
    globs: Vec<String>,
    security: KafkaSecurity,
    client: Mutex<Option<Arc<Client>>>,
}

impl KafkaInspector {
    pub fn new(brokers: Vec<String>, router: &TopicRouter, security: KafkaSecurity) -> Self {
        Self {
            brokers,
            globs: router.topic_globs(),
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

        let mut events = Vec::new();
        for (name, parts) in &topics {
            for &p in parts {
                let pc = client
                    .partition_client(name, p, UnknownTopicHandling::Error)
                    .await
                    .map_err(|e| Error::Sink(format!("kafka partition client: {e}")))?;
                tail(&pc, name, p, limit, &mut events).await?;
            }
        }
        events.sort_by(|a, b| {
            (b.timestamp_ms, &b.topic, b.partition, b.offset).cmp(&(
                a.timestamp_ms,
                &a.topic,
                a.partition,
                a.offset,
            ))
        });
        events.truncate(limit);
        Ok(Inspection {
            topics: names,
            events,
        })
    }
}

/// Append the last `limit` records of one partition to `out`.
async fn tail(
    pc: &PartitionClient,
    topic: &str,
    partition: i32,
    limit: usize,
    out: &mut Vec<InspectedEvent>,
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
            let event = match r.record.value {
                None => serde_json::Value::Null,
                Some(v) => serde_json::from_slice(&v)
                    .unwrap_or_else(|_| String::from_utf8_lossy(&v).into_owned().into()),
            };
            out.push(InspectedEvent {
                topic: topic.to_string(),
                partition,
                offset: r.offset,
                timestamp_ms: r.record.timestamp.timestamp_millis(),
                key: r
                    .record
                    .key
                    .map(|k| String::from_utf8_lossy(&k).into_owned()),
                event,
            });
        }
        next = last + 1;
    }
    Ok(())
}
