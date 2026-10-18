use std::collections::BTreeMap;
use std::sync::Arc;

use aqueduct_core::{ChangeEvent, Error, Result, Sink};
use async_trait::async_trait;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;

/// Produces each event as a JSON record to a Kafka topic. The record key is
/// `schema.table`; the partition is chosen by hashing the key, so changes to one
/// table keep their order. `send` returns once the broker has acknowledged the
/// record, so the pipeline only checkpoints durably produced events.
pub struct KafkaSink {
    topic: String,
    partitions: Vec<Arc<PartitionClient>>,
}

impl KafkaSink {
    /// Connect to the brokers, creating `topic` with `partitions` partitions if
    /// it does not exist yet.
    pub async fn connect(brokers: Vec<String>, topic: String, partitions: i32) -> Result<Self> {
        let err = |what: &str, e: &dyn std::fmt::Display| Error::Sink(format!("kafka {what}: {e}"));
        let client = ClientBuilder::new(brokers)
            .build()
            .await
            .map_err(|e| err("connect", &e))?;

        let existing = client
            .list_topics()
            .await
            .map_err(|e| err("list topics", &e))?
            .into_iter()
            .find(|t| t.name == topic);
        let ids: Vec<i32> = match existing {
            Some(t) => t.partitions.into_iter().collect(),
            None => {
                let ctrl = client
                    .controller_client()
                    .map_err(|e| err("controller", &e))?;
                // Ignore "already exists" races; the re-list below is authoritative.
                let _ = ctrl.create_topic(&topic, partitions.max(1), 1, 5_000).await;
                client
                    .list_topics()
                    .await
                    .map_err(|e| err("list topics", &e))?
                    .into_iter()
                    .find(|t| t.name == topic)
                    .ok_or_else(|| Error::Sink(format!("kafka topic {topic} not available")))?
                    .partitions
                    .into_iter()
                    .collect()
            }
        };
        if ids.is_empty() {
            return Err(Error::Sink(format!(
                "kafka topic {topic} has no partitions"
            )));
        }
        let mut clients = Vec::with_capacity(ids.len());
        for id in ids {
            let pc = client
                .partition_client(&topic, id, UnknownTopicHandling::Retry)
                .await
                .map_err(|e| err("partition client", &e))?;
            clients.push(Arc::new(pc));
        }
        Ok(Self {
            topic,
            partitions: clients,
        })
    }

    fn partition_for(&self, key: &str) -> usize {
        partition_index(key, self.partitions.len())
    }
}

/// FNV-1a: stable across runs and platforms, unlike `DefaultHasher`.
fn partition_index(key: &str, n: usize) -> usize {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in key.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h % n as u64) as usize
}

fn record(event: &ChangeEvent) -> Result<Record> {
    let key = format!("{}.{}", event.schema, event.table);
    let mut headers = BTreeMap::new();
    headers.insert(
        "op".to_string(),
        format!("{:?}", event.op).to_lowercase().into_bytes(),
    );
    headers.insert("lsn".to_string(), event.lsn.0.to_string().into_bytes());
    let timestamp = chrono::DateTime::from_timestamp_micros(event.commit_ts_micros)
        .unwrap_or_else(chrono::Utc::now);
    Ok(Record {
        key: Some(key.into_bytes()),
        value: Some(crate::encode(event)?.into_bytes()),
        headers,
        timestamp,
    })
}

#[async_trait]
impl Sink for KafkaSink {
    async fn send(&mut self, event: &ChangeEvent) -> Result<()> {
        let rec = record(event)?;
        let key = format!("{}.{}", event.schema, event.table);
        let pc = &self.partitions[self.partition_for(&key)];
        pc.produce(vec![rec], Compression::NoCompression)
            .await
            .map_err(|e| Error::Sink(format!("kafka produce to {}: {e}", self.topic)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_is_stable_and_in_range() {
        for k in ["public.users", "public.orders", "sales.items"] {
            let p = partition_index(k, 6);
            assert!(p < 6);
            assert_eq!(p, partition_index(k, 6));
        }
        assert_eq!(partition_index("anything", 1), 0);
    }

    #[test]
    fn record_has_key_headers_and_json_value() {
        let r = record(&crate::sample()).unwrap();
        assert_eq!(r.key.as_deref(), Some(&b"public.users"[..]));
        assert_eq!(r.headers["op"], b"insert");
        let v: serde_json::Value = serde_json::from_slice(r.value.as_ref().unwrap()).unwrap();
        assert_eq!(v["table"], "users");
    }
}
