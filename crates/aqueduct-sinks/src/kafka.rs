use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use aqueduct_core::{ChangeEvent, Error, Result, Sink};
use async_trait::async_trait;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;

/// Sends events whose `schema.table` matches `pattern` to `topic`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaRoute {
    /// `schema.table` with `*` wildcards, e.g. `public.*`, `*.audit_log`, `*`.
    pub pattern: String,
    /// Destination topic; `{schema}` and `{table}` are substituted.
    pub topic: String,
}

/// Picks the topic for a table: first matching route wins, else `default`.
#[derive(Debug, Clone)]
pub struct TopicRouter {
    routes: Vec<KafkaRoute>,
    default: Option<String>,
}

impl TopicRouter {
    pub fn new(routes: Vec<KafkaRoute>, default: Option<String>) -> Result<Self> {
        if routes.is_empty() && default.is_none() {
            return Err(Error::Config(
                "kafka sink needs `topic` and/or at least one route".into(),
            ));
        }
        Ok(Self { routes, default })
    }

    pub fn topic_for(&self, schema: &str, table: &str) -> Option<String> {
        let name = format!("{schema}.{table}");
        let template = self
            .routes
            .iter()
            .find(|r| glob_match(&r.pattern, &name))
            .map(|r| r.topic.as_str())
            .or(self.default.as_deref())?;
        Some(
            template
                .replace("{schema}", schema)
                .replace("{table}", table),
        )
    }
}

/// `*` matches any run of characters (including none and dots); everything else is literal.
fn glob_match(pattern: &str, text: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == text;
    }
    let (first, last) = (parts[0], parts[parts.len() - 1]);
    if !text.starts_with(first) || text.len() < first.len() + last.len() || !text.ends_with(last) {
        return false;
    }
    let mut rest = &text[first.len()..text.len() - last.len()];
    for mid in &parts[1..parts.len() - 1] {
        match rest.find(mid) {
            Some(i) => rest = &rest[i + mid.len()..],
            None => return false,
        }
    }
    true
}

/// Produces each event as a JSON record to a Kafka topic chosen by a
/// [`TopicRouter`]. The record key is `schema.table`; the partition is chosen
/// by hashing the key, so changes to one table keep their order. Topics are
/// opened (and created if missing) on first use. `send` returns once the
/// broker has acknowledged the record, so the pipeline only checkpoints
/// durably produced events.
pub struct KafkaSink {
    client: Client,
    router: TopicRouter,
    new_topic_partitions: i32,
    topics: HashMap<String, Vec<Arc<PartitionClient>>>,
}

fn err(what: &str, e: &dyn std::fmt::Display) -> Error {
    Error::Sink(format!("kafka {what}: {e}"))
}

impl KafkaSink {
    /// Connect to the brokers. Topics missing at first use are created with
    /// `new_topic_partitions` partitions.
    pub async fn connect(
        brokers: Vec<String>,
        router: TopicRouter,
        new_topic_partitions: i32,
    ) -> Result<Self> {
        let client = ClientBuilder::new(brokers)
            .build()
            .await
            .map_err(|e| err("connect", &e))?;
        let mut sink = Self {
            client,
            router,
            new_topic_partitions: new_topic_partitions.max(1),
            topics: HashMap::new(),
        };
        // Fail fast on a bad broker/topic: open the default topic now (a templated
        // default like `cdc.{table}` only resolves per event, so it opens on first use).
        if let Some(t) = sink.router.default.clone().filter(|t| !t.contains('{')) {
            sink.open_topic(&t).await?;
        }
        Ok(sink)
    }

    async fn partition_ids(&self, topic: &str) -> Result<Vec<i32>> {
        let find = |topics: Vec<rskafka::topic::Topic>| {
            topics
                .into_iter()
                .find(|t| t.name == topic)
                .map(|t| t.partitions.into_iter().collect::<Vec<i32>>())
        };
        let listed = self
            .client
            .list_topics()
            .await
            .map_err(|e| err("list topics", &e))?;
        if let Some(ids) = find(listed) {
            return Ok(ids);
        }
        let ctrl = self
            .client
            .controller_client()
            .map_err(|e| err("controller", &e))?;
        // Ignore "already exists" races; the re-list below is authoritative.
        let _ = ctrl
            .create_topic(topic, self.new_topic_partitions, 1, 5_000)
            .await;
        let listed = self
            .client
            .list_topics()
            .await
            .map_err(|e| err("list topics", &e))?;
        find(listed).ok_or_else(|| Error::Sink(format!("kafka topic {topic} not available")))
    }

    async fn open_topic(&mut self, topic: &str) -> Result<()> {
        if self.topics.contains_key(topic) {
            return Ok(());
        }
        let ids = self.partition_ids(topic).await?;
        if ids.is_empty() {
            return Err(Error::Sink(format!(
                "kafka topic {topic} has no partitions"
            )));
        }
        let mut clients = Vec::with_capacity(ids.len());
        for id in ids {
            let pc = self
                .client
                .partition_client(topic, id, UnknownTopicHandling::Retry)
                .await
                .map_err(|e| err("partition client", &e))?;
            clients.push(Arc::new(pc));
        }
        self.topics.insert(topic.to_string(), clients);
        Ok(())
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
        let topic = self
            .router
            .topic_for(&event.schema, &event.table)
            .ok_or_else(|| {
                Error::Sink(format!(
                    "no kafka route or default topic for {}.{}",
                    event.schema, event.table
                ))
            })?;
        self.open_topic(&topic).await?;
        let rec = record(event)?;
        let key = format!("{}.{}", event.schema, event.table);
        let parts = &self.topics[&topic];
        parts[partition_index(&key, parts.len())]
            .produce(vec![rec], Compression::NoCompression)
            .await
            .map_err(|e| Error::Sink(format!("kafka produce to {topic}: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(p: &str, t: &str) -> KafkaRoute {
        KafkaRoute {
            pattern: p.into(),
            topic: t.into(),
        }
    }

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

    #[test]
    fn glob_matching() {
        assert!(glob_match("public.users", "public.users"));
        assert!(!glob_match("public.users", "public.users2"));
        assert!(glob_match("*", "a.b"));
        assert!(glob_match("public.*", "public.orders"));
        assert!(!glob_match("public.*", "sales.orders"));
        assert!(glob_match("*.audit_*", "hr.audit_log"));
        assert!(glob_match("a*b*c", "aXXbYYc"));
        assert!(!glob_match("a*b*c", "aXXcYYb"));
        assert!(!glob_match("ab*ba", "aba"));
    }

    #[test]
    fn first_match_wins_then_default_with_placeholders() {
        let r = TopicRouter::new(
            vec![
                route("public.orders", "orders-events"),
                route("public.*", "pub.{table}"),
                route("*.audit_*", "audit"),
            ],
            Some("cdc.{schema}.{table}".into()),
        )
        .unwrap();
        assert_eq!(r.topic_for("public", "orders").unwrap(), "orders-events");
        assert_eq!(r.topic_for("public", "users").unwrap(), "pub.users");
        assert_eq!(r.topic_for("hr", "audit_log").unwrap(), "audit");
        assert_eq!(r.topic_for("hr", "staff").unwrap(), "cdc.hr.staff");
    }

    #[test]
    fn unrouted_event_without_default_has_no_topic() {
        let r = TopicRouter::new(vec![route("public.*", "p")], None).unwrap();
        assert_eq!(r.topic_for("sales", "x"), None);
        assert!(TopicRouter::new(vec![], None).is_err());
    }
}
