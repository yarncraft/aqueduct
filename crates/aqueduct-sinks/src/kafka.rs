use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use aqueduct_core::{ChangeEvent, Error, Op, Result, Sink};
use async_trait::async_trait;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder, Credentials, SaslConfig};
use rskafka::record::Record;

use crate::route::{OutRecord, RoutePlan};

/// SASL mechanism for broker authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl std::str::FromStr for SaslMechanism {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "plain" => Ok(Self::Plain),
            "scram-sha-256" => Ok(Self::ScramSha256),
            "scram-sha-512" => Ok(Self::ScramSha512),
            o => Err(format!(
                "unknown sasl mechanism `{o}` (expected plain, scram-sha-256 or scram-sha-512)"
            )),
        }
    }
}

/// Broker credentials. The password is never included in `Debug` output.
#[derive(Clone)]
pub struct KafkaSasl {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for KafkaSasl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSasl")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// Transport security for the broker connection.
#[derive(Debug, Clone, Default)]
pub struct KafkaSecurity {
    /// Encrypt with TLS and verify the broker certificate and hostname.
    pub tls: bool,
    /// PEM CA bundle; defaults to the bundled public roots.
    pub tls_ca_file: Option<String>,
    pub sasl: Option<KafkaSasl>,
}

impl KafkaSecurity {
    pub fn apply(&self, mut builder: ClientBuilder) -> Result<ClientBuilder> {
        if self.sasl.is_some() && !self.tls {
            tracing::warn!(
                "kafka SASL without TLS: credentials are sent unencrypted; set tls = true"
            );
        }
        if self.tls {
            let cfg = aqueduct_tls::client_config(
                self.tls_ca_file.as_deref(),
                aqueduct_tls::Verify::Full,
            )
            .map_err(Error::Config)?;
            builder = builder.tls_config(cfg);
        }
        if let Some(s) = &self.sasl {
            let creds = Credentials::new(s.username.clone(), s.password.clone());
            builder = builder.sasl_config(match s.mechanism {
                SaslMechanism::Plain => SaslConfig::Plain(creds),
                SaslMechanism::ScramSha256 => SaslConfig::ScramSha256(creds),
                SaslMechanism::ScramSha512 => SaslConfig::ScramSha512(creds),
            });
        }
        Ok(builder)
    }
}

/// How the record key, and so the partition, is chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionBy {
    /// Key `schema.table`: all changes to a table share a partition (total per-table order).
    #[default]
    Table,
    /// Key `schema.table` plus the primary-key values: rows of one table spread across
    /// partitions, and changes to one row keep their order. Tables without a key fall back
    /// to `Table`.
    Key,
}

impl std::str::FromStr for PartitionBy {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "table" => Ok(Self::Table),
            "key" => Ok(Self::Key),
            o => Err(format!(
                "unknown partition_by `{o}` (expected table or key)"
            )),
        }
    }
}

/// `*` matches any run of characters (including none and dots); everything else is literal.
pub fn glob_match(pattern: &str, text: &str) -> bool {
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
/// [`RoutePlan`]. The record key is `schema.table`; the partition is chosen
/// by hashing the key, so changes to one table keep their order. Topics are
/// opened (and created if missing) on first use. `send` returns once the
/// broker has acknowledged the record, so the pipeline only checkpoints
/// durably produced events.
pub struct KafkaSink {
    client: Client,
    plan: RoutePlan,
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
        plan: RoutePlan,
        new_topic_partitions: i32,
        security: &KafkaSecurity,
    ) -> Result<Self> {
        let client = security
            .apply(ClientBuilder::new(brokers))?
            .build()
            .await
            .map_err(|e| err("connect", &e))?;
        let mut sink = Self {
            client,
            plan,
            new_topic_partitions: new_topic_partitions.max(1),
            topics: HashMap::new(),
        };
        // Fail fast on a bad broker/topic: open the default topic now (a templated
        // default like `cdc.{table}` only resolves per event, so it opens on first use).
        if let Some(t) = sink
            .plan
            .default_topic()
            .map(str::to_string)
            .filter(|t| !t.contains('{'))
        {
            sink.open_topic(&t).await?;
        }
        Ok(sink)
    }

    /// Choose how records are keyed and partitioned (default: by table).
    pub fn with_partition_by(mut self, by: PartitionBy) -> Self {
        self.plan.set_partition_by(by);
        self
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

/// Record key: `schema.table`, plus `:` and the primary-key values when partitioning by key.
pub(crate) fn record_key(event: &ChangeEvent, by: PartitionBy) -> String {
    match (by, &event.key) {
        (PartitionBy::Key, Some(k)) => format!("{}.{}:{k}", event.schema, event.table),
        _ => format!("{}.{}", event.schema, event.table),
    }
}

fn record(out: OutRecord) -> Record {
    let mut headers = BTreeMap::new();
    headers.insert("op".to_string(), out.op.into_bytes());
    headers.insert("lsn".to_string(), out.lsn.into_bytes());
    let timestamp = chrono::DateTime::from_timestamp_micros(out.commit_ts_micros)
        .unwrap_or_else(chrono::Utc::now);
    Record {
        key: Some(out.key.into_bytes()),
        value: Some(out.value.into_bytes()),
        headers,
        timestamp,
    }
}

/// The events that become Kafka records: heartbeats carry no row and produce nothing.
fn deliverable(events: &[ChangeEvent]) -> impl Iterator<Item = &ChangeEvent> {
    events.iter().filter(|e| e.op != Op::Heartbeat)
}

#[async_trait]
impl Sink for KafkaSink {
    async fn send(&mut self, event: &ChangeEvent) -> Result<()> {
        self.send_batch(std::slice::from_ref(event)).await
    }

    /// Group the batch by destination partition (keeping event order inside each) and
    /// produce every group with one request, all partitions in parallel. Returns once the
    /// broker has acknowledged all of them.
    async fn send_batch(&mut self, events: &[ChangeEvent]) -> Result<()> {
        let mut groups: HashMap<(String, usize), Vec<Record>> = HashMap::new();
        let mut rendered = Vec::new();
        for event in deliverable(events) {
            rendered.clear();
            self.plan.render_into(event, &mut rendered)?;
            for out in rendered.drain(..) {
                self.open_topic(&out.topic).await?;
                let part = partition_index(&out.key, self.topics[&out.topic].len());
                groups
                    .entry((out.topic.clone(), part))
                    .or_default()
                    .push(record(out));
            }
        }
        let sends = groups.into_iter().map(|((topic, part), records)| {
            let client = self.topics[&topic][part].clone();
            async move {
                client
                    .produce(records, Compression::NoCompression)
                    .await
                    .map(|_| ())
                    .map_err(|e| Error::Sink(format!("kafka produce to {topic}: {e}")))
            }
        });
        futures::future::try_join_all(sends).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sasl_debug_redacts_password() {
        let s = KafkaSasl {
            mechanism: SaslMechanism::ScramSha512,
            username: "svc".into(),
            password: "hunter2".into(),
        };
        let d = format!("{s:?}");
        assert!(d.contains("svc") && !d.contains("hunter2"));
    }

    #[test]
    fn sasl_mechanism_parsing() {
        assert_eq!(
            "SCRAM-SHA-256".parse::<SaslMechanism>(),
            Ok(SaslMechanism::ScramSha256)
        );
        assert!("gssapi".parse::<SaslMechanism>().is_err());
    }

    #[test]
    fn security_builds_with_tls_and_sasl() {
        let sec = KafkaSecurity {
            tls: true,
            tls_ca_file: None,
            sasl: Some(KafkaSasl {
                mechanism: SaslMechanism::Plain,
                username: "u".into(),
                password: "p".into(),
            }),
        };
        assert!(sec.apply(ClientBuilder::new(vec!["b:9093".into()])).is_ok());
        let bad = KafkaSecurity {
            tls: true,
            tls_ca_file: Some("/nope.pem".into()),
            sasl: None,
        };
        assert!(bad
            .apply(ClientBuilder::new(vec!["b:9093".into()]))
            .is_err());
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
    fn heartbeats_produce_no_records() {
        let hb = ChangeEvent::heartbeat(aqueduct_core::Position::Text("t".into()), 0);
        let batch = [hb.clone(), crate::sample(), hb];
        assert_eq!(deliverable(&batch).count(), 1);
        assert_eq!(deliverable(&batch[..1]).count(), 0);
    }

    #[test]
    fn record_has_key_headers_and_json_value() {
        let e = crate::sample();
        let plan = RoutePlan::new(vec![], Some("t".into())).unwrap();
        let r = record(plan.render(&e).unwrap().remove(0));
        assert_eq!(r.key.as_deref(), Some(&b"public.users"[..]));
        assert_eq!(r.headers["op"], b"insert");
        let v: serde_json::Value = serde_json::from_slice(r.value.as_ref().unwrap()).unwrap();
        assert_eq!(v["table"], "users");
    }

    #[test]
    fn key_mode_adds_primary_key_and_spreads_rows() {
        let mut e = crate::sample();
        assert_eq!(record_key(&e, PartitionBy::Key), "public.users:1");
        e.key = None;
        assert_eq!(record_key(&e, PartitionBy::Key), "public.users");
        let parts: std::collections::HashSet<usize> = (0..200)
            .map(|i| partition_index(&format!("public.users:{i}"), 6))
            .collect();
        assert!(parts.len() > 1);
        assert_eq!("key".parse::<PartitionBy>(), Ok(PartitionBy::Key));
        assert!("x".parse::<PartitionBy>().is_err());
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
}
