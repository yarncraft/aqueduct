use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::route::{OutRecord, RoutePlan};
use crate::topic_stats::TopicStats;
use crate::SourceInfo;
use aqueduct_core::{ChangeEvent, Error, Op, Result, Sink};
use async_trait::async_trait;
use rskafka::client::error::{Error as ClientError, ProtocolError};
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder, Credentials, SaslConfig};
use rskafka::record::Record;

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
    /// All changes to a table share a partition (total per-table order).
    Table,
    /// By the whole record key (table and primary key), as Debezium does: rows of one table
    /// spread across partitions, and changes to one row keep their order. Rows of a table
    /// without a primary key share the table's partition.
    #[default]
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

impl PartitionBy {
    /// The config spelling: `key` or `table`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Key => "key",
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

/// Replicas for auto-created topics when `replication_factor` is not configured.
pub const DEFAULT_REPLICATION_FACTOR: i16 = 3;

/// Metadata reads after creating a topic before it counts as not available (200 ms apart).
const CREATE_LIST_ATTEMPTS: u32 = 15;

/// What to do after the first attempt to create a missing topic.
#[derive(Debug, PartialEq, Eq)]
enum AfterCreate {
    /// The topic exists now (or a concurrent create won): wait for it in the metadata.
    Created,
    /// Too few brokers for the default factor: try again with 1.
    FallBack,
    /// The broker refused the configured factor.
    Reject,
    /// Any other failure (no permission, invalid config, broker down): report it.
    Failed,
}

/// What a create-topic request returned, reduced to what [`after_create`] decides on.
#[derive(Debug, Clone, Copy)]
enum CreateOutcome<'a> {
    Ok,
    Protocol(&'a ProtocolError),
    /// Not a broker answer (connection, timeout, ...).
    Other,
}

/// A create refusal that a retry will not change: the name, a create policy or the permissions.
/// A content-routed record that hits one goes to its fallback instead of failing the batch.
fn refusal(outcome: &std::result::Result<(), ClientError>) -> Option<&'static str> {
    match create_outcome(outcome) {
        CreateOutcome::Protocol(ProtocolError::InvalidTopicException) => {
            Some("InvalidTopicException")
        }
        CreateOutcome::Protocol(ProtocolError::PolicyViolation) => Some("PolicyViolation"),
        CreateOutcome::Protocol(ProtocolError::TopicAuthorizationFailed) => {
            Some("TopicAuthorizationFailed")
        }
        CreateOutcome::Protocol(ProtocolError::ClusterAuthorizationFailed) => {
            Some("ClusterAuthorizationFailed")
        }
        _ => None,
    }
}

/// Why a topic could not be opened; `refused` is set for a [`refusal`] of the create.
struct OpenError {
    error: Error,
    refused: Option<&'static str>,
}

impl From<Error> for OpenError {
    fn from(error: Error) -> Self {
        Self {
            error,
            refused: None,
        }
    }
}

fn create_outcome(outcome: &std::result::Result<(), ClientError>) -> CreateOutcome<'_> {
    match outcome {
        Ok(()) => CreateOutcome::Ok,
        Err(ClientError::ServerError { protocol_error, .. }) => {
            CreateOutcome::Protocol(protocol_error)
        }
        Err(_) => CreateOutcome::Other,
    }
}

/// `configured` is the operator's `replication_factor`. Only a success or a `TopicAlreadyExists`
/// race goes on to the re-list; every other error is reported as the broker gave it. Only an unset
/// factor falls back; an explicit one the broker rejects is an error.
fn after_create(configured: Option<i16>, outcome: CreateOutcome<'_>) -> AfterCreate {
    match (outcome, configured) {
        // A timed-out create may still have gone through: the re-list decides.
        (
            CreateOutcome::Ok
            | CreateOutcome::Protocol(
                ProtocolError::TopicAlreadyExists | ProtocolError::RequestTimedOut,
            ),
            _,
        ) => AfterCreate::Created,
        (CreateOutcome::Protocol(ProtocolError::InvalidReplicationFactor), None) => {
            AfterCreate::FallBack
        }
        (CreateOutcome::Protocol(ProtocolError::InvalidReplicationFactor), Some(_)) => {
            AfterCreate::Reject
        }
        _ => AfterCreate::Failed,
    }
}

/// Produces each event as a JSON record to the Kafka topic a [`RoutePlan`] renders it to.
/// The partition is a hash of the record key (or of the table, see [`PartitionBy`]), so
/// changes to one row (or table) keep their order. Topics are
/// opened (and created if missing) on first use. `send` returns once the
/// broker has acknowledged the record, so the pipeline only checkpoints
/// durably produced events.
pub struct KafkaSink {
    client: Client,
    plan: RoutePlan,
    new_topic_partitions: i32,
    replication_factor: Option<i16>,
    topics: HashMap<String, Vec<Arc<PartitionClient>>>,
    /// Create-topic requests sent, for diagnostics and tests.
    create_attempts: AtomicU64,
    /// Records acknowledged per topic, shared with the admin server.
    topic_stats: Arc<TopicStats>,
}

fn err(what: &str, e: &dyn std::fmt::Display) -> Error {
    Error::Sink(format!("kafka {what}: {e}"))
}

impl KafkaSink {
    /// Connect to the brokers. Topics missing at first use are created with
    /// `new_topic_partitions` partitions and `replication_factor` replicas (`None`: try
    /// [`DEFAULT_REPLICATION_FACTOR`], and use 1 with a warning if the cluster is too small).
    pub async fn connect(
        brokers: Vec<String>,
        plan: RoutePlan,
        new_topic_partitions: i32,
        replication_factor: Option<i16>,
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
            replication_factor,
            topics: HashMap::new(),
            create_attempts: AtomicU64::new(0),
            topic_stats: Arc::new(TopicStats::new()),
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
        // The dead-letter topic is opened (created if missing) now, so a missing or unusable one
        // fails startup instead of the first batch with a row that cannot be evaluated.
        if let Some(t) = sink.plan.dead_letter_topic().map(str::to_string) {
            sink.open_topic(&t)
                .await
                .map_err(|e| Error::Sink(format!("sink.dead_letter_topic {t}: {e}")))?;
        }
        // Content routing: the fallbacks take every event a row cannot route, so they must work
        // from the start; and the topics the templates already created count against
        // `max_dynamic_topics`, so the cap holds across restarts.
        let fallbacks: Vec<String> = sink.plan.fallback_topics().map(str::to_string).collect();
        for t in &fallbacks {
            sink.open_topic(t)
                .await
                .map_err(|e| Error::Sink(format!("topic_fallback {t}: {e}")))?;
        }
        if !fallbacks.is_empty() {
            let listed = sink
                .client
                .list_topics()
                .await
                .map_err(|e| err("list topics", &e))?;
            let n = sink
                .plan
                .add_existing_topics(listed.iter().map(|t| t.name.as_str()));
            tracing::info!(
                topics = n,
                "content-routed topics already on the broker (counted against sink.max_dynamic_topics)"
            );
        }
        Ok(sink)
    }

    /// How many create-topic requests this sink has sent.
    pub fn create_attempts(&self) -> u64 {
        self.create_attempts.load(Ordering::Relaxed)
    }

    /// The route plan, e.g. to share its counters and description with the admin server.
    pub fn plan(&self) -> &RoutePlan {
        &self.plan
    }

    /// Records acknowledged per topic; clone the `Arc` to read them from elsewhere.
    pub fn topic_stats(&self) -> Arc<TopicStats> {
        self.topic_stats.clone()
    }

    /// Choose how records are partitioned (default: by key).
    pub fn with_partition_by(mut self, by: PartitionBy) -> Self {
        self.plan.set_partition_by(by);
        self
    }

    /// Name the source in every record's `source` block and key.
    pub fn with_source(mut self, source: SourceInfo) -> Self {
        self.plan.set_source(source);
        self
    }

    async fn partition_ids(&self, topic: &str) -> std::result::Result<Vec<i32>, OpenError> {
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
        let create = |rf: i16| {
            self.create_attempts.fetch_add(1, Ordering::Relaxed);
            ctrl.create_topic(topic, self.new_topic_partitions, rf, 5_000)
        };
        let failed = |outcome: &std::result::Result<(), ClientError>, e: &ClientError| OpenError {
            error: err(&format!("create topic {topic}"), e),
            refused: refusal(outcome),
        };
        let first = self
            .replication_factor
            .unwrap_or(DEFAULT_REPLICATION_FACTOR);
        let outcome = create(first).await;
        match after_create(self.replication_factor, create_outcome(&outcome)) {
            AfterCreate::Created => {}
            AfterCreate::FallBack => {
                tracing::warn!(
                    topic,
                    "cluster has fewer than {DEFAULT_REPLICATION_FACTOR} brokers; creating topic with replication factor 1, set `replication_factor` to choose"
                );
                let retry = create(1).await;
                // The fallback is an explicit factor of 1: anything but success fails.
                if after_create(Some(1), create_outcome(&retry)) != AfterCreate::Created {
                    if let Err(e) = &retry {
                        return Err(failed(&retry, e));
                    }
                }
            }
            AfterCreate::Reject => {
                return Err(Error::Sink(format!(
                    "kafka broker rejected replication_factor = {first} for topic {topic} (fewer than {first} brokers?)"
                ))
                .into());
            }
            AfterCreate::Failed => {
                if let Err(e) = &outcome {
                    return Err(failed(&outcome, e));
                }
            }
        }
        // A new topic can take a moment to show up in the metadata of the broker we ask, so
        // re-list a few times before giving up (startup opens the dead-letter topic this way).
        for attempt in 0..CREATE_LIST_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            let listed = self
                .client
                .list_topics()
                .await
                .map_err(|e| err("list topics", &e))?;
            if let Some(ids) = find(listed) {
                return Ok(ids);
            }
        }
        Err(Error::Sink(format!("kafka topic {topic} not available")).into())
    }

    async fn open_topic(&mut self, topic: &str) -> Result<()> {
        self.try_open_topic(topic).await.map_err(|e| e.error)
    }

    async fn try_open_topic(&mut self, topic: &str) -> std::result::Result<(), OpenError> {
        if self.topics.contains_key(topic) {
            return Ok(());
        }
        let ids = self.partition_ids(topic).await?;
        if ids.is_empty() {
            return Err(Error::Sink(format!("kafka topic {topic} has no partitions")).into());
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
        self.plan.topic_opened(topic);
        Ok(())
    }
}

/// FNV-1a: stable across runs and platforms, unlike `DefaultHasher`.
pub(crate) fn partition_index(key: &str, n: usize) -> usize {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in key.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h % n as u64) as usize
}

/// What decides the partition: the whole record key, or only the table.
pub(crate) fn partition_input(event: &ChangeEvent, key: &str, by: PartitionBy) -> String {
    match by {
        PartitionBy::Key => key.to_owned(),
        PartitionBy::Table => format!("{}.{}", event.schema, event.table),
    }
}

fn record(out: OutRecord) -> Record {
    let mut headers = BTreeMap::new();
    headers.insert("op".to_string(), out.op.as_bytes().to_vec());
    headers.insert("lsn".to_string(), out.lsn.into_bytes());
    for (name, value) in out.headers {
        headers.insert(name.to_string(), value.into_bytes());
    }
    let timestamp = chrono::DateTime::from_timestamp_micros(out.commit_ts_micros)
        .unwrap_or_else(chrono::Utc::now);
    Record {
        key: Some(out.key.into_bytes()),
        value: Some(out.value.into_bytes()),
        headers,
        timestamp,
    }
}

/// Awaits `produce` and, once the broker acknowledged it, counts its `n` records for `topic`.
/// A failed produce counts nothing: its batch is retried and counted then.
async fn counted(
    stats: &TopicStats,
    topic: &str,
    n: u64,
    produce: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    produce.await?;
    stats.acked(topic, n);
    Ok(())
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
        let now_us = chrono::Utc::now().timestamp_micros();
        for event in deliverable(events) {
            rendered.clear();
            self.plan.render_into(event, now_us, &mut rendered)?;
            for mut out in rendered.drain(..) {
                if let Err(e) = self.try_open_topic(&out.topic).await {
                    match (out.content.is_some(), e.refused) {
                        // A content-routed topic the broker will never create: its fallback.
                        (true, Some(kind)) => {
                            self.plan.topic_refused(&mut out, kind);
                            self.open_topic(&out.topic).await?;
                        }
                        // Passing: the batch fails and is retried, without holding a cap slot.
                        (true, None) => {
                            self.plan.topic_failed(&out.topic);
                            return Err(e.error);
                        }
                        (false, _) => return Err(e.error),
                    }
                }
                let part = partition_index(&out.partition_key, self.topics[&out.topic].len());
                groups
                    .entry((out.topic.clone(), part))
                    .or_default()
                    .push(record(out));
            }
        }
        let stats = &*self.topic_stats;
        let sends = groups.into_iter().map(|((topic, part), records)| {
            let client = self.topics[&topic][part].clone();
            async move {
                let n = records.len() as u64;
                let produce = async {
                    client
                        .produce(records, Compression::NoCompression)
                        .await
                        .map(|_| ())
                        .map_err(|e| Error::Sink(format!("kafka produce to {topic}: {e}")))
                };
                counted(stats, &topic, n, produce).await
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
    fn replication_factor_fallback_only_when_unset() {
        use CreateOutcome::{Ok as Created, Protocol};
        let bad = ProtocolError::InvalidReplicationFactor;
        let exists = ProtocolError::TopicAlreadyExists;
        assert_eq!(after_create(None, Protocol(&bad)), AfterCreate::FallBack);
        assert_eq!(after_create(Some(3), Protocol(&bad)), AfterCreate::Reject);
        for cfg in [None, Some(1), Some(3)] {
            assert_eq!(after_create(cfg, Created), AfterCreate::Created);
            assert_eq!(after_create(cfg, Protocol(&exists)), AfterCreate::Created);
        }
    }

    #[test]
    fn create_errors_are_reported_not_hidden_by_the_re_list() {
        use CreateOutcome::{Other, Protocol};
        for e in [
            ProtocolError::TopicAuthorizationFailed,
            ProtocolError::ClusterAuthorizationFailed,
            ProtocolError::InvalidTopicException,
            ProtocolError::PolicyViolation,
        ] {
            for cfg in [None, Some(1), Some(3)] {
                assert_eq!(
                    after_create(cfg, Protocol(&e)),
                    AfterCreate::Failed,
                    "{e:?}"
                );
            }
        }
        assert_eq!(after_create(None, Other), AfterCreate::Failed);
        let timeout = ProtocolError::RequestTimedOut;
        assert_eq!(after_create(None, Protocol(&timeout)), AfterCreate::Created);
        // The fallback create(1) is judged as an explicit factor: a refusal is an error too.
        let bad = ProtocolError::InvalidReplicationFactor;
        assert_eq!(after_create(Some(1), Protocol(&bad)), AfterCreate::Reject);
    }

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

    #[tokio::test]
    async fn only_acknowledged_produces_count_per_topic() {
        let stats = TopicStats::new();
        counted(&stats, "t", 3, async { Ok(()) }).await.unwrap();
        let failed = counted(&stats, "t", 5, async {
            Err(Error::Sink("kafka produce to t: broker down".into()))
        })
        .await;
        assert!(failed.is_err());
        counted(&stats, "u", 0, async { Ok(()) }).await.unwrap();
        let failed_new = counted(&stats, "v", 2, async { Err(Error::Sink("x".into())) }).await;
        assert!(failed_new.is_err());
        assert_eq!(
            stats.snapshot().into_iter().collect::<Vec<_>>(),
            vec![("t".to_string(), 3)]
        );
    }

    #[test]
    fn heartbeats_produce_no_records() {
        let hb = ChangeEvent::heartbeat(aqueduct_core::Position::Text("t".into()), 0);
        let batch = [hb.clone(), crate::sample(), hb];
        assert_eq!(deliverable(&batch).count(), 1);
        assert_eq!(deliverable(&batch[..1]).count(), 0);
    }

    fn pg() -> SourceInfo {
        SourceInfo::Postgres { db: "app".into() }
    }

    /// The record the sink builds for `e` with a one-topic plan.
    fn produced(e: &ChangeEvent) -> Record {
        let mut plan = RoutePlan::new(vec![], Some("t".into())).unwrap();
        plan.set_source(pg());
        record(plan.render(e, 0).unwrap().remove(0))
    }

    #[test]
    fn read_events_get_a_read_op_header() {
        let mut e = crate::sample();
        e.op = Op::Read;
        let r = produced(&e);
        assert_eq!(r.headers["op"], b"r");
        let v: serde_json::Value = serde_json::from_slice(r.value.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "r");
        assert_eq!(v["source"]["snapshot"], "incremental");
    }

    #[test]
    fn record_has_key_headers_and_json_value() {
        let r = produced(&crate::sample());
        assert_eq!(
            r.key.as_deref(),
            Some(&br#"{"db":"app","schema":"public","table":"users","key":{"id":1}}"#[..])
        );
        assert_eq!(r.headers["op"], b"c");
        let v: serde_json::Value = serde_json::from_slice(r.value.as_ref().unwrap()).unwrap();
        assert_eq!(v["source"]["table"], "users");
        assert_eq!(r.headers.len(), 2, "only op and lsn on a normal record");
    }

    #[test]
    fn dead_letter_record_has_the_error_header() {
        let route = crate::KafkaRoute {
            pattern: "public.*".into(),
            topic: "t".into(),
            filter: Some("row.name > 1".into()),
            ..Default::default()
        };
        let mut plan = RoutePlan::with_dead_letter(vec![route], None, Some("dlq".into())).unwrap();
        plan.set_source(pg());
        let r = record(plan.render(&crate::sample(), 0).unwrap().remove(0));
        assert_eq!(
            r.headers["aqueduct-error"],
            b"route `public.*`: where `row.name > 1`: type error: `row.name` is not a finite number"
        );
        assert_eq!(r.headers["op"], b"c");
        assert_eq!(
            r.key.as_deref(),
            Some(&br#"{"db":"app","schema":"public","table":"users","key":null}"#[..])
        );
    }

    #[test]
    fn rows_spread_by_key_unless_partitioned_by_table() {
        assert_eq!(PartitionBy::default(), PartitionBy::Key);
        let e = crate::sample();
        use crate::record_key;
        let parts: std::collections::HashSet<usize> = (0..200)
            .map(|i| {
                let mut e = e.clone();
                e.key = Some(format!(r#"{{"id":{i}}}"#));
                partition_index(
                    &partition_input(&e, &record_key(&e, &pg()), PartitionBy::Key),
                    6,
                )
            })
            .collect();
        assert!(parts.len() > 1, "rows spread over partitions");
        let tables: std::collections::HashSet<usize> = (0..200)
            .map(|i| {
                let mut e = e.clone();
                e.key = Some(format!(r#"{{"id":{i}}}"#));
                partition_index(
                    &partition_input(&e, &record_key(&e, &pg()), PartitionBy::Table),
                    6,
                )
            })
            .collect();
        assert_eq!(tables.len(), 1, "one partition per table");
        assert_eq!("key".parse::<PartitionBy>(), Ok(PartitionBy::Key));
        assert_eq!("table".parse::<PartitionBy>(), Ok(PartitionBy::Table));
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
