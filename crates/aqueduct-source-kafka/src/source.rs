use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use aqueduct_core::{ChangeEvent, Error, Op, Position, Result, Source};
use aqueduct_sinks::{glob_match, KafkaSecurity};
use async_trait::async_trait;
use rskafka::client::partition::{OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::record::RecordAndOffset;
use serde_json::value::RawValue;

/// Byte window requested per partition per fetch.
const FETCH_BYTES: std::ops::Range<i32> = 1..4 * 1024 * 1024;
/// How long a fetch waits for data before returning empty; bounds idle shutdown latency.
const FETCH_WAIT_MS: i32 = 500;

/// Where to begin a partition that has no checkpoint yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StartAt {
    /// Read the whole retained log (default: nothing is skipped).
    #[default]
    Earliest,
    /// Only records produced after the source starts.
    Latest,
}

impl std::str::FromStr for StartAt {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "earliest" => Ok(Self::Earliest),
            "latest" => Ok(Self::Latest),
            o => Err(format!(
                "unknown start_at `{o}` (expected earliest or latest)"
            )),
        }
    }
}

/// How record values are turned into [`ChangeEvent`]s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Format {
    /// A value that is an Aqueduct `ChangeEvent` JSON is passed on as is; anything else is
    /// wrapped like `Raw`.
    #[default]
    Auto,
    /// Always wrap: `schema = "kafka"`, `table = <topic>`, `op = insert`, `after = <value>`.
    Raw,
}

impl std::str::FromStr for Format {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "raw" => Ok(Self::Raw),
            o => Err(format!("unknown format `{o}` (expected auto or raw)")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct KafkaSourceConfig {
    pub brokers: Vec<String>,
    /// Topic names; `*` matches any run of characters.
    pub topics: Vec<String>,
    pub security: KafkaSecurity,
    pub start_at: StartAt,
    pub format: Format,
}

struct Part {
    topic: String,
    partition: i32,
    client: Arc<PartitionClient>,
    /// Next offset to read.
    next: i64,
}

pub struct KafkaSource {
    cfg: KafkaSourceConfig,
    parts: Vec<Part>,
    /// Next offset per `topic:partition`: the checkpoint position.
    offsets: BTreeMap<String, i64>,
    buffer: VecDeque<ChangeEvent>,
}

fn err(what: &str, e: &dyn std::fmt::Display) -> Error {
    Error::Source(format!("kafka {what}: {e}"))
}

fn part_key(topic: &str, partition: i32) -> String {
    format!("{topic}:{partition}")
}

impl KafkaSource {
    pub fn new(cfg: KafkaSourceConfig) -> Self {
        Self {
            cfg,
            parts: Vec::new(),
            offsets: BTreeMap::new(),
            buffer: VecDeque::new(),
        }
    }

    fn position(&self) -> Position {
        // invariant: a BTreeMap<String, i64> always serialises to JSON.
        #[allow(clippy::expect_used)]
        Position::Text(serde_json::to_string(&self.offsets).expect("offset map serialises"))
    }

    fn convert(&self, topic: &str, r: &RecordAndOffset) -> ChangeEvent {
        let value = r.record.value.as_deref().unwrap_or(&[]);
        let mut event = match self.cfg.format {
            Format::Auto => serde_json::from_slice::<ChangeEvent>(value).ok(),
            Format::Raw => None,
        }
        .unwrap_or_else(|| wrap(topic, value, r.record.timestamp.timestamp_micros()));
        if event.key.is_none() {
            event.key = r
                .record
                .key
                .as_deref()
                .map(|k| String::from_utf8_lossy(k).into_owned());
        }
        event
    }

    /// Fetch every partition concurrently and buffer the records, advancing the offsets.
    async fn fill(&mut self) -> Result<()> {
        let fetches = self.parts.iter().map(|p| {
            let client = p.client.clone();
            let (topic, partition, next) = (p.topic.clone(), p.partition, p.next);
            async move {
                client
                    .fetch_records(next, FETCH_BYTES, FETCH_WAIT_MS)
                    .await
                    .map_err(|e| err(&format!("fetch {topic}/{partition}"), &e))
            }
        });
        let results = futures::future::join_all(fetches).await;
        for (i, res) in results.into_iter().enumerate() {
            let (records, _) = res?;
            for r in records {
                if r.offset < self.parts[i].next {
                    continue;
                }
                let mut event = self.convert(&self.parts[i].topic, &r);
                self.parts[i].next = r.offset + 1;
                self.offsets.insert(
                    part_key(&self.parts[i].topic, self.parts[i].partition),
                    r.offset + 1,
                );
                event.position = self.position();
                self.buffer.push_back(event);
            }
        }
        Ok(())
    }
}

/// Wrap an arbitrary record value: JSON passes through unparsed, other bytes become a string.
fn wrap(topic: &str, value: &[u8], ts_micros: i64) -> ChangeEvent {
    let after = std::str::from_utf8(value)
        .ok()
        .and_then(|s| RawValue::from_string(s.to_string()).ok())
        .or_else(|| {
            let s = serde_json::to_string(&String::from_utf8_lossy(value)).ok()?;
            RawValue::from_string(s).ok()
        });
    ChangeEvent {
        position: Position::Lsn(0),
        xid: 0,
        commit_ts_micros: ts_micros,
        schema: "kafka".into(),
        table: topic.into(),
        op: Op::Insert,
        before: None,
        after,
        key: None,
    }
}

#[async_trait]
impl Source for KafkaSource {
    async fn start(&mut self, start: Option<Position>) -> Result<()> {
        let saved: BTreeMap<String, i64> = match start {
            None => BTreeMap::new(),
            Some(Position::Text(t)) => serde_json::from_str(&t)
                .map_err(|e| Error::Checkpoint(format!("kafka checkpoint {t:?}: {e}")))?,
            Some(Position::Lsn(_)) => {
                return Err(Error::Checkpoint(
                    "checkpoint is a Postgres LSN; it belongs to another source type".into(),
                ))
            }
        };
        let client = self
            .cfg
            .security
            .apply(ClientBuilder::new(self.cfg.brokers.clone()))?
            .build()
            .await
            .map_err(|e| err("connect", &e))?;
        let mut topics: Vec<_> = client
            .list_topics()
            .await
            .map_err(|e| err("list topics", &e))?
            .into_iter()
            .filter(|t| self.cfg.topics.iter().any(|g| glob_match(g, &t.name)))
            .collect();
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        if topics.is_empty() {
            return Err(Error::Source(format!(
                "no Kafka topic matches {:?}",
                self.cfg.topics
            )));
        }
        for t in topics {
            for &p in &t.partitions {
                let pc = Arc::new(
                    client
                        .partition_client(&t.name, p, UnknownTopicHandling::Error)
                        .await
                        .map_err(|e| err(&format!("partition client {}/{p}", t.name), &e))?,
                );
                let earliest = pc
                    .get_offset(OffsetAt::Earliest)
                    .await
                    .map_err(|e| err("offsets", &e))?;
                let latest = pc
                    .get_offset(OffsetAt::Latest)
                    .await
                    .map_err(|e| err("offsets", &e))?;
                let key = part_key(&t.name, p);
                let next = match saved.get(&key) {
                    Some(&n) if n > latest => {
                        return Err(Error::Source(format!(
                            "checkpoint for {key} is offset {n} but the log ends at {latest} \
                             (topic recreated?); remove the checkpoint to start over"
                        )))
                    }
                    Some(&n) if n < earliest => {
                        tracing::warn!(
                            partition = %key, wanted = n, earliest,
                            "checkpointed offset was deleted by retention; starting at the earliest"
                        );
                        earliest
                    }
                    Some(&n) => n,
                    None => match self.cfg.start_at {
                        StartAt::Earliest => earliest,
                        StartAt::Latest => latest,
                    },
                };
                self.offsets.insert(key, next);
                self.parts.push(Part {
                    topic: t.name.clone(),
                    partition: p,
                    client: pc,
                    next,
                });
            }
        }
        tracing::info!(partitions = self.parts.len(), "kafka source started");
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<ChangeEvent>> {
        loop {
            if let Some(ev) = self.buffer.pop_front() {
                return Ok(Some(ev));
            }
            self.fill().await?;
        }
    }

    fn try_next(&mut self) -> Option<ChangeEvent> {
        self.buffer.pop_front()
    }

    /// Offsets live in Aqueduct's checkpoint, not on the broker, so there is nothing to commit.
    async fn ack(&mut self, _position: Position) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rskafka::record::Record;

    fn rec(value: &str, key: Option<&str>, offset: i64) -> RecordAndOffset {
        RecordAndOffset {
            record: Record {
                key: key.map(|k| k.as_bytes().to_vec()),
                value: Some(value.as_bytes().to_vec()),
                headers: Default::default(),
                timestamp: Default::default(),
            },
            offset,
        }
    }

    fn src(format: Format) -> KafkaSource {
        KafkaSource::new(KafkaSourceConfig {
            brokers: vec![],
            topics: vec![],
            security: Default::default(),
            start_at: StartAt::Earliest,
            format,
        })
    }

    #[test]
    fn json_value_is_wrapped_unparsed_with_record_key() {
        let ev = src(Format::Raw).convert("orders", &rec(r#"{"a":1}"#, Some("k1"), 5));
        assert_eq!((ev.schema.as_str(), ev.table.as_str()), ("kafka", "orders"));
        assert_eq!(ev.after.unwrap().get(), r#"{"a":1}"#);
        assert_eq!(ev.key.as_deref(), Some("k1"));
    }

    #[test]
    fn non_json_value_becomes_a_string() {
        let ev = src(Format::Auto).convert("t", &rec("hello", None, 0));
        assert_eq!(ev.after.unwrap().get(), r#""hello""#);
    }

    #[test]
    fn auto_passes_through_an_aqueduct_event() {
        let json = r#"{"lsn":42,"xid":1,"commit_ts_micros":7,"schema":"public","table":"users","op":"update","before":null,"after":{"id":1}}"#;
        let ev = src(Format::Auto).convert("t", &rec(json, None, 0));
        assert_eq!((ev.schema.as_str(), ev.table.as_str()), ("public", "users"));
        assert_eq!(ev.op, Op::Update);
        // Raw ignores the shape and wraps it.
        let raw = src(Format::Raw).convert("t", &rec(json, None, 0));
        assert_eq!(raw.schema, "kafka");
    }

    #[test]
    fn position_is_an_offset_map_that_roundtrips() {
        let mut s = src(Format::Auto);
        s.offsets.insert("a:0".into(), 3);
        s.offsets.insert("a:1".into(), 9);
        let Position::Text(t) = s.position() else {
            panic!("text position")
        };
        let back: BTreeMap<String, i64> = serde_json::from_str(&t).unwrap();
        assert_eq!(back["a:1"], 9);
    }
}
