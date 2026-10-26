//! The compiled routing table and its pure `render` step.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use aqueduct_core::{ChangeEvent, Error, Op, Result};
use serde::Serialize;
use serde_json::value::RawValue;

use super::expr::{EventView, Expr, FieldRef, OP_NAMES, Template, Tri};
use super::fields::{FieldError, FieldOps, FieldSpec, HASH_KEY_ID_HEADER, HashKey};
use crate::kafka::{PartitionBy, glob_match};
use crate::{
    OpCounts, SourceInfo, encode, encode_rows, key_json, key_object, op_code, record_key, table_key,
};

mod build;
mod outbox;
mod render;
mod resolve;
mod shape;
mod stats;
#[cfg(test)]
mod tests;
mod topics;

pub use self::stats::{
    BatchCounts, RouteCount, RouteCounts, RouteDescription, RouteStats, RoutesDescription,
};
pub use self::topics::DEFAULT_MAX_DYNAMIC_TOPICS;
use self::{build::*, outbox::*, resolve::*, shape::*, stats::*, topics::*};

/// Rows of a matched table seen without a field a route references, while the field has never been
/// seen, before the field is reported as unseen (warn-once log, metric, cockpit).
pub const UNSEEN_FIELD_ROWS: u64 = 1000;

/// The label of the `sink.topic` fallback in route stats; no route may use it.
const DEFAULT_ROUTE: &str = "default";

/// Sends events whose `schema.table` matches `pattern` to `topic`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KafkaRoute {
    /// Label in route stats; defaults to `pattern`.
    pub name: Option<String>,
    /// `schema.table` with `*` wildcards, e.g. `public.*`, `*.audit_log`, `*`. Empty for a route
    /// reached only through an earlier route's `otherwise`.
    pub pattern: String,
    /// Destination topic; `{schema}` and `{table}` are substituted.
    pub topic: String,
    /// The `where` predicate (ADR 0002 §3); only events it is definitely true for pass. It filters
    /// operations too: `op in ['insert', 'update']`.
    pub filter: Option<String>,
    /// Top-level columns of `row` that make the record key, in order; overrides `partition_by`.
    pub key: Option<Vec<String>>,
    /// Where events go whose content-routed `topic` cannot be used. Required when `topic` reads
    /// a row field, refused otherwise.
    pub topic_fallback: Option<String>,
    /// The only values a content-routed `topic`'s row fields may substitute.
    pub topic_values: Option<Vec<String>>,
    /// `outbox = {...}`: publish the rows of an outbox table as their own events (ADR 0002 §5).
    /// The route's `topic` must then be empty: the outbox has its own `topic` template.
    pub outbox: Option<OutboxSpec>,
    /// `include` | `exclude`, `mask`, `hash` and `rename` (ADR 0002 §5 "Fields"), applied to
    /// `before`, `after` and, without `key`, the record key's `key` object.
    pub fields: FieldSpec,
    /// `envelope` (the default) or `row`: the record value is the change event envelope, or the
    /// row itself (`after`, after field operations).
    pub format: Option<String>,
    /// With `format = "row"`, what a delete becomes: `tombstone` (the default, a null value),
    /// `drop` (no record) or `row` (the before-image with `"__deleted": true`).
    pub deletes: Option<String>,
    /// Envelope format: after every delete, also a null-value record with the same key, so a
    /// compacted topic forgets the row. Needs a per-row key.
    pub tombstones: bool,
    /// `omit` (the default) or `dead_letter`: what happens to a row the source sent without some
    /// unchanged columns (Postgres TOAST). `dead_letter` sends it to the route's dead-letter topic.
    pub unchanged: Option<String>,
    /// The route's own dead-letter topic, a fixed name. Unset: `sink.dead_letter_topic`, else the
    /// route's topic plus `.dlq` ([`DLQ_SUFFIX`]).
    pub dead_letter_topic: Option<String>,
    /// Where the rows this route's `where` rejects (false or unknown) go instead of being dropped:
    /// a later route without `match`, or a fixed topic. A type error still dead-letters.
    pub otherwise: Option<Otherwise>,
}

/// A route's `otherwise`, `{ route = "<name>" }` or `{ topic = "<name>" }`; serialized the same
/// way in `/api/routes`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Otherwise {
    /// The `name` of a later route without `match`; its `where` decides next.
    Route(String),
    /// A fixed topic, written with this route's key and field operations.
    Topic(String),
}

/// Appended to a route's topic (or its `topic_fallback`, for a content-routed topic) to name its
/// dead-letter topic when neither the route nor the sink sets `dead_letter_topic`.
pub const DLQ_SUFFIX: &str = ".dlq";

/// A row value a content-routed topic never substitutes (the event goes to `topic_fallback`), so
/// no row can render a name ending in [`DLQ_SUFFIX`] from a placeholder.
pub const RESERVED_SEGMENT: &str = "dlq";

/// Debezium's outbox event router defaults, used for every `outbox` key left unset.
pub const OUTBOX_AGGREGATE_TYPE: &str = "aggregatetype";

pub const OUTBOX_AGGREGATE_ID: &str = "aggregateid";

pub const OUTBOX_PAYLOAD: &str = "payload";

pub const OUTBOX_TOPIC: &str = "outbox.event.{aggregate_type}";

/// A route's `outbox = {...}`: column names of the outbox table and the topic template. Only
/// inserts are published: key = the `aggregate_id` value, value = the `payload` value, headers
/// `id` and `type` from those columns when configured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OutboxSpec {
    /// Column the topic template's `{aggregate_type}` reads.
    pub aggregate_type: String,
    /// Column whose value is the record key.
    pub aggregate_id: String,
    /// Column whose value is the record value.
    pub payload: String,
    /// Topic template over the outbox columns: `{aggregate_type}`, `{schema}`, `{table}`,
    /// `{after.<col>}` or `{row.<col>}`.
    pub topic: String,
    /// Column for the `id` header; no header when unset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Column for the `type` header; no header when unset.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

impl Default for OutboxSpec {
    fn default() -> Self {
        Self {
            aggregate_type: OUTBOX_AGGREGATE_TYPE.into(),
            aggregate_id: OUTBOX_AGGREGATE_ID.into(),
            payload: OUTBOX_PAYLOAD.into(),
            topic: OUTBOX_TOPIC.into(),
            id: None,
            kind: None,
        }
    }
}

/// One Kafka record, before it is handed to the client. Topic and key decide the partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutRecord {
    pub topic: String,
    pub key: String,
    /// What the partition is hashed from: the key, or only the table (`partition_by`).
    pub partition_key: PartitionKey,
    /// The JSON event (the envelope), or the transformed value once a route reshapes it
    /// (`format = "row"`). `None` is a tombstone: a record with a null value.
    pub value: Option<String>,
    /// The `op` header (Debezium's letter).
    pub op: &'static str,
    /// The `lsn` header: the checkpoint form of the event position.
    pub lsn: String,
    pub commit_ts_micros: i64,
    /// Headers besides `op` and `lsn`, e.g. `aqueduct-error` on a dead-letter record. Never row
    /// data. Empty (no allocation) on the normal path.
    pub headers: Vec<(&'static str, String)>,
    /// Set when `topic` was chosen from row content: if the broker refuses to create it, the sink
    /// sends the record to this route's fallback instead ([`RoutePlan::topic_refused`]).
    pub content: Option<ContentTopic>,
}

/// What a record's partition is hashed from, without a copy of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionKey {
    /// The record key.
    Key,
    /// `schema.table` (`partition_by = "table"`), shared by every record of the table.
    Table(Arc<str>),
}

impl OutRecord {
    /// What the partition is hashed from: the key, or only the table.
    pub fn partition_input(&self) -> &str {
        match &self.partition_key {
            PartitionKey::Key => &self.key,
            PartitionKey::Table(t) => t,
        }
    }
}

/// The route and fallback of a record whose topic came from row content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentTopic {
    pub route: usize,
    pub fallback: String,
    /// The tombstone that follows a delete (`tombstones = true`): re-routed with the delete, so it
    /// stays on the delete's partition, but not counted again.
    pub tombstone: bool,
}

/// Picks the topic for a table (first matching route wins, else the default) and renders the
/// Kafka record. Compiled once from `[[sink.routes]]`.
#[derive(Debug, Clone)]
pub struct RoutePlan {
    routes: Vec<CompiledRoute>,
    default: Option<TopicTemplate>,
    /// `sink.dead_letter_topic`, the global override.
    dead_letter: Option<String>,
    /// Per route, then the `sink.topic` fallback (as in the stats): its dead-letter topic.
    dead_letters: Vec<Option<DeadLetter>>,
    partition_by: PartitionBy,
    source: SourceInfo,
    stats: Arc<RouteStats>,
    dynamic: Arc<DynamicTopics>,
    /// [`RoutePlan::static_topics`], computed once.
    fixed: Vec<String>,
    /// Present when configured; required as soon as a route uses `hash`.
    hash_key: Option<HashKey>,
    /// Each table's route, matched on its first event.
    resolved: Resolved,
}

impl RoutePlan {
    /// `sink.max_dynamic_topics`: how many content-routed topics may exist, counting the ones
    /// already on the broker ([`RoutePlan::add_existing_topics`]).
    pub fn set_max_dynamic_topics(&mut self, n: usize) {
        self.dynamic.cap.store(n, Ordering::Relaxed);
    }

    /// Counts the topics among `listed` (the broker's topic names) that a content-routed template
    /// could have created: those matching a template's glob, except the configured fixed topics
    /// (fallbacks, `sink.topic`, the dead-letter topic). They count against the cap and stay
    /// usable. Returns how many content-routed topics are counted now.
    pub fn add_existing_topics<'a>(&self, listed: impl IntoIterator<Item = &'a str>) -> usize {
        if !self.routes.iter().any(|r| r.topic.reads_row) {
            return 0;
        }
        // Every listed name is kept for the `.`/`_` collision check; only names a row could
        // actually render count against the cap.
        self.dynamic.exists(
            listed
                .into_iter()
                .map(|t| (t, !self.fixed.iter().any(|f| f == t) && self.producible(t))),
        );
        self.dynamic.len()
    }

    /// True when a content-routed template renders `topic` for some row: every row placeholder
    /// matches an allowed segment. Topics a `{schema}`/`{table}` template could write are not
    /// content-routed (startup rejects templates whose globs overlap, so this is a safeguard).
    fn producible(&self, topic: &str) -> bool {
        let plain = self
            .routes
            .iter()
            .map(|r| &r.topic)
            .chain(self.default.as_ref())
            .filter(|t| !t.reads_row && !t.is_static());
        if plain.into_iter().any(|t| glob_match(&t.glob(), topic)) {
            return false;
        }
        self.routes
            .iter()
            .filter(|r| r.topic.reads_row)
            .any(|r| r.topic.parsed.matches(topic, |f, v| value_allowed(r, f, v)))
    }

    /// Content-routed topics counted against `sink.max_dynamic_topics`.
    pub fn dynamic_topic_count(&self) -> usize {
        self.dynamic.len()
    }

    /// Every fixed topic name in the plan: static route topics, a static `sink.topic`, the
    /// fallbacks and the fixed dead-letter topics.
    fn static_topics(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let fixed = self
            .routes
            .iter()
            .map(|r| &r.topic)
            .chain(self.default.as_ref())
            .filter(|t| t.is_static())
            .map(|t| t.raw.as_str())
            .chain(self.fallback_topics())
            .chain(
                self.routes
                    .iter()
                    .filter_map(CompiledRoute::otherwise_topic),
            )
            .chain(
                self.dead_letters
                    .iter()
                    .flatten()
                    .filter_map(DeadLetter::fixed),
            )
            .chain(self.dead_letter.as_deref());
        for t in fixed {
            if !out.iter().any(|o| o == t) {
                out.push(t.to_string());
            }
        }
        out
    }

    /// The fixed dead-letter topics of the routes built to dead-letter rows, which the sink opens
    /// at startup, so a missing or unwritable one fails startup rather than the first bad row.
    /// Templated ones (`cdc.{schema}.{table}.dlq`) are created on first use, like their topics.
    pub fn dead_letter_topics(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for (r, d) in self.routes.iter().zip(&self.dead_letters) {
            if let Some(t) = d.as_ref().and_then(DeadLetter::fixed)
                && r.expects_dead_letters()
                && !out.iter().any(|o| o == t)
            {
                out.push(t.to_string());
            }
        }
        out
    }

    /// The routes' `topic_fallback`s, which the sink opens at startup.
    pub fn fallback_topics(&self) -> impl Iterator<Item = &str> {
        self.routes
            .iter()
            .filter_map(|r| r.topic_fallback.as_deref())
    }

    /// How the record key, and so the partition, is chosen.
    pub fn set_partition_by(&mut self, by: PartitionBy) {
        self.partition_by = by;
    }

    /// The startup checks that depend on `partition_by`, which is set after [`build`](Self::build):
    /// a route whose deletes produce a null-value record (`tombstones = true`, or
    /// `format = "row"` with `deletes = "tombstone"`, the default) needs a per-row key,
    /// `key = [...]` or `partition_by = "key"`, so a tombstone never stands for more than one
    /// row. At render time [`key_is_per_row`](Self::key_is_per_row) checks each delete as well.
    pub fn check_partition_by(&self, by: PartitionBy) -> Result<()> {
        if by == PartitionBy::Key {
            return Ok(());
        }
        for (r, name) in self.routes.iter().zip(&self.stats.names) {
            if r.shape.null_value_on_delete() && r.key.is_none() {
                let what = if r.shape.tombstones {
                    "`tombstones`"
                } else {
                    "`format = \"row\"` with `deletes = \"tombstone\"` (the default)"
                };
                return Err(Error::Config(format!(
                    "sink route `{name}` uses {what}, which needs a per-row key: set `key = [...]` on the route, or `partition_by = \"key\"` (or use `deletes = \"drop\"` or `\"row\"`)"
                )));
            }
        }
        Ok(())
    }

    /// The source named in every record's `source` block and key.
    pub fn set_source(&mut self, source: SourceInfo) {
        self.source = source;
    }

    /// Counters per route; clone the `Arc` to read them from elsewhere.
    pub fn stats(&self) -> Arc<RouteStats> {
        self.stats.clone()
    }

    /// The `sink.topic` fallback as configured, placeholders included.
    pub fn default_topic(&self) -> Option<&str> {
        self.default.as_ref().map(|t| t.raw.as_str())
    }

    /// True when a route uses `outbox`, whose records carry `id` and `type` headers.
    pub fn has_outbox(&self) -> bool {
        self.routes.iter().any(|r| r.outbox.is_some())
    }

    /// `sink.dead_letter_topic`, the global override, if configured.
    pub fn dead_letter_topic(&self) -> Option<&str> {
        self.dead_letter.as_deref()
    }

    /// The dead-letter topic of the route at stats index `i`, or its template.
    fn dead_letter_name(&self, i: usize) -> Option<String> {
        self.dead_letters
            .get(i)
            .and_then(Option::as_ref)
            .map(|d| d.name().to_string())
    }
}
