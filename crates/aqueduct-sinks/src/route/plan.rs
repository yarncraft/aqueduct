//! The compiled routing table and its pure `render` step.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use aqueduct_core::{ChangeEvent, Error, Op, Result};
use serde::Serialize;

use super::expr::{op_name, EventView, Expr, FieldRef, Template, Tri};
use crate::kafka::{glob_match, partition_input, PartitionBy};
use crate::{encode, key_json, op_code, record_key, table_key, SourceInfo};

/// Rows of a matched table seen without a field a route references, while the field has never been
/// seen, before the field is reported as unseen (warn-once log, metric, cockpit).
pub const UNSEEN_FIELD_ROWS: u64 = 1000;

/// The label of the `sink.topic` fallback in route stats; no route may use it.
const DEFAULT_ROUTE: &str = "default";

/// The ops a route's `ops` list may name, in the order the docs give them.
const OPS: [Op; 5] = [Op::Insert, Op::Update, Op::Delete, Op::Truncate, Op::Read];

/// Sends events whose `schema.table` matches `pattern` to `topic`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KafkaRoute {
    /// Label in route stats; defaults to `pattern`.
    pub name: Option<String>,
    /// `schema.table` with `*` wildcards, e.g. `public.*`, `*.audit_log`, `*`.
    pub pattern: String,
    /// Destination topic; `{schema}` and `{table}` are substituted.
    pub topic: String,
    /// Only these ops pass (`insert`, `update`, `delete`, `truncate`, `read`); `None` passes all.
    pub ops: Option<Vec<String>>,
    /// The `where` predicate (ADR 0002 §3); only events it is definitely true for pass.
    pub filter: Option<String>,
    /// Top-level columns of `row` that make the record key, in order; overrides `partition_by`.
    pub key: Option<Vec<String>>,
    /// Where events go whose content-routed `topic` cannot be used. Required when `topic` reads
    /// a row field, refused otherwise.
    pub topic_fallback: Option<String>,
    /// The only values a content-routed `topic`'s row fields may substitute.
    pub topic_values: Option<Vec<String>>,
}

/// One Kafka record, before it is handed to the client. Topic and key decide the partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutRecord {
    pub topic: String,
    pub key: String,
    /// What the partition is hashed from: the key, or only the table (`partition_by`).
    pub partition_key: String,
    /// The JSON event (the envelope), or the transformed value once a route reshapes it.
    pub value: String,
    /// The `op` header (Debezium's letter).
    pub op: &'static str,
    /// The `lsn` header: the checkpoint form of the event position.
    pub lsn: String,
    pub commit_ts_micros: i64,
    /// Headers besides `op` and `lsn`, e.g. `aqueduct-error` on a dead-letter record. Never row
    /// data. Empty (no allocation) on the normal path.
    pub headers: Vec<(&'static str, String)>,
}

/// The longest value one placeholder of a content-routed topic may substitute.
const MAX_SEGMENT: usize = 64;

/// A topic template (ADR 0002 §3, §5). `{schema}`/`{table}`-only templates are filled by string
/// replacement, exactly as before content routing; templates with `{op}` or a row field are
/// rendered from the event.
#[derive(Debug, Clone)]
struct TopicTemplate {
    raw: String,
    parsed: Template,
    /// Only `{schema}`/`{table}` placeholders, or none.
    simple: bool,
    /// Reads a row field: content routing, with its rules and fallback.
    reads_row: bool,
}

impl TopicTemplate {
    fn new(raw: &str) -> std::result::Result<Self, String> {
        let parsed = Template::parse(raw).map_err(|e| e.to_string())?;
        let simple = parsed
            .fields()
            .all(|f| matches!(f.as_str(), "schema" | "table"));
        let reads_row = parsed.fields().any(|f| f.column().is_some());
        Ok(Self {
            raw: raw.to_string(),
            parsed,
            simple,
            reads_row,
        })
    }

    /// The topic for a table, without an event: `{schema}` and `{table}` filled, other
    /// placeholders kept as written.
    fn fill(&self, schema: &str, table: &str) -> String {
        if self.parsed.is_static() {
            self.raw.clone()
        } else {
            self.raw
                .replace("{schema}", schema)
                .replace("{table}", table)
        }
    }

    /// The topic for an event, for a template that reads no row field: `{schema}`/`{table}` by
    /// string replacement as before content routing, `{op}` from the event.
    fn render(&self, view: &EventView<'_>) -> String {
        let ev = view.event();
        if self.simple {
            return self.fill(&ev.schema, &ev.table);
        }
        // Without row fields, every placeholder is `op`, `schema` or `table`: never missing.
        self.parsed
            .render(view)
            .unwrap_or_else(|_| self.fill(&ev.schema, &ev.table))
    }

    /// The template as a glob: every placeholder turned into `*`.
    fn glob(&self) -> String {
        self.parsed.glob()
    }

    /// True when no placeholder is used: one fixed topic name.
    fn is_static(&self) -> bool {
        self.parsed.is_static()
    }
}

/// One substituted value of a content-routed topic: `[A-Za-z0-9_-]{1,64}`.
fn valid_segment(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= MAX_SEGMENT
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

/// The topics content routing created or found, counted against `sink.max_dynamic_topics`.
/// Shared by every clone of a plan; the sink seeds it from the broker at connect.
#[derive(Debug)]
struct DynamicTopics {
    cap: AtomicUsize,
    known: RwLock<HashSet<String>>,
    warned: AtomicBool,
}

impl DynamicTopics {
    fn new() -> Self {
        Self {
            cap: AtomicUsize::new(DEFAULT_MAX_DYNAMIC_TOPICS),
            known: RwLock::new(HashSet::new()),
            warned: AtomicBool::new(false),
        }
    }

    fn len(&self) -> usize {
        self.known.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Adds topics that exist already; they always stay usable, even above the cap.
    fn add(&self, topics: impl IntoIterator<Item = String>) {
        let mut w = self.known.write().unwrap_or_else(|e| e.into_inner());
        w.extend(topics);
    }

    /// True when `topic` is known, or is new and the cap leaves room (it is then counted).
    fn admit(&self, topic: &str) -> bool {
        if self
            .known
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(topic)
        {
            return true;
        }
        let mut w = self.known.write().unwrap_or_else(|e| e.into_inner());
        if w.contains(topic) {
            return true;
        }
        let cap = self.cap.load(Ordering::Relaxed);
        if w.len() >= cap {
            if !self.warned.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    max_dynamic_topics = cap,
                    "sink.max_dynamic_topics reached: events that would create another content-routed topic go to their route's topic_fallback"
                );
            }
            return false;
        }
        w.insert(topic.to_string());
        true
    }
}

/// `sink.max_dynamic_topics` when unset.
pub const DEFAULT_MAX_DYNAMIC_TOPICS: usize = 100;

#[derive(Debug, Clone)]
struct CompiledRoute {
    pattern: String,
    topic: TopicTemplate,
    /// `None`: every op passes.
    ops: Option<Vec<Op>>,
    filter: Option<Expr>,
    /// `key = [...]`: the columns and the `row.<column>` paths that read them, in config order.
    key: Option<RouteKey>,
    /// Where an event goes when its content-routed topic cannot be used. Set exactly when the
    /// topic reads a row field.
    topic_fallback: Option<String>,
    /// The values a content-routed topic's row fields may take; `None` allows any valid segment.
    topic_values: Option<Vec<String>>,
}

/// A route's `key = [...]`.
#[derive(Debug, Clone)]
struct RouteKey {
    columns: Vec<String>,
    fields: Vec<FieldRef>,
}

impl CompiledRoute {
    /// The top-level columns `key = [...]` reads, in order; `None` without a per-route key.
    /// Field operations (R6) check these: no masked, hashed or excluded column may be in the key.
    pub(crate) fn key_columns(&self) -> Option<&[String]> {
        self.key.as_ref().map(|k| k.columns.as_slice())
    }

    /// The row fields the topic template reads (`row.region`, `after.address.city`, ...); empty
    /// for a topic without content routing. `FieldRef::column` gives each one's top-level column.
    /// Field operations (R6) check these, like [`CompiledRoute::key_columns`].
    #[allow(dead_code)] // R6 (field operations) checks these; tests use them until then.
    pub(crate) fn topic_fields(&self) -> impl Iterator<Item = &FieldRef> {
        self.topic.parsed.fields().filter(|f| f.column().is_some())
    }

    /// The `topic_values` allow-list, if any.
    pub(crate) fn topic_values(&self) -> Option<&[String]> {
        self.topic_values.as_deref()
    }
}

/// Validates `key = [...]`: a non-empty list of distinct top-level column names.
fn parse_key(name: &str, cols: &[String]) -> Result<RouteKey> {
    let bad = |msg: String| Error::Config(format!("sink route `{name}`: `key` {msg}"));
    if cols.is_empty() {
        return Err(bad(
            "is empty; list the columns to key by, or remove it to use `partition_by`".into(),
        ));
    }
    let mut fields = Vec::with_capacity(cols.len());
    for (i, c) in cols.iter().enumerate() {
        if c.is_empty() {
            return Err(bad("has an empty column name".into()));
        }
        if c.contains('.') {
            return Err(bad(format!(
                "column `{c}` is a dotted path; only top-level column names are supported"
            )));
        }
        if cols[..i].contains(c) {
            return Err(bad(format!("names column `{c}` twice")));
        }
        fields.push(
            FieldRef::parse(&format!("row.{c}")).map_err(|e| bad(format!("column `{c}`: {e}")))?,
        );
    }
    Ok(RouteKey {
        columns: cols.to_vec(),
        fields,
    })
}

/// One field path a route references, watched for the unseen-field warning (ADR 0002 §5).
#[derive(Debug)]
struct WatchedField {
    field: FieldRef,
    /// Rows that had the row the path reads but not the path.
    missing: AtomicU64,
    /// Set once a row had the path; a field seen once is never reported.
    seen: AtomicBool,
    warned: AtomicBool,
}

/// Counters per route, shared between the sink (writer) and the admin server (reader).
#[derive(Debug)]
pub struct RouteStats {
    names: Vec<String>,
    matched: Vec<AtomicU64>,
    produced: Vec<AtomicU64>,
    filtered: Vec<AtomicU64>,
    /// Events sent to a route's `topic_fallback` (content routing).
    fallback: Vec<AtomicU64>,
    errors: Vec<AtomicU64>,
    /// Per route, every field path it references. `where` registers its paths here; templates,
    /// `key`, field operations and outbox columns register theirs the same way.
    fields: Vec<Vec<WatchedField>>,
    /// Per route, whether the first dead-lettered event was logged.
    error_logged: Vec<AtomicBool>,
    /// Per route, whether the first event sent to `topic_fallback` was logged.
    fallback_logged: Vec<AtomicBool>,
}

/// The outcome counters of one route. Every event a route claims counts `matched`, then exactly
/// one of `produced`, `filtered`, `fallback` or `errors`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RouteCounts {
    /// Events the route claimed (first match wins).
    pub matched: u64,
    /// Events produced to the route's topic.
    pub produced: u64,
    /// Events dropped by `ops` or `where` (including a `where` that ends unknown).
    pub filtered: u64,
    /// Events sent to the route's `topic_fallback`.
    pub fallback: u64,
    /// Events sent to `sink.dead_letter_topic` because a row could not be evaluated.
    pub errors: u64,
}

/// A point-in-time copy of one route's counters. `route` is the route's `name`, else its `match`
/// pattern; the `sink.topic` fallback is `default`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteCount {
    pub route: String,
    #[serde(flatten)]
    pub counts: RouteCounts,
    /// Field paths the route references that no row has had in [`UNSEEN_FIELD_ROWS`] rows.
    pub unseen_fields: Vec<String>,
}

impl RouteStats {
    fn new(names: Vec<String>, fields: Vec<Vec<FieldRef>>) -> Self {
        let zeros = || names.iter().map(|_| AtomicU64::new(0)).collect();
        let fields = (0..names.len())
            .map(|i| {
                let mut watched: Vec<WatchedField> = Vec::new();
                for f in fields.get(i).into_iter().flatten() {
                    if watched.iter().all(|w| w.field != *f) {
                        watched.push(WatchedField {
                            field: f.clone(),
                            missing: AtomicU64::new(0),
                            seen: AtomicBool::new(false),
                            warned: AtomicBool::new(false),
                        });
                    }
                }
                watched
            })
            .collect();
        Self {
            matched: zeros(),
            produced: zeros(),
            filtered: zeros(),
            fallback: zeros(),
            errors: zeros(),
            fields,
            error_logged: names.iter().map(|_| AtomicBool::new(false)).collect(),
            fallback_logged: names.iter().map(|_| AtomicBool::new(false)).collect(),
            names,
        }
    }

    /// Counts an event `route` claimed under `outcome`, together with `matched`. Called only
    /// once the outcome is final (the record is rendered), so an encode error never leaves
    /// `matched` above the sum of the outcomes.
    fn claimed(&self, outcome: &[AtomicU64], route: usize) {
        self.matched[route].fetch_add(1, Ordering::Relaxed);
        outcome[route].fetch_add(1, Ordering::Relaxed);
    }

    fn counts(&self, i: usize) -> RouteCounts {
        let get = |v: &[AtomicU64]| v[i].load(Ordering::Relaxed);
        RouteCounts {
            matched: get(&self.matched),
            produced: get(&self.produced),
            filtered: get(&self.filtered),
            fallback: get(&self.fallback),
            errors: get(&self.errors),
        }
    }

    /// Paths of route `i` reported as unseen.
    fn unseen(&self, i: usize) -> Vec<String> {
        self.fields[i]
            .iter()
            .filter(|w| {
                !w.seen.load(Ordering::Relaxed)
                    && w.missing.load(Ordering::Relaxed) >= UNSEEN_FIELD_ROWS
            })
            .map(|w| w.field.as_str().to_string())
            .collect()
    }

    /// Notes, for every watched path of `route` not yet seen, whether this event's row has it.
    /// Cheap once every path has been seen: one atomic load per path.
    fn observe(&self, route: usize, view: &EventView<'_>) {
        for w in &self.fields[route] {
            if w.seen.load(Ordering::Relaxed) || !view.has_row(&w.field) {
                continue;
            }
            if view.present(&w.field) {
                w.seen.store(true, Ordering::Relaxed);
                continue;
            }
            let n = w.missing.fetch_add(1, Ordering::Relaxed) + 1;
            if n >= UNSEEN_FIELD_ROWS && !w.warned.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    route = %self.names[route],
                    field = %w.field,
                    rows = n,
                    "a field this route references has not appeared in any row yet; check the spelling"
                );
            }
        }
    }

    pub fn snapshot(&self) -> Vec<RouteCount> {
        (0..self.names.len())
            .map(|i| RouteCount {
                route: self.names[i].clone(),
                counts: self.counts(i),
                unseen_fields: self.unseen(i),
            })
            .collect()
    }
}

/// A static description of the compiled plan with live counts, for `/api/routes` and the
/// cockpit's routing graph. Never holds row data or secrets. Later slices add optional keys
/// (`topic_fallback`, `key`, field operations, `format`, `outbox`) to [`RouteDescription`];
/// readers must ignore keys they do not know.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoutesDescription {
    /// `sink.topic`, placeholders included.
    pub default_topic: Option<String>,
    pub dead_letter_topic: Option<String>,
    /// `key` or `table`.
    pub partition_by: &'static str,
    /// In match order; the `sink.topic` fallback, if any, is last, named `default`.
    pub routes: Vec<RouteDescription>,
}

/// One route of [`RoutesDescription`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteDescription {
    /// The route's `name`, else its `match`; `default` for the `sink.topic` fallback.
    pub route: String,
    /// The `match` glob; `null` for the `default` pseudo-route (it takes what no route claims).
    #[serde(rename = "match")]
    pub pattern: Option<String>,
    /// The `ops` filter; `null` when every op passes.
    pub ops: Option<Vec<&'static str>>,
    /// The `where` expression as configured; `null` without one.
    #[serde(rename = "where")]
    pub filter: Option<String>,
    /// The `key` columns, in order; `null` when the record key follows `partition_by`.
    pub key: Option<Vec<String>>,
    /// The topic template, placeholders included.
    pub topic: String,
    /// Where events go whose content-routed topic cannot be used; `null` for other topics.
    pub topic_fallback: Option<String>,
    /// The allow-list of a content-routed topic's values; `null` without one.
    pub topic_values: Option<Vec<String>>,
    pub counts: RouteCounts,
    pub unseen_fields: Vec<String>,
}

/// Picks the topic for a table (first matching route wins, else the default) and renders the
/// Kafka record. Compiled once from `[[sink.routes]]`.
#[derive(Debug, Clone)]
pub struct RoutePlan {
    routes: Vec<CompiledRoute>,
    default: Option<TopicTemplate>,
    dead_letter: Option<String>,
    partition_by: PartitionBy,
    source: SourceInfo,
    stats: Arc<RouteStats>,
    dynamic: Arc<DynamicTopics>,
}

/// A Kafka topic name: 1 to 249 of `[A-Za-z0-9._-]`, and not `.` or `..`.
fn valid_topic_name(t: &str) -> bool {
    !t.is_empty()
        && t.len() <= 249
        && t != "."
        && t != ".."
        && t.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn parse_ops(name: &str, ops: &[String]) -> Result<Vec<Op>> {
    if ops.is_empty() {
        return Err(Error::Config(format!(
            "sink route `{name}`: `ops` is empty, so it would drop every event; list the ops to keep or remove the key"
        )));
    }
    let mut out = Vec::with_capacity(ops.len());
    for o in ops {
        let op = OPS
            .into_iter()
            .find(|op| op_name(*op).eq_ignore_ascii_case(o))
            .ok_or_else(|| {
                Error::Config(format!(
                    "sink route `{name}`: unknown op `{o}` in `ops` (expected insert, update, delete, truncate or read)"
                ))
            })?;
        if !out.contains(&op) {
            out.push(op);
        }
    }
    Ok(out)
}

/// Parses a topic or topic template. Fixed text must be valid in a Kafka topic name, and no topic
/// may start with `_` (Kafka's internal topics, such as `__consumer_offsets`).
fn parse_topic(what: &str, raw: &str) -> Result<TopicTemplate> {
    let t = TopicTemplate::new(raw)
        .map_err(|e| Error::Config(format!("{what} `{raw}` is not a valid template: {e}")))?;
    if raw.starts_with('_') {
        return Err(Error::Config(format!(
            "{what} `{raw}` must not start with `_` (reserved for internal topics)"
        )));
    }
    let ok = if t.is_static() {
        valid_topic_name(raw)
    } else {
        t.parsed.literals().all(|l| {
            l.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        })
    };
    if !ok {
        return Err(Error::Config(format!(
            "{what} `{raw}` is not a valid Kafka topic name (1 to 249 of A-Z a-z 0-9 . _ -)"
        )));
    }
    Ok(t)
}

/// Checks a route's content-routing keys (ADR 0002 §5): a template that reads a row field needs
/// a fixed, non-empty prefix and a fixed `topic_fallback`; `topic_values`, if given, lists valid
/// segments. Other topics may not set either key.
fn check_content_routing(
    name: &str,
    r: &KafkaRoute,
    topic: &TopicTemplate,
) -> Result<(Option<String>, Option<Vec<String>>)> {
    let err = |msg: String| Err(Error::Config(format!("sink route `{name}`: {msg}")));
    if !topic.reads_row {
        if r.topic_fallback.is_some() {
            return err(format!(
                "`topic_fallback` is only for a `topic` that reads a row field, and `{}` reads none",
                topic.raw
            ));
        }
        if r.topic_values.is_some() {
            return err(format!(
                "`topic_values` is only for a `topic` that reads a row field, and `{}` reads none",
                topic.raw
            ));
        }
        return Ok((None, None));
    }
    if topic.parsed.static_prefix().is_empty() {
        return err(format!(
            "`topic` `{}` reads a row field, so it must start with fixed text (e.g. `orders.{{row.region}}`), not a placeholder",
            topic.raw
        ));
    }
    let Some(fallback) = &r.topic_fallback else {
        return err(format!(
            "`topic` `{}` reads a row field, so it needs `topic_fallback`: events whose value is null, missing or not allowed go there",
            topic.raw
        ));
    };
    if !valid_topic_name(fallback) {
        return err(format!(
            "`topic_fallback` `{fallback}` is not a valid Kafka topic name (1 to 249 of A-Z a-z 0-9 . _ -, no placeholders)"
        ));
    }
    if fallback.starts_with('_') {
        return err(format!(
            "`topic_fallback` `{fallback}` must not start with `_` (reserved for internal topics)"
        ));
    }
    if let Some(values) = &r.topic_values {
        if values.is_empty() {
            return err(
                "`topic_values` is empty, so every event would go to `topic_fallback`; list the allowed values or remove it".into(),
            );
        }
        if let Some(v) = values.iter().find(|v| !valid_segment(v)) {
            return err(format!(
                "`topic_values` entry `{v}` can never be used: a value must be 1 to {MAX_SEGMENT} of A-Z a-z 0-9 _ -"
            ));
        }
    }
    Ok((Some(fallback.clone()), r.topic_values.clone()))
}

/// No fixed topic (a static route topic, a static `sink.topic`, a fallback, the dead-letter
/// topic) may match a content-routed template read as a glob, except that route's own fallback:
/// otherwise a row value could steer events into a topic another part of the config owns.
fn check_collisions(
    names: &[String],
    routes: &[CompiledRoute],
    default: Option<&TopicTemplate>,
    dead_letter: Option<&str>,
) -> Result<()> {
    // (topic, what it is, the route whose fallback it is)
    let mut fixed: Vec<(&str, String, Option<usize>)> = routes
        .iter()
        .enumerate()
        .filter(|(_, r)| r.topic.is_static())
        .map(|(i, r)| {
            (
                r.topic.raw.as_str(),
                format!("route `{}`'s `topic`", names[i]),
                None,
            )
        })
        .collect();
    if let Some(d) = default.filter(|d| d.is_static()) {
        fixed.push((d.raw.as_str(), "sink.topic".into(), None));
    }
    for (i, r) in routes.iter().enumerate() {
        if let Some(f) = &r.topic_fallback {
            fixed.push((
                f.as_str(),
                format!("route `{}`'s `topic_fallback`", names[i]),
                Some(i),
            ));
        }
    }
    if let Some(d) = dead_letter {
        fixed.push((d, "sink.dead_letter_topic".into(), None));
    }
    for (i, r) in routes.iter().enumerate().filter(|(_, r)| r.topic.reads_row) {
        let glob = r.topic.glob();
        if let Some((topic, what, _)) = fixed
            .iter()
            .find(|(t, _, owner)| *owner != Some(i) && glob_match(&glob, t))
        {
            return Err(Error::Config(format!(
                "sink route `{}`: `topic` `{}` (as the glob `{glob}`) could produce `{topic}`, which is {what}; a row value must not be able to choose a topic another part of the config owns",
                names[i], r.topic.raw
            )));
        }
    }
    Ok(())
}

impl RoutePlan {
    /// A plan without a dead-letter topic, so no route may use `where`.
    pub fn new(routes: Vec<KafkaRoute>, default: Option<String>) -> Result<Self> {
        Self::with_dead_letter(routes, default, None)
    }

    /// Compiles and validates the routes. `dead_letter_topic` is required as soon as a route has
    /// a `where`, may not start with `_`, and may not be a topic a route or the default writes to.
    pub fn with_dead_letter(
        routes: Vec<KafkaRoute>,
        default: Option<String>,
        dead_letter_topic: Option<String>,
    ) -> Result<Self> {
        if routes.is_empty() && default.is_none() {
            return Err(Error::Config(
                "kafka sink needs `topic` and/or at least one route".into(),
            ));
        }
        let mut names: Vec<String> = Vec::with_capacity(routes.len() + 1);
        let mut compiled = Vec::with_capacity(routes.len());
        let mut fields: Vec<Vec<FieldRef>> = Vec::with_capacity(routes.len() + 1);
        for r in &routes {
            let name = r.name.clone().unwrap_or_else(|| r.pattern.clone());
            if name.is_empty() {
                return Err(Error::Config("sink route `name` must not be empty".into()));
            }
            // Labels key the per-route counters and Prometheus series, so they must be unique,
            // and `default` is the `sink.topic` fallback's.
            if name == DEFAULT_ROUTE {
                return Err(Error::Config(format!(
                    "sink route label `{DEFAULT_ROUTE}` is reserved for the `sink.topic` fallback; give the route another `name`"
                )));
            }
            if let Some(i) = names.iter().position(|n| *n == name) {
                let earlier = &routes[i];
                return Err(Error::Config(
                    if r.name.is_none() && earlier.name.is_none() {
                        format!(
                            "two sink routes have `match = \"{name}\"`: the second can never match (the first claims every event it would); remove it"
                        )
                    } else {
                        format!(
                            "duplicate sink route label `{name}` (a route's `name`, or its `match` when it has no name); labels must be unique"
                        )
                    },
                ));
            }
            let ops = r.ops.as_deref().map(|o| parse_ops(&name, o)).transpose()?;
            let filter = r
                .filter
                .as_deref()
                .map(|w| {
                    Expr::parse(w).map_err(|e| {
                        Error::Config(format!("sink route `{name}`: invalid `where`: {e}"))
                    })
                })
                .transpose()?;
            if filter.is_some() && dead_letter_topic.is_none() {
                return Err(Error::Config(format!(
                    "sink route `{name}` uses `where`, which needs `sink.dead_letter_topic`: rows it cannot evaluate go there instead of stopping the pipeline"
                )));
            }
            let key = r.key.as_deref().map(|k| parse_key(&name, k)).transpose()?;
            let topic = parse_topic(&format!("sink route `{name}`: `topic`"), &r.topic)?;
            let (topic_fallback, topic_values) = check_content_routing(&name, r, &topic)?;
            fields.push(
                filter
                    .iter()
                    .flat_map(|f| f.fields())
                    .chain(key.iter().flat_map(|k| k.fields.iter()))
                    .chain(topic.parsed.fields())
                    .cloned()
                    .collect(),
            );
            compiled.push(CompiledRoute {
                pattern: r.pattern.clone(),
                topic,
                ops,
                filter,
                key,
                topic_fallback,
                topic_values,
            });
            names.push(name);
        }
        if default.is_some() {
            names.push(DEFAULT_ROUTE.into());
        }
        let default = default
            .as_deref()
            .map(|t| parse_topic("sink.topic", t))
            .transpose()?;
        if default.as_ref().is_some_and(|d| d.reads_row) {
            return Err(Error::Config(
                "sink.topic cannot read a row field (it has no topic_fallback); route such tables with a [[sink.routes]] entry that sets `topic_fallback`".into(),
            ));
        }
        if let Some(dlq) = &dead_letter_topic {
            if !valid_topic_name(dlq) {
                return Err(Error::Config(format!(
                    "sink.dead_letter_topic `{dlq}` is not a valid Kafka topic name (1 to 249 of A-Z a-z 0-9 . _ -)"
                )));
            }
            if dlq.starts_with('_') {
                return Err(Error::Config(format!(
                    "sink.dead_letter_topic `{dlq}` must not start with `_` (reserved for internal topics)"
                )));
            }
            if let Some(t) = compiled
                .iter()
                .map(|r| &r.topic)
                .chain(default.as_ref())
                .find(|t| glob_match(&t.glob(), dlq))
            {
                return Err(Error::Config(format!(
                    "sink.dead_letter_topic `{dlq}` is also a topic events are routed to (`{}`); pick a separate topic",
                    t.raw
                )));
            }
            if compiled
                .iter()
                .any(|r| r.topic_fallback.as_deref() == Some(dlq.as_str()))
            {
                return Err(Error::Config(format!(
                    "sink.dead_letter_topic `{dlq}` is also a route's `topic_fallback`; pick a separate topic"
                )));
            }
        }
        check_collisions(
            &names,
            &compiled,
            default.as_ref(),
            dead_letter_topic.as_deref(),
        )?;
        Ok(Self {
            routes: compiled,
            default,
            dead_letter: dead_letter_topic,
            partition_by: PartitionBy::default(),
            source: SourceInfo::default(),
            stats: Arc::new(RouteStats::new(names, fields)),
            dynamic: Arc::new(DynamicTopics::new()),
        })
    }

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
        let globs: Vec<String> = self
            .routes
            .iter()
            .filter(|r| r.topic.reads_row)
            .map(|r| r.topic.glob())
            .collect();
        if globs.is_empty() {
            return 0;
        }
        let fixed = self.static_topics();
        self.dynamic.add(
            listed
                .into_iter()
                .filter(|t| globs.iter().any(|g| glob_match(g, t)))
                .filter(|t| !fixed.contains(t))
                .map(str::to_string),
        );
        self.dynamic.len()
    }

    /// Content-routed topics counted against `sink.max_dynamic_topics`.
    pub fn dynamic_topic_count(&self) -> usize {
        self.dynamic.len()
    }

    /// Every fixed topic name in the plan: static route topics, a static `sink.topic`, the
    /// fallbacks and the dead-letter topic.
    fn static_topics(&self) -> Vec<&str> {
        self.routes
            .iter()
            .map(|r| &r.topic)
            .chain(self.default.as_ref())
            .filter(|t| t.is_static())
            .map(|t| t.raw.as_str())
            .chain(self.fallback_topics())
            .chain(self.dead_letter.as_deref())
            .collect()
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

    /// `sink.dead_letter_topic`, if configured.
    pub fn dead_letter_topic(&self) -> Option<&str> {
        self.dead_letter.as_deref()
    }

    /// The topic for a table, or `None` when no route matches and there is no default. A
    /// template's `{schema}`/`{table}` are filled; other placeholders (`{op}`, row fields) are
    /// kept, since they depend on the event.
    pub fn topic_for(&self, schema: &str, table: &str) -> Option<String> {
        self.resolve(schema, table)
            .map(|(_, topic)| topic.fill(schema, table))
    }

    /// Index into the stats (routes first, default last) and the topic template.
    fn resolve(&self, schema: &str, table: &str) -> Option<(usize, &TopicTemplate)> {
        let name = format!("{schema}.{table}");
        if let Some((i, r)) = self
            .routes
            .iter()
            .enumerate()
            .find(|(_, r)| glob_match(&r.pattern, &name))
        {
            return Some((i, &r.topic));
        }
        self.default.as_ref().map(|t| (self.routes.len(), t))
    }

    /// Globs matching every topic this plan can produce to: each route (a content-routed template
    /// read as a glob covers every topic it can create), the default topic, the fallbacks and the
    /// dead-letter topic, with placeholders turned into `*`.
    pub fn topic_globs(&self) -> Vec<String> {
        self.routes
            .iter()
            .map(|r| r.topic.glob())
            .chain(self.default.as_ref().map(TopicTemplate::glob))
            .chain(self.fallback_topics().map(str::to_string))
            .chain(self.dead_letter.clone())
            .collect()
    }

    /// The compiled plan and its live counts, for `/api/routes`.
    pub fn describe(&self) -> RoutesDescription {
        let route = |i: usize, pattern: Option<String>, r: Option<&CompiledRoute>, topic: &str| {
            RouteDescription {
                route: self.stats.names[i].clone(),
                pattern,
                ops: r
                    .and_then(|r| r.ops.as_ref())
                    .map(|ops| ops.iter().map(|o| op_name(*o)).collect()),
                filter: r
                    .and_then(|r| r.filter.as_ref())
                    .map(|f| f.source().to_string()),
                key: r.and_then(|r| r.key_columns()).map(<[String]>::to_vec),
                topic: topic.to_string(),
                topic_fallback: r.and_then(|r| r.topic_fallback.clone()),
                topic_values: r.and_then(|r| r.topic_values().map(<[String]>::to_vec)),
                counts: self.stats.counts(i),
                unseen_fields: self.stats.unseen(i),
            }
        };
        let mut routes: Vec<RouteDescription> = self
            .routes
            .iter()
            .enumerate()
            .map(|(i, r)| route(i, Some(r.pattern.clone()), Some(r), &r.topic.raw))
            .collect();
        if let Some(d) = &self.default {
            routes.push(route(self.routes.len(), None, None, &d.raw));
        }
        RoutesDescription {
            default_topic: self.default_topic().map(str::to_string),
            dead_letter_topic: self.dead_letter.clone(),
            partition_by: self.partition_by.as_str(),
            routes,
        }
    }

    /// Appends the records for `event` to `out`. Pure: no I/O, and the clock is the caller's
    /// `now_us` (the envelope's top-level `ts_ms`/`ts_us`). Heartbeats produce nothing. An event
    /// no route claims is an error, as before.
    ///
    /// The first matching route owns the event (ADR 0002 §2): `ops`, then `where` on the original
    /// event. A `where` that is false or unknown filters the event (no record); one that hits a
    /// type error sends it to the dead-letter topic. Neither falls through to a later route.
    pub fn render_into(
        &self,
        event: &ChangeEvent,
        now_us: i64,
        out: &mut Vec<OutRecord>,
    ) -> Result<()> {
        if event.op == Op::Heartbeat {
            return Ok(());
        }
        let (route, template) = self.resolve(&event.schema, &event.table).ok_or_else(|| {
            Error::Sink(format!(
                "no kafka route or default topic for {}.{}",
                event.schema, event.table
            ))
        })?;
        // Parses the rows only on first use, so a route that reads no field pays nothing.
        let view = EventView::new(event);
        // The record key and the partition input, when the route sets its own key.
        let mut route_key = None;
        let mut outcome = &self.stats.produced;
        let topic = match self.routes.get(route) {
            None => template.render(&view),
            Some(r) => {
                if r.ops.as_ref().is_some_and(|ops| !ops.contains(&event.op)) {
                    self.stats.claimed(&self.stats.filtered, route);
                    return Ok(());
                }
                self.stats.observe(route, &view);
                if let Some(filter) = &r.filter {
                    match filter.eval(&view) {
                        Ok(Tri::True) => {}
                        Ok(Tri::False | Tri::Unknown) => {
                            self.stats.claimed(&self.stats.filtered, route);
                            return Ok(());
                        }
                        Err(e) => {
                            let reason = format!(
                                "route `{}`: where `{}`: {e}",
                                self.stats.names[route],
                                filter.source()
                            );
                            self.dead_letter(route, reason, event, now_us, out)?;
                            self.stats.claimed(&self.stats.errors, route);
                            return Ok(());
                        }
                    }
                }
                route_key = r.key.as_ref().map(|k| self.route_key(k, &view));
                match self.content_topic(route, r, &view) {
                    Ok(topic) => topic,
                    Err(fallback) => {
                        outcome = &self.stats.fallback;
                        fallback
                    }
                }
            }
        };
        let (key, partition_key) = match route_key {
            // A per-route key partitions by the whole key, whatever `partition_by` says.
            Some(key) => (key.clone(), key),
            None => {
                let key = record_key(event, &self.source);
                let partition_key = partition_input(event, &key, self.partition_by);
                (key, partition_key)
            }
        };
        out.push(OutRecord {
            topic,
            partition_key,
            key,
            value: encode(event, &self.source, now_us)?,
            op: op_code(event.op),
            lsn: event.position.to_checkpoint(),
            commit_ts_micros: event.commit_ts_micros,
            headers: Vec::new(),
        });
        self.stats.claimed(outcome, route);
        Ok(())
    }

    /// The topic of route `r` for this event. A content-routed template (one that reads a row
    /// field) gives `Err(topic_fallback)` when a field is null or missing, a value is not one
    /// segment of `[A-Za-z0-9_-]{1,64}` or not in `topic_values`, the result is not a valid topic
    /// name, or the topic would be new beyond `sink.max_dynamic_topics`. Never a row value in the
    /// log line, only the field path.
    fn content_topic(
        &self,
        route: usize,
        r: &CompiledRoute,
        view: &EventView<'_>,
    ) -> std::result::Result<String, String> {
        let Some(fallback) = r.topic_fallback.as_ref().filter(|_| r.topic.reads_row) else {
            return Ok(r.topic.render(view));
        };
        let rendered = r.topic.parsed.render_checked(view, |f, v| {
            f.column().is_none()
                || (valid_segment(v)
                    && r.topic_values
                        .as_ref()
                        .is_none_or(|vals| vals.iter().any(|a| a == v)))
        });
        let why = match rendered {
            Ok(t) if !valid_topic_name(&t) => {
                "the rendered topic is not a valid Kafka topic name".to_string()
            }
            Ok(t) if self.dynamic.admit(&t) => return Ok(t),
            Ok(_) => "sink.max_dynamic_topics is reached".to_string(),
            Err(e) => e.to_string(),
        };
        if !self.stats.fallback_logged[route].swap(true, Ordering::Relaxed) {
            tracing::warn!(
                route = %self.stats.names[route],
                topic = %r.topic.raw,
                fallback = %fallback,
                reason = %why,
                "an event went to the route's topic_fallback; later ones are only counted under fallback"
            );
        }
        Err(fallback.clone())
    }

    /// The record key of a route with `key = [...]`: the usual JSON key with `"key"` built from the
    /// listed columns of `row` (`after`, else `before`, so a delete keys by its before-image), in
    /// order, each value copied as raw JSON and `null` when the row lacks the column. An event
    /// without a row (a truncate) gets the table-only key.
    fn route_key(&self, key: &RouteKey, view: &EventView<'_>) -> String {
        let event = view.event();
        if event.after.is_none() && event.before.is_none() {
            return table_key(event, &self.source);
        }
        let mut obj = String::with_capacity(16 * key.columns.len() + 2);
        obj.push('{');
        for (i, (col, field)) in key.columns.iter().zip(&key.fields).enumerate() {
            if i > 0 {
                obj.push(',');
            }
            obj.push_str(&serde_json::to_string(col).unwrap_or_default());
            obj.push(':');
            obj.push_str(view.raw(field).map_or("null", |v| v.get()));
        }
        obj.push('}');
        key_json(event, &self.source, &obj)
    }

    /// The dead-letter record for an event route `route` could not evaluate. `reason` names the
    /// route, the expression and the type problem, never a row value: it becomes the
    /// `aqueduct-error` header and a log line. The key is the table-only record key (no row data),
    /// and the partition is hashed from it.
    fn dead_letter(
        &self,
        route: usize,
        reason: String,
        event: &ChangeEvent,
        now_us: i64,
        out: &mut Vec<OutRecord>,
    ) -> Result<()> {
        let topic = self.dead_letter.clone().ok_or_else(|| {
            Error::Sink(format!(
                "{reason}, and no sink.dead_letter_topic is configured"
            ))
        })?;
        let key = table_key(event, &self.source);
        // R6 (field operations) must apply the route's include/exclude, mask, hash and rename to
        // this value before it is produced, so the dead-letter topic never holds data the route
        // would have masked (ADR 0002 §5a). Until then a route has no field operations to apply.
        let value = encode(event, &self.source, now_us)?;
        out.push(OutRecord {
            topic,
            partition_key: key.clone(),
            key,
            value,
            op: op_code(event.op),
            lsn: event.position.to_checkpoint(),
            commit_ts_micros: event.commit_ts_micros,
            headers: vec![("aqueduct-error", reason.clone())],
        });
        if !self.stats.error_logged[route].swap(true, Ordering::Relaxed) {
            tracing::warn!(
                route = %self.stats.names[route],
                topic = %out.last().map_or("", |r| r.topic.as_str()),
                error = %reason,
                "an event could not be evaluated and went to the dead-letter topic; later ones are only counted under errors"
            );
        }
        Ok(())
    }

    /// The records for one event.
    pub fn render(&self, event: &ChangeEvent, now_us: i64) -> Result<Vec<OutRecord>> {
        let mut out = Vec::with_capacity(1);
        self.render_into(event, now_us, &mut out)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aqueduct_core::Position;

    fn route(p: &str, t: &str) -> KafkaRoute {
        KafkaRoute {
            name: None,
            pattern: p.into(),
            topic: t.into(),
            ..Default::default()
        }
    }

    fn named(n: &str, p: &str, t: &str) -> KafkaRoute {
        KafkaRoute {
            name: Some(n.into()),
            ..route(p, t)
        }
    }

    #[test]
    fn first_match_wins_then_default_with_placeholders() {
        let p = RoutePlan::new(
            vec![
                route("public.users", "users-topic"),
                route("public.*", "cdc.{table}"),
            ],
            Some("all.{schema}.{table}".into()),
        )
        .unwrap();
        assert_eq!(
            p.topic_for("public", "users").as_deref(),
            Some("users-topic")
        );
        assert_eq!(
            p.topic_for("public", "orders").as_deref(),
            Some("cdc.orders")
        );
        assert_eq!(p.topic_for("sales", "x").as_deref(), Some("all.sales.x"));
    }

    #[test]
    fn topic_globs_cover_routes_and_default() {
        let p = RoutePlan::new(
            vec![
                route("public.users", "users"),
                route("*.audit", "a.{schema}"),
            ],
            Some("cdc.{schema}.{table}".into()),
        )
        .unwrap();
        assert_eq!(p.topic_globs(), vec!["users", "a.*", "cdc.*.*"]);
    }

    #[test]
    fn unrouted_event_without_default_is_an_error() {
        let p = RoutePlan::new(vec![route("public.*", "p")], None).unwrap();
        assert_eq!(p.topic_for("sales", "x"), None);
        let mut e = crate::sample();
        e.schema = "sales".into();
        let err = p.render(&e, 0).unwrap_err().to_string();
        assert!(
            err.contains("no kafka route or default topic for sales.users"),
            "{err}"
        );
        assert!(RoutePlan::new(vec![], None).is_err());
    }

    #[test]
    fn route_names_are_validated() {
        assert!(RoutePlan::new(vec![named("", "a.*", "t")], None).is_err());
        assert!(
            RoutePlan::new(vec![named("x", "a.*", "t"), named("x", "b.*", "u")], None).is_err()
        );
        let err = |routes: Vec<KafkaRoute>, default: Option<&str>| {
            RoutePlan::new(routes, default.map(Into::into))
                .map(|_| ())
                .unwrap_err()
                .to_string()
        };
        // Unnamed routes label by pattern: the same `match` twice can never reach the second.
        let e = err(vec![route("a.*", "t"), route("a.*", "u")], None);
        assert!(e.contains("the second can never match"), "{e}");
        // A name may not collide with another route's pattern label, in either order.
        for routes in [
            vec![route("a.*", "t"), named("a.*", "b.*", "u")],
            vec![named("a.*", "b.*", "u"), route("a.*", "t")],
        ] {
            let e = err(routes, None);
            assert!(e.contains("duplicate sink route label `a.*`"), "{e}");
        }
        // `default` is the `sink.topic` fallback's label, with or without a `sink.topic`.
        for default in [None, Some("rest")] {
            let e = err(vec![named("default", "a.*", "t")], default);
            assert!(e.contains("reserved"), "{e}");
            let e = err(vec![route("default", "t")], default);
            assert!(e.contains("reserved"), "{e}");
        }
        // Distinct labels are fine, and every Prometheus series is unique.
        let p = RoutePlan::new(
            vec![named("x", "a.*", "t"), route("a.*", "u"), route("b.*", "v")],
            Some("rest".into()),
        )
        .unwrap();
        let labels: Vec<String> = p.stats().snapshot().into_iter().map(|c| c.route).collect();
        assert_eq!(labels, ["x", "a.*", "b.*", "default"]);
    }

    #[test]
    fn heartbeats_render_nothing() {
        let p = RoutePlan::new(vec![], Some("t".into())).unwrap();
        let hb = ChangeEvent::heartbeat(Position::Text("x".into()), 0);
        assert!(p.render(&hb, 0).unwrap().is_empty());
        assert!(p.stats().snapshot().iter().all(|c| c.counts.matched == 0));
    }

    #[test]
    fn stats_count_per_route_and_default() {
        let p = RoutePlan::new(
            vec![
                named("users", "public.users", "u"),
                route("sales.*", "s.{table}"),
            ],
            Some("rest".into()),
        )
        .unwrap();
        let e = |schema: &str, table: &str| {
            let mut e = crate::sample();
            e.schema = schema.into();
            e.table = table.into();
            e
        };
        for ev in [
            e("public", "users"),
            e("public", "users"),
            e("sales", "x"),
            e("hr", "y"),
        ] {
            p.render(&ev, 0).unwrap();
        }
        let c = |route: &str, n| RouteCount {
            route: route.into(),
            counts: RouteCounts {
                matched: n,
                produced: n,
                ..Default::default()
            },
            unseen_fields: vec![],
        };
        assert_eq!(
            p.stats().snapshot(),
            vec![c("users", 2), c("sales.*", 1), c("default", 1)]
        );
    }

    // --- filter: ops, where, dead-letter topic (R3) ---------------------------------------------

    const DLQ: &str = "aqueduct.dlq";

    fn filtered(name: &str, p: &str, ops: Option<&[&str]>, filter: Option<&str>) -> KafkaRoute {
        KafkaRoute {
            ops: ops.map(|o| o.iter().map(|s| s.to_string()).collect()),
            filter: filter.map(Into::into),
            ..named(name, p, &format!("{name}.topic"))
        }
    }

    fn plan_with(routes: Vec<KafkaRoute>) -> RoutePlan {
        let mut p =
            RoutePlan::with_dead_letter(routes, Some("rest".into()), Some(DLQ.into())).unwrap();
        p.set_source(pg());
        p
    }

    fn row(table: &str, op: Op, json: &str) -> ChangeEvent {
        let mut e = event("public", table, op, Some(r#"{"id":1}"#), Position::Lsn(7));
        let r = raw(json);
        match op {
            Op::Delete => (e.before, e.after) = (r, None),
            Op::Truncate => (e.before, e.after, e.key) = (None, None, None),
            _ => (e.before, e.after) = (None, r),
        }
        e
    }

    /// What became of one event: the topic it was produced to, `filtered`, or `dlq`.
    fn outcome(p: &RoutePlan, e: &ChangeEvent) -> String {
        let out = p.render(e, NOW).unwrap();
        match out.as_slice() {
            [] => "filtered".into(),
            [r] if r.topic == DLQ => "dlq".into(),
            [r] => r.topic.clone(),
            _ => panic!("one event, several records"),
        }
    }

    #[test]
    fn ops_and_where_filter_per_route_table_driven() {
        let p = plan_with(vec![
            filtered("ins", "public.ins", Some(&["insert"]), None),
            filtered("w", "public.orders", None, Some("row.status == 'paid'")),
            filtered(
                "both",
                "public.both",
                Some(&["insert", "update"]),
                Some("row.amount > 10"),
            ),
            filtered(
                "keep_null",
                "public.k",
                None,
                Some("row.region is null or not (row.region == 'eu')"),
            ),
            filtered("neg", "public.neg", None, Some("not (row.region == 'eu')")),
        ]);
        let cases: &[(&str, Op, &str, &str)] = &[
            // ops
            ("ins", Op::Insert, r#"{"id":1}"#, "ins.topic"),
            ("ins", Op::Update, r#"{"id":1}"#, "filtered"),
            ("ins", Op::Delete, r#"{"id":1}"#, "filtered"),
            ("ins", Op::Truncate, "{}", "filtered"),
            // where: true, false, unknown (missing field), null field
            ("orders", Op::Insert, r#"{"status":"paid"}"#, "w.topic"),
            ("orders", Op::Insert, r#"{"status":"open"}"#, "filtered"),
            ("orders", Op::Insert, r#"{"id":1}"#, "filtered"),
            ("orders", Op::Insert, r#"{"status":null}"#, "filtered"),
            ("orders", Op::Truncate, "{}", "filtered"),
            // where on a delete reads the before-image through `row`
            ("orders", Op::Delete, r#"{"status":"paid"}"#, "w.topic"),
            // type error: a number literal against text that is not a number
            ("orders", Op::Insert, r#"{"status":1}"#, "dlq"),
            // ops first, then where; an op the list drops never reaches where (no type error)
            ("both", Op::Insert, r#"{"amount":11}"#, "both.topic"),
            ("both", Op::Update, r#"{"amount":"10.5"}"#, "both.topic"),
            ("both", Op::Insert, r#"{"amount":9}"#, "filtered"),
            ("both", Op::Insert, r#"{"amount":"abc"}"#, "dlq"),
            ("both", Op::Delete, r#"{"amount":"abc"}"#, "filtered"),
            // `not` over null is unknown, so filtered; `is null or` keeps the row
            ("neg", Op::Insert, r#"{"region":"us"}"#, "neg.topic"),
            ("neg", Op::Insert, r#"{"region":"eu"}"#, "filtered"),
            ("neg", Op::Insert, r#"{"id":1}"#, "filtered"),
            ("k", Op::Insert, r#"{"id":1}"#, "keep_null.topic"),
            ("k", Op::Insert, r#"{"region":"eu"}"#, "filtered"),
            // tables no filtered route claims go to the default, unfiltered
            ("other", Op::Delete, r#"{"id":1}"#, "rest"),
        ];
        for (table, op, json, want) in cases {
            let e = row(table, *op, json);
            assert_eq!(&outcome(&p, &e), want, "{table} {op:?} {json}");
        }
    }

    #[test]
    fn a_claimed_event_never_falls_through_to_a_later_route() {
        // The second route would pass everything; the first owns public.orders.
        let p = plan_with(vec![
            filtered("strict", "public.orders", None, Some("row.n > 0")),
            route("public.*", "everything"),
        ]);
        assert_eq!(
            outcome(&p, &row("orders", Op::Insert, r#"{"n":0}"#)),
            "filtered"
        );
        assert_eq!(
            outcome(&p, &row("orders", Op::Insert, r#"{"n":"x"}"#)),
            "dlq"
        );
        assert_eq!(
            outcome(&p, &row("users", Op::Insert, r#"{"n":0}"#)),
            "everything"
        );
    }

    #[test]
    fn dead_letter_record_carries_the_reason_but_no_row_data() {
        let p = plan_with(vec![filtered(
            "orders",
            "public.orders",
            None,
            Some("row.amount > 10 and row.status == 'paid'"),
        )]);
        let mut e = row(
            "orders",
            Op::Update,
            r#"{"amount":"secret-text","status":"paid"}"#,
        );
        e.key = Some(r#"{"id":4242}"#.into());
        let out = p.render(&e, NOW).unwrap();
        assert_eq!(out.len(), 1);
        let r = &out[0];
        assert_eq!(r.topic, DLQ);
        // Table-only key, partitioned by it: no primary-key values in the dead-letter key.
        assert_eq!(
            r.key,
            r#"{"db":"app","schema":"public","table":"orders","key":null}"#
        );
        assert_eq!(r.partition_key, r.key);
        // The value is the envelope (field operations come with R6).
        assert_eq!(r.value, encode(&e, &pg(), NOW).unwrap());
        assert_eq!((r.op, r.lsn.as_str()), ("u", "7"));
        assert_eq!(r.headers.len(), 1);
        let (name, reason) = &r.headers[0];
        assert_eq!(*name, "aqueduct-error");
        assert_eq!(
            reason,
            "route `orders`: where `row.amount > 10 and row.status == 'paid'`: type error: `row.amount` is not a finite number"
        );
        assert!(!reason.contains("secret-text") && !reason.contains("4242"));
    }

    #[test]
    fn counters_add_up_per_route() {
        let p = plan_with(vec![
            filtered("o", "public.orders", Some(&["insert"]), Some("row.n > 1")),
            route("public.*", "p"),
        ]);
        for (table, op, json) in [
            ("orders", Op::Insert, r#"{"n":2}"#),   // produced
            ("orders", Op::Insert, r#"{"n":1}"#),   // filtered by where
            ("orders", Op::Update, r#"{"n":2}"#),   // filtered by ops
            ("orders", Op::Insert, r#"{"n":"x"}"#), // error
            ("orders", Op::Insert, r#"{}"#),        // unknown: filtered
            ("users", Op::Insert, r#"{}"#),         // second route
            ("x", Op::Insert, r#"{}"#),             // also the second route (public.*)
        ] {
            p.render(&row(table, op, json), NOW).unwrap();
        }
        let mut sales = row("x", Op::Insert, "{}");
        sales.schema = "sales".into();
        p.render(&sales, NOW).unwrap();
        let snap = p.stats().snapshot();
        let counts: Vec<(&str, RouteCounts)> =
            snap.iter().map(|c| (c.route.as_str(), c.counts)).collect();
        let c = |matched, produced, filtered, errors| RouteCounts {
            matched,
            produced,
            filtered,
            fallback: 0,
            errors,
        };
        assert_eq!(
            counts,
            vec![
                ("o", c(5, 1, 3, 1)),
                ("public.*", c(2, 2, 0, 0)),
                ("default", c(1, 1, 0, 0)),
            ]
        );
        for c in &snap {
            let k = c.counts;
            assert_eq!(k.matched, k.produced + k.filtered + k.fallback + k.errors);
        }
    }

    #[test]
    fn where_and_ops_are_validated_at_startup() {
        let dlq = Some(DLQ.to_string());
        let build = |r: KafkaRoute, dlq: Option<String>| {
            RoutePlan::with_dead_letter(vec![r], Some("rest".into()), dlq)
                .map(|_| ())
                .map_err(|e| e.to_string())
        };
        // (ops, where, dead_letter_topic, expected error or "" for ok)
        type Case<'a> = (
            Option<&'a [&'a str]>,
            Option<&'a str>,
            Option<&'a str>,
            &'a str,
        );
        let cases: &[Case] = &[
            (Some(&["insert", "UPDATE", "read"]), None, None, ""),
            (
                Some(&["upsert"]),
                None,
                None,
                "sink route `r`: unknown op `upsert`",
            ),
            (Some(&[]), None, None, "sink route `r`: `ops` is empty"),
            (None, Some("row.n > 1"), Some(DLQ), ""),
            (
                None,
                Some("row.n >"),
                Some(DLQ),
                "sink route `r`: invalid `where`",
            ),
            (
                None,
                Some("row.ts < '2024-01-01'"),
                Some(DLQ),
                "sink route `r`: invalid `where`",
            ),
            (None, Some(""), Some(DLQ), "sink route `r`: invalid `where`"),
            (
                None,
                Some("row.n > 1"),
                None,
                "sink route `r` uses `where`, which needs `sink.dead_letter_topic`",
            ),
            (
                None,
                Some("row.n > 1"),
                Some("_dlq"),
                "must not start with `_`",
            ),
            (
                None,
                Some("row.n > 1"),
                Some("r.topic"),
                "also a topic events are routed to",
            ),
            (
                None,
                Some("row.n > 1"),
                Some("rest"),
                "also a topic events are routed to",
            ),
            (
                None,
                Some("row.n > 1"),
                Some("bad topic"),
                "not a valid Kafka topic name",
            ),
            (
                None,
                Some("row.n > 1"),
                Some(""),
                "not a valid Kafka topic name",
            ),
        ];
        for (ops, filter, dlq_topic, want) in cases {
            let r = filtered("r", "public.*", *ops, *filter);
            let got = build(r, dlq_topic.map(str::to_string));
            if want.is_empty() {
                assert_eq!(got, Ok(()), "{ops:?} {filter:?} {dlq_topic:?}");
            } else {
                let err = got.expect_err(want);
                assert!(err.contains(want), "{err} should contain {want}");
            }
        }
        // A dead-letter topic matching a templated topic is rejected too.
        let r = KafkaRoute {
            filter: Some("row.n > 1".into()),
            ..route("public.*", "cdc.{table}")
        };
        assert!(build(r.clone(), Some("cdc.dlq".into())).is_err());
        assert!(build(r, dlq.clone()).is_ok());
        // A dead-letter topic without any `where` is allowed (and still checked).
        assert!(RoutePlan::with_dead_letter(vec![], Some("t".into()), Some("_x".into())).is_err());
        assert!(RoutePlan::with_dead_letter(vec![], Some("t".into()), dlq).is_ok());
    }

    #[test]
    fn unseen_where_fields_are_reported_after_enough_rows() {
        let p = plan_with(vec![
            filtered(
                "o",
                "public.orders",
                None,
                Some("row.stauts == 'paid' or row.status == 'paid' or op == 'delete' or before.n > 0"),
            ),
            route("public.*", "p"),
        ]);
        let ev = row("orders", Op::Insert, r#"{"status":"paid"}"#);
        for _ in 0..UNSEEN_FIELD_ROWS - 1 {
            p.render(&ev, NOW).unwrap();
        }
        assert!(p.stats().snapshot()[0].unseen_fields.is_empty(), "not yet");
        p.render(&ev, NOW).unwrap();
        // `op` is not a row field; `before.n` never had a before-image to look in.
        assert_eq!(p.stats().snapshot()[0].unseen_fields, vec!["row.stauts"]);
        assert_eq!(p.describe().routes[0].unseen_fields, vec!["row.stauts"]);
        // Seen once, the field is no longer reported.
        p.render(&row("orders", Op::Insert, r#"{"stauts":"x"}"#), NOW)
            .unwrap();
        assert!(p.stats().snapshot()[0].unseen_fields.is_empty());
        assert!(p.stats().snapshot()[1].unseen_fields.is_empty());
    }

    #[test]
    fn describe_shows_the_plan_and_live_counts_without_row_data() {
        let mut p = plan_with(vec![
            filtered(
                "orders",
                "public.orders",
                Some(&["insert", "update"]),
                Some("row.status == 'paid'"),
            ),
            keyed("public.*", "cdc.{table}", &["id", "tenant"]),
        ]);
        p.set_partition_by(PartitionBy::Table);
        p.render(
            &row("orders", Op::Insert, r#"{"status":"paid","card":"4111"}"#),
            NOW,
        )
        .unwrap();
        let v = serde_json::to_value(p.describe()).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "default_topic": "rest",
                "dead_letter_topic": DLQ,
                "partition_by": "table",
                "routes": [
                    {"route": "orders", "match": "public.orders", "ops": ["insert", "update"],
                     "where": "row.status == 'paid'", "key": null, "topic": "orders.topic", "topic_fallback": null, "topic_values": null,
                     "counts": {"matched": 1, "produced": 1, "filtered": 0, "fallback": 0, "errors": 0},
                     "unseen_fields": []},
                    {"route": "public.*", "match": "public.*", "ops": null, "where": null,
                     "key": ["id", "tenant"], "topic": "cdc.{table}", "topic_fallback": null, "topic_values": null,
                     "counts": {"matched": 0, "produced": 0, "filtered": 0, "fallback": 0, "errors": 0},
                     "unseen_fields": []},
                    {"route": "default", "match": null, "ops": null, "where": null, "key": null,
                     "topic": "rest", "topic_fallback": null, "topic_values": null,
                     "counts": {"matched": 0, "produced": 0, "filtered": 0, "fallback": 0, "errors": 0},
                     "unseen_fields": []}
                ]
            })
        );
        assert!(!v.to_string().contains("4111"));
        // The metrics snapshot is flat: {route, matched, ..., unseen_fields}.
        let m = serde_json::to_value(p.stats().snapshot()).unwrap();
        assert_eq!(m[0]["route"], "orders");
        assert_eq!(m[0]["produced"], 1);
        assert_eq!(m[0]["fallback"], 0);
        assert!(m[0]["unseen_fields"].as_array().unwrap().is_empty());
    }

    #[test]
    fn dead_letter_topic_is_among_the_inspected_topics() {
        let p = plan_with(vec![route("public.users", "users")]);
        assert_eq!(p.topic_globs(), vec!["users", "rest", DLQ]);
        assert_eq!(p.dead_letter_topic(), Some(DLQ));
    }

    // --- per-route key (R5) -----------------------------------------------------------------------

    fn keyed(p: &str, t: &str, cols: &[&str]) -> KafkaRoute {
        KafkaRoute {
            key: Some(cols.iter().map(|c| c.to_string()).collect()),
            ..route(p, t)
        }
    }

    fn keyed_plan(info: SourceInfo, by: PartitionBy) -> RoutePlan {
        let mut p = RoutePlan::new(
            vec![keyed("*.orders", "orders", &["tenant", "id"])],
            Some("rest".into()),
        )
        .unwrap();
        p.set_source(info);
        p.set_partition_by(by);
        p
    }

    #[test]
    fn key_columns_make_the_record_key_in_order_and_the_partition_input() {
        for by in [PartitionBy::Key, PartitionBy::Table] {
            let p = keyed_plan(pg(), by);
            let e = row(
                "orders",
                Op::Insert,
                r#"{"id":5,"n":"x","tenant":"acme","addr":{"c":"eu"}}"#,
            );
            let r = p.render(&e, NOW).unwrap().remove(0);
            assert_eq!(
                r.key,
                r#"{"db":"app","schema":"public","table":"orders","key":{"tenant":"acme","id":5}}"#
            );
            // The whole key is the partition input, whatever `partition_by` says.
            assert_eq!(r.partition_key, r.key, "{by:?}");
            assert_eq!(r.value, encode(&e, &pg(), NOW).unwrap());
        }
    }

    #[test]
    fn deletes_key_by_the_before_image_and_updates_by_the_after_image() {
        let p = keyed_plan(pg(), PartitionBy::Key);
        let ins = row("orders", Op::Insert, r#"{"id":5,"tenant":"acme","n":1}"#);
        let del = row("orders", Op::Delete, r#"{"tenant":"acme","id":5}"#);
        let mut upd = row("orders", Op::Update, r#"{"id":5,"tenant":"acme","n":2}"#);
        upd.before = raw(r#"{"id":4,"tenant":"old"}"#);
        let keys: Vec<String> = [ins, upd, del]
            .iter()
            .map(|e| p.render(e, NOW).unwrap().remove(0).key)
            .collect();
        assert_eq!(keys[0], keys[1], "an update keys by `after`");
        assert_eq!(keys[0], keys[2], "a delete keys by `before`");
        // So all three land on the same partition.
        let parts: Vec<usize> = keys
            .iter()
            .map(|k| crate::kafka::partition_index(k, 12))
            .collect();
        assert!(parts.iter().all(|&x| x == parts[0]));
    }

    #[test]
    fn key_values_are_raw_json_and_missing_columns_are_null() {
        let mut p = RoutePlan::new(
            vec![keyed("public.*", "t", &["a", "missing", "b", "c", "d"])],
            None,
        )
        .unwrap();
        p.set_source(pg());
        let e = row(
            "x",
            Op::Insert,
            r#"{"d":null,"c":"é\"q","b":{"k":[1,2.50]},"a":1.0e3}"#,
        );
        let r = p.render(&e, NOW).unwrap().remove(0);
        assert_eq!(
            r.key,
            r#"{"db":"app","schema":"public","table":"x","key":{"a":1.0e3,"missing":null,"b":{"k":[1,2.50]},"c":"é\"q","d":null}}"#
        );
        let v: serde_json::Value = serde_json::from_str(&r.key).unwrap();
        assert_eq!(v["key"]["c"], "é\"q");
    }

    #[test]
    fn truncates_get_the_table_key() {
        let p = keyed_plan(pg(), PartitionBy::Key);
        let r = p
            .render(&row("orders", Op::Truncate, "{}"), NOW)
            .unwrap()
            .remove(0);
        assert_eq!(
            r.key,
            r#"{"db":"app","schema":"public","table":"orders","key":null}"#
        );
        assert_eq!(r.partition_key, r.key);
    }

    #[test]
    fn mongodb_keys_name_the_database_and_collection() {
        let p = keyed_plan(SourceInfo::Mongodb, PartitionBy::Table);
        let mut e = row(
            "orders",
            Op::Insert,
            r#"{"_id":{"$oid":"6530"},"tenant":"acme","id":7}"#,
        );
        e.schema = "shop".into();
        let r = p.render(&e, NOW).unwrap().remove(0);
        assert_eq!(
            r.key,
            r#"{"db":"shop","collection":"orders","key":{"tenant":"acme","id":7}}"#
        );
        assert_eq!(r.partition_key, r.key);
    }

    #[test]
    fn a_keyed_route_with_where_keys_what_passes() {
        let p = plan_with(vec![KafkaRoute {
            key: Some(vec!["id".into()]),
            ..filtered("o", "public.orders", None, Some("row.n > 1"))
        }]);
        let r = p
            .render(&row("orders", Op::Insert, r#"{"id":3,"n":2}"#), NOW)
            .unwrap()
            .remove(0);
        assert_eq!(
            r.key,
            r#"{"db":"app","schema":"public","table":"orders","key":{"id":3}}"#
        );
        // Dead-letter records keep the table-only key.
        let r = p
            .render(&row("orders", Op::Insert, r#"{"id":3,"n":"x"}"#), NOW)
            .unwrap()
            .remove(0);
        assert_eq!(r.topic, DLQ);
        assert!(r.key.ends_with(r#""key":null}"#), "{}", r.key);
    }

    #[test]
    fn key_lists_are_validated_at_startup() {
        let build = |cols: &[&str]| {
            RoutePlan::new(vec![keyed("public.*", "t", cols)], None)
                .map(|_| ())
                .map_err(|e| e.to_string())
        };
        assert_eq!(build(&["id"]), Ok(()));
        assert_eq!(build(&["tenant", "id"]), Ok(()));
        for (cols, want) in [
            (&[][..], "sink route `public.*`: `key` is empty"),
            (&["a.b"][..], "column `a.b` is a dotted path"),
            (&["id", "x", "id"][..], "names column `id` twice"),
            (&[""][..], "has an empty column name"),
        ] {
            let err = build(cols).expect_err(want);
            assert!(err.contains(want), "{err} should contain {want}");
        }
    }

    #[test]
    fn key_columns_are_exposed_for_field_operation_checks() {
        let p = keyed_plan(pg(), PartitionBy::Key);
        assert_eq!(
            p.routes[0].key_columns(),
            Some(&["tenant".to_string(), "id".to_string()][..])
        );
        let p = RoutePlan::new(vec![route("a.*", "t")], None).unwrap();
        assert_eq!(p.routes[0].key_columns(), None);
    }

    #[test]
    fn unseen_key_columns_are_reported_on_routes_without_where() {
        let p = RoutePlan::new(vec![keyed("public.*", "t", &["id", "tenant_id"])], None).unwrap();
        let ev = row("orders", Op::Insert, r#"{"id":1,"tenant":"acme"}"#);
        for _ in 0..UNSEEN_FIELD_ROWS {
            p.render(&ev, NOW).unwrap();
        }
        assert_eq!(p.stats().snapshot()[0].unseen_fields, vec!["row.tenant_id"]);
        // Truncates have no row and never count.
        let q = RoutePlan::new(vec![keyed("public.*", "t", &["id"])], None).unwrap();
        for _ in 0..UNSEEN_FIELD_ROWS {
            q.render(&row("orders", Op::Truncate, "{}"), NOW).unwrap();
        }
        assert!(q.stats().snapshot()[0].unseen_fields.is_empty());
    }

    // --- content routing (R4) ---------------------------------------------------------------------

    fn content(
        name: &str,
        topic: &str,
        fallback: Option<&str>,
        values: Option<&[&str]>,
    ) -> KafkaRoute {
        KafkaRoute {
            topic_fallback: fallback.map(Into::into),
            topic_values: values.map(|v| v.iter().map(|s| s.to_string()).collect()),
            ..named(name, "public.orders", topic)
        }
    }

    fn content_plan(values: Option<&[&str]>, cap: usize) -> RoutePlan {
        let mut p = RoutePlan::new(
            vec![content(
                "o",
                "orders.{row.region}",
                Some("orders.unknown"),
                values,
            )],
            Some("rest".into()),
        )
        .unwrap();
        p.set_source(pg());
        p.set_max_dynamic_topics(cap);
        p
    }

    #[test]
    fn content_routing_picks_the_topic_from_the_row_or_falls_back() {
        let p = content_plan(None, 100);
        let long = "a".repeat(MAX_SEGMENT);
        let too_long = "a".repeat(MAX_SEGMENT + 1);
        let cases: Vec<(Op, String, &str)> = vec![
            (Op::Insert, r#"{"region":"eu"}"#.into(), "orders.eu"),
            (
                Op::Update,
                r#"{"region":"us-east_1"}"#.into(),
                "orders.us-east_1",
            ),
            // `row` is the before-image on a delete
            (Op::Delete, r#"{"region":"apac"}"#.into(), "orders.apac"),
            (Op::Insert, r#"{"region":7}"#.into(), "orders.7"),
            (Op::Insert, r#"{"region":true}"#.into(), "orders.true"),
            (
                Op::Insert,
                format!(r#"{{"region":"{long}"}}"#),
                "orders.aaaa",
            ),
            // null, missing, no row at all
            (Op::Insert, r#"{"region":null}"#.into(), "orders.unknown"),
            (Op::Insert, r#"{"id":1}"#.into(), "orders.unknown"),
            (Op::Truncate, "{}".into(), "orders.unknown"),
            // not one segment of [A-Za-z0-9_-]{1,64}
            (
                Op::Insert,
                r#"{"region":"x.payments"}"#.into(),
                "orders.unknown",
            ),
            (Op::Insert, r#"{"region":""}"#.into(), "orders.unknown"),
            (Op::Insert, r#"{"region":"e u"}"#.into(), "orders.unknown"),
            (Op::Insert, r#"{"region":"../x"}"#.into(), "orders.unknown"),
            (Op::Insert, r#"{"region":1.5}"#.into(), "orders.unknown"),
            (Op::Insert, r#"{"region":"é"}"#.into(), "orders.unknown"),
            (
                Op::Insert,
                format!(r#"{{"region":"{too_long}"}}"#),
                "orders.unknown",
            ),
            (Op::Insert, r#"{"region":{"a":1}}"#.into(), "orders.unknown"),
        ];
        for (op, json, want) in &cases {
            let e = row("orders", *op, json);
            let r = p.render(&e, NOW).unwrap().remove(0);
            if *want == "orders.aaaa" {
                assert_eq!(r.topic, format!("orders.{long}"));
            } else {
                assert_eq!(r.topic, *want, "{op:?} {json}");
            }
            // Topic aside, the record is the same: key, value, headers.
            assert_eq!(r.key, record_key(&e, &pg()));
            assert_eq!(r.value, encode(&e, &pg(), NOW).unwrap());
            assert!(r.headers.is_empty());
        }
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced, c.fallback), (17, 6, 11));
        // Tables no content route claims are unaffected.
        assert_eq!(
            outcome(&p, &row("users", Op::Insert, r#"{"region":"eu"}"#)),
            "rest"
        );
    }

    #[test]
    fn topic_values_allow_list() {
        let p = content_plan(Some(&["eu", "us"]), 100);
        for (json, want) in [
            (r#"{"region":"eu"}"#, "orders.eu"),
            (r#"{"region":"us"}"#, "orders.us"),
            (r#"{"region":"de"}"#, "orders.unknown"),
            (r#"{"region":"EU"}"#, "orders.unknown"),
        ] {
            assert_eq!(
                outcome(&p, &row("orders", Op::Insert, json)),
                want,
                "{json}"
            );
        }
    }

    #[test]
    fn every_substituted_row_value_is_checked_but_schema_and_table_are_not() {
        let mut p = RoutePlan::new(
            vec![KafkaRoute {
                topic_fallback: Some("fb".into()),
                ..route("*.*", "cdc.{schema}.{table}.{row.a}.{after.b}")
            }],
            None,
        )
        .unwrap();
        p.set_source(pg());
        let r = |json: &str| outcome(&p, &row("orders", Op::Insert, json));
        assert_eq!(r(r#"{"a":"x","b":"y"}"#), "cdc.public.orders.x.y");
        assert_eq!(r(r#"{"a":"x","b":"y.z"}"#), "fb");
        assert_eq!(r(r#"{"a":"x"}"#), "fb");
    }

    #[test]
    fn the_dynamic_topic_cap_sends_new_topics_to_the_fallback() {
        let p = content_plan(None, 2);
        let r = |region: &str| {
            outcome(
                &p,
                &row("orders", Op::Insert, &format!(r#"{{"region":"{region}"}}"#)),
            )
        };
        assert_eq!(r("a"), "orders.a");
        assert_eq!(r("b"), "orders.b");
        assert_eq!(r("c"), "orders.unknown", "a third topic is over the cap");
        assert_eq!(r("a"), "orders.a", "known topics stay usable");
        assert_eq!(p.dynamic_topic_count(), 2);
        // A clone (the admin server's copy) shares the count.
        assert_eq!(p.clone().dynamic_topic_count(), 2);
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.produced, c.fallback), (3, 1));
    }

    #[test]
    fn the_cap_counts_topics_already_on_the_broker() {
        let p = content_plan(None, 2);
        let n = p.add_existing_topics([
            "orders.a",
            "orders.b",
            // its own fallback is a fixed topic, not a content-routed one
            "orders.unknown",
            "rest",
            "other.x",
        ]);
        assert_eq!(n, 2);
        let r = |region: &str| {
            outcome(
                &p,
                &row("orders", Op::Insert, &format!(r#"{{"region":"{region}"}}"#)),
            )
        };
        assert_eq!(r("c"), "orders.unknown");
        assert_eq!(r("b"), "orders.b");
        // Topics above the cap that exist already stay usable (a lowered cap never strands them).
        let q = content_plan(None, 1);
        assert_eq!(q.add_existing_topics(["orders.a", "orders.b"]), 2);
        let r = |region: &str| {
            outcome(
                &q,
                &row("orders", Op::Insert, &format!(r#"{{"region":"{region}"}}"#)),
            )
        };
        assert_eq!(r("b"), "orders.b");
        assert_eq!(r("c"), "orders.unknown");
        // Without content routing nothing is counted.
        let s = RoutePlan::new(vec![route("public.*", "orders.{table}")], None).unwrap();
        assert_eq!(s.add_existing_topics(["orders.a"]), 0);
    }

    #[test]
    fn op_and_table_templates_need_no_fallback() {
        let mut p = RoutePlan::new(
            vec![route("public.*", "cdc.{table}.{op}")],
            Some("all.{op}".into()),
        )
        .unwrap();
        p.set_source(pg());
        assert_eq!(
            outcome(&p, &row("orders", Op::Delete, r#"{"id":1}"#)),
            "cdc.orders.delete"
        );
        let mut e = row("orders", Op::Insert, r#"{"id":1}"#);
        e.schema = "sales".into();
        assert_eq!(outcome(&p, &e), "all.insert");
        assert_eq!(p.topic_for("public", "x").as_deref(), Some("cdc.x.{op}"));
    }

    #[test]
    fn content_routing_is_validated_at_startup() {
        let build = |routes: Vec<KafkaRoute>, default: Option<&str>, dlq: Option<&str>| {
            RoutePlan::with_dead_letter(routes, default.map(Into::into), dlq.map(Into::into))
                .map(|_| ())
                .map_err(|e| e.to_string())
        };
        let ok = content("o", "orders.{row.region}", Some("orders.unknown"), None);
        assert_eq!(build(vec![ok.clone()], Some("rest"), Some("dlq")), Ok(()));
        let c = |topic: &str, fb: Option<&str>, values: Option<&[&str]>| {
            content("o", topic, fb, values)
        };
        type Case<'a> = (Vec<KafkaRoute>, Option<&'a str>, Option<&'a str>, &'a str);
        let cases: Vec<Case> = vec![
            // prefix: fixed, non-empty, no leading `_`
            (
                vec![c("{row.tenant}", Some("fb"), None)],
                None,
                None,
                "must start with fixed text",
            ),
            (
                vec![c("{schema}.{row.tenant}", Some("fb"), None)],
                None,
                None,
                "must start with fixed text",
            ),
            (
                vec![c("_x.{row.r}", Some("fb"), None)],
                None,
                None,
                "must not start with `_`",
            ),
            (
                vec![c("orders {row.r}", Some("fb"), None)],
                None,
                None,
                "not a valid Kafka topic name",
            ),
            (
                vec![c("orders.{nope}", Some("fb"), None)],
                None,
                None,
                "not a valid template",
            ),
            (
                vec![c("orders.{row.r", Some("fb"), None)],
                None,
                None,
                "not a valid template",
            ),
            // fallback: required, fixed, valid, no leading `_`
            (
                vec![c("orders.{row.r}", None, None)],
                None,
                None,
                "needs `topic_fallback`",
            ),
            (
                vec![c("orders.{row.r}", Some("fb.{table}"), None)],
                None,
                None,
                "`topic_fallback` `fb.{table}` is not a valid",
            ),
            (
                vec![c("orders.{row.r}", Some("bad topic"), None)],
                None,
                None,
                "is not a valid Kafka topic name",
            ),
            (
                vec![c("orders.{row.r}", Some("_fb"), None)],
                None,
                None,
                "`topic_fallback` `_fb` must not start with `_`",
            ),
            // only content-routed topics take topic_fallback / topic_values
            (
                vec![c("orders", Some("fb"), None)],
                None,
                None,
                "`topic_fallback` is only for a `topic` that reads a row field",
            ),
            (
                vec![c("orders.{table}", None, Some(&["eu"]))],
                None,
                None,
                "`topic_values` is only for",
            ),
            // topic_values: non-empty, each one segment
            (
                vec![c("orders.{row.r}", Some("fb"), Some(&[]))],
                None,
                None,
                "`topic_values` is empty",
            ),
            (
                vec![c("orders.{row.r}", Some("fb"), Some(&["eu", "x.y"]))],
                None,
                None,
                "entry `x.y` can never be used",
            ),
            (
                vec![c("orders.{row.r}", Some("fb"), Some(&["eu", ""]))],
                None,
                None,
                "entry `` can never be used",
            ),
            // collisions with fixed topics: another route, sink.topic, another fallback, the DLQ
            (
                vec![ok.clone(), named("eu", "public.eu", "orders.eu")],
                None,
                None,
                "could produce `orders.eu`, which is route `eu`'s `topic`",
            ),
            (
                vec![named("eu", "public.eu", "orders.eu"), ok.clone()],
                None,
                None,
                "could produce `orders.eu`",
            ),
            (
                vec![ok.clone()],
                Some("orders.all"),
                None,
                "could produce `orders.all`, which is sink.topic",
            ),
            (
                vec![
                    ok.clone(),
                    KafkaRoute {
                        topic_fallback: Some("orders.other".into()),
                        ..named("p", "public.p", "p.{row.x}")
                    },
                ],
                None,
                None,
                "could produce `orders.other`, which is route `p`'s `topic_fallback`",
            ),
            (
                vec![ok.clone()],
                None,
                Some("orders.dlq"),
                "sink.dead_letter_topic `orders.dlq` is also a topic events are routed to",
            ),
            (
                vec![ok.clone()],
                None,
                Some("orders.unknown"),
                "sink.dead_letter_topic `orders.unknown` is also a topic events are routed to",
            ),
            (
                vec![c("p.{row.r}", Some("fb"), None)],
                None,
                Some("fb"),
                "sink.dead_letter_topic `fb` is also a route's `topic_fallback`",
            ),
            // a row-reading sink.topic has nowhere to fall back to
            (
                vec![],
                Some("cdc.{row.r}"),
                None,
                "sink.topic cannot read a row field",
            ),
            // no fixed topic starts with `_`
            (
                vec![route("public.*", "_internal")],
                None,
                None,
                "must not start with `_`",
            ),
            (
                vec![],
                Some("__consumer_offsets"),
                None,
                "must not start with `_`",
            ),
            (
                vec![route("public.*", "bad topic")],
                None,
                None,
                "not a valid Kafka topic name",
            ),
        ];
        for (routes, default, dlq, want) in cases {
            let got = build(routes, default, dlq);
            let err = got.expect_err(want);
            assert!(err.contains(want), "{err}\nshould contain: {want}");
        }
        // A route's own fallback may match its own template; templated topics may share a prefix
        // with another `{schema}`/`{table}` template.
        assert_eq!(
            build(
                vec![ok.clone(), route("public.*", "orders.{table}")],
                Some("cdc.{schema}.{table}"),
                None
            ),
            Ok(())
        );
    }

    #[test]
    fn content_routes_are_described_inspected_and_watched() {
        let p = content_plan(Some(&["eu", "us"]), 100);
        assert_eq!(p.topic_globs(), vec!["orders.*", "rest", "orders.unknown"]);
        assert_eq!(
            p.topic_for("public", "orders").as_deref(),
            Some("orders.{row.region}")
        );
        let d = serde_json::to_value(p.describe()).unwrap();
        assert_eq!(d["routes"][0]["topic"], "orders.{row.region}");
        assert_eq!(d["routes"][0]["topic_fallback"], "orders.unknown");
        assert_eq!(
            d["routes"][0]["topic_values"],
            serde_json::json!(["eu", "us"])
        );
        assert_eq!(d["routes"][1]["topic_fallback"], serde_json::Value::Null);
        // For R6: the template's row fields and the allow-list.
        let fields: Vec<(&str, Option<&str>)> = p.routes[0]
            .topic_fields()
            .map(|f| (f.as_str(), f.column()))
            .collect();
        assert_eq!(fields, [("row.region", Some("region"))]);
        assert_eq!(
            p.routes[0].topic_values(),
            Some(&["eu".to_string(), "us".to_string()][..])
        );
        // A misspelled template field is reported like a `where` field.
        let q = RoutePlan::new(
            vec![content(
                "o",
                "orders.{row.regoin}",
                Some("orders.unknown"),
                None,
            )],
            None,
        )
        .unwrap();
        for _ in 0..UNSEEN_FIELD_ROWS {
            q.render(&row("orders", Op::Insert, r#"{"region":"eu"}"#), NOW)
                .unwrap();
        }
        assert_eq!(q.stats().snapshot()[0].unseen_fields, vec!["row.regoin"]);
    }

    // --- golden: byte-identical to the router and record builders this plan replaced ----------

    const NOW: i64 = 1_792_502_627_093_140;

    fn pg() -> SourceInfo {
        SourceInfo::Postgres { db: "app".into() }
    }

    /// The pre-plan `TopicRouter::topic_for`, verbatim.
    fn legacy_topic(
        routes: &[KafkaRoute],
        default: Option<&str>,
        schema: &str,
        table: &str,
    ) -> Option<String> {
        let name = format!("{schema}.{table}");
        let template = routes
            .iter()
            .find(|r| glob_match(&r.pattern, &name))
            .map(|r| r.topic.as_str())
            .or(default)?;
        Some(
            template
                .replace("{schema}", schema)
                .replace("{table}", table),
        )
    }

    fn raw(s: &str) -> Option<Box<serde_json::value::RawValue>> {
        serde_json::value::RawValue::from_string(s.into()).ok()
    }

    fn event(schema: &str, table: &str, op: Op, key: Option<&str>, pos: Position) -> ChangeEvent {
        ChangeEvent {
            position: pos,
            xid: 9,
            commit_ts_micros: 1_700_000_123_456_789,
            schema: schema.into(),
            table: table.into(),
            op,
            before: matches!(op, Op::Update | Op::Delete)
                .then(|| raw(r#"{"id":1,"n":"a"}"#))
                .flatten(),
            after: matches!(op, Op::Insert | Op::Update)
                .then(|| raw(r#"{"id":1,"n":"é\"x"}"#))
                .flatten(),
            key: key.map(Into::into),
            checkpoint: None,
        }
    }

    #[test]
    fn output_is_byte_identical_to_the_topic_router_it_replaced() {
        let configs: Vec<(Vec<KafkaRoute>, Option<&str>)> = vec![
            (vec![], Some("cdc")),
            (vec![], Some("cdc.{schema}.{table}")),
            (
                vec![
                    route("public.users", "users"),
                    route("public.*", "cdc.{table}"),
                    route("*.audit_*", "audit.{schema}"),
                    route("*", "everything"),
                ],
                None,
            ),
            (
                vec![route("sales.*", "s.{schema}.{table}")],
                Some("fallback.{table}"),
            ),
        ];
        let events = [
            event(
                "public",
                "users",
                Op::Insert,
                Some("1"),
                Position::Lsn(0x16_B374_D848),
            ),
            event(
                "public",
                "orders",
                Op::Update,
                Some("7\u{1f}x"),
                Position::Lsn(42),
            ),
            event(
                "sales",
                "items",
                Op::Delete,
                None,
                Position::Text("resume-token".into()),
            ),
            event("hr", "audit_log", Op::Truncate, None, Position::Lsn(0)),
            event(
                "a.b",
                "c{d}",
                Op::Insert,
                Some("k"),
                Position::Lsn(u64::MAX),
            ),
        ];
        for (routes, default) in &configs {
            for (by, info) in [PartitionBy::Table, PartitionBy::Key]
                .into_iter()
                .flat_map(|by| [(by, pg()), (by, SourceInfo::Mongodb)])
            {
                let mut plan = RoutePlan::new(routes.clone(), default.map(Into::into)).unwrap();
                plan.set_partition_by(by);
                plan.set_source(info.clone());
                for e in &events {
                    let want_topic = legacy_topic(routes, *default, &e.schema, &e.table);
                    let got = plan.render(e, NOW);
                    let Some(topic) = want_topic else {
                        assert!(got.is_err());
                        continue;
                    };
                    let got = got.unwrap();
                    assert_eq!(got.len(), 1);
                    let r = &got[0];
                    assert_eq!(r.topic, topic);
                    let key = record_key(e, &info);
                    assert_eq!(r.partition_key, partition_input(e, &key, by));
                    assert_eq!(r.key, key);
                    assert_eq!(r.value, encode(e, &info, NOW).unwrap());
                    assert_eq!(r.op, op_code(e.op));
                    assert_eq!(r.lsn, e.position.to_checkpoint());
                    assert_eq!(r.commit_ts_micros, e.commit_ts_micros);
                }
            }
        }
    }

    #[test]
    fn pinned_output_for_the_sample_event() {
        // Literal expectations, so a change to key, value or header encoding is a visible diff.
        let mut p = RoutePlan::new(vec![], Some("cdc.{schema}.{table}".into())).unwrap();
        p.set_source(pg());
        let r = p.render(&crate::sample(), NOW).unwrap().remove(0);
        assert_eq!(r.topic, "cdc.public.users");
        assert_eq!(
            r.key,
            r#"{"db":"app","schema":"public","table":"users","key":{"id":1}}"#
        );
        assert_eq!(r.partition_key, r.key);
        assert_eq!(r.op, "c");
        assert_eq!(r.lsn, "97500059720");
        assert_eq!(
            r.value,
            concat!(
                r#"{"before":null,"after":{"id":1,"name":"ada"},"source":{"version":""#,
                env!("CARGO_PKG_VERSION"),
                r#"","connector":"postgresql","ts_ms":1700000000000,"ts_us":1700000000000000,"#,
                r#""snapshot":"false","db":"app","schema":"public","table":"users","txId":7,"#,
                r#""lsn":97500059720},"op":"c","ts_ms":1792502627093,"ts_us":1792502627093140}"#
            )
        );
    }

    /// `cargo test --release -p aqueduct-sinks -- --ignored --nocapture render_cost`
    #[test]
    #[ignore = "timing, run by hand in release mode"]
    fn render_cost_against_the_legacy_path() {
        use std::time::Instant;
        let routes = vec![
            route("public.users", "users"),
            route("public.*", "cdc.{table}"),
        ];
        let mut plan = RoutePlan::new(routes.clone(), Some("rest".into())).unwrap();
        plan.set_source(pg());
        let e = event("public", "orders", Op::Update, Some("7"), Position::Lsn(42));
        let n = 2_000_000;
        let mut sink = 0usize;
        let t = Instant::now();
        for _ in 0..n {
            let t = legacy_topic(&routes, Some("rest"), &e.schema, &e.table).unwrap();
            let k = record_key(&e, &pg());
            let pk = partition_input(&e, &k, PartitionBy::Key);
            let v = encode(&e, &pg(), NOW).unwrap();
            // The old `record()` also built both header values.
            let op = op_code(e.op);
            let lsn = e.position.to_checkpoint();
            sink += t.len() + k.len() + pk.len() + v.len() + op.len() + lsn.len();
        }
        let legacy = t.elapsed();
        let mut out = Vec::new();
        let t = Instant::now();
        for _ in 0..n {
            out.clear();
            plan.render_into(&e, NOW, &mut out).unwrap();
            sink += out[0].topic.len() + out[0].key.len() + out[0].value.len();
        }
        let new = t.elapsed();
        println!("legacy {legacy:?}, plan {new:?} for {n} events (sink {sink})");
    }
}
