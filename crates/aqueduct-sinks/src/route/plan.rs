//! The compiled routing table and its pure `render` step.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use aqueduct_core::{ChangeEvent, Error, Op, Result};
use serde::Serialize;
use serde_json::value::RawValue;

use super::expr::{EventView, Expr, FieldRef, Template, Tri, OP_NAMES};
use super::fields::{FieldError, FieldOps, FieldSpec, HashKey, HASH_KEY_ID_HEADER};
use crate::kafka::{glob_match, partition_input, PartitionBy};
use crate::{
    encode, encode_rows, key_json, key_object, op_code, record_key, table_key, OpCounts, SourceInfo,
};

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
    /// `schema.table` with `*` wildcards, e.g. `public.*`, `*.audit_log`, `*`.
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
    pub partition_key: String,
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

/// The route and fallback of a record whose topic came from row content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentTopic {
    pub route: usize,
    pub fallback: String,
    /// The tombstone that follows a delete (`tombstones = true`): re-routed with the delete, so it
    /// stays on the delete's partition, but not counted again.
    pub tombstone: bool,
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

    /// True when the template renders exactly `name` for some event. Row fields take the values
    /// content routing allows on route `r` ([`value_allowed`]); `{op}` one of the operation names;
    /// `{schema}` and `{table}` one dot-free name (names with dots, which need quoting in Postgres,
    /// are not considered).
    fn produces(&self, r: Option<&CompiledRoute>, name: &str) -> bool {
        if self.is_static() {
            return self.raw == name;
        }
        self.parsed.matches(name, |f, v| match f.column() {
            Some(_) => r.is_some_and(|r| value_allowed(r, f, v)),
            None if f.as_str() == "op" => OP_NAMES.contains(&v),
            None => !v.contains('.'),
        })
    }
}

/// `raw` without its `{op}` placeholders, each removed with one separator (`.`, `_` or `-`) next
/// to it: `cdc.{table}.{op}` becomes `cdc.{table}`. A per-operation dead-letter topic is of no
/// use. Unchanged when nothing would be left (a topic that is only `{op}`).
fn without_op(raw: &str) -> String {
    const OP: &str = "{op}";
    let sep = |c: Option<char>| matches!(c, Some('.' | '_' | '-'));
    let mut s = raw.to_string();
    while let Some(i) = s.find(OP) {
        let (head, tail) = (&s[..i], &s[i + OP.len()..]);
        s = if sep(head.chars().last()) {
            format!("{}{tail}", &head[..head.len() - 1])
        } else if sep(tail.chars().next()) {
            format!("{head}{}", &tail[1..])
        } else {
            format!("{head}{tail}")
        };
    }
    if s.is_empty() {
        raw.to_string()
    } else {
        s
    }
}

/// Where a route's dead-letter records go (ADR 0002, "As built (2026-10-21)").
#[derive(Debug, Clone)]
enum DeadLetter {
    /// One topic: a `dead_letter_topic` override, `<static topic>.dlq` or `<topic_fallback>.dlq`.
    Fixed(String),
    /// `<{schema}/{table} template>.dlq`, rendered for each event like the route's topic, and
    /// created on first use like it.
    Template(TopicTemplate),
}

impl DeadLetter {
    /// The default for a route whose topic is `topic`: its fallback's DLQ for a content-routed
    /// topic (the row is the problem, so the template cannot be rendered), else the topic plus
    /// [`DLQ_SUFFIX`], `{op}` dropped.
    fn derived(topic: &TopicTemplate, fallback: Option<&str>) -> Result<Self> {
        if let (true, Some(f)) = (topic.reads_row, fallback) {
            return Ok(Self::Fixed(format!("{f}{DLQ_SUFFIX}")));
        }
        if topic.is_static() {
            return Ok(Self::Fixed(format!("{}{DLQ_SUFFIX}", topic.raw)));
        }
        let raw = format!("{}{DLQ_SUFFIX}", without_op(&topic.raw));
        let t = parse_topic("dead-letter topic", &raw)?;
        Ok(if t.is_static() {
            Self::Fixed(raw)
        } else {
            Self::Template(t)
        })
    }

    /// The name, or the template with its placeholders.
    fn name(&self) -> &str {
        match self {
            Self::Fixed(t) => t,
            Self::Template(t) => &t.raw,
        }
    }

    fn glob(&self) -> String {
        match self {
            Self::Fixed(t) => t.clone(),
            Self::Template(t) => t.glob(),
        }
    }

    fn fixed(&self) -> Option<&str> {
        match self {
            Self::Fixed(t) => Some(t),
            Self::Template(_) => None,
        }
    }
}

/// One substituted value of a content-routed topic: `[A-Za-z0-9_-]{1,64}`.
fn valid_segment(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= MAX_SEGMENT
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

/// Kafka refuses a topic whose name equals an existing one once `.` and `_` are made the same
/// (the two collide in its metric names).
fn collision_key(topic: &str) -> String {
    topic.replace('.', "_")
}

/// Why a content-routed topic cannot be used.
#[derive(Debug, PartialEq, Eq)]
enum Admit {
    Yes,
    /// Another topic has the same name up to `.` vs `_`; Kafka would refuse to create this one.
    Collides,
    /// New, and `sink.max_dynamic_topics` is reached.
    Cap,
    /// The broker refused to create it earlier in this run.
    Refused,
}

#[derive(Debug, Default)]
struct TopicNames {
    /// Content-routed topics counted against the cap: found on the broker or admitted since.
    counted: HashSet<String>,
    /// Topics known to exist (the broker's at connect, the fixed ones, every one opened since),
    /// which are never un-counted.
    existing: HashSet<String>,
    /// [`collision_key`] -> the spelling that holds it: every existing and counted topic.
    keys: HashMap<String, String>,
    /// Names the broker refused to create for good, so later rows that render them go to the
    /// fallback without another create attempt. Bounded by [`MAX_REFUSED_TOPICS`] (oldest dropped
    /// first) and cleared by a restart, which picks up a pre-created topic or a new ACL.
    refused: HashSet<String>,
    refused_order: VecDeque<String>,
}

/// How many refused topic names the plan remembers.
const MAX_REFUSED_TOPICS: usize = 10_000;

/// The topics content routing created or found, counted against `sink.max_dynamic_topics`, and
/// every topic name known to exist, to refuse a new name Kafka would reject. Shared by every
/// clone of a plan; the sink seeds it from the broker at connect.
#[derive(Debug)]
struct DynamicTopics {
    cap: AtomicUsize,
    names: RwLock<TopicNames>,
    warned: AtomicBool,
}

impl DynamicTopics {
    fn new() -> Self {
        Self {
            cap: AtomicUsize::new(DEFAULT_MAX_DYNAMIC_TOPICS),
            names: RwLock::new(TopicNames::default()),
            warned: AtomicBool::new(false),
        }
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, TopicNames> {
        self.names.write().unwrap_or_else(|e| e.into_inner())
    }

    fn len(&self) -> usize {
        self.names
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .counted
            .len()
    }

    /// Notes topics that exist; `counted` ones also count against the cap and stay usable,
    /// even above it.
    fn exists<'a>(&self, topics: impl IntoIterator<Item = (&'a str, bool)>) {
        let mut w = self.write();
        for (t, counted) in topics {
            w.existing.insert(t.to_string());
            w.keys
                .entry(collision_key(t))
                .or_insert_with(|| t.to_string());
            if counted {
                w.counted.insert(t.to_string());
            }
        }
    }

    /// Whether a content-routed `topic` may be used; a new one is counted.
    fn admit(&self, topic: &str) -> Admit {
        {
            let r = self.names.read().unwrap_or_else(|e| e.into_inner());
            if r.counted.contains(topic) {
                return Admit::Yes;
            }
            if r.refused.contains(topic) {
                return Admit::Refused;
            }
        }
        let mut w = self.write();
        if w.counted.contains(topic) {
            return Admit::Yes;
        }
        if w.refused.contains(topic) {
            return Admit::Refused;
        }
        let key = collision_key(topic);
        if w.keys.get(&key).is_some_and(|held| held != topic) {
            return Admit::Collides;
        }
        let cap = self.cap.load(Ordering::Relaxed);
        if w.counted.len() >= cap {
            if !self.warned.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    max_dynamic_topics = cap,
                    "sink.max_dynamic_topics reached: events that would create another content-routed topic go to their route's topic_fallback"
                );
            }
            return Admit::Cap;
        }
        w.counted.insert(topic.to_string());
        w.keys.insert(key, topic.to_string());
        Admit::Yes
    }

    /// Releases an admitted topic that could not be created, so it holds no cap slot and no name.
    fn release(&self, topic: &str) {
        let mut w = self.write();
        if w.existing.contains(topic) {
            return;
        }
        w.counted.remove(topic);
        let key = collision_key(topic);
        if w.keys.get(&key).is_some_and(|held| held == topic) {
            w.keys.remove(&key);
        }
    }

    /// Releases a topic the broker refused for good and remembers the name for this run.
    fn refuse(&self, topic: &str) {
        self.release(topic);
        let mut w = self.write();
        if w.existing.contains(topic) || !w.refused.insert(topic.to_string()) {
            return;
        }
        w.refused_order.push_back(topic.to_string());
        if w.refused_order.len() > MAX_REFUSED_TOPICS {
            if let Some(oldest) = w.refused_order.pop_front() {
                w.refused.remove(&oldest);
            }
        }
    }
}

/// `sink.max_dynamic_topics` when unset.
pub const DEFAULT_MAX_DYNAMIC_TOPICS: usize = 100;

#[derive(Debug, Clone)]
struct CompiledRoute {
    pattern: String,
    topic: TopicTemplate,
    filter: Option<Expr>,
    /// `key = [...]`: the columns and the `row.<column>` paths that read them, in config order.
    key: Option<RouteKey>,
    /// Where an event goes when its content-routed topic cannot be used. Set exactly when the
    /// topic reads a row field.
    topic_fallback: Option<String>,
    /// The values a content-routed topic's row fields may take; `None` allows any valid segment.
    topic_values: Option<Vec<String>>,
    /// `outbox = {...}`; its topic template is `topic`.
    outbox: Option<Outbox>,
    /// `None`: no field operation, the record is built exactly as without routing.
    fields: Option<FieldOps>,
    /// `format`, `deletes`, `tombstones` and `unchanged`; the default builds the envelope.
    shape: Shape,
}

/// The record value: `format`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Format {
    /// The change event envelope (`before`, `after`, `source`, `op`, ...).
    #[default]
    Envelope,
    /// The row alone: `after`, or for a delete what `deletes` says.
    Row,
}

/// What a delete becomes under `format = "row"`: `deletes`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Deletes {
    /// A null value with the row's key.
    #[default]
    Tombstone,
    /// No record; counted as filtered.
    Drop,
    /// The before-image with `"__deleted": true`.
    Row,
}

impl Deletes {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tombstone => "tombstone",
            Self::Drop => "drop",
            Self::Row => "row",
        }
    }
}

/// How a route shapes its records (ADR 0002 §5, step 5 and 6 of the fixed order).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Shape {
    format: Format,
    deletes: Deletes,
    /// Envelope format: a tombstone after every delete.
    tombstones: bool,
    /// `unchanged = "dead_letter"`: a row with columns the source left out goes to the
    /// dead-letter topic.
    unchanged_dead_letter: bool,
}

impl Shape {
    /// True when the route produces no record for `event` because of its shape: under
    /// `format = "row"`, a truncate (no row), a delete with `deletes = "drop"`, or a change without
    /// the row: no `after`, or an `after` that is only the key (`ChangeEvent::after_is_key`, a
    /// MongoDB update whose document was deleted before the lookup; its delete follows).
    fn drops(&self, event: &ChangeEvent) -> bool {
        self.format == Format::Row
            && match event.op {
                Op::Truncate => true,
                Op::Delete => self.deletes == Deletes::Drop,
                _ => event.after.is_none() || event.after_is_key,
            }
    }

    /// True when a delete produces a record with a null value: the envelope's extra tombstone,
    /// or a row-format delete under `deletes = "tombstone"`. Such a record stands for every
    /// record with its key in a compacted topic, so it needs a per-row key.
    fn null_value_on_delete(&self) -> bool {
        self.tombstones || (self.format == Format::Row && self.deletes == Deletes::Tombstone)
    }
}

/// A column name for the `aqueduct-error` header and the log: control characters and backticks
/// (which quote the name) become `?`, so a hostile name cannot forge lines or quoting.
fn safe_name(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_control() || c == '`' { '?' } else { c })
        .collect()
}

/// The marker a `deletes = "row"` value ends with.
const DELETED_MARK: &str = r#""__deleted":true"#;

/// Matches a string one of `names`, ignoring ASCII case, and says what was expected otherwise.
fn parse_choice<T: Copy>(name: &str, key: &str, value: &str, names: &[(&str, T)]) -> Result<T> {
    names
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(value))
        .map(|(_, t)| *t)
        .ok_or_else(|| {
            let expected: Vec<String> = names.iter().map(|(n, _)| format!("`{n}`")).collect();
            Error::Config(format!(
                "sink route `{name}`: unknown {key} `{value}` (expected {})",
                expected.join(" or ")
            ))
        })
}

/// Validates `format`, `deletes`, `tombstones` and `unchanged`. What depends on `partition_by`
/// (tombstones need a per-row key) is checked by [`RoutePlan::check_partition_by`].
fn parse_shape(name: &str, r: &KafkaRoute) -> Result<Shape> {
    let format = r
        .format
        .as_deref()
        .map(|f| {
            parse_choice(
                name,
                "format",
                f,
                &[("envelope", Format::Envelope), ("row", Format::Row)],
            )
        })
        .transpose()?
        .unwrap_or_default();
    let deletes = match (&r.deletes, format) {
        (None, _) => Deletes::default(),
        (Some(_), Format::Envelope) => {
            return Err(Error::Config(format!(
                "sink route `{name}`: `deletes` applies to `format = \"row\"` only; an envelope route keeps deletes as `op = \"d\"` events (add `tombstones = true` for a tombstone after each)"
            )))
        }
        (Some(d), Format::Row) => parse_choice(
            name,
            "deletes",
            d,
            &[
                ("tombstone", Deletes::Tombstone),
                ("drop", Deletes::Drop),
                ("row", Deletes::Row),
            ],
        )?,
    };
    if r.tombstones && format == Format::Row {
        return Err(Error::Config(format!(
            "sink route `{name}`: `tombstones` is for the envelope format; with `format = \"row\"`, `deletes = \"tombstone\"` (the default) already makes each delete a tombstone"
        )));
    }
    let unchanged_dead_letter = r
        .unchanged
        .as_deref()
        .map(|u| {
            parse_choice(
                name,
                "unchanged",
                u,
                &[("omit", false), ("dead_letter", true)],
            )
        })
        .transpose()?
        .unwrap_or(false);
    // `outbox` routes never get here with a shape key set: `RoutePlan::build` refuses them.
    Ok(Shape {
        format,
        deletes,
        tombstones: r.tombstones,
        unchanged_dead_letter,
    })
}

/// A delete's value under `deletes = "row"`: the (rewritten) before-image with
/// `"__deleted": true` as its last member. A `__deleted` column of the row is replaced by the
/// marker. Without a before-image, or if it is not a JSON object, the value is the marker alone.
fn deleted_row(before: Option<&RawValue>) -> String {
    let marker_only = || format!("{{{DELETED_MARK}}}");
    let Some(text) = before.map(|b| b.get().trim()) else {
        return marker_only();
    };
    let Some(body) = text.strip_prefix('{').and_then(|t| t.strip_suffix('}')) else {
        return marker_only();
    };
    let mut out = String::with_capacity(text.len() + DELETED_MARK.len() + 1);
    out.push('{');
    // Outside a string a quote is never escaped, so `"__deleted"` appears only as that column's
    // name or as a whole string value; anything else keeps the fast path.
    if !body.contains(r#""__deleted""#) {
        if !body.trim().is_empty() {
            out.push_str(body);
            out.push(',');
        }
    } else {
        let Ok(Members(members)) = serde_json::from_str::<Members<'_>>(text) else {
            return marker_only();
        };
        for (k, v) in members.into_iter().filter(|(k, _)| k != "__deleted") {
            out.push_str(&serde_json::to_string(&k).unwrap_or_default());
            out.push(':');
            out.push_str(v.get());
            out.push(',');
        }
    }
    out.push_str(DELETED_MARK);
    out.push('}');
    out
}

/// The top-level members of a JSON object in order, values borrowed raw.
struct Members<'a>(Vec<(String, &'a RawValue)>);

impl<'de> serde::Deserialize<'de> for Members<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct Visit;
        impl<'de> serde::de::Visitor<'de> for Visit {
            type Value = Members<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                while let Some(k) = map.next_key::<String>()? {
                    out.push((k, map.next_value::<&'de RawValue>()?));
                }
                Ok(Members(out))
            }
        }
        d.deserialize_map(Visit)
    }
}

/// `before` and `after` after field operations.
type Rows = (Option<Box<RawValue>>, Option<Box<RawValue>>);

/// A record's parts after the route's field operations, computed before its topic is chosen.
struct Rewritten {
    /// The record key and partition input from the rewritten key object; `None` with `key = [...]`.
    keys: Option<(String, String)>,
    rows: Rows,
    headers: Vec<(&'static str, String)>,
}

/// A compiled `outbox = {...}`: the spec as configured and the `after.<col>` paths it reads.
#[derive(Debug, Clone)]
struct Outbox {
    spec: OutboxSpec,
    aggregate_id: FieldRef,
    payload: FieldRef,
    id: Option<FieldRef>,
    kind: Option<FieldRef>,
}

/// Why an outbox column cannot be used for one row; never holds the value.
enum OutboxError {
    /// Missing or `null`.
    Missing,
    /// A string with an escape that is not Unicode (a lone surrogate), so it has no UTF-8 form.
    NotUnicode,
}

impl Outbox {
    /// Validates the column names (non-empty, top-level) and the topic template, and compiles
    /// the template with `{aggregate_type}` read from `after.<aggregate_type>`.
    fn compile(name: &str, spec: &OutboxSpec) -> Result<(Self, TopicTemplate)> {
        let bad = |msg: String| Error::Config(format!("sink route `{name}`: `outbox` {msg}"));
        let field = |key: &str, col: &str| {
            if col.is_empty() {
                return Err(bad(format!("`{key}` is an empty column name")));
            }
            if col.contains('.') || col.contains('{') || col.contains('}') {
                return Err(bad(format!(
                    "`{key}` column `{col}` is not a top-level column name (no dots or braces)"
                )));
            }
            FieldRef::parse(&format!("after.{col}"))
                .map_err(|e| bad(format!("`{key}` column `{col}`: {e}")))
        };
        field("aggregate_type", &spec.aggregate_type)?;
        let aggregate_id = field("aggregate_id", &spec.aggregate_id)?;
        let payload = field("payload", &spec.payload)?;
        let id = spec.id.as_deref().map(|c| field("id", c)).transpose()?;
        let kind = spec.kind.as_deref().map(|c| field("type", c)).transpose()?;
        let what = format!("sink route `{name}`: `outbox` `topic`");
        let replacement = format!("{{after.{}}}", spec.aggregate_type);
        let rewritten = spec.topic.replace(AGGREGATE_TYPE, &replacement);
        // Syntax errors name the template as written, with the position in it.
        if let Err(e) = Template::parse(&rewritten) {
            let pos = written_pos(&spec.topic, replacement.len(), e.pos);
            return Err(Error::Config(format!(
                "{what} `{}` is not a valid template: {} (at byte {pos})",
                spec.topic, e.msg
            )));
        }
        let mut topic = parse_topic(&what, &rewritten).map_err(|e| {
            // Report the template as written, not the rewritten form.
            Error::Config(e.to_string().replace(&rewritten, &spec.topic))
        })?;
        // Only inserts and snapshot reads are published, and they have no before-image.
        if let Some(f) = topic
            .parsed
            .fields()
            .find(|f| f.as_str().starts_with("before."))
        {
            return Err(bad(format!(
                "`topic` `{}` reads `{f}`, but only inserts and snapshot reads are published and they have no before-image; use `{{aggregate_type}}` or `{{after.<column>}}`",
                spec.topic
            )));
        }
        topic.raw = spec.topic.clone();
        Ok((
            Self {
                spec: spec.clone(),
                aggregate_id,
                payload,
                id,
                kind,
            },
            topic,
        ))
    }

    /// Every row path the outbox reads besides the topic template's, for the unseen-field check.
    fn fields(&self) -> impl Iterator<Item = &FieldRef> {
        [&self.aggregate_id, &self.payload]
            .into_iter()
            .chain(self.id.as_ref())
            .chain(self.kind.as_ref())
    }
}

/// The outbox topic placeholder for the `aggregate_type` column.
const AGGREGATE_TYPE: &str = "{aggregate_type}";

/// Maps byte `pos` in a template whose [`AGGREGATE_TYPE`] placeholders were each replaced by
/// `replacement_len` bytes back to `written`, the template as configured. A position inside a
/// replacement maps to the start of its placeholder.
fn written_pos(written: &str, replacement_len: usize, pos: usize) -> usize {
    let mut shift: isize = 0;
    for (i, _) in written.match_indices(AGGREGATE_TYPE) {
        let start = i.saturating_add_signed(shift);
        if pos < start {
            break;
        }
        if pos < start + replacement_len {
            return i;
        }
        shift += replacement_len as isize - AGGREGATE_TYPE.len() as isize;
    }
    pos.saturating_add_signed(-shift)
}

/// The text of an outbox column as Kafka gets it: a JSON string's content, unescaped, and any
/// other value its JSON text as the row holds it (`json`/`jsonb` columns are already JSON in the
/// row). `null` and a missing column are [`OutboxError::Missing`].
fn outbox_text(view: &EventView<'_>, f: &FieldRef) -> std::result::Result<String, OutboxError> {
    let raw = view.raw(f).ok_or(OutboxError::Missing)?;
    let text = raw.get().trim();
    match text.as_bytes().first() {
        None | Some(b'n') => Err(OutboxError::Missing),
        Some(b'"') => serde_json::from_str::<String>(text).map_err(|_| OutboxError::NotUnicode),
        Some(_) => Ok(text.to_string()),
    }
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
    pub(crate) fn topic_fields(&self) -> impl Iterator<Item = &FieldRef> {
        self.topic.parsed.fields().filter(|f| f.column().is_some())
    }

    /// The `topic_values` allow-list, if any.
    pub(crate) fn topic_values(&self) -> Option<&[String]> {
        self.topic_values.as_deref()
    }

    /// Whether the route is built to dead-letter rows: a `where` on row data (a type error), an
    /// outbox (a row without `aggregate_id` or `payload`), `unchanged = "dead_letter"`, `hash` (a
    /// string it cannot hash) or `rename` without `include` (a name that collides). Its fixed
    /// dead-letter topic is opened at startup. Any other route with field operations dead-letters
    /// only a row that is not a JSON object, which the sources never send.
    fn expects_dead_letters(&self) -> bool {
        self.filter
            .as_ref()
            .is_some_and(|f| f.row_fields().next().is_some())
            || self.outbox.is_some()
            || self.shape.unchanged_dead_letter
            || self
                .fields
                .as_ref()
                .is_some_and(|f| f.hashes() || f.may_collide())
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
    /// `produced` per operation, `[insert, update, delete, truncate, read]`; sums to `produced`.
    produced_ops: Vec<[AtomicU64; 5]>,
    filtered: Vec<AtomicU64>,
    /// Events sent to a route's `topic_fallback` (content routing).
    fallback: Vec<AtomicU64>,
    errors: Vec<AtomicU64>,
    /// Deletes that got no tombstone because their key is null (`tombstones = true`).
    tombstones_skipped: Vec<AtomicU64>,
    /// Per route, whether the first skipped tombstone was logged.
    tombstone_logged: Vec<AtomicBool>,
    /// Per route, whether the first row-format change with only a key as `after` was logged.
    key_only_logged: Vec<AtomicBool>,
    /// Per route, every field path it references. `where` registers its paths here; templates,
    /// `key`, field operations and outbox columns register theirs the same way.
    fields: Vec<Vec<WatchedField>>,
    /// Per route, whether the first dead-lettered event was logged.
    error_logged: Vec<AtomicBool>,
    /// Per route, whether the first event sent to `topic_fallback` was logged.
    fallback_logged: Vec<AtomicBool>,
    /// Per route, whether the first refused topic create was logged.
    refused_logged: Vec<AtomicBool>,
    /// Per route, the counts of the batch being rendered, applied to the counters above only once
    /// the broker acknowledged the batch ([`RoutePlan::commit_counts`]), so a failed batch the
    /// pipeline retries is not counted twice.
    pending: Mutex<Vec<PendingCounts>>,
}

/// The counts of one route in a batch the broker has not acknowledged yet. Wrapping arithmetic,
/// so `topic_refused` can move a count from `produced` to `fallback` as a delta.
#[derive(Debug, Clone, Copy, Default)]
struct PendingCounts {
    matched: u64,
    produced: u64,
    produced_ops: [u64; 5],
    filtered: u64,
    fallback: u64,
    errors: u64,
    tombstones_skipped: u64,
}

/// The final outcome of an event a route claimed, other than `produced`.
#[derive(Debug, Clone, Copy)]
enum Outcome {
    Filtered,
    Fallback,
    Errors,
}

/// The outcome counters of one route. Every event a route claims counts `matched`, then exactly
/// one of `produced`, `filtered`, `fallback` or `errors`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RouteCounts {
    /// Events the route claimed (first match wins).
    pub matched: u64,
    /// Events produced to the route's topic.
    pub produced: u64,
    /// Events dropped by `where` (including a `where` that ends unknown), and outbox changes
    /// other than inserts and snapshot reads.
    pub filtered: u64,
    /// Events sent to the route's `topic_fallback`.
    pub fallback: u64,
    /// Events sent to the route's dead-letter topic because a row could not be evaluated.
    pub errors: u64,
    /// Not an outcome: deletes produced without the tombstone `tombstones = true` asks for,
    /// because their key is null (a table without a primary key), so the tombstone would erase
    /// the whole table from a compacted topic.
    pub tombstones_skipped: u64,
    /// `produced` per operation of the event (a delete's tombstone is not counted again).
    pub ops: OpCounts,
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
            produced_ops: names
                .iter()
                .map(|_| std::array::from_fn(|_| AtomicU64::new(0)))
                .collect(),
            filtered: zeros(),
            fallback: zeros(),
            errors: zeros(),
            tombstones_skipped: zeros(),
            tombstone_logged: names.iter().map(|_| AtomicBool::new(false)).collect(),
            key_only_logged: names.iter().map(|_| AtomicBool::new(false)).collect(),
            fields,
            error_logged: names.iter().map(|_| AtomicBool::new(false)).collect(),
            fallback_logged: names.iter().map(|_| AtomicBool::new(false)).collect(),
            refused_logged: names.iter().map(|_| AtomicBool::new(false)).collect(),
            pending: Mutex::new(vec![PendingCounts::default(); names.len()]),
            names,
        }
    }

    /// Updates the pending counts of `route`.
    fn pend(&self, route: usize, f: impl FnOnce(&mut PendingCounts)) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut pending[route]);
    }

    /// Counts an event `route` claimed under `outcome`, together with `matched`. Called only
    /// once the outcome is final (the record is rendered), so an encode error never leaves
    /// `matched` above the sum of the outcomes. Pending until [`commit`](Self::commit).
    fn claimed(&self, outcome: Outcome, route: usize) {
        self.pend(route, |p| {
            p.matched = p.matched.wrapping_add(1);
            let n = match outcome {
                Outcome::Filtered => &mut p.filtered,
                Outcome::Fallback => &mut p.fallback,
                Outcome::Errors => &mut p.errors,
            };
            *n = n.wrapping_add(1);
        });
    }

    /// Counts an event `route` produced to its topic: `matched`, `produced` and its op. Pending
    /// until [`commit`](Self::commit).
    fn produced(&self, route: usize, op: &str) {
        self.pend(route, |p| {
            p.matched = p.matched.wrapping_add(1);
            p.produced = p.produced.wrapping_add(1);
            if let Some(i) = OpCounts::slot(op) {
                p.produced_ops[i] = p.produced_ops[i].wrapping_add(1);
            }
        });
    }

    /// Applies the pending counts to the counters and clears them.
    fn commit(&self) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        for (i, p) in pending.iter_mut().enumerate() {
            // `fetch_add` wraps like the deltas, so a moved count (`topic_refused`) nets out.
            let add = |v: &[AtomicU64], n: u64| {
                if n != 0 {
                    v[i].fetch_add(n, Ordering::Relaxed);
                }
            };
            add(&self.matched, p.matched);
            add(&self.produced, p.produced);
            add(&self.filtered, p.filtered);
            add(&self.fallback, p.fallback);
            add(&self.errors, p.errors);
            add(&self.tombstones_skipped, p.tombstones_skipped);
            for (k, &n) in p.produced_ops.iter().enumerate() {
                if n != 0 {
                    self.produced_ops[i][k].fetch_add(n, Ordering::Relaxed);
                }
            }
            *p = PendingCounts::default();
        }
    }

    /// Drops the pending counts: their batch failed, is retried and is counted then.
    fn discard(&self) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.fill(PendingCounts::default());
    }

    fn counts(&self, i: usize) -> RouteCounts {
        let get = |v: &[AtomicU64]| v[i].load(Ordering::Relaxed);
        RouteCounts {
            matched: get(&self.matched),
            produced: get(&self.produced),
            filtered: get(&self.filtered),
            fallback: get(&self.fallback),
            errors: get(&self.errors),
            tombstones_skipped: get(&self.tombstones_skipped),
            ops: OpCounts::from_slots(std::array::from_fn(|k| {
                self.produced_ops[i][k].load(Ordering::Relaxed)
            })),
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
    /// `sink.dead_letter_topic`, the global override; each route's own is in its description.
    pub dead_letter_topic: Option<String>,
    /// `key` or `table`.
    pub partition_by: &'static str,
    /// `sink.hash_key_id`, when a hash key is configured. Never the key itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_key_id: Option<String>,
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
    /// `outbox = {...}`: the column names and the topic template (also in `topic`); left out
    /// for other routes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outbox: Option<OutboxSpec>,
    /// Field operations: column names (and the rename map), left out when unset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mask: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rename: Option<BTreeMap<String, String>>,
    /// `row` for a route with `format = "row"`; left out for the envelope.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<&'static str>,
    /// With `format = "row"`: `tombstone`, `drop` or `row`, defaults included; left out otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletes: Option<&'static str>,
    /// `true` for an envelope route with `tombstones = true`; left out otherwise.
    #[serde(skip_serializing_if = "is_false")]
    pub tombstones: bool,
    /// `dead_letter` for a route with `unchanged = "dead_letter"`; left out for `omit`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unchanged: Option<&'static str>,
    /// Where rows the route cannot evaluate go: a topic, or a `{schema}`/`{table}` template
    /// rendered per event (`cdc.{schema}.{table}.dlq`). `null` only when the default name is not
    /// a usable topic name and the route cannot dead-letter anyway.
    pub dead_letter_topic: Option<String>,
    pub counts: RouteCounts,
    pub unseen_fields: Vec<String>,
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
}

fn is_false(b: &bool) -> bool {
    !*b
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

/// Whether a content-routed template may substitute `v` for `f`: row fields must be one segment
/// and in `topic_values`; `{schema}`, `{table}` and `{op}` are not checked.
fn value_allowed(r: &CompiledRoute, f: &FieldRef, v: &str) -> bool {
    f.column().is_none()
        || (valid_segment(v)
            && v != RESERVED_SEGMENT
            && r.topic_values
                .as_ref()
                .is_none_or(|vals| vals.iter().any(|a| a == v)))
}

/// True when some string matches both globs (`*` matches any run of characters).
fn globs_intersect(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    // memo[i][j]: Some(result) for the suffixes a[i..] and b[j..].
    let mut memo = vec![vec![None; b.len() + 1]; a.len() + 1];
    fn go(a: &[u8], b: &[u8], i: usize, j: usize, memo: &mut Vec<Vec<Option<bool>>>) -> bool {
        if let Some(r) = memo[i][j] {
            return r;
        }
        let r = if i == a.len() && j == b.len() {
            true
        } else if i < a.len() && a[i] == b'*' {
            go(a, b, i + 1, j, memo) || (j < b.len() && go(a, b, i, j + 1, memo))
        } else if j < b.len() && b[j] == b'*' {
            go(a, b, i, j + 1, memo) || (i < a.len() && go(a, b, i + 1, j, memo))
        } else {
            i < a.len() && j < b.len() && a[i] == b[j] && go(a, b, i + 1, j + 1, memo)
        };
        memo[i][j] = Some(r);
        r
    }
    go(a, b, 0, 0, &mut memo)
}

/// Parses a topic or topic template. Fixed text must be valid in a Kafka topic name (Kafka would
/// refuse anything else on the first send). The leading-`_` rule is checked by the plan, since it
/// only applies when content routing is in use.
fn parse_topic(what: &str, raw: &str) -> Result<TopicTemplate> {
    let t = TopicTemplate::new(raw)
        .map_err(|e| Error::Config(format!("{what} `{raw}` is not a valid template: {e}")))?;
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
        if values.iter().any(|v| v == RESERVED_SEGMENT) {
            return err(format!(
                "`topic_values` entry `{RESERVED_SEGMENT}` can never be used: it is reserved, so no row can render a dead-letter topic name (`...{DLQ_SUFFIX}`)"
            ));
        }
    }
    Ok((Some(fallback.clone()), r.topic_values.clone()))
}

/// No fixed topic (a static route topic, a static `sink.topic`, a fallback) may match a
/// content-routed template read as a glob, except that route's own fallback: otherwise a row value
/// could steer events into a topic another part of the config owns. Dead-letter topics are checked
/// by [`check_dead_letter`].
fn check_collisions(
    names: &[String],
    routes: &[CompiledRoute],
    default: Option<&TopicTemplate>,
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
    // Topics a `{schema}`/`{table}`/`{op}` template writes: a row value must not reach them either.
    let plain: Vec<(String, &str, String)> = routes
        .iter()
        .enumerate()
        .filter(|(_, r)| !r.topic.reads_row && !r.topic.is_static())
        .map(|(i, r)| {
            (
                r.topic.glob(),
                r.topic.raw.as_str(),
                format!("route `{}`'s `topic`", names[i]),
            )
        })
        .chain(
            default
                .filter(|d| !d.is_static())
                .map(|d| (d.glob(), d.raw.as_str(), "sink.topic".to_string())),
        )
        .collect();
    for (i, r) in routes.iter().enumerate().filter(|(_, r)| r.topic.reads_row) {
        let glob = r.topic.glob();
        if let Some((other, raw, what)) = plain.iter().find(|(g, _, _)| globs_intersect(&glob, g)) {
            return Err(Error::Config(format!(
                "sink route `{}`: `topic` `{}` (as the glob `{glob}`) overlaps {what} `{raw}` (as `{other}`): a row value could write into that route's topics. Give the content-routed template a fixed text no other template can produce",
                names[i], r.topic.raw
            )));
        }
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

/// Checks one dead-letter topic; `what` names where it comes from. A fixed name must be a valid
/// topic name that does not start with `_`, is not a route's `topic_fallback`, and is not a name
/// any route's topic (or `sink.topic`) can render ([`TopicTemplate::produces`]): no row value,
/// table or operation may steer events into it. A route's own `<topic_fallback>.dlq` passes this
/// for its content-routed `<prefix>.{row.x}`: a row value is one segment without dots and never
/// [`RESERVED_SEGMENT`]. A template (`<{schema}/{table} template>.dlq`) must not start with `_` or
/// overlap a content-routed template read as a glob.
fn check_dead_letter(
    what: &str,
    hint: Option<&str>,
    dlq: &DeadLetter,
    routes: &[CompiledRoute],
    default: Option<&TopicTemplate>,
) -> Result<()> {
    // Every error says what to do: `hint` for a default name, else pick another one.
    let pick = hint.unwrap_or("; pick a separate topic");
    let hint = hint.unwrap_or_default();
    let bad = |msg: String| Err(Error::Config(format!("{what} `{}` {msg}", dlq.name())));
    if dlq.name().starts_with('_') {
        return bad(format!(
            "must not start with `_` (reserved for internal topics){hint}"
        ));
    }
    match dlq {
        DeadLetter::Fixed(t) => {
            if !valid_topic_name(t) {
                return bad(format!(
                    "is not a valid Kafka topic name (1 to 249 of A-Z a-z 0-9 . _ -){hint}"
                ));
            }
            let topics = routes
                .iter()
                .map(|r| (&r.topic, Some(r)))
                .chain(default.map(|d| (d, None)));
            if let Some((owner, _)) = topics.into_iter().find(|(tpl, r)| tpl.produces(*r, t)) {
                return bad(format!(
                    "is also a topic events are routed to (`{}`){pick}",
                    owner.raw
                ));
            }
            if routes
                .iter()
                .any(|r| r.topic_fallback.as_deref() == Some(t.as_str()))
            {
                return bad(format!("is also a route's `topic_fallback`{pick}"));
            }
        }
        DeadLetter::Template(t) => {
            let glob = t.glob();
            if let Some(r) = routes
                .iter()
                .find(|r| r.topic.reads_row && globs_intersect(&r.topic.glob(), &glob))
            {
                return bad(format!(
                    "(as the glob `{glob}`) overlaps the content-routed `topic` `{}`: a row value could write into it{hint}",
                    r.topic.raw
                ));
            }
        }
    }
    Ok(())
}

/// Startup checks of a route's field operations: `hash` needs the key, and no row data may
/// bypass the operations (ADR 0002
/// §5): `key = [...]` and a content-routed topic read the original row, so neither may name a
/// column the route removes, masks or hashes. `topic_values` constrains the values of those same
/// template fields, and `topic_fallback` is a fixed topic, so checking the template's fields covers
/// both.
fn check_field_ops(name: &str, r: &CompiledRoute, hash_key: Option<&HashKey>) -> Result<()> {
    let Some(f) = &r.fields else {
        return Ok(());
    };
    if f.hashes() && hash_key.is_none() {
        return Err(Error::Config(format!(
            "sink route `{name}` uses `hash`, which needs a key: set AQUEDUCT_HASH_KEY or sink.hash_key_file, and sink.hash_key_id"
        )));
    }
    for c in r.key_columns().unwrap_or_default() {
        if let Some(how) = f.protection(c) {
            return Err(Error::Config(format!(
                "sink route `{name}`: `key` column `{c}` is {how} by the route's field operations, so the record key would carry it unprotected; key by other columns"
            )));
        }
    }
    for field in r.topic_fields() {
        let Some(c) = field.column() else { continue };
        if let Some(how) = f.protection(c) {
            let values = if r.topic_values().is_some() {
                " (and `topic_values`)"
            } else {
                ""
            };
            return Err(Error::Config(format!(
                "sink route `{name}`: `topic` reads `{field}`, but column `{c}` is {how} by the route's field operations, so the topic name{values} would carry it unprotected; route by another column"
            )));
        }
    }
    // `format = "row"` values and `deletes = "row"` before-images come from `rewrite_rows`, and a
    // tombstone copies its delete's record key (the route key, or the `rewrite_key` object), so
    // neither bypasses the operations (`RoutePlan::render_into`).
    Ok(())
}

impl RoutePlan {
    /// A plan without `sink.dead_letter_topic`: each route dead-letters to its own topic (route
    /// `dead_letter_topic`, else `<topic>.dlq`).
    pub fn new(routes: Vec<KafkaRoute>, default: Option<String>) -> Result<Self> {
        Self::with_dead_letter(routes, default, None)
    }

    /// Compiles and validates the routes. `dead_letter_topic` (`sink.dead_letter_topic`) is the
    /// dead-letter topic of every route without its own; it may not start with `_`, and may not
    /// be a topic a route or the default writes to.
    pub fn with_dead_letter(
        routes: Vec<KafkaRoute>,
        default: Option<String>,
        dead_letter_topic: Option<String>,
    ) -> Result<Self> {
        Self::build(routes, default, dead_letter_topic, None)
    }

    /// [`with_dead_letter`](Self::with_dead_letter) with the key for `hash`, which is required
    /// as soon as a route hashes a column.
    pub fn build(
        routes: Vec<KafkaRoute>,
        default: Option<String>,
        dead_letter_topic: Option<String>,
        hash_key: Option<HashKey>,
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
            let filter = r
                .filter
                .as_deref()
                .map(|w| {
                    Expr::parse(w).map_err(|e| {
                        Error::Config(format!("sink route `{name}`: invalid `where`: {e}"))
                    })
                })
                .transpose()?;
            let key = r.key.as_deref().map(|k| parse_key(&name, k)).transpose()?;
            let (topic, outbox) = match &r.outbox {
                None => {
                    if r.topic.is_empty() {
                        return Err(Error::Config(format!(
                            "sink route `{name}` has no `topic`; set one (or `outbox`)"
                        )));
                    }
                    let what = format!("sink route `{name}`: `topic`");
                    (parse_topic(&what, &r.topic)?, None)
                }
                Some(spec) => {
                    // The outbox decides the topic, the key and the value (ADR 0002 §5), so the
                    // keys that would set them too are refused rather than silently ignored.
                    // Field operations are refused as a whole: an outbox route has none, so no
                    // protected column can reach its `aggregate_id`, `topic` or payload.
                    // The shape keys are refused too: the outbox record is its own shape, and an
                    // outbox route publishes no deletes to tombstone.
                    let shaped: Vec<&str> = [
                        ("format", r.format.is_some()),
                        ("deletes", r.deletes.is_some()),
                        ("tombstones", r.tombstones),
                        ("unchanged", r.unchanged.is_some()),
                    ]
                    .into_iter()
                    .filter_map(|(k, on)| on.then_some(k))
                    .collect();
                    if !shaped.is_empty() {
                        return Err(Error::Config(format!(
                            "sink route `{name}`: `outbox` and `{}` are mutually exclusive: an outbox record is the `payload` column as is, keyed by `aggregate_id`",
                            shaped.join("`, `")
                        )));
                    }
                    let f = &r.fields;
                    let set: Vec<&str> = [
                        ("include", f.include.is_some()),
                        ("exclude", f.exclude.is_some()),
                        ("mask", !f.mask.is_empty()),
                        ("hash", !f.hash.is_empty()),
                        ("rename", !f.rename.is_empty()),
                    ]
                    .into_iter()
                    .filter_map(|(k, on)| on.then_some(k))
                    .collect();
                    if !set.is_empty() {
                        return Err(Error::Config(format!(
                            "sink route `{name}`: `outbox` and the field operations (`{}`) are mutually exclusive: an outbox record is the `payload` column as is, so shape the payload in the application, or route the table without `outbox`",
                            set.join("`, `")
                        )));
                    }
                    if !r.topic.is_empty() {
                        return Err(Error::Config(format!(
                            "sink route `{name}`: `topic` and `outbox` are mutually exclusive; set the topic template as `outbox.topic`"
                        )));
                    }
                    if r.key.is_some() {
                        return Err(Error::Config(format!(
                            "sink route `{name}`: `key` and `outbox` are mutually exclusive; an outbox record is keyed by its `aggregate_id`"
                        )));
                    }
                    // Only inserts and snapshot reads are published, so a `where` on the
                    // before-image drops every row.
                    if let Some(f) = filter.as_ref().and_then(|w| {
                        w.fields()
                            .into_iter()
                            .find(|f| f.as_str().starts_with("before."))
                    }) {
                        return Err(Error::Config(format!(
                            "sink route `{name}`: `where` reads `{f}`, but an outbox route only publishes inserts and snapshot reads, which have no before-image; use `after.` or `row.`"
                        )));
                    }
                    let (outbox, topic) = Outbox::compile(&name, spec)?;
                    (topic, Some(outbox))
                }
            };
            let (topic_fallback, topic_values) = check_content_routing(&name, r, &topic)?;
            let field_ops = FieldOps::compile(&name, &r.fields)?;
            let shape = parse_shape(&name, r)?;
            // `op`, `schema` and `table` are never missing, so only the row paths of `where` are
            // watched.
            let mut watched: Vec<FieldRef> = filter
                .iter()
                .flat_map(|f| f.row_fields())
                .chain(key.iter().flat_map(|k| k.fields.iter()))
                .chain(topic.parsed.fields())
                .chain(outbox.iter().flat_map(Outbox::fields))
                .cloned()
                .collect();
            for c in r.fields.columns() {
                // Top-level names only (checked by `FieldOps::compile`), so `row.<name>` parses.
                watched.extend(FieldRef::parse(&format!("row.{c}")).ok());
            }
            fields.push(watched);
            let route = CompiledRoute {
                pattern: r.pattern.clone(),
                topic,
                filter,
                key,
                topic_fallback,
                topic_values,
                outbox,
                fields: field_ops,
                shape,
            };
            check_field_ops(&name, &route, hash_key.as_ref())?;
            compiled.push(route);
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
        // Once row data can choose topics, no topic in the config may start with `_` (Kafka's
        // internal topics, such as `__consumer_offsets`). Without content routing, topics are
        // accepted as before. Fallbacks and the dead-letter topic are checked on their own.
        if compiled.iter().any(|r| r.topic.reads_row) {
            let topics = compiled
                .iter()
                .zip(&names)
                .map(|(r, n)| (format!("sink route `{n}`: `topic`"), &r.topic))
                .chain(default.as_ref().map(|d| ("sink.topic".to_string(), d)));
            for (what, t) in topics {
                if t.raw.starts_with('_') {
                    return Err(Error::Config(format!(
                        "{what} `{}` must not start with `_` (reserved for internal topics) when a route chooses its topic from row content",
                        t.raw
                    )));
                }
            }
        }
        check_collisions(&names, &compiled, default.as_ref())?;
        // Dead-letter topics: the route's own, else `sink.dead_letter_topic`, else `<topic>.dlq`.
        // The global override is checked once, as itself.
        if let Some(dlq) = &dead_letter_topic {
            let d = DeadLetter::Fixed(dlq.clone());
            check_dead_letter(
                "sink.dead_letter_topic",
                None,
                &d,
                &compiled,
                default.as_ref(),
            )?;
        }
        let mut dead_letters = Vec::with_capacity(names.len());
        let topics = compiled
            .iter()
            .zip(&routes)
            .map(|(c, r)| (&c.topic, c.topic_fallback.as_deref(), Some((c, r))))
            .chain(default.as_ref().map(|d| (d, None, None)));
        for (i, (topic, fallback, route)) in topics.enumerate() {
            let name = &names[i];
            let own = route.and_then(|(_, r)| r.dead_letter_topic.as_ref());
            if let Some(dlq) = own {
                let what = format!("sink route `{name}`: `dead_letter_topic`");
                let d = DeadLetter::Fixed(dlq.clone());
                check_dead_letter(&what, None, &d, &compiled, default.as_ref())?;
                dead_letters.push(Some(d));
            } else if let Some(dlq) = &dead_letter_topic {
                dead_letters.push(Some(DeadLetter::Fixed(dlq.clone())));
            } else {
                // A default name that is not usable fails startup only for a route built to
                // dead-letter rows; any other gets none (a row it cannot rewrite stops the
                // pipeline, as before dead-letter topics).
                let what = format!(
                    "sink route `{name}`: the dead-letter topic (its `{}` plus `{DLQ_SUFFIX}`)",
                    if fallback.is_some() {
                        "topic_fallback"
                    } else {
                        "topic"
                    }
                );
                let hint = "; set `dead_letter_topic` on the route or `sink.dead_letter_topic`";
                let d = DeadLetter::derived(topic, fallback).and_then(|d| {
                    check_dead_letter(&what, Some(hint), &d, &compiled, default.as_ref())
                        .map(|()| d)
                });
                match d {
                    Ok(d) => dead_letters.push(Some(d)),
                    Err(e) if route.is_some_and(|(c, _)| c.expects_dead_letters()) => {
                        return Err(e)
                    }
                    Err(_) => dead_letters.push(None),
                }
            }
        }
        let mut plan = Self {
            routes: compiled,
            default,
            dead_letter: dead_letter_topic,
            dead_letters,
            partition_by: PartitionBy::default(),
            source: SourceInfo::default(),
            stats: Arc::new(RouteStats::new(names, fields)),
            dynamic: Arc::new(DynamicTopics::new()),
            fixed: Vec::new(),
            hash_key,
        };
        plan.fixed = plan.static_topics();
        // A content-routed name must not collide (`.` vs `_`) with a fixed topic either.
        plan.dynamic
            .exists(plan.fixed.iter().map(|t| (t.as_str(), false)));
        Ok(plan)
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
            if let Some(t) = d.as_ref().and_then(DeadLetter::fixed) {
                if r.expects_dead_letters() && !out.iter().any(|o| o == t) {
                    out.push(t.to_string());
                }
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
    /// dead-letter topics (each once), with placeholders turned into `*`.
    pub fn topic_globs(&self) -> Vec<String> {
        let mut globs: Vec<String> = self
            .routes
            .iter()
            .map(|r| r.topic.glob())
            .chain(self.default.as_ref().map(TopicTemplate::glob))
            .chain(self.fallback_topics().map(str::to_string))
            .collect();
        for g in self.dead_letters.iter().flatten().map(DeadLetter::glob) {
            if !globs.contains(&g) {
                globs.push(g);
            }
        }
        globs
    }

    /// The compiled plan and its live counts, for `/api/routes`.
    pub fn describe(&self) -> RoutesDescription {
        let route = |i: usize, pattern: Option<String>, r: Option<&CompiledRoute>, topic: &str| {
            let spec = r.and_then(|r| r.fields.as_ref()).map(FieldOps::spec);
            let list = |v: &Vec<String>| (!v.is_empty()).then(|| v.clone());
            let shape = r.map(|r| r.shape).unwrap_or_default();
            let row = shape.format == Format::Row;
            RouteDescription {
                format: row.then_some("row"),
                deletes: row.then(|| shape.deletes.as_str()),
                tombstones: shape.tombstones,
                unchanged: shape.unchanged_dead_letter.then_some("dead_letter"),
                include: spec.and_then(|s| s.include.clone()),
                exclude: spec.and_then(|s| s.exclude.as_ref().and_then(list)),
                mask: spec.and_then(|s| list(&s.mask)),
                hash: spec.and_then(|s| list(&s.hash)),
                rename: spec.and_then(|s| (!s.rename.is_empty()).then(|| s.rename.clone())),
                route: self.stats.names[i].clone(),
                pattern,
                filter: r
                    .and_then(|r| r.filter.as_ref())
                    .map(|f| f.source().to_string()),
                key: r.and_then(|r| r.key_columns()).map(<[String]>::to_vec),
                topic: topic.to_string(),
                topic_fallback: r.and_then(|r| r.topic_fallback.clone()),
                topic_values: r.and_then(|r| r.topic_values().map(<[String]>::to_vec)),
                outbox: r.and_then(|r| r.outbox.as_ref()).map(|o| o.spec.clone()),
                dead_letter_topic: self.dead_letter_name(i),
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
            hash_key_id: self.hash_key.as_ref().map(|k| k.id().to_string()),
            routes,
        }
    }

    /// Appends the records for `event` to `out`. Pure: no I/O, and the clock is the caller's
    /// `now_us` (the envelope's top-level `ts_ms`/`ts_us`). Heartbeats produce nothing. An event
    /// no route claims is an error, as before. The route counts stay pending until
    /// [`commit_counts`](Self::commit_counts), so a batch the broker refused is not counted.
    ///
    /// The first matching route owns the event (ADR 0002 §2): its `where` decides on the original
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
        // Field operations, applied before the topic is chosen, so a row they cannot rewrite goes
        // to the dead-letter topic without taking a content-routing cap slot.
        let mut rewritten = None;
        let mut fell_back = false;
        let mut content = None;
        // The delete's key is not per-row, so the envelope tombstone is left out.
        let mut skip_tombstone = false;
        let topic = match self.routes.get(route) {
            None => template.render(&view),
            Some(r) => {
                // An outbox publishes inserts, and snapshot reads only when the route has a
                // `where` (checked below, so it must be true for them; ADR 0002 §5, "As built"):
                // updates, deletes and truncates of the outbox table are filtered before anything
                // else.
                if r.outbox.is_some()
                    && !(event.op == Op::Insert || (event.op == Op::Read && r.filter.is_some()))
                {
                    self.stats.claimed(Outcome::Filtered, route);
                    return Ok(());
                }
                self.stats.observe(route, &view);
                if let Some(filter) = &r.filter {
                    match filter.eval(&view) {
                        Ok(Tri::True) => {}
                        Ok(Tri::False | Tri::Unknown) => {
                            self.stats.claimed(Outcome::Filtered, route);
                            return Ok(());
                        }
                        Err(e) => {
                            let reason = format!(
                                "route `{}`: where `{}`: {e}",
                                self.stats.names[route],
                                filter.source()
                            );
                            self.dead_letter(route, reason, event, now_us, out)?;
                            self.stats.claimed(Outcome::Errors, route);
                            return Ok(());
                        }
                    }
                }
                if let Some(outbox) = &r.outbox {
                    return self.render_outbox(route, r, outbox, &view, now_us, out);
                }
                // Row format: a truncate, a dropped delete or a change without a row is no record.
                if r.shape.drops(event) {
                    if event.after_is_key && event.op != Op::Delete {
                        self.key_only_dropped(route);
                    }
                    self.stats.claimed(Outcome::Filtered, route);
                    return Ok(());
                }
                // A row without its unchanged TOAST columns, where the route wants none: the
                // dead-letter topic. Columns the route drops anyway (excluded, not included) do
                // not count. The reason names the columns, never values.
                let missing: Vec<String> = if r.shape.unchanged_dead_letter {
                    event
                        .unchanged
                        .iter()
                        .filter(|c| {
                            r.fields.as_ref().is_none_or(|f| {
                                !matches!(f.protection(c), Some("excluded" | "not included"))
                            })
                        })
                        .map(|c| format!("`{}`", safe_name(c)))
                        .collect()
                } else {
                    Vec::new()
                };
                if !missing.is_empty() {
                    let cols = missing;
                    let reason = format!(
                        "route `{}`: unchanged = \"dead_letter\": the source left out unchanged column(s) {}",
                        self.stats.names[route],
                        cols.join(", ")
                    );
                    self.dead_letter(route, reason, event, now_us, out)?;
                    self.stats.claimed(Outcome::Errors, route);
                    return Ok(());
                }
                route_key = r.key.as_ref().map(|k| self.route_key(k, &view));
                if let Some(f) = &r.fields {
                    match self.rewrite(f, event, route_key.is_none()) {
                        Ok(w) => rewritten = Some(w),
                        Err(e) => {
                            let reason = format!("route `{}`: {e}", self.stats.names[route]);
                            self.dead_letter(route, reason, event, now_us, out)?;
                            self.stats.claimed(Outcome::Errors, route);
                            return Ok(());
                        }
                    }
                }
                // A null-value record stands for every record with its key in a compacted topic,
                // so it is produced only under a key that names this one row. Checked before the
                // topic is chosen, so a skipped row-format delete takes no content-routing slot.
                if event.op == Op::Delete
                    && r.shape.null_value_on_delete()
                    && !self.key_is_per_row(r, &view)
                {
                    self.tombstone_skipped(route);
                    if r.shape.format == Format::Row {
                        // The delete is the tombstone itself: nothing safe is left to produce.
                        self.stats.claimed(Outcome::Filtered, route);
                        return Ok(());
                    }
                    skip_tombstone = true;
                }
                let (topic, c, fallback) = self.route_topic(route, r, &view);
                content = c;
                fell_back = fallback;
                topic
            }
        };
        let shape = self.routes.get(route).map(|r| r.shape).unwrap_or_default();
        let wants_tombstone = shape.tombstones && event.op == Op::Delete && !skip_tombstone;
        let rewritten_keys = rewritten.as_mut().and_then(|w| w.keys.take());
        let (key, partition_key) = match (route_key, rewritten_keys) {
            // A per-route key partitions by the whole key, whatever `partition_by` says.
            (Some(key), _) => (key.clone(), key),
            // The key object after field operations, partitioned like the plain key.
            (None, Some(keys)) => keys,
            (None, None) => {
                let key = record_key(event, &self.source);
                let partition_key = partition_input(event, &key, self.partition_by);
                (key, partition_key)
            }
        };
        // The value and headers are fixed here, before the sink may re-route the record to the
        // fallback (`topic_refused` changes only the topic), so a fallback record carries the
        // same rewritten value, key and `aqueduct-hash-key-id` as any other. The rows are the
        // rewritten ones when the route has field operations, so no shape bypasses them.
        let (before, after) = match &rewritten {
            Some(w) => (w.rows.0.as_deref(), w.rows.1.as_deref()),
            None => (event.before.as_deref(), event.after.as_deref()),
        };
        let value = match shape.format {
            Format::Envelope => Some(encode_rows(event, &self.source, now_us, before, after)?),
            Format::Row => match event.op {
                // `deletes = "tombstone"`: the delete itself is the tombstone. `drop` never
                // gets here (`Shape::drops`).
                Op::Delete => (shape.deletes == Deletes::Row).then(|| deleted_row(before)),
                // `Shape::drops` filtered a change without `after`.
                _ => Some(after.map_or("null", |a| a.get()).to_string()),
            },
        };
        let headers = rewritten.map(|w| w.headers).unwrap_or_default();
        let record = OutRecord {
            topic,
            partition_key,
            key,
            value,
            op: op_code(event.op),
            lsn: event.position.to_checkpoint(),
            commit_ts_micros: event.commit_ts_micros,
            headers,
            content,
        };
        // Envelope `tombstones`: a null value with the delete's (rewritten) key, topic and
        // partition input, right after it, so it lands on the same partition behind it.
        let tombstone = wants_tombstone.then(|| OutRecord {
            topic: record.topic.clone(),
            key: record.key.clone(),
            partition_key: record.partition_key.clone(),
            value: None,
            op: record.op,
            lsn: record.lsn.clone(),
            commit_ts_micros: record.commit_ts_micros,
            headers: record.headers.clone(),
            content: record.content.as_ref().map(|c| ContentTopic {
                tombstone: true,
                ..c.clone()
            }),
        });
        let op = record.op;
        out.push(record);
        out.extend(tombstone);
        if fell_back {
            self.stats.claimed(Outcome::Fallback, route);
        } else {
            self.stats.produced(route, op);
        }
        Ok(())
    }

    /// Whether the record key of a delete on route `r` names exactly one row, so a record with a
    /// null value under it removes only that row from a compacted topic. One check for envelope
    /// tombstones and row-format delete tombstones:
    ///
    /// - `key = [...]`: every listed column has a non-null value in the before-image. The columns
    ///   must also be unique, which cannot be checked.
    /// - Field operations on the key object: there is a primary key, and none of its columns is
    ///   excluded, left out of `include` or masked (all rows would share `"***"`). Hashing (HMAC)
    ///   and renaming keep it unique.
    /// - Otherwise: the event has a primary key (`"key"` is not null).
    fn key_is_per_row(&self, r: &CompiledRoute, view: &EventView<'_>) -> bool {
        if let Some(k) = &r.key {
            return k
                .fields
                .iter()
                .all(|f| view.raw(f).is_some_and(|v| v.get().trim() != "null"));
        }
        let Some(obj) = key_object(view.event()) else {
            return false;
        };
        let Ok(Members(cols)) = serde_json::from_str::<Members<'_>>(obj) else {
            return false;
        };
        !cols.is_empty()
            && r.fields.as_ref().is_none_or(|f| {
                cols.iter()
                    .all(|(c, _)| matches!(f.protection(c), None | Some("hashed")))
            })
    }

    /// A delete whose key is not per-row (see [`key_is_per_row`](Self::key_is_per_row)) got no
    /// null-value record: one would erase every row sharing the key (the whole table, for a
    /// null key) from a compacted topic. Counted, and logged once per route.
    fn tombstone_skipped(&self, route: usize) {
        self.stats.pend(route, |p| {
            p.tombstones_skipped = p.tombstones_skipped.wrapping_add(1)
        });
        if !self.stats.tombstone_logged[route].swap(true, Ordering::Relaxed) {
            tracing::warn!(
                route = %self.stats.names[route],
                "a delete whose record key does not name a single row (no primary key, a missing or null `key` column, or a key column the field operations remove or mask) got no tombstone, since it would erase other rows from a compacted topic; later ones are only counted under tombstones_skipped"
            );
        }
    }

    /// A row-format route dropped a change whose `after` is only the key (a MongoDB update whose
    /// document was deleted before the lookup). Logged once per route; counted as `filtered`.
    fn key_only_dropped(&self, route: usize) {
        if !self.stats.key_only_logged[route].swap(true, Ordering::Relaxed) {
            tracing::warn!(
                route = %self.stats.names[route],
                "an update without its full document (deleted before the lookup) was dropped: a row-format route never publishes the key alone as the row; later ones are only counted under filtered"
            );
        }
    }

    /// The topic of route `r` for this event, the [`ContentTopic`] of a topic chosen from row
    /// content, and whether the event went to the route's `topic_fallback`.
    fn route_topic(
        &self,
        route: usize,
        r: &CompiledRoute,
        view: &EventView<'_>,
    ) -> (String, Option<ContentTopic>, bool) {
        match self.content_topic(route, r, view) {
            Ok((topic, from_row)) => {
                let content = r
                    .topic_fallback
                    .clone()
                    .filter(|_| from_row)
                    .map(|fallback| ContentTopic {
                        route,
                        fallback,
                        tombstone: false,
                    });
                (topic, content, false)
            }
            Err(fallback) => (fallback, None, true),
        }
    }

    /// The record for an insert into an outbox table (ADR 0002 §5): topic from the outbox's
    /// template, key = the `aggregate_id` value, value = the `payload` value, headers `id` and
    /// `type` when configured and present. A row without an `aggregate_id` or `payload` goes to
    /// the dead-letter topic, with an `aqueduct-error` header naming the column, never the value.
    fn render_outbox(
        &self,
        route: usize,
        r: &CompiledRoute,
        outbox: &Outbox,
        view: &EventView<'_>,
        now_us: i64,
        out: &mut Vec<OutRecord>,
    ) -> Result<()> {
        let event = view.event();
        let spec = &outbox.spec;
        let required = [
            ("aggregate_id", &spec.aggregate_id, &outbox.aggregate_id),
            ("payload", &spec.payload, &outbox.payload),
        ];
        let optional = [
            ("id", spec.id.as_ref(), outbox.id.as_ref()),
            ("type", spec.kind.as_ref(), outbox.kind.as_ref()),
        ];
        let mut texts = Vec::with_capacity(2);
        let mut headers = Vec::new();
        let mut failed = None;
        for (what, col, f) in required {
            match outbox_text(view, f) {
                Ok(t) => texts.push(t),
                Err(e) => {
                    failed = Some((what, col, e));
                    break;
                }
            }
        }
        if failed.is_none() {
            for (what, col, f) in optional {
                let (Some(col), Some(f)) = (col, f) else {
                    continue;
                };
                match outbox_text(view, f) {
                    Ok(t) => headers.push((what, t)),
                    Err(OutboxError::Missing) => {}
                    Err(e) => {
                        failed = Some((what, col, e));
                        break;
                    }
                }
            }
        }
        if let Some((what, col, e)) = failed {
            let why = match e {
                OutboxError::Missing => "is missing or null",
                OutboxError::NotUnicode => "is a string that is not valid Unicode",
            };
            let reason = format!(
                "route `{}`: outbox `{what}` column `{col}` {why}",
                self.stats.names[route]
            );
            self.dead_letter(route, reason, event, now_us, out)?;
            self.stats.claimed(Outcome::Errors, route);
            return Ok(());
        }
        let payload = texts.pop().unwrap_or_default();
        let key = texts.pop().unwrap_or_default();
        let (topic, content, fell_back) = self.route_topic(route, r, view);
        out.push(OutRecord {
            topic,
            partition_key: key.clone(),
            key,
            value: Some(payload),
            op: op_code(event.op),
            lsn: event.position.to_checkpoint(),
            commit_ts_micros: event.commit_ts_micros,
            headers,
            content,
        });
        if fell_back {
            self.stats.claimed(Outcome::Fallback, route);
        } else {
            self.stats.produced(route, op_code(event.op));
        }
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
    ) -> std::result::Result<(String, bool), String> {
        let Some(fallback) = r.topic_fallback.as_ref().filter(|_| r.topic.reads_row) else {
            return Ok((r.topic.render(view), false));
        };
        let rendered = r
            .topic
            .parsed
            .render_checked(view, |f, v| value_allowed(r, f, v));
        let why = match rendered {
            Ok(t) if !valid_topic_name(&t) => {
                "the rendered topic is not a valid Kafka topic name".to_string()
            }
            // The route's own fallback (the only fixed topic its glob may match): no cap slot.
            Ok(t) if self.fixed.contains(&t) => "the value names a fixed topic".to_string(),
            Ok(t) => match self.dynamic.admit(&t) {
                Admit::Yes => return Ok((t, true)),
                Admit::Collides => {
                    "a topic with the same name up to `.` vs `_` exists, so Kafka would refuse it"
                        .to_string()
                }
                Admit::Cap => "sink.max_dynamic_topics is reached".to_string(),
                Admit::Refused => {
                    "the broker refused to create this topic earlier in this run".to_string()
                }
            },
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

    /// The broker refused to create a content-routed topic for a reason that will not change on a
    /// retry (`kind`: an invalid name, a create policy, no create permission). Sends `out` to its
    /// route's fallback instead, counts it under `fallback` rather than `produced`, and frees the
    /// name and its cap slot. Logged once per route, with the template, never the value.
    pub fn topic_refused(&self, out: &mut OutRecord, kind: &str) {
        let Some(c) = out.content.take() else {
            return;
        };
        self.dynamic.refuse(&out.topic);
        // A delete's tombstone follows it to the fallback; the event was counted once, with
        // the delete.
        if !c.tombstone {
            self.stats.pend(c.route, |p| {
                p.produced = p.produced.wrapping_sub(1);
                if let Some(i) = OpCounts::slot(out.op) {
                    p.produced_ops[i] = p.produced_ops[i].wrapping_sub(1);
                }
                p.fallback = p.fallback.wrapping_add(1);
            });
        }
        if !self.stats.refused_logged[c.route].swap(true, Ordering::Relaxed) {
            tracing::warn!(
                route = %self.stats.names[c.route],
                topic = %self.routes.get(c.route).map_or("", |r| r.topic.raw.as_str()),
                fallback = %c.fallback,
                error = kind,
                "the broker refused to create a content-routed topic; such events go to the route's topic_fallback (pre-create the topics or set topic_values)"
            );
        }
        out.topic = c.fallback;
    }

    /// A content-routed topic could not be created for a passing reason: free its cap slot, so a
    /// failed create never holds one. The batch fails and is retried.
    pub fn topic_failed(&self, topic: &str) {
        self.dynamic.release(topic);
    }

    /// The sink opened `topic`: it exists, so no other spelling of it may be created.
    pub fn topic_opened(&self, topic: &str) {
        if self.routes.iter().any(|r| r.topic.reads_row) {
            self.dynamic.exists([(topic, false)]);
        }
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

    /// The value rows, key and headers of a record of a route with field operations. `own_key`:
    /// the route has no `key = [...]`, so the key object is rewritten too.
    fn rewrite(
        &self,
        fields: &FieldOps,
        event: &ChangeEvent,
        own_key: bool,
    ) -> std::result::Result<Rewritten, FieldError> {
        let rows = self.rewrite_rows(fields, event, true)?;
        let keys = if own_key {
            let k = self.rewrite_key(fields, event)?;
            let key = key_json(event, &self.source, k.as_deref().unwrap_or("null"));
            // Partitioned by the rewritten key, so a hashed key column keeps per-row order.
            let partition_key = partition_input(event, &key, self.partition_by);
            Some((key, partition_key))
        } else {
            None
        };
        Ok(Rewritten {
            keys,
            rows,
            headers: self.hash_headers(fields),
        })
    }

    /// `before` and `after` with the route's field operations applied.
    fn rewrite_rows(
        &self,
        fields: &FieldOps,
        event: &ChangeEvent,
        renames: bool,
    ) -> std::result::Result<Rows, FieldError> {
        let key = self.hash_key.as_ref();
        let row = |r: &Option<Box<RawValue>>| {
            r.as_deref()
                .map(|r| fields.rewrite_raw(r, key, renames))
                .transpose()
        };
        Ok((row(&event.before)?, row(&event.after)?))
    }

    /// The record key's `key` object with the route's field operations applied: excluded columns
    /// removed, masked, hashed and renamed ones as in the value. An object left empty is `None`
    /// (`"key": null`).
    fn rewrite_key(
        &self,
        fields: &FieldOps,
        event: &ChangeEvent,
    ) -> std::result::Result<Option<String>, FieldError> {
        let Some(k) = key_object(event) else {
            return Ok(None);
        };
        let mut out = String::with_capacity(k.len());
        let n = fields.rewrite(k, self.hash_key.as_ref(), true, &mut out)?;
        Ok((n > 0).then_some(out))
    }

    /// `aqueduct-hash-key-id` on every record of a route that hashes.
    fn hash_headers(&self, fields: &FieldOps) -> Vec<(&'static str, String)> {
        match &self.hash_key {
            Some(k) if fields.hashes() => vec![(HASH_KEY_ID_HEADER, k.id().to_string())],
            _ => Vec::new(),
        }
    }

    /// The dead-letter record for an event route `route` could not evaluate. `reason` names the
    /// route, the expression and the type problem, never a row value: it becomes the
    /// `aqueduct-error` header and a log line. The key is the table-only record key (no row data),
    /// and the partition is hashed from it. The value has the route's field operations applied
    /// (ADR 0002 §5a), so the dead-letter topic never holds data the route would have removed,
    /// masked or hashed: renamed if the renames apply, else under the original names, and with
    /// `before`/`after` null if a row is not an object the operations can rewrite.
    fn dead_letter(
        &self,
        route: usize,
        reason: String,
        event: &ChangeEvent,
        now_us: i64,
        out: &mut Vec<OutRecord>,
    ) -> Result<()> {
        let topic = match self.dead_letters.get(route).and_then(Option::as_ref) {
            Some(DeadLetter::Fixed(t)) => t.clone(),
            Some(DeadLetter::Template(t)) => t.render(&EventView::new(event)),
            None => {
                return Err(Error::Sink(format!(
                "{reason}, and the route has no usable dead-letter topic; set `dead_letter_topic`"
            )))
            }
        };
        // A rendered name (`{schema}`/`{table}` from the event) that Kafka would refuse, or that
        // starts with `_`, fails the batch like any topic that cannot be created.
        if !valid_topic_name(&topic) || topic.starts_with('_') {
            return Err(Error::Sink(format!(
                "{reason}, and its dead-letter topic `{topic}` is not a usable topic name"
            )));
        }
        let key = table_key(event, &self.source);
        let fields = self.routes.get(route).and_then(|r| r.fields.as_ref());
        let mut headers = vec![("aqueduct-error", reason.clone())];
        let value = match fields {
            None => encode(event, &self.source, now_us)?,
            Some(f) => {
                headers.extend(self.hash_headers(f));
                let (before, after) = self
                    .rewrite_rows(f, event, true)
                    .or_else(|_| self.rewrite_rows(f, event, false))
                    .unwrap_or((None, None));
                encode_rows(
                    event,
                    &self.source,
                    now_us,
                    before.as_deref(),
                    after.as_deref(),
                )?
            }
        };
        out.push(OutRecord {
            topic,
            partition_key: key.clone(),
            key,
            value: Some(value),
            op: op_code(event.op),
            lsn: event.position.to_checkpoint(),
            commit_ts_micros: event.commit_ts_micros,
            headers,
            content: None,
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

    /// The records for one event, counted at once ([`commit_counts`](Self::commit_counts)): for
    /// callers that deliver nothing. The sink renders with [`render_into`](Self::render_into).
    pub fn render(&self, event: &ChangeEvent, now_us: i64) -> Result<Vec<OutRecord>> {
        let mut out = Vec::with_capacity(1);
        self.render_into(event, now_us, &mut out)?;
        self.commit_counts();
        Ok(out)
    }

    /// Applies the route counts of everything rendered since the last commit or discard. The sink
    /// calls it once the broker acknowledged the batch, so the counters show delivered events.
    pub fn commit_counts(&self) {
        self.stats.commit();
    }

    /// Drops the route counts of everything rendered since the last commit or discard: the batch
    /// failed and the pipeline retries it, so it is counted when it is delivered.
    pub fn discard_counts(&self) {
        self.stats.discard();
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
        // Then each route's dead-letter topic: `<topic>.dlq`, templates as globs.
        assert_eq!(
            p.topic_globs(),
            vec![
                "users",
                "a.*",
                "cdc.*.*",
                "users.dlq",
                "a.*.dlq",
                "cdc.*.*.dlq"
            ]
        );
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
                ops: OpCounts {
                    insert: n,
                    ..Default::default()
                },
                ..Default::default()
            },
            unseen_fields: vec![],
        };
        assert_eq!(
            p.stats().snapshot(),
            vec![c("users", 2), c("sales.*", 1), c("default", 1)]
        );
    }

    // --- filter: where (op, schema, table, rows), dead-letter topic (R3) ------------------------

    const DLQ: &str = "aqueduct.dlq";

    fn filtered(name: &str, p: &str, filter: Option<&str>) -> KafkaRoute {
        KafkaRoute {
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
    fn where_filters_ops_and_rows_per_route_table_driven() {
        let p = plan_with(vec![
            filtered("ins", "public.ins", Some("op == 'insert'")),
            filtered("w", "public.orders", Some("row.status == 'paid'")),
            filtered(
                "both",
                "public.both",
                Some("op in ['insert', 'update'] and row.amount > 10"),
            ),
            filtered(
                "keep_null",
                "public.k",
                Some("row.region is null or not (row.region == 'eu')"),
            ),
            filtered("neg", "public.neg", Some("not (row.region == 'eu')")),
            filtered(
                "nodel",
                "public.nodel",
                Some("not (op in ['delete', 'truncate'])"),
            ),
        ]);
        let cases: &[(&str, Op, &str, &str)] = &[
            // op
            ("ins", Op::Insert, r#"{"id":1}"#, "ins.topic"),
            ("ins", Op::Update, r#"{"id":1}"#, "filtered"),
            ("ins", Op::Delete, r#"{"id":1}"#, "filtered"),
            ("ins", Op::Truncate, "{}", "filtered"),
            ("ins", Op::Read, r#"{"id":1}"#, "filtered"),
            ("nodel", Op::Update, r#"{"id":1}"#, "nodel.topic"),
            ("nodel", Op::Read, r#"{"id":1}"#, "nodel.topic"),
            ("nodel", Op::Delete, r#"{"id":1}"#, "filtered"),
            ("nodel", Op::Truncate, "{}", "filtered"),
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
            // op and row data in one predicate
            ("both", Op::Insert, r#"{"amount":11}"#, "both.topic"),
            ("both", Op::Update, r#"{"amount":"10.5"}"#, "both.topic"),
            ("both", Op::Insert, r#"{"amount":9}"#, "filtered"),
            ("both", Op::Delete, r#"{"amount":11}"#, "filtered"),
            ("both", Op::Insert, r#"{"amount":"abc"}"#, "dlq"),
            // a type error wins over the op part too (no short-circuit, ADR 0002 §3)
            ("both", Op::Delete, r#"{"amount":"abc"}"#, "dlq"),
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
            filtered("strict", "public.orders", Some("row.n > 0")),
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
        // The route has no field operations, so the value is the envelope as is.
        assert_eq!(r.value.text(), encode(&e, &pg(), NOW).unwrap());
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
            filtered("o", "public.orders", Some("op == 'insert' and row.n > 1")),
            route("public.*", "p"),
        ]);
        for (table, op, json) in [
            ("orders", Op::Insert, r#"{"n":2}"#),   // produced
            ("orders", Op::Insert, r#"{"n":1}"#),   // filtered by where
            ("orders", Op::Update, r#"{"n":2}"#),   // filtered by op
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
            tombstones_skipped: 0,
            // Every produced event here is an insert.
            ops: OpCounts {
                insert: produced,
                ..Default::default()
            },
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
            assert_eq!(k.ops.total(), k.produced);
        }
    }

    #[test]
    fn route_counts_wait_for_the_broker_ack() {
        let p = plan_with(vec![filtered("o", "public.orders", Some("row.n > 1"))]);
        let batch = [
            row("orders", Op::Insert, r#"{"n":2}"#),   // produced
            row("orders", Op::Insert, r#"{"n":1}"#),   // filtered
            row("orders", Op::Insert, r#"{"n":"x"}"#), // error
        ];
        let render = || {
            let mut out = Vec::new();
            for e in &batch {
                p.render_into(e, NOW, &mut out).unwrap();
            }
        };
        let counts = || p.stats().snapshot()[0].counts;
        // The sink renders a batch the broker then refuses: nothing is counted, and the retry
        // starts from a clean slate.
        render();
        assert_eq!(counts(), RouteCounts::default());
        p.discard_counts();
        p.commit_counts();
        assert_eq!(counts(), RouteCounts::default());
        // The retry is acknowledged: the batch counts once.
        render();
        p.commit_counts();
        let c = counts();
        assert_eq!(
            (c.matched, c.produced, c.filtered, c.errors, c.ops.insert),
            (3, 1, 1, 1, 1)
        );
        p.commit_counts();
        assert_eq!(counts(), c);
    }

    #[test]
    fn produced_counts_per_operation_with_tombstones_under_their_delete() {
        let p = plan_with(vec![KafkaRoute {
            tombstones: true,
            ..route("public.*", "p")
        }]);
        for (op, n) in [
            (Op::Insert, 3),
            (Op::Update, 2),
            (Op::Delete, 2),
            (Op::Truncate, 1),
            (Op::Read, 4),
        ] {
            for _ in 0..n {
                p.render(&row("orders", op, r#"{"n":1}"#), NOW).unwrap();
            }
        }
        // Heartbeats are no event of any route.
        p.render(&row("orders", Op::Heartbeat, "{}"), NOW).unwrap();
        let c = p.stats().snapshot()[0].counts;
        // A delete and its tombstone are one produced event, counted under delete.
        assert_eq!(c.ops.slots(), [3, 2, 2, 1, 4]);
        assert_eq!(c.ops.total(), c.produced);
        assert_eq!(
            serde_json::to_value(c.ops).unwrap(),
            serde_json::json!({"insert": 3, "update": 2, "delete": 2, "truncate": 1, "read": 4})
        );
    }

    #[test]
    fn where_is_validated_at_startup() {
        let dlq = Some(DLQ.to_string());
        let build = |r: KafkaRoute, dlq: Option<String>| {
            RoutePlan::with_dead_letter(vec![r], Some("rest".into()), dlq)
                .map(|_| ())
                .map_err(|e| e.to_string())
        };
        // (where, dead_letter_topic, expected error or "" for ok)
        let cases: &[(&str, Option<&str>, &str)] = &[
            // `op`, `schema` and `table` alone cannot fail, so no dead-letter topic is needed.
            ("op in ['insert', 'update', 'read']", None, ""),
            (
                "op == 'delete' or schema == 'audit' and table != 'log'",
                None,
                "",
            ),
            ("op is not null", None, ""),
            (
                "op == 'upsert'",
                None,
                "sink route `r`: invalid `where`: unknown op `upsert`",
            ),
            ("op in ['insert', 'Update']", None, "unknown op `Update`"),
            ("op == 1", None, "sink route `r`: invalid `where`"),
            ("table in [true]", None, "`table` is a string"),
            ("op < 'insert'", None, "ordering a string is not supported"),
            // Reading row data needs a dead-letter topic, and there always is one: the route's
            // `r.topic.dlq` without `sink.dead_letter_topic`.
            ("row.n > 1", Some(DLQ), ""),
            ("row.n > 1", None, ""),
            ("op == 'insert' and before.n is null", None, ""),
            ("row.n >", Some(DLQ), "sink route `r`: invalid `where`"),
            (
                "row.ts < '2024-01-01'",
                Some(DLQ),
                "sink route `r`: invalid `where`",
            ),
            ("", Some(DLQ), "sink route `r`: invalid `where`"),
            ("row.n > 1", Some("_dlq"), "must not start with `_`"),
            ("op == 'insert'", Some("_dlq"), "must not start with `_`"),
            (
                "row.n > 1",
                Some("r.topic"),
                "also a topic events are routed to",
            ),
            (
                "row.n > 1",
                Some("rest"),
                "also a topic events are routed to",
            ),
            (
                "row.n > 1",
                Some("bad topic"),
                "not a valid Kafka topic name",
            ),
            ("row.n > 1", Some(""), "not a valid Kafka topic name"),
        ];
        for (filter, dlq_topic, want) in cases {
            let r = filtered("r", "public.*", Some(filter));
            let got = build(r, dlq_topic.map(str::to_string));
            if want.is_empty() {
                assert_eq!(got, Ok(()), "{filter:?} {dlq_topic:?}");
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
                Some("op in ['insert', 'update'] and row.status == 'paid'"),
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
                    {"route": "orders", "match": "public.orders",
                     "where": "op in ['insert', 'update'] and row.status == 'paid'", "key": null, "topic": "orders.topic", "topic_fallback": null, "topic_values": null,
                     "dead_letter_topic": DLQ,
                     "counts": {"matched": 1, "produced": 1, "filtered": 0, "fallback": 0, "errors": 0, "tombstones_skipped": 0,
                                "ops": {"insert": 1, "update": 0, "delete": 0, "truncate": 0, "read": 0}},
                     "unseen_fields": []},
                    {"route": "public.*", "match": "public.*", "where": null,
                     "key": ["id", "tenant"], "topic": "cdc.{table}", "topic_fallback": null, "topic_values": null,
                     "dead_letter_topic": DLQ,
                     "counts": {"matched": 0, "produced": 0, "filtered": 0, "fallback": 0, "errors": 0, "tombstones_skipped": 0,
                                "ops": {"insert": 0, "update": 0, "delete": 0, "truncate": 0, "read": 0}},
                     "unseen_fields": []},
                    {"route": "default", "match": null, "where": null, "key": null,
                     "topic": "rest", "topic_fallback": null, "topic_values": null,
                     "dead_letter_topic": DLQ,
                     "counts": {"matched": 0, "produced": 0, "filtered": 0, "fallback": 0, "errors": 0, "tombstones_skipped": 0,
                                "ops": {"insert": 0, "update": 0, "delete": 0, "truncate": 0, "read": 0}},
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
            assert_eq!(r.value.text(), encode(&e, &pg(), NOW).unwrap());
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
            ..filtered("o", "public.orders", Some("row.n > 1"))
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
            assert_eq!(r.value.text(), encode(&e, &pg(), NOW).unwrap());
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
                Some("orders.eu"),
                "sink.dead_letter_topic `orders.eu` is also a topic events are routed to",
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
            // with content routing, no topic starts with `_`
            (
                vec![ok.clone(), route("public.*", "_internal")],
                None,
                None,
                "sink route `public.*`: `topic` `_internal` must not start with `_`",
            ),
            (
                vec![ok.clone()],
                Some("__consumer_offsets"),
                None,
                "sink.topic `__consumer_offsets` must not start with `_`",
            ),
            (
                vec![ok.clone()],
                Some("_cdc.{table}"),
                None,
                "sink.topic `_cdc.{table}` must not start with `_`",
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
        // Without content routing, `_`-prefixed topics are accepted as before.
        assert_eq!(
            build(
                vec![
                    route("public.*", "_internal"),
                    route("hr.*", "_hr.{table}.{op}")
                ],
                Some("__cdc.{schema}"),
                None
            ),
            Ok(())
        );
        // A route's own fallback may match its own template, and `{schema}`/`{table}` templates
        // with other fixed text are fine.
        assert_eq!(
            build(
                vec![ok.clone(), route("public.*", "cdc.{table}")],
                Some("all.{schema}.{table}"),
                None
            ),
            Ok(())
        );
        // A content template may not overlap a `{schema}`/`{table}`/`{op}` template: a row value
        // could write into that route's topics (`cdc.{row.kind}` = `customers` -> `cdc.customers`).
        for (routes, default, want) in [
            (
                vec![
                    content("o", "cdc.{row.kind}", Some("fb"), None),
                    route("public.*", "cdc.{table}"),
                ],
                None,
                "overlaps route `public.*`'s `topic` `cdc.{table}`",
            ),
            (
                vec![
                    route("public.*", "cdc.{table}"),
                    content("o", "cdc.{row.kind}", Some("fb"), None),
                ],
                None,
                "overlaps route `public.*`'s `topic` `cdc.{table}`",
            ),
            (
                vec![ok.clone()],
                Some("orders.{schema}.{table}"),
                "overlaps sink.topic `orders.{schema}.{table}`",
            ),
            (
                vec![ok.clone(), route("public.*", "ord{table}")],
                None,
                "overlaps route `public.*`'s `topic` `ord{table}`",
            ),
            (
                vec![ok.clone(), route("public.*", "x.{op}")],
                Some("{table}"),
                "overlaps sink.topic `{table}`",
            ),
        ] {
            let err = build(routes, default, None).expect_err(want);
            assert!(err.contains(want), "{err}\nshould contain: {want}");
        }
    }

    #[test]
    fn glob_intersection() {
        for (a, b, want) in [
            ("orders.*", "orders.*", true),
            ("orders.*", "cdc.*", false),
            ("orders.*", "ord*", true),
            ("orders.*", "*", true),
            ("orders.*", "*.eu", true),
            ("orders.*.x", "orders.y.*", true),
            ("a*b", "a*c", false),
            ("orders.unknown", "orders.*", true),
            ("x", "x", true),
            ("x", "y", false),
            ("*a", "b*", true),
        ] {
            assert_eq!(globs_intersect(a, b), want, "{a} {b}");
            assert_eq!(globs_intersect(b, a), want, "{b} {a}");
        }
    }

    #[test]
    fn a_name_kafka_would_refuse_as_a_collision_goes_to_the_fallback() {
        // Two placeholders: `a_b` + `c` and `a` + `b_c` render names equal up to `.` vs `_`.
        let mut p = RoutePlan::new(
            vec![content(
                "e",
                "events.{row.tenant}.{row.kind}",
                Some("events-fallback"),
                None,
            )],
            None,
        )
        .unwrap();
        p.set_source(pg());
        let r = |tenant: &str, kind: &str| {
            let json = format!(r#"{{"tenant":"{tenant}","kind":"{kind}"}}"#);
            outcome(&p, &row("orders", Op::Insert, &json))
        };
        assert_eq!(r("a_b", "c"), "events.a_b.c");
        assert_eq!(r("a", "b_c"), "events-fallback");
        assert_eq!(
            r("a_b", "c"),
            "events.a_b.c",
            "the first spelling stays usable"
        );
        assert_eq!(
            p.dynamic_topic_count(),
            1,
            "a refused name holds no cap slot"
        );
        // A broker topic that only differs by `.` vs `_`, even one no template produces.
        let q = content_plan(None, 100);
        q.add_existing_topics(["orders_eu", "unrelated"]);
        assert_eq!(q.dynamic_topic_count(), 0);
        let r = |region: &str| {
            outcome(
                &q,
                &row("orders", Op::Insert, &format!(r#"{{"region":"{region}"}}"#)),
            )
        };
        assert_eq!(r("eu"), "orders.unknown");
        assert_eq!(r("us"), "orders.us");
        // The same against a fixed topic of the config.
        let s = RoutePlan::new(
            vec![
                content("o", "orders.{row.region}", Some("orders.unknown"), None),
                route("hr.*", "orders_us"),
            ],
            None,
        )
        .unwrap();
        assert_eq!(
            outcome(&s, &row("orders", Op::Insert, r#"{"region":"us"}"#)),
            "orders.unknown"
        );
    }

    #[test]
    fn a_value_naming_the_fallback_uses_no_cap_slot() {
        let p = content_plan(None, 1);
        let r = |region: &str| {
            outcome(
                &p,
                &row("orders", Op::Insert, &format!(r#"{{"region":"{region}"}}"#)),
            )
        };
        assert_eq!(r("unknown"), "orders.unknown");
        assert_eq!(p.dynamic_topic_count(), 0);
        assert_eq!(p.stats().snapshot()[0].counts.fallback, 1);
        assert_eq!(r("eu"), "orders.eu", "the one slot is still free");
    }

    #[test]
    fn only_topics_a_row_could_produce_count_against_the_cap() {
        let long = format!("orders.{}", "a".repeat(MAX_SEGMENT + 1));
        let p = content_plan(None, 100);
        let n = p.add_existing_topics([
            "orders.a",
            "orders.a.b",
            long.as_str(),
            "orders.",
            "orders.é",
        ]);
        assert_eq!(n, 1, "only orders.a is one valid segment");
        let q = content_plan(Some(&["eu"]), 100);
        assert_eq!(
            q.add_existing_topics(["orders.eu", "orders.us"]),
            1,
            "topic_values"
        );
        // Topics a `{schema}`/`{table}` template writes never count.
        let s = RoutePlan::new(
            vec![
                content("o", "orders.{row.region}", Some("fb"), None),
                route("public.*", "cdc.{table}"),
            ],
            None,
        )
        .unwrap();
        assert_eq!(s.add_existing_topics(["cdc.customers", "orders.eu"]), 1);
    }

    #[test]
    fn a_refused_or_failed_create_frees_the_cap_slot() {
        let p = content_plan(None, 1);
        let mut out = p
            .render(&row("orders", Op::Insert, r#"{"region":"eu"}"#), NOW)
            .unwrap()
            .remove(0);
        assert_eq!(out.topic, "orders.eu");
        assert_eq!(
            out.content,
            Some(ContentTopic {
                route: 0,
                fallback: "orders.unknown".into(),
                tombstone: false,
            })
        );
        assert_eq!(p.dynamic_topic_count(), 1);
        // The broker refuses it for good: the record goes to the fallback, counted there.
        p.topic_refused(&mut out, "PolicyViolation");
        assert_eq!(
            (out.topic.as_str(), out.content.as_ref()),
            ("orders.unknown", None)
        );
        assert_eq!(p.dynamic_topic_count(), 0);
        // The fallback record is acknowledged.
        p.commit_counts();
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced, c.fallback), (1, 0, 1));
        // Its op leaves the produced breakdown with it.
        assert_eq!(c.ops.total(), 0);
        // A passing failure frees the slot too; the batch is retried.
        let us = p
            .render(&row("orders", Op::Insert, r#"{"region":"us"}"#), NOW)
            .unwrap()
            .remove(0);
        assert_eq!(us.topic, "orders.us");
        p.topic_failed("orders.us");
        assert_eq!(p.dynamic_topic_count(), 0);
        // An opened topic is never released, and blocks other spellings.
        p.topic_opened("orders.ap");
        p.topic_failed("orders.ap");
        assert_eq!(
            outcome(&p, &row("orders", Op::Insert, r#"{"region":"ap"}"#)),
            "orders.ap"
        );
        // Records of other routes, and fallbacks, are not content-routed.
        let rest = p
            .render(&row("users", Op::Insert, "{}"), NOW)
            .unwrap()
            .remove(0);
        assert_eq!(rest.content, None);
    }

    #[test]
    fn a_refused_name_goes_to_the_fallback_at_render_time_from_then_on() {
        let p = content_plan(None, 100);
        let eu = || row("orders", Op::Insert, r#"{"region":"eu"}"#);
        let mut out = p.render(&eu(), NOW).unwrap().remove(0);
        assert!(out.content.is_some());
        p.topic_refused(&mut out, "TopicAuthorizationFailed");
        // Every later `eu` row: the fallback, with no content marker, so the sink never tries
        // to create the topic again.
        for _ in 0..3 {
            let r = p.render(&eu(), NOW).unwrap().remove(0);
            assert_eq!(
                (r.topic.as_str(), r.content.as_ref()),
                ("orders.unknown", None)
            );
        }
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced, c.fallback), (4, 0, 4));
        assert_eq!(p.dynamic_topic_count(), 0);
        // Other values are unaffected.
        assert_eq!(
            outcome(&p, &row("orders", Op::Insert, r#"{"region":"us"}"#)),
            "orders.us"
        );
        // The memory is bounded: the oldest refused name is forgotten first.
        let d = DynamicTopics::new();
        for i in 0..=MAX_REFUSED_TOPICS {
            d.refuse(&format!("orders.t{i}"));
        }
        assert_eq!(d.admit("orders.t0"), Admit::Yes, "the oldest was dropped");
        assert_eq!(d.admit("orders.t1"), Admit::Refused);
        assert_eq!(
            d.admit(&format!("orders.t{MAX_REFUSED_TOPICS}")),
            Admit::Refused
        );
    }

    #[test]
    fn a_zero_cap_allows_only_existing_topics() {
        let p = content_plan(None, 0);
        p.add_existing_topics(["orders.eu"]);
        let r = |region: &str| {
            outcome(
                &p,
                &row("orders", Op::Insert, &format!(r#"{{"region":"{region}"}}"#)),
            )
        };
        assert_eq!(r("eu"), "orders.eu");
        assert_eq!(r("us"), "orders.unknown");
    }

    #[test]
    fn content_routes_are_described_inspected_and_watched() {
        let p = content_plan(Some(&["eu", "us"]), 100);
        // The content route's dead-letter topic is its fallback's: `orders.*` cannot render it.
        assert_eq!(
            p.topic_globs(),
            vec![
                "orders.*",
                "rest",
                "orders.unknown",
                "orders.unknown.dlq",
                "rest.dlq"
            ]
        );
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

    // --- outbox (R8) -----------------------------------------------------------------------------

    fn outbox_spec() -> OutboxSpec {
        OutboxSpec {
            topic: "outbox.{aggregate_type}".into(),
            id: Some("id".into()),
            kind: Some("type".into()),
            ..Default::default()
        }
    }

    fn outbox_route(spec: OutboxSpec) -> KafkaRoute {
        KafkaRoute {
            name: Some("ob".into()),
            pattern: "public.outbox".into(),
            topic_fallback: Some("outbox.unknown".into()),
            outbox: Some(spec),
            ..Default::default()
        }
    }

    fn outbox_plan() -> RoutePlan {
        plan_with(vec![outbox_route(outbox_spec())])
    }

    const OUTBOX_ROW: &str = r#"{"id":"0f6e","aggregatetype":"order","aggregateid":"A-1","type":"OrderCreated","payload":{"total":12.50,"items":[1,2]}}"#;

    #[test]
    fn outbox_publishes_inserts_only() {
        let p = outbox_plan();
        assert_eq!(
            outcome(&p, &row("outbox", Op::Insert, OUTBOX_ROW)),
            "outbox.order"
        );
        // Updates, deletes, truncates and, without a `where`, snapshot reads are filtered.
        for op in [Op::Update, Op::Delete, Op::Truncate, Op::Read] {
            assert_eq!(
                outcome(&p, &row("outbox", op, OUTBOX_ROW)),
                "filtered",
                "{op:?}"
            );
        }
        // Even a row that would be dead-lettered as an insert.
        assert_eq!(outcome(&p, &row("outbox", Op::Update, "{}")), "filtered");
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced, c.filtered), (6, 1, 5));
    }

    #[test]
    fn outbox_publishes_snapshot_reads_only_when_where_is_true_for_them() {
        let with_where = |w: &str| {
            plan_with(vec![KafkaRoute {
                filter: Some(w.into()),
                ..outbox_route(outbox_spec())
            }])
        };
        let p = with_where("op in ['insert', 'read', 'update', 'delete', 'truncate']");
        // A snapshot read is published like an insert, with the `r` op header.
        let r = p
            .render(&row("outbox", Op::Read, OUTBOX_ROW), NOW)
            .unwrap()
            .remove(0);
        assert_eq!(
            (r.topic.as_str(), r.key.as_str(), r.op),
            ("outbox.order", "A-1", "r")
        );
        assert_eq!(r.value.text(), r#"{"total":12.50,"items":[1,2]}"#);
        assert_eq!(
            outcome(&p, &row("outbox", Op::Insert, OUTBOX_ROW)),
            "outbox.order"
        );
        // A `where` true for them never publishes updates, deletes or truncates.
        for op in [Op::Update, Op::Delete, Op::Truncate] {
            assert_eq!(
                outcome(&p, &row("outbox", op, OUTBOX_ROW)),
                "filtered",
                "{op:?}"
            );
        }
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced, c.filtered), (5, 2, 3));
        // `op == 'read'` publishes snapshot reads only.
        let q = with_where("op == 'read'");
        assert_eq!(
            outcome(&q, &row("outbox", Op::Read, OUTBOX_ROW)),
            "outbox.order"
        );
        assert_eq!(
            outcome(&q, &row("outbox", Op::Insert, OUTBOX_ROW)),
            "filtered"
        );
        // A `where` false for reads filters them; so does no `where` (`outbox_plan`).
        let q = with_where("op == 'insert'");
        assert_eq!(
            outcome(&q, &row("outbox", Op::Read, OUTBOX_ROW)),
            "filtered"
        );
        assert_eq!(
            outcome(&q, &row("outbox", Op::Insert, OUTBOX_ROW)),
            "outbox.order"
        );
        assert_eq!(
            outcome(&outbox_plan(), &row("outbox", Op::Read, OUTBOX_ROW)),
            "filtered"
        );
        // A row `where` applies to reads and inserts alike.
        let q = with_where("row.aggregatetype == 'order'");
        assert_eq!(
            outcome(&q, &row("outbox", Op::Read, OUTBOX_ROW)),
            "outbox.order"
        );
        let other = OUTBOX_ROW.replace(r#""order""#, r#""invoice""#);
        assert_eq!(outcome(&q, &row("outbox", Op::Read, &other)), "filtered");
        assert_eq!(outcome(&q, &row("outbox", Op::Insert, &other)), "filtered");
        // A snapshot read without an aggregate id goes to the dead-letter topic too.
        assert_eq!(
            outcome(&p, &row("outbox", Op::Read, r#"{"payload":"p"}"#)),
            "dlq"
        );
    }

    #[test]
    fn outbox_template_errors_point_into_the_template_as_written() {
        assert_eq!(written_pos("a.{aggregate_type}.{x", 24, 1), 1);
        // Inside the replacement: the placeholder's start.
        assert_eq!(written_pos("a.{aggregate_type}.{x", 24, 10), 2);
        // After it: shifted back by the length difference (24 - 16).
        assert_eq!(written_pos("a.{aggregate_type}.{x", 24, 27), 19);
        assert_eq!(
            written_pos("{aggregate_type}.{aggregate_type}.}", 9, 20),
            34
        );
        let err = RoutePlan::with_dead_letter(
            vec![outbox_route(OutboxSpec {
                topic: "outbox.{aggregate_type}.{x".into(),
                ..outbox_spec()
            })],
            None,
            Some(DLQ.into()),
        )
        .map(|_| ())
        .unwrap_err()
        .to_string();
        assert!(
            err.contains(
                "`outbox.{aggregate_type}.{x` is not a valid template: unclosed `{` (at byte 24)"
            ),
            "{err}"
        );
    }

    #[test]
    fn outbox_key_is_the_raw_aggregate_id_and_value_the_raw_payload() {
        let p = outbox_plan();
        let r = p
            .render(&row("outbox", Op::Insert, OUTBOX_ROW), NOW)
            .unwrap()
            .remove(0);
        assert_eq!(r.topic, "outbox.order");
        // A string's content, not the JSON key of the table.
        assert_eq!(r.key, "A-1");
        assert_eq!(r.partition_key, r.key);
        // A `json`/`jsonb` payload is the JSON text as the row holds it.
        assert_eq!(r.value.text(), r#"{"total":12.50,"items":[1,2]}"#);
        assert_eq!((r.op, r.lsn.as_str()), ("c", "7"));

        // A text payload is its string content, unescaped; a numeric aggregate id its JSON text.
        let r = p
            .render(
                &row(
                    "outbox",
                    Op::Insert,
                    r#"{"aggregatetype":"order","aggregateid":42,"payload":"say \"hi\"\n é"}"#,
                ),
                NOW,
            )
            .unwrap()
            .remove(0);
        assert_eq!(r.key, "42");
        assert_eq!(r.value.text(), "say \"hi\"\n é");
        // An escaped string aggregate id is unescaped too; other values keep their JSON text.
        for (id, key) in [
            (r#""a\"bé""#, "a\"bé"),
            ("1.0e3", "1.0e3"),
            ("true", "true"),
            (r#"{"$oid":"6530"}"#, r#"{"$oid":"6530"}"#),
        ] {
            let json = format!(r#"{{"aggregatetype":"o","aggregateid":{id},"payload":"{{}}"}}"#);
            let r = p
                .render(&row("outbox", Op::Insert, &json), NOW)
                .unwrap()
                .remove(0);
            assert_eq!(r.key, key, "{id}");
            assert_eq!(r.value.text(), "{}");
        }
    }

    #[test]
    fn outbox_headers_are_id_and_type_when_configured_and_present() {
        let p = outbox_plan();
        let headers = |json: &str| {
            p.render(&row("outbox", Op::Insert, json), NOW)
                .unwrap()
                .remove(0)
                .headers
        };
        assert_eq!(
            headers(OUTBOX_ROW),
            vec![
                ("id", "0f6e".to_string()),
                ("type", "OrderCreated".to_string())
            ]
        );
        // A null or missing header column leaves the header out.
        assert_eq!(
            headers(r#"{"aggregatetype":"o","aggregateid":"a","payload":"p","id":7,"type":null}"#),
            vec![("id", "7".to_string())]
        );
        // Without `id` and `type` in the spec there are no extra headers.
        let q = plan_with(vec![outbox_route(OutboxSpec {
            topic: "outbox.{aggregate_type}".into(),
            ..Default::default()
        })]);
        let r = q
            .render(&row("outbox", Op::Insert, OUTBOX_ROW), NOW)
            .unwrap()
            .remove(0);
        assert!(r.headers.is_empty());
        // The sink adds `op` and `lsn` to every record.
        assert_eq!((r.op, r.lsn.as_str()), ("c", "7"));
    }

    #[test]
    fn outbox_rows_without_aggregate_id_or_payload_go_to_the_dead_letter_topic() {
        let p = outbox_plan();
        let cases = [
            (
                r#"{"aggregatetype":"o","payload":"secret"}"#,
                "`aggregate_id` column `aggregateid` is missing or null",
            ),
            (
                r#"{"aggregatetype":"o","aggregateid":null,"payload":"secret"}"#,
                "`aggregate_id` column `aggregateid` is missing or null",
            ),
            (
                r#"{"aggregatetype":"o","aggregateid":"secret"}"#,
                "`payload` column `payload` is missing or null",
            ),
            (
                r#"{"aggregatetype":"o","aggregateid":"secret","payload":null}"#,
                "`payload` column `payload` is missing or null",
            ),
            (
                r#"{"aggregatetype":"o","aggregateid":"\ud800","payload":"secret"}"#,
                "`aggregate_id` column `aggregateid` is a string that is not valid Unicode",
            ),
            (
                r#"{"aggregatetype":"o","aggregateid":"secret","payload":"p","type":"\udc00"}"#,
                "`type` column `type` is a string that is not valid Unicode",
            ),
        ];
        for (json, why) in cases {
            let e = row("outbox", Op::Insert, json);
            let out = p.render(&e, NOW).unwrap();
            assert_eq!(out.len(), 1, "{json}");
            let r = &out[0];
            assert_eq!(r.topic, DLQ, "{json}");
            assert_eq!(
                r.key,
                r#"{"db":"app","schema":"public","table":"outbox","key":null}"#
            );
            assert_eq!(r.value.text(), encode(&e, &pg(), NOW).unwrap());
            assert_eq!(r.headers.len(), 1);
            assert_eq!(r.headers[0].0, "aqueduct-error");
            assert_eq!(r.headers[0].1, format!("route `ob`: outbox {why}"));
            assert!(!r.headers[0].1.contains("secret"));
        }
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.errors, c.produced), (6, 6, 0));
        // A dead-lettered row takes no content-routed topic slot.
        assert_eq!(p.dynamic_topic_count(), 0);
    }

    #[test]
    fn outbox_topic_follows_the_content_routing_rules_and_falls_back() {
        let mut p = plan_with(vec![KafkaRoute {
            topic_values: Some(vec!["order".into(), "invoice".into(), "late".into()]),
            ..outbox_route(outbox_spec())
        }]);
        p.set_max_dynamic_topics(2);
        let r = |agg: &str| {
            outcome(
                &p,
                &row(
                    "outbox",
                    Op::Insert,
                    &format!(r#"{{"aggregatetype":{agg},"aggregateid":"a","payload":"p"}}"#),
                ),
            )
        };
        assert_eq!(r(r#""order""#), "outbox.order");
        assert_eq!(r(r#""invoice""#), "outbox.invoice");
        for bad in [
            r#"null"#,
            r#""x.y""#,
            r#""shipment""#,
            r#""""#,
            r#"{"a":1}"#,
        ] {
            assert_eq!(r(bad), "outbox.unknown", "{bad}");
        }
        assert_eq!(r(r#""late""#), "outbox.unknown", "over max_dynamic_topics");
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.produced, c.fallback), (2, 6));
        // A fallback record is still the outbox record: same key, value and headers.
        let rec = p
            .render(
                &row(
                    "outbox",
                    Op::Insert,
                    r#"{"aggregatetype":null,"aggregateid":"a","payload":"p","id":"e1"}"#,
                ),
                NOW,
            )
            .unwrap()
            .remove(0);
        assert_eq!(
            (rec.topic.as_str(), rec.key.as_str(), rec.value.text()),
            ("outbox.unknown", "a", "p")
        );
        assert_eq!(rec.headers, vec![("id", "e1".to_string())]);

        // A template without row fields needs no fallback; `{schema}`, `{table}` and
        // `{after.<col>}` work too.
        let q = plan_with(vec![KafkaRoute {
            topic_fallback: None,
            ..outbox_route(OutboxSpec {
                topic: "events.{table}".into(),
                ..outbox_spec()
            })
        }]);
        assert_eq!(
            outcome(&q, &row("outbox", Op::Insert, OUTBOX_ROW)),
            "events.outbox"
        );
        let q = plan_with(vec![outbox_route(OutboxSpec {
            topic: "ev.{schema}.{after.type}".into(),
            ..outbox_spec()
        })]);
        assert_eq!(
            outcome(&q, &row("outbox", Op::Insert, OUTBOX_ROW)),
            "ev.public.OrderCreated"
        );
    }

    #[test]
    fn outbox_routes_refuse_every_field_operation() {
        let specs: Vec<(&str, FieldSpec)> = vec![
            (
                "include",
                FieldSpec {
                    include: Some(strs(&["payload"])),
                    ..Default::default()
                },
            ),
            (
                "exclude",
                FieldSpec {
                    exclude: Some(strs(&["secret"])),
                    ..Default::default()
                },
            ),
            // An empty `exclude` is still a field key on an outbox route.
            (
                "exclude",
                FieldSpec {
                    exclude: Some(vec![]),
                    ..Default::default()
                },
            ),
            (
                "mask",
                FieldSpec {
                    mask: strs(&["aggregateid"]),
                    ..Default::default()
                },
            ),
            (
                "hash",
                FieldSpec {
                    hash: strs(&["aggregateid"]),
                    ..Default::default()
                },
            ),
            (
                "rename",
                FieldSpec {
                    rename: [("payload".to_string(), "body".to_string())].into(),
                    ..Default::default()
                },
            ),
        ];
        for (key, fields) in specs {
            let err = RoutePlan::build(
                vec![KafkaRoute {
                    fields,
                    ..outbox_route(outbox_spec())
                }],
                None,
                Some(DLQ.into()),
                Some(hash_key()),
            )
            .map(|_| ())
            .unwrap_err()
            .to_string();
            assert!(
                err.contains(&format!(
                    "sink route `ob`: `outbox` and the field operations (`{key}`) are mutually exclusive"
                )),
                "{key}: {err}"
            );
        }
        // Several at once are all named.
        let err = RoutePlan::build(
            vec![KafkaRoute {
                fields: FieldSpec {
                    mask: strs(&["a"]),
                    hash: strs(&["b"]),
                    ..Default::default()
                },
                ..outbox_route(outbox_spec())
            }],
            None,
            Some(DLQ.into()),
            Some(hash_key()),
        )
        .map(|_| ())
        .unwrap_err()
        .to_string();
        assert!(err.contains("(`mask`, `hash`)"), "{err}");
    }

    #[test]
    fn outbox_routes_are_validated_at_startup() {
        let build = |r: KafkaRoute, dlq: Option<&str>| {
            RoutePlan::with_dead_letter(vec![r], Some("rest".into()), dlq.map(Into::into))
                .map(|_| ())
                .map_err(|e| e.to_string())
        };
        let ok = outbox_route(outbox_spec());
        assert_eq!(build(ok.clone(), Some(DLQ)), Ok(()));
        let spec = |f: fn(&mut OutboxSpec)| {
            let mut s = outbox_spec();
            f(&mut s);
            outbox_route(s)
        };
        // Without `sink.dead_letter_topic`, the outbox dead-letters to `outbox.unknown.dlq`.
        assert_eq!(build(ok.clone(), None), Ok(()));
        let cases: Vec<(KafkaRoute, Option<&str>, &str)> = vec![
            (
                KafkaRoute {
                    topic: "t".into(),
                    ..ok.clone()
                },
                Some(DLQ),
                "`topic` and `outbox` are mutually exclusive",
            ),
            (
                KafkaRoute {
                    key: Some(vec!["id".into()]),
                    ..ok.clone()
                },
                Some(DLQ),
                "`key` and `outbox` are mutually exclusive",
            ),
            (
                spec(|s| s.aggregate_id = String::new()),
                Some(DLQ),
                "`aggregate_id` is an empty column name",
            ),
            (
                spec(|s| s.payload = "a.b".into()),
                Some(DLQ),
                "`payload` column `a.b` is not a top-level column name",
            ),
            (
                spec(|s| s.aggregate_type = "{x}".into()),
                Some(DLQ),
                "`aggregate_type` column `{x}` is not a top-level",
            ),
            (
                spec(|s| s.id = Some(String::new())),
                Some(DLQ),
                "`id` is an empty column name",
            ),
            (
                spec(|s| s.kind = Some("t.x".into())),
                Some(DLQ),
                "`type` column `t.x`",
            ),
            (
                spec(|s| s.topic = "outbox.{before.kind}".into()),
                Some(DLQ),
                "reads `before.kind`, but only inserts and snapshot reads are published",
            ),
            (
                KafkaRoute { filter: Some("before.status == 'x' or after.n > 1".into()), ..ok.clone() },
                Some(DLQ),
                "`where` reads `before.status`, but an outbox route only publishes inserts and snapshot reads",
            ),
            (
                spec(|s| s.topic = "outbox.{aggregate_id}".into()),
                Some(DLQ),
                "`outbox` `topic` `outbox.{aggregate_id}` is not a valid template",
            ),
            // The content-routing rules: a fixed prefix, a fallback, no collisions.
            (
                spec(|s| s.topic = "{aggregate_type}".into()),
                Some(DLQ),
                "`topic` `{aggregate_type}` reads a row field, so it must start with fixed text",
            ),
            (
                KafkaRoute {
                    topic_fallback: None,
                    ..ok.clone()
                },
                Some(DLQ),
                "`topic` `outbox.{aggregate_type}` reads a row field, so it needs `topic_fallback`",
            ),
            (
                spec(|s| s.topic = "outbox.events".into()),
                Some(DLQ),
                "`topic_fallback` is only for a `topic` that reads a row field",
            ),
            (
                spec(|s| s.topic = "bad topic".into()),
                Some(DLQ),
                "is not a valid Kafka topic name",
            ),
        ];
        for (r, dlq, want) in cases {
            let err = build(r, dlq).expect_err(want);
            assert!(err.contains("sink route `ob`"), "{err}");
            assert!(err.contains(want), "{err} should contain {want}");
        }
        // A topic the template could produce cannot be the dead-letter topic or another route's.
        let err = build(ok.clone(), Some("outbox.order")).unwrap_err();
        assert!(
            err.contains("is also a topic events are routed to"),
            "{err}"
        );
        // `dlq` is a reserved value, so `outbox.{aggregate_type}` never renders `outbox.dlq`.
        assert_eq!(build(ok.clone(), Some("outbox.dlq")), Ok(()));
        let err = RoutePlan::with_dead_letter(
            vec![ok.clone(), route("public.x", "outbox.x")],
            None,
            Some(DLQ.into()),
        )
        .map(|_| ())
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("could produce `outbox.x`, which is route `public.x`'s `topic`"),
            "{err}"
        );
        // A fixed topic with no fallback is fine; so are Debezium's defaults.
        assert_eq!(
            build(
                KafkaRoute {
                    topic_fallback: None,
                    ..spec(|s| s.topic = "outbox.events".into())
                },
                Some(DLQ)
            ),
            Ok(())
        );
        assert_eq!(
            build(outbox_route(OutboxSpec::default()), Some(DLQ)),
            Ok(())
        );
    }

    #[test]
    fn outbox_routes_are_described_inspected_and_watched() {
        let p = outbox_plan();
        assert_eq!(
            p.topic_globs(),
            vec!["outbox.*", "rest", "outbox.unknown", DLQ]
        );
        let d = serde_json::to_value(p.describe()).unwrap();
        assert_eq!(d["routes"][0]["topic"], "outbox.{aggregate_type}");
        assert_eq!(d["routes"][0]["topic_fallback"], "outbox.unknown");
        assert_eq!(
            d["routes"][0]["outbox"],
            serde_json::json!({
                "aggregate_type": "aggregatetype", "aggregate_id": "aggregateid",
                "payload": "payload", "topic": "outbox.{aggregate_type}",
                "id": "id", "type": "type"
            })
        );
        // Other routes have no `outbox` key at all.
        assert!(d["routes"][1].get("outbox").is_none());
        // Misspelled outbox columns are reported as unseen fields, on inserts only.
        let q = plan_with(vec![outbox_route(OutboxSpec {
            aggregate_id: "aggregate_id".into(),
            ..outbox_spec()
        })]);
        for _ in 0..UNSEEN_FIELD_ROWS {
            q.render(&row("outbox", Op::Insert, OUTBOX_ROW), NOW)
                .unwrap();
            q.render(&row("outbox", Op::Delete, "{}"), NOW).unwrap();
        }
        assert_eq!(
            q.stats().snapshot()[0].unseen_fields,
            vec!["after.aggregate_id"]
        );
    }

    // --- fields: include/exclude, mask, hash, rename (R6) ---------------------------------------

    const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

    fn hash_key() -> HashKey {
        HashKey::new(SECRET, "k1").unwrap()
    }

    /// The expected digest of `text`, computed with ring directly.
    fn hmac_hex(text: &str) -> String {
        let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, SECRET);
        ring::hmac::sign(&k, text.as_bytes())
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    fn with_fields(name: &str, p: &str, f: FieldSpec) -> KafkaRoute {
        KafkaRoute {
            fields: f,
            ..named(name, p, &format!("{name}.topic"))
        }
    }

    fn strs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn fields_plan(routes: Vec<KafkaRoute>) -> RoutePlan {
        let mut p = RoutePlan::build(
            routes,
            Some("rest".into()),
            Some(DLQ.into()),
            Some(hash_key()),
        )
        .unwrap();
        p.set_source(pg());
        p
    }

    /// JSON text in tests: a key, an encoded value, or a record value that is not a tombstone.
    trait Text {
        fn text(&self) -> &str;
    }

    impl Text for str {
        fn text(&self) -> &str {
            self
        }
    }

    impl Text for String {
        fn text(&self) -> &str {
            self
        }
    }

    impl Text for Option<String> {
        fn text(&self) -> &str {
            self.as_deref().expect("a record value, not a tombstone")
        }
    }

    fn json<T: Text + ?Sized>(s: &T) -> serde_json::Value {
        serde_json::from_str(s.text()).unwrap()
    }

    #[test]
    fn field_ops_rewrite_before_and_after_but_not_source() {
        let p = fields_plan(vec![with_fields(
            "people",
            "public.people",
            FieldSpec {
                exclude: Some(strs(&["note"])),
                mask: strs(&["card", "age"]),
                hash: strs(&["email"]),
                rename: [("cust_id".to_string(), "customer_id".to_string())].into(),
                ..Default::default()
            },
        )]);
        let mut e = event("public", "people", Op::Update, None, Position::Lsn(7));
        e.before =
            raw(r#"{"id":1,"email":"old@x.y","card":"4111","age":40,"note":"n","cust_id":3}"#);
        e.after = raw(r#"{"id":1,"email":"new@x.y","card":null,"age":41,"note":"n","cust_id":3}"#);
        let r = p.render(&e, NOW).unwrap().remove(0);
        assert_eq!(r.topic, "people.topic");
        let v = json(&r.value);
        assert_eq!(
            v["before"],
            serde_json::json!({"id": 1, "email": hmac_hex("old@x.y"), "card": "***", "age": null, "customer_id": 3})
        );
        assert_eq!(
            v["after"],
            serde_json::json!({"id": 1, "email": hmac_hex("new@x.y"), "card": null, "age": null, "customer_id": 3})
        );
        // `source`, `op` and the times are the envelope's, untouched.
        let plain = json(&encode(&e, &pg(), NOW).unwrap());
        for k in ["source", "op", "ts_ms", "ts_us"] {
            assert_eq!(v[k], plain[k], "{k}");
        }
        assert_eq!(r.headers, vec![(HASH_KEY_ID_HEADER, "k1".to_string())]);
        assert!(!r.value.text().contains("old@x.y") && !r.value.text().contains("4111"));
    }

    #[test]
    fn the_key_object_gets_the_same_field_ops_and_decides_the_partition() {
        let route = |f: FieldSpec| with_fields("t", "public.t", f);
        let e = |email: &str| {
            let mut e = event("public", "t", Op::Insert, None, Position::Lsn(1));
            e.after = raw(&format!(r#"{{"email":"{email}","id":1,"n":2}}"#));
            e.key = Some(format!(r#"{{"email":"{email}","id":1}}"#));
            e
        };
        let k = |key: &str| format!(r#"{{"db":"app","schema":"public","table":"t","key":{key}}}"#);
        let cases: Vec<(FieldSpec, String)> = vec![
            (
                FieldSpec {
                    hash: strs(&["email"]),
                    ..Default::default()
                },
                k(&format!(r#"{{"email":"{}","id":1}}"#, hmac_hex("a@b.c"))),
            ),
            (
                FieldSpec {
                    mask: strs(&["email"]),
                    rename: [("id".to_string(), "user_id".to_string())].into(),
                    ..Default::default()
                },
                k(r#"{"email":"***","user_id":1}"#),
            ),
            (
                FieldSpec {
                    exclude: Some(strs(&["email"])),
                    ..Default::default()
                },
                k(r#"{"id":1}"#),
            ),
            // Every key column removed: `"key": null`, so the table's partition.
            (
                FieldSpec {
                    include: Some(strs(&["n"])),
                    ..Default::default()
                },
                k("null"),
            ),
        ];
        for (spec, want) in cases {
            for by in [PartitionBy::Key, PartitionBy::Table] {
                let mut p = fields_plan(vec![route(spec.clone())]);
                p.set_partition_by(by);
                let r = p.render(&e("a@b.c"), NOW).unwrap().remove(0);
                assert_eq!(r.key, want, "{spec:?}");
                assert!(!r.key.contains("a@b.c"), "{}", r.key);
                let part = match by {
                    PartitionBy::Key => want.clone(),
                    PartitionBy::Table => "public.t".to_string(),
                };
                assert_eq!(r.partition_key, part, "{spec:?} {by:?}");
            }
        }
        // A hashed key column keeps rows apart and the same row together: per-row order holds.
        let p = fields_plan(vec![route(FieldSpec {
            hash: strs(&["email"]),
            ..Default::default()
        })]);
        let pk = |email: &str| p.render(&e(email), NOW).unwrap().remove(0).partition_key;
        assert_eq!(pk("a@b.c"), pk("a@b.c"));
        assert_ne!(pk("a@b.c"), pk("x@y.z"));
        // A MongoDB `_id` object is hashed from its compact JSON, in the key as in the value.
        let mut p = fields_plan(vec![with_fields(
            "m",
            "shop.users",
            FieldSpec {
                hash: strs(&["_id"]),
                ..Default::default()
            },
        )]);
        p.set_source(SourceInfo::Mongodb);
        let mut m = event(
            "shop",
            "users",
            Op::Insert,
            None,
            Position::Text("t".into()),
        );
        m.after = raw(r#"{"_id":{"$oid":"6530"},"n":1}"#);
        m.key = Some(r#"{"_id": {"$oid": "6530"}}"#.into());
        let r = p.render(&m, NOW).unwrap().remove(0);
        let digest = hmac_hex(r#"{"$oid":"6530"}"#);
        assert_eq!(json(&r.key)["key"]["_id"], digest.as_str());
        assert_eq!(json(&r.value)["after"]["_id"], digest.as_str());
    }

    #[test]
    fn dead_letter_values_have_the_field_ops_applied() {
        let p = fields_plan(vec![KafkaRoute {
            filter: Some("row.amount > 10".into()),
            fields: FieldSpec {
                mask: strs(&["card"]),
                hash: strs(&["email"]),
                exclude: Some(strs(&["note"])),
                ..Default::default()
            },
            ..named("o", "public.orders", "orders")
        }]);
        let e = row(
            "orders",
            Op::Insert,
            r#"{"amount":"abc","card":"4111","email":"a@b.c","note":"secret"}"#,
        );
        let r = p.render(&e, NOW).unwrap().remove(0);
        assert_eq!(r.topic, DLQ);
        assert_eq!(
            json(&r.value)["after"],
            serde_json::json!({"amount": "abc", "card": "***", "email": hmac_hex("a@b.c")})
        );
        assert!(!r.value.text().contains("4111") && !r.value.text().contains("secret"));
        assert_eq!(r.headers[0].0, "aqueduct-error");
        assert_eq!(r.headers[1], (HASH_KEY_ID_HEADER, "k1".to_string()));
        assert_eq!(json(&r.key)["key"], serde_json::Value::Null);
    }

    #[test]
    fn an_unhashable_string_goes_to_the_dead_letter_topic_without_the_row() {
        let p = fields_plan(vec![with_fields(
            "h",
            "public.users",
            FieldSpec {
                hash: strs(&["email"]),
                ..Default::default()
            },
        )]);
        let e = row(
            "users",
            Op::Insert,
            r#"{"email":"\ud800secret","card":"4111"}"#,
        );
        let r = p.render(&e, NOW).unwrap().remove(0);
        assert_eq!(r.topic, DLQ);
        assert_eq!(
            r.headers[0],
            (
                "aqueduct-error",
                "route `h`: hash `email`: the value is a string that is not valid Unicode"
                    .to_string()
            )
        );
        let v = json(&r.value);
        assert_eq!(v["after"], serde_json::Value::Null);
        assert!(!r.value.text().contains("secret") && !r.value.text().contains("4111"));
    }

    #[test]
    fn a_rename_collision_goes_to_the_dead_letter_topic_with_original_names() {
        let p = fields_plan(vec![with_fields(
            "r",
            "public.users",
            FieldSpec {
                mask: strs(&["card"]),
                rename: [("cust".to_string(), "customer".to_string())].into(),
                ..Default::default()
            },
        )]);
        let ok = row("users", Op::Insert, r#"{"cust":1,"card":"4111"}"#);
        assert_eq!(
            json(&p.render(&ok, NOW).unwrap()[0].value)["after"],
            serde_json::json!({"customer": 1, "card": "***"})
        );
        let clash = row(
            "users",
            Op::Insert,
            r#"{"cust":1,"customer":2,"card":"4111"}"#,
        );
        let r = p.render(&clash, NOW).unwrap().remove(0);
        assert_eq!(r.topic, DLQ);
        assert_eq!(
            r.headers,
            vec![(
                "aqueduct-error",
                "route `r`: rename `cust` to `customer`: the row already has a column `customer`"
                    .to_string()
            )]
        );
        // Masked even though the rename could not apply.
        assert_eq!(
            json(&r.value)["after"],
            serde_json::json!({"cust": 1, "customer": 2, "card": "***"})
        );
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced, c.errors), (2, 1, 1));
    }

    #[test]
    fn field_ops_are_validated_at_startup() {
        let build = |f: FieldSpec, dlq: Option<&str>, key: Option<HashKey>| {
            RoutePlan::build(
                vec![with_fields("r", "public.*", f)],
                None,
                dlq.map(Into::into),
                key,
            )
            .map(|_| ())
            .map_err(|e| e.to_string())
        };
        let hash = FieldSpec {
            hash: strs(&["email"]),
            ..Default::default()
        };
        let e = build(hash.clone(), None, None).unwrap_err();
        assert!(
            e.contains("sink route `r` uses `hash`") && e.contains("AQUEDUCT_HASH_KEY"),
            "{e}"
        );
        assert!(build(hash, None, Some(hash_key())).is_ok());
        let rename = FieldSpec {
            rename: [("a".to_string(), "b".to_string())].into(),
            ..Default::default()
        };
        // A collision goes to the route's dead-letter topic, `<topic>.dlq` by default.
        assert!(build(rename.clone(), None, None).is_ok());
        assert!(build(rename, Some(DLQ), None).is_ok());
        // With `include`, collisions are ruled out at startup.
        let included = FieldSpec {
            include: Some(strs(&["a"])),
            rename: [("a".to_string(), "b".to_string())].into(),
            ..Default::default()
        };
        assert!(build(included, None, None).is_ok());
        // Spec errors name the route.
        let both = FieldSpec {
            include: Some(strs(&["a"])),
            exclude: Some(strs(&["b"])),
            ..Default::default()
        };
        let e = build(both, None, None).unwrap_err();
        assert!(e.contains("sink route `r`: `include` and `exclude`"), "{e}");
        let dotted = FieldSpec {
            mask: strs(&["address.city"]),
            ..Default::default()
        };
        assert!(build(dotted, None, None).unwrap_err().contains("dotted"));
    }

    #[test]
    fn unseen_field_op_columns_are_reported_without_a_where() {
        let p = fields_plan(vec![with_fields(
            "u",
            "public.users",
            FieldSpec {
                mask: strs(&["card_last_4"]),
                hash: strs(&["email"]),
                ..Default::default()
            },
        )]);
        let ev = row(
            "users",
            Op::Insert,
            r#"{"email":"a@b.c","card_last4":"1234"}"#,
        );
        for _ in 0..UNSEEN_FIELD_ROWS {
            p.render(&ev, NOW).unwrap();
        }
        assert_eq!(
            p.stats().snapshot()[0].unseen_fields,
            vec!["row.card_last_4"]
        );
    }

    #[test]
    fn describe_lists_field_ops_and_the_key_id_but_never_the_key() {
        let p = fields_plan(vec![
            with_fields(
                "u",
                "public.users",
                FieldSpec {
                    exclude: Some(strs(&["note"])),
                    mask: strs(&["card"]),
                    hash: strs(&["email"]),
                    rename: [("cust".to_string(), "customer".to_string())].into(),
                    ..Default::default()
                },
            ),
            with_fields(
                "i",
                "public.items",
                FieldSpec {
                    include: Some(strs(&["id"])),
                    ..Default::default()
                },
            ),
        ]);
        let v = serde_json::to_value(p.describe()).unwrap();
        assert_eq!(v["hash_key_id"], "k1");
        let u = &v["routes"][0];
        assert_eq!(u["exclude"], serde_json::json!(["note"]));
        assert_eq!(u["mask"], serde_json::json!(["card"]));
        assert_eq!(u["hash"], serde_json::json!(["email"]));
        assert_eq!(u["rename"], serde_json::json!({"cust": "customer"}));
        assert!(u.get("include").is_none());
        assert_eq!(v["routes"][1]["include"], serde_json::json!(["id"]));
        assert!(v["routes"][1].get("hash").is_none());
        let text = v.to_string();
        assert!(!text.contains("0123456789abcdef"), "{text}");
        assert!(!format!("{p:?}").contains("0123456789abcdef"));
    }

    #[test]
    fn a_route_key_may_not_name_a_protected_column() {
        let build = |key: &[&str], f: FieldSpec| {
            RoutePlan::build(
                vec![KafkaRoute {
                    key: Some(strs(key)),
                    fields: f,
                    ..named("r", "public.users", "users")
                }],
                None,
                Some(DLQ.into()),
                Some(hash_key()),
            )
            .map(|_| ())
            .map_err(|e| e.to_string())
        };
        let cases: Vec<(FieldSpec, &str)> = vec![
            (
                FieldSpec {
                    hash: strs(&["email"]),
                    ..Default::default()
                },
                "hashed",
            ),
            (
                FieldSpec {
                    mask: strs(&["email"]),
                    ..Default::default()
                },
                "masked",
            ),
            (
                FieldSpec {
                    exclude: Some(strs(&["email"])),
                    ..Default::default()
                },
                "excluded",
            ),
            (
                FieldSpec {
                    include: Some(strs(&["id"])),
                    ..Default::default()
                },
                "not included",
            ),
        ];
        for (f, how) in cases {
            let e = build(&["id", "email"], f.clone()).unwrap_err();
            assert!(
                e.contains(&format!("sink route `r`: `key` column `email` is {how}")),
                "{e}"
            );
            // Keying by an unprotected column is fine.
            assert_eq!(build(&["id"], f), Ok(()));
        }
        // A renamed column may key the route: it is not protected.
        let renamed = FieldSpec {
            rename: [("id".to_string(), "user_id".to_string())].into(),
            ..Default::default()
        };
        assert_eq!(build(&["id"], renamed), Ok(()));
    }

    #[test]
    fn a_route_key_is_kept_and_the_value_rewritten() {
        let p = fields_plan(vec![KafkaRoute {
            key: Some(strs(&["id"])),
            fields: FieldSpec {
                hash: strs(&["email"]),
                ..Default::default()
            },
            ..named("r", "public.users", "users")
        }]);
        let mut e = row("users", Op::Insert, r#"{"id":5,"email":"a@b.c"}"#);
        e.key = Some(r#"{"email":"a@b.c"}"#.into());
        let r = p.render(&e, NOW).unwrap().remove(0);
        assert_eq!(
            r.key,
            r#"{"db":"app","schema":"public","table":"users","key":{"id":5}}"#
        );
        assert_eq!(r.partition_key, r.key);
        assert_eq!(json(&r.value)["after"]["email"], hmac_hex("a@b.c").as_str());
        assert!(!r.key.contains("a@b.c"));
    }

    #[test]
    fn a_content_routed_topic_may_not_read_a_protected_column() {
        let build = |topic: &str, values: Option<&[&str]>, f: FieldSpec| {
            RoutePlan::build(
                vec![KafkaRoute {
                    fields: f,
                    ..content("r", topic, Some("orders.unknown"), values)
                }],
                None,
                Some(DLQ.into()),
                Some(hash_key()),
            )
            .map(|_| ())
            .map_err(|e| e.to_string())
        };
        let spec = |how: &str| match how {
            "hashed" => FieldSpec {
                hash: strs(&["region"]),
                ..Default::default()
            },
            "masked" => FieldSpec {
                mask: strs(&["region"]),
                ..Default::default()
            },
            "excluded" => FieldSpec {
                exclude: Some(strs(&["region"])),
                ..Default::default()
            },
            _ => FieldSpec {
                include: Some(strs(&["id"])),
                ..Default::default()
            },
        };
        for how in ["hashed", "masked", "excluded", "not included"] {
            // Any row path into the column counts: `row.`, `after.`, `before.`, nested too.
            for path in ["row.region", "after.region", "before.region.code"] {
                let e = build(&format!("orders.{{{path}}}"), None, spec(how)).unwrap_err();
                assert!(
                    e.contains(&format!(
                        "sink route `r`: `topic` reads `{path}`, but column `region` is {how}"
                    )),
                    "{e}"
                );
            }
            // With an allow-list the message says `topic_values` would carry it too.
            let e = build("orders.{row.region}", Some(&["eu", "us"]), spec(how)).unwrap_err();
            assert!(e.contains("the topic name (and `topic_values`)"), "{e}");
            // Routing by an unprotected column is fine.
            let ok = build("orders.{row.tenant}", None, spec(how));
            assert_eq!(ok.is_ok(), how != "not included", "{ok:?}");
        }
        // `not included` also covers the tenant column in that last case.
        let e = build("orders.{row.tenant}", None, spec("not included")).unwrap_err();
        assert!(e.contains("column `tenant` is not included"), "{e}");
        // A renamed column may route: the template reads the original row.
        let renamed = FieldSpec {
            rename: [("region".to_string(), "area".to_string())].into(),
            ..Default::default()
        };
        assert_eq!(build("orders.{row.region}", None, renamed), Ok(()));
    }

    #[test]
    fn fallback_records_carry_the_rewritten_value_key_and_key_id() {
        let mut p = RoutePlan::build(
            vec![KafkaRoute {
                fields: FieldSpec {
                    hash: strs(&["email"]),
                    mask: strs(&["card"]),
                    ..Default::default()
                },
                ..content("o", "orders.{row.region}", Some("orders.unknown"), None)
            }],
            None,
            None,
            Some(hash_key()),
        )
        .unwrap();
        p.set_source(pg());
        let ev = |region: &str| {
            let mut e = row(
                "orders",
                Op::Insert,
                &format!(r#"{{"email":"a@b.c","card":"4111","region":{region}}}"#),
            );
            e.key = Some(r#"{"email":"a@b.c"}"#.into());
            e
        };
        let check = |r: &OutRecord, topic: &str| {
            assert_eq!(r.topic, topic);
            let v = json(&r.value);
            assert_eq!(v["after"]["email"], hmac_hex("a@b.c").as_str());
            assert_eq!(v["after"]["card"], "***");
            assert_eq!(json(&r.key)["key"]["email"], hmac_hex("a@b.c").as_str());
            assert_eq!(r.partition_key, r.key);
            assert_eq!(r.headers, vec![(HASH_KEY_ID_HEADER, "k1".to_string())]);
            assert!(!r.value.text().contains("a@b.c") && !r.key.contains("a@b.c"));
            assert!(!r.value.text().contains("4111"));
        };
        // Normal content-routed record.
        let normal = p.render(&ev(r#""eu""#), NOW).unwrap().remove(0);
        check(&normal, "orders.eu");
        // Render-time fallback: a null routing value.
        let fallback = p.render(&ev("null"), NOW).unwrap().remove(0);
        check(&fallback, "orders.unknown");
        // The sink's refusal re-route changes only the topic.
        let mut refused = p.render(&ev(r#""us""#), NOW).unwrap().remove(0);
        p.topic_refused(&mut refused, "TopicAuthorizationFailed");
        check(&refused, "orders.unknown");
        // Apart from the topic, all three records are the same.
        for r in [&fallback, &refused] {
            assert_eq!(
                (&r.key, &r.headers, r.op),
                (&normal.key, &normal.headers, normal.op)
            );
        }
        p.commit_counts();
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced, c.fallback), (3, 1, 2));
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
            unchanged: Vec::new(),
            after_is_key: false,
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
                    assert_eq!(r.value.text(), encode(e, &info, NOW).unwrap());
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
            r.value.text(),
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
            sink += out[0].topic.len() + out[0].key.len() + out[0].value.text().len();
        }
        let new = t.elapsed();
        println!("legacy {legacy:?}, plan {new:?} for {n} events (sink {sink})");

        // Field operations, one class at a time, on a 10-column row with a 2-column key.
        let mut e = event("public", "users", Op::Update, None, Position::Lsn(42));
        let row = r#"{"id":7,"tenant":3,"email":"ada@example.com","name":"Ada Lovelace","card":"4111111111111111","phone":"+32 470 00 00 00","note":"likes \"engines\"","cust_id":99,"active":true,"created_at":1792502627093140}"#;
        e.before = raw(row);
        e.after = raw(row);
        e.key = Some(r#"{"id":7,"tenant":3}"#.into());
        let s = strs;
        let classes: Vec<(&str, FieldSpec)> = vec![
            ("none", FieldSpec::default()),
            (
                "include",
                FieldSpec {
                    include: Some(s(&["id", "tenant", "name", "active"])),
                    ..Default::default()
                },
            ),
            (
                "exclude",
                FieldSpec {
                    exclude: Some(s(&["note", "phone"])),
                    ..Default::default()
                },
            ),
            (
                "mask",
                FieldSpec {
                    mask: s(&["card", "phone"]),
                    ..Default::default()
                },
            ),
            (
                "hash",
                FieldSpec {
                    hash: s(&["email", "phone"]),
                    ..Default::default()
                },
            ),
            (
                "rename",
                FieldSpec {
                    rename: [("cust_id".to_string(), "customer_id".to_string())].into(),
                    ..Default::default()
                },
            ),
            (
                "all",
                FieldSpec {
                    exclude: Some(s(&["note"])),
                    mask: s(&["card"]),
                    hash: s(&["email", "phone"]),
                    rename: [("cust_id".to_string(), "customer_id".to_string())].into(),
                    ..Default::default()
                },
            ),
        ];
        let n = 500_000;
        for (class, spec) in classes {
            let plan = fields_plan(vec![with_fields("u", "public.users", spec)]);
            let t = Instant::now();
            for _ in 0..n {
                out.clear();
                plan.render_into(&e, NOW, &mut out).unwrap();
                sink += out[0].value.text().len();
            }
            let per = t.elapsed().as_nanos() / n as u128;
            println!("fields {class:>8}: {per} ns/event (sink {sink})");
        }

        // An op filter in `where` reads no row: an update that passes, a delete it filters.
        let mut del = e.clone();
        (del.op, del.after) = (Op::Delete, None);
        for (class, filter) in [
            ("none", None),
            ("op where", Some("op in ['insert', 'update']")),
        ] {
            let mut plan = RoutePlan::new(
                vec![KafkaRoute {
                    filter: filter.map(Into::into),
                    ..route("public.users", "users")
                }],
                None,
            )
            .unwrap();
            plan.set_source(pg());
            for ev in [&e, &del] {
                let t = Instant::now();
                for _ in 0..n {
                    out.clear();
                    plan.render_into(ev, NOW, &mut out).unwrap();
                    sink += out.len();
                }
                let per = t.elapsed().as_nanos() / n as u128;
                println!(
                    "filter {class:>8} {:?}: {per} ns/event (sink {sink})",
                    ev.op
                );
            }
        }
    }

    // --- shapes (R7) ------------------------------------------------------------------------------

    fn shaped(name: &str, format: Option<&str>, deletes: Option<&str>) -> KafkaRoute {
        KafkaRoute {
            format: format.map(Into::into),
            deletes: deletes.map(Into::into),
            ..named(name, "public.*", "t")
        }
    }

    fn tombstoned(name: &str) -> KafkaRoute {
        KafkaRoute {
            tombstones: true,
            ..named(name, "public.*", "t")
        }
    }

    const PG_KEY: &str = r#"{"db":"app","schema":"public","table":"orders","key":{"id":1}}"#;

    #[test]
    fn row_format_values_are_the_row_after_field_ops_are_none() {
        let p = plan_with(vec![shaped("rows", Some("row"), None)]);
        for op in [Op::Insert, Op::Update, Op::Read] {
            let mut e = row("orders", op, r#"{"id":1,"n":"a"}"#);
            if op == Op::Update {
                e.before = raw(r#"{"id":1,"n":"old"}"#);
            }
            let out = p.render(&e, NOW).unwrap();
            assert_eq!(out.len(), 1, "{op:?}");
            let r = &out[0];
            assert_eq!(
                r.value.text(),
                r#"{"id":1,"n":"a"}"#,
                "{op:?}: the row as is"
            );
            assert_eq!(r.key, PG_KEY);
            assert_eq!((r.op, r.lsn.as_str()), (op_code(op), "7"), "headers stay");
            assert!(r.headers.is_empty());
        }
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced), (3, 3));
    }

    #[test]
    fn each_delete_mode_and_truncates_in_row_format() {
        let del = row("orders", Op::Delete, r#"{"id":1,"n":"a"}"#);
        let trunc = row("orders", Op::Truncate, "{}");
        // Default and explicit `tombstone`: the delete itself is a null value under the row's key.
        for deletes in [None, Some("tombstone")] {
            let p = plan_with(vec![shaped("rows", Some("row"), deletes)]);
            let out = p.render(&del, NOW).unwrap();
            assert_eq!(out.len(), 1, "no extra record: the delete is the tombstone");
            assert_eq!(out[0].value, None);
            assert_eq!((out[0].key.as_str(), out[0].op), (PG_KEY, "d"));
            assert_eq!(out[0].partition_key, PG_KEY);
            assert!(p.render(&trunc, NOW).unwrap().is_empty());
            let c = p.stats().snapshot()[0].counts;
            assert_eq!((c.matched, c.produced, c.filtered), (2, 1, 1));
        }
        // `drop`: no record, counted as filtered, like the truncate.
        let p = plan_with(vec![shaped("rows", Some("row"), Some("drop"))]);
        assert!(p.render(&del, NOW).unwrap().is_empty());
        assert!(p.render(&trunc, NOW).unwrap().is_empty());
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced, c.filtered), (2, 0, 2));
        // `row`: the before-image with `"__deleted": true` written into the object.
        let p = plan_with(vec![shaped("rows", Some("row"), Some("row"))]);
        let out = p.render(&del, NOW).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].value.text(), r#"{"id":1,"n":"a","__deleted":true}"#);
        assert_eq!((out[0].key.as_str(), out[0].op), (PG_KEY, "d"));
        // A change without a row (a MongoDB update whose document is gone) is no record.
        let mut gone = row("orders", Op::Update, "{}");
        gone.after = None;
        assert!(p.render(&gone, NOW).unwrap().is_empty());
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced, c.filtered), (2, 1, 1));
    }

    #[test]
    fn deleted_rows_replace_a_deleted_column_and_keep_order() {
        let d = |s: Option<&str>| deleted_row(s.and_then(raw).as_deref());
        assert_eq!(d(None), r#"{"__deleted":true}"#);
        assert_eq!(d(Some("{}")), r#"{"__deleted":true}"#);
        assert_eq!(d(Some(" { } ")), r#"{"__deleted":true}"#);
        assert_eq!(d(Some(r#"{"a":1}"#)), r#"{"a":1,"__deleted":true}"#);
        assert_eq!(
            d(Some(r#"{"a":1,"__deleted":false,"b":"x"}"#)),
            r#"{"a":1,"b":"x","__deleted":true}"#
        );
        // A string value "__deleted" takes the slow path and is kept.
        assert_eq!(
            d(Some(r#"{"a":"__deleted"}"#)),
            r#"{"a":"__deleted","__deleted":true}"#
        );
        // An escaped quote inside a string never looks like the column.
        assert_eq!(
            d(Some(r#"{"a":"x\"__deleted\""}"#)),
            r#"{"a":"x\"__deleted\"","__deleted":true}"#
        );
        assert_eq!(d(Some("[1]")), r#"{"__deleted":true}"#);
        for s in [r#"{"a":1,"__deleted":false}"#, r#"{"a":{"b":[1,2]}}"#] {
            let v: serde_json::Value = serde_json::from_str(&d(Some(s))).unwrap();
            assert_eq!(v["__deleted"], true, "{s}");
        }
    }

    #[test]
    fn field_ops_apply_to_row_values_deleted_rows_and_tombstone_keys() {
        let spec = || FieldSpec {
            exclude: Some(strs(&["note"])),
            mask: strs(&["card"]),
            hash: strs(&["email"]),
            rename: [("cust_id".to_string(), "customer_id".to_string())].into(),
            ..Default::default()
        };
        let users = |op: Op| {
            let mut e = row(
                "users",
                op,
                r#"{"email":"a@b.c","card":"4111","note":"secret","cust_id":9}"#,
            );
            e.key = Some(r#"{"email":"a@b.c"}"#.into());
            e
        };
        let check = |v: &serde_json::Value| {
            assert_eq!(v["email"], hmac_hex("a@b.c").as_str());
            assert_eq!(v["card"], "***");
            assert_eq!(v["customer_id"], 9);
            assert!(v.get("note").is_none() && v.get("cust_id").is_none(), "{v}");
        };
        // Row format: the value and a `deletes = "row"` before-image are rewritten.
        let p = fields_plan(vec![KafkaRoute {
            format: Some("row".into()),
            deletes: Some("row".into()),
            ..with_fields("u", "public.users", spec())
        }]);
        let r = p.render(&users(Op::Insert), NOW).unwrap().remove(0);
        check(&json(&r.value));
        assert_eq!(r.headers, vec![(HASH_KEY_ID_HEADER, "k1".to_string())]);
        let r = p.render(&users(Op::Delete), NOW).unwrap().remove(0);
        let v = json(&r.value);
        check(&v);
        assert_eq!(v["__deleted"], true);
        assert!(!r.value.text().contains("a@b.c") && !r.value.text().contains("4111"));
        assert!(!r.key.contains("a@b.c"), "the key is rewritten too");
        assert_eq!(json(&r.key)["key"]["email"], hmac_hex("a@b.c").as_str());

        // Row-format tombstone and envelope tombstone: the rewritten key, never the original.
        for route in [
            KafkaRoute {
                format: Some("row".into()),
                ..with_fields("u", "public.users", spec())
            },
            KafkaRoute {
                tombstones: true,
                ..with_fields("u", "public.users", spec())
            },
        ] {
            let p = fields_plan(vec![route]);
            let out = p.render(&users(Op::Delete), NOW).unwrap();
            let t = out.last().unwrap();
            assert_eq!(t.value, None);
            assert!(!t.key.contains("a@b.c"), "{}", t.key);
            assert_eq!(json(&t.key)["key"]["email"], hmac_hex("a@b.c").as_str());
            assert_eq!(t.partition_key, t.key);
        }
    }

    #[test]
    fn a_tombstone_follows_its_delete_with_the_same_key_and_partition() {
        let p = plan_with(vec![tombstoned("orders")]);
        let ins = row("orders", Op::Insert, r#"{"id":1}"#);
        let del = row("orders", Op::Delete, r#"{"id":1}"#);
        assert_eq!(p.render(&ins, NOW).unwrap().len(), 1, "only deletes");
        let out = p.render(&del, NOW).unwrap();
        assert_eq!(out.len(), 2);
        let (d, t) = (&out[0], &out[1]);
        assert_eq!(
            json(&d.value)["op"],
            "d",
            "the delete first, as an envelope"
        );
        assert_eq!(t.value, None, "then the tombstone");
        assert_eq!(
            (&t.topic, &t.key, &t.partition_key, t.op, &t.lsn),
            (&d.topic, &d.key, &d.partition_key, d.op, &d.lsn)
        );
        assert_eq!(t.key, PG_KEY);
        // Counted once, as the delete.
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced, c.tombstones_skipped), (2, 2, 0));

        // A per-route key: the tombstone carries it too.
        let p = plan_with(vec![KafkaRoute {
            key: Some(strs(&["id"])),
            ..tombstoned("orders")
        }]);
        let out = p.render(&del, NOW).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].key, out[0].key);
        assert_eq!(json(&out[1].key)["key"], serde_json::json!({"id": 1}));
    }

    #[test]
    fn a_delete_with_a_null_key_gets_no_tombstone() {
        // No primary key: the record key's `key` is null.
        let p = plan_with(vec![tombstoned("orders")]);
        let mut del = row("orders", Op::Delete, r#"{"id":1}"#);
        del.key = None;
        let out = p.render(&del, NOW).unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0].value.is_some(), "the delete itself is produced");
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.produced, c.tombstones_skipped), (1, 1));
        // A per-route key whose columns the before-image lacks is null too.
        let p = plan_with(vec![KafkaRoute {
            key: Some(strs(&["email"])),
            ..tombstoned("orders")
        }]);
        let del = row("orders", Op::Delete, r#"{"id":1}"#);
        assert_eq!(p.render(&del, NOW).unwrap().len(), 1);
        let mut with_email = row("orders", Op::Delete, r#"{"id":1,"email":"x"}"#);
        with_email.key = None;
        assert_eq!(p.render(&with_email, NOW).unwrap().len(), 2);
        assert_eq!(p.stats().snapshot()[0].counts.tombstones_skipped, 1);
        // Field operations that remove every key column leave a null key as well.
        let p = fields_plan(vec![KafkaRoute {
            tombstones: true,
            ..with_fields(
                "u",
                "public.orders",
                FieldSpec {
                    exclude: Some(strs(&["id"])),
                    ..Default::default()
                },
            )
        }]);
        assert_eq!(p.render(&del, NOW).unwrap().len(), 1);
        assert_eq!(p.stats().snapshot()[0].counts.tombstones_skipped, 1);
    }

    #[test]
    fn a_tombstone_follows_its_delete_to_the_fallback_and_is_not_counted_twice() {
        let mut p = RoutePlan::new(
            vec![KafkaRoute {
                tombstones: true,
                ..content("o", "orders.{row.region}", Some("orders.unknown"), None)
            }],
            None,
        )
        .unwrap();
        p.set_source(pg());
        let del = row("orders", Op::Delete, r#"{"id":1,"region":"eu"}"#);
        let mut out = p.render(&del, NOW).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].topic, "orders.eu");
        assert!(!out[0].content.as_ref().unwrap().tombstone);
        assert!(out[1].content.as_ref().unwrap().tombstone);
        // The broker refuses the topic: both go to the fallback, the event is counted once.
        for r in &mut out {
            p.topic_refused(r, "PolicyViolation");
        }
        assert_eq!(out[0].topic, out[1].topic);
        assert_eq!(out[1].value, None);
        p.commit_counts();
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced, c.fallback), (1, 0, 1));
    }

    #[test]
    fn unchanged_dead_letter_sends_rows_with_omitted_toast_columns_to_the_dead_letter_topic() {
        let route = |unchanged: Option<&str>| KafkaRoute {
            unchanged: unchanged.map(Into::into),
            ..shaped("docs", Some("row"), None)
        };
        // The decoder's signal: `doc` was an unchanged TOAST value, so `after` lacks it.
        let mut upd = row("docs", Op::Update, r#"{"id":1,"title":"t"}"#);
        upd.unchanged = vec!["doc".into()];
        let p = plan_with(vec![route(Some("dead_letter"))]);
        let out = p.render(&upd, NOW).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].topic, DLQ);
        assert_eq!(
            out[0].headers,
            vec![(
                "aqueduct-error",
                "route `docs`: unchanged = \"dead_letter\": the source left out unchanged column(s) `doc`"
                    .to_string()
            )]
        );
        assert_eq!(
            json(&out[0].value)["op"],
            "u",
            "the dead-letter value is the envelope"
        );
        // A complete row passes.
        let mut full = upd.clone();
        full.unchanged.clear();
        assert_eq!(p.render(&full, NOW).unwrap()[0].topic, "t");
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced, c.errors), (2, 1, 1));
        // `omit` (the default): the incomplete row is produced as it is.
        for u in [None, Some("omit")] {
            let p = plan_with(vec![route(u)]);
            let out = p.render(&upd, NOW).unwrap();
            assert_eq!(out[0].topic, "t");
            assert_eq!(out[0].value.text(), r#"{"id":1,"title":"t"}"#);
        }
    }

    #[test]
    fn shapes_are_validated_at_startup() {
        let err = |r: KafkaRoute, dlq: Option<&str>| {
            RoutePlan::with_dead_letter(vec![r], None, dlq.map(Into::into))
                .unwrap_err()
                .to_string()
        };
        let e = err(shaped("r", None, Some("row")), None);
        assert!(
            e.contains("`deletes` applies to `format = \"row\"` only"),
            "{e}"
        );
        let e = err(shaped("r", Some("flat"), None), None);
        assert!(
            e.contains("route `r`: unknown format `flat` (expected `envelope` or `row`)"),
            "{e}"
        );
        let e = err(shaped("r", Some("row"), Some("delete")), None);
        assert!(e.contains("unknown deletes `delete`"), "{e}");
        let e = err(
            KafkaRoute {
                tombstones: true,
                ..shaped("r", Some("row"), None)
            },
            None,
        );
        assert!(e.contains("`tombstones` is for the envelope format"), "{e}");
        let unchanged = |u: &str| KafkaRoute {
            unchanged: Some(u.into()),
            ..named("r", "public.*", "t")
        };
        assert!(RoutePlan::new(vec![unchanged("dead_letter")], None).is_ok());
        let e = err(unchanged("fill"), Some(DLQ));
        assert!(e.contains("unknown unchanged `fill`"), "{e}");
        assert!(RoutePlan::with_dead_letter(
            vec![unchanged("dead_letter")],
            None,
            Some(DLQ.into())
        )
        .is_ok());
        // Case does not matter.
        assert!(RoutePlan::new(vec![shaped("r", Some("ROW"), Some("Drop"))], None).is_ok());
        // Outbox routes take none of the shape keys.
        let e = err(
            KafkaRoute {
                tombstones: true,
                unchanged: Some("omit".into()),
                outbox: Some(OutboxSpec::default()),
                ..named("o", "public.outbox", "")
            },
            Some(DLQ),
        );
        assert!(
            e.contains("`outbox` and `tombstones`, `unchanged` are mutually exclusive"),
            "{e}"
        );

        // Tombstones need a per-row key: `partition_by = "key"`, or the route's own `key`.
        let p = RoutePlan::new(vec![tombstoned("r")], None).unwrap();
        assert!(p.check_partition_by(PartitionBy::Key).is_ok());
        let e = p
            .check_partition_by(PartitionBy::Table)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("route `r` uses `tombstones`, which needs a per-row key"),
            "{e}"
        );
        let keyed = RoutePlan::new(
            vec![KafkaRoute {
                key: Some(strs(&["id"])),
                ..tombstoned("r")
            }],
            None,
        )
        .unwrap();
        assert!(keyed.check_partition_by(PartitionBy::Table).is_ok());
        assert!(RoutePlan::new(vec![route("public.*", "t")], None)
            .unwrap()
            .check_partition_by(PartitionBy::Table)
            .is_ok());
    }

    #[test]
    fn shapes_are_described() {
        let p = plan_with(vec![
            KafkaRoute {
                unchanged: Some("dead_letter".into()),
                ..KafkaRoute {
                    pattern: "public.a".into(),
                    ..shaped("rows", Some("row"), None)
                }
            },
            KafkaRoute {
                pattern: "public.b".into(),
                ..tombstoned("events")
            },
            named("plain", "public.*", "t"),
        ]);
        let d = p.describe();
        let r = &d.routes;
        assert_eq!(
            (r[0].format, r[0].deletes, r[0].tombstones, r[0].unchanged),
            (Some("row"), Some("tombstone"), false, Some("dead_letter"))
        );
        assert_eq!(
            (r[1].format, r[1].deletes, r[1].tombstones, r[1].unchanged),
            (None, None, true, None)
        );
        // Unset keys are left out of the JSON, so plain routes describe as before.
        let v = serde_json::to_value(&r[2]).unwrap();
        for k in ["format", "deletes", "tombstones", "unchanged"] {
            assert!(v.get(k).is_none(), "{k}: {v}");
        }
        let v = serde_json::to_value(&r[1]).unwrap();
        assert_eq!(v["tombstones"], true);
    }

    /// What one delete on `route` produced: the number of records and the route's counts
    /// (produced, filtered, tombstones_skipped).
    fn delete_outcome(route: KafkaRoute, del: &ChangeEvent) -> (usize, (u64, u64, u64)) {
        let p = fields_plan(vec![route]);
        let n = p.render(del, NOW).unwrap().len();
        let c = p.stats().snapshot()[0].counts;
        (n, (c.produced, c.filtered, c.tombstones_skipped))
    }

    #[test]
    fn no_null_value_record_is_produced_under_a_key_shared_by_other_rows() {
        let row_route = |r: KafkaRoute| KafkaRoute {
            format: Some("row".into()),
            pattern: "public.*".into(),
            ..r
        };
        let env_route = |r: KafkaRoute| KafkaRoute {
            tombstones: true,
            pattern: "public.*".into(),
            ..r
        };
        let plain = || named("o", "public.*", "t");
        let ops = |f: FieldSpec| with_fields("o", "public.*", f);
        let mut no_pk = row("orders", Op::Delete, r#"{"id":1,"email":"a@b.c"}"#);
        no_pk.key = None;
        let composite = {
            let mut e = row(
                "orders",
                Op::Delete,
                r#"{"id":1,"tenant":2,"email":"a@b.c"}"#,
            );
            e.key = Some(r#"{"id":1,"tenant":2}"#.into());
            e
        };
        let one_col = |c: &str| FieldSpec {
            exclude: Some(strs(&[c])),
            ..Default::default()
        };
        // (case, route base, event, per-row?)
        let cases: Vec<(&str, KafkaRoute, ChangeEvent, bool)> = vec![
            ("primary key", plain(), composite.clone(), true),
            (
                "no primary key (FULL or none)",
                plain(),
                no_pk.clone(),
                false,
            ),
            (
                "route key present",
                KafkaRoute {
                    key: Some(strs(&["email"])),
                    ..plain()
                },
                no_pk.clone(),
                true,
            ),
            (
                "route key absent from the before-image",
                KafkaRoute {
                    key: Some(strs(&["region"])),
                    ..plain()
                },
                composite.clone(),
                false,
            ),
            (
                "one route key column null",
                KafkaRoute {
                    key: Some(strs(&["id", "region"])),
                    ..plain()
                },
                row("orders", Op::Delete, r#"{"id":1,"region":null}"#),
                false,
            ),
            (
                "every key column excluded",
                ops(one_col("id")),
                row("orders", Op::Delete, r#"{"id":1}"#),
                false,
            ),
            (
                "one column of a composite key excluded",
                ops(one_col("tenant")),
                composite.clone(),
                false,
            ),
            (
                "a key column not included",
                ops(FieldSpec {
                    include: Some(strs(&["id", "email"])),
                    ..Default::default()
                }),
                composite.clone(),
                false,
            ),
            (
                "a key column masked",
                ops(FieldSpec {
                    mask: strs(&["id"]),
                    ..Default::default()
                }),
                composite.clone(),
                false,
            ),
            (
                "a key column hashed (HMAC keeps it unique)",
                ops(FieldSpec {
                    hash: strs(&["id"]),
                    ..Default::default()
                }),
                composite.clone(),
                true,
            ),
            (
                "a key column renamed",
                ops(FieldSpec {
                    rename: [("tenant".to_string(), "org".to_string())].into(),
                    ..Default::default()
                }),
                composite.clone(),
                true,
            ),
            (
                "another column excluded",
                ops(one_col("email")),
                composite.clone(),
                true,
            ),
        ];
        for (case, base, del, per_row) in cases {
            // Row format, `deletes = "tombstone"`: the delete is the null-value record, so a
            // shared key drops it (filtered) and counts the skipped tombstone.
            let got = delete_outcome(row_route(base.clone()), &del);
            let want = if per_row {
                (1, (1, 0, 0))
            } else {
                (0, (0, 1, 1))
            };
            assert_eq!(got, want, "row format: {case}");
            // Envelope: the delete is always produced, its tombstone only under a per-row key.
            let got = delete_outcome(env_route(base), &del);
            let want = if per_row {
                (2, (1, 0, 0))
            } else {
                (1, (1, 0, 1))
            };
            assert_eq!(got, want, "envelope: {case}");
        }
        // `deletes = "row"` and `"drop"` produce no null value: no check.
        let r = KafkaRoute {
            deletes: Some("row".into()),
            ..row_route(plain())
        };
        assert_eq!(delete_outcome(r, &no_pk), (1, (1, 0, 0)));
    }

    #[test]
    fn row_format_tombstones_need_a_per_row_key_at_startup() {
        let p = RoutePlan::new(vec![shaped("r", Some("row"), None)], None).unwrap();
        let e = p
            .check_partition_by(PartitionBy::Table)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("`format = \"row\"` with `deletes = \"tombstone\"`")
                && e.contains("per-row key"),
            "{e}"
        );
        for ok in [
            shaped("r", Some("row"), Some("drop")),
            shaped("r", Some("row"), Some("row")),
            KafkaRoute {
                key: Some(strs(&["id"])),
                ..shaped("r", Some("row"), None)
            },
        ] {
            let p = RoutePlan::new(vec![ok], None).unwrap();
            assert!(p.check_partition_by(PartitionBy::Table).is_ok());
        }
    }

    #[test]
    fn a_change_whose_after_is_only_the_key_is_no_row() {
        // As the MongoDB mapping builds it for an update without a fullDocument: `after` is the
        // documentKey and the event says so.
        let mut e = row("orders", Op::Update, r#"{"_id":1}"#);
        e.after_is_key = true;
        let p = plan_with(vec![shaped("rows", Some("row"), None)]);
        assert!(p.render(&e, NOW).unwrap().is_empty(), "not a row");
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.filtered), (1, 1));
        // The envelope is unchanged: `after` is the key, as before.
        let p = plan_with(vec![named("env", "public.*", "t")]);
        let r = p.render(&e, NOW).unwrap().remove(0);
        assert_eq!(json(&r.value)["after"], serde_json::json!({"_id": 1}));
    }

    #[test]
    fn unchanged_dead_letter_ignores_dropped_columns_and_escapes_names() {
        let route = |f: FieldSpec| KafkaRoute {
            unchanged: Some("dead_letter".into()),
            ..with_fields("docs", "public.*", f)
        };
        let mut upd = row("docs", Op::Update, r#"{"id":1}"#);
        upd.unchanged = vec!["blob".into()];
        // `blob` is excluded (or not included): its absence changes nothing the route sends.
        for f in [
            FieldSpec {
                exclude: Some(strs(&["blob"])),
                ..Default::default()
            },
            FieldSpec {
                include: Some(strs(&["id"])),
                ..Default::default()
            },
        ] {
            let p = fields_plan(vec![route(f)]);
            assert_eq!(p.render(&upd, NOW).unwrap()[0].topic, "docs.topic");
        }
        // A masked column is still sent, so its absence counts.
        let p = fields_plan(vec![route(FieldSpec {
            mask: strs(&["blob"]),
            ..Default::default()
        })]);
        assert_eq!(p.render(&upd, NOW).unwrap()[0].topic, DLQ);
        // Control characters and backticks in a name cannot forge the header or the log.
        upd.unchanged = vec!["a`b\nc\u{1b}".into()];
        let p = plan_with(vec![KafkaRoute {
            unchanged: Some("dead_letter".into()),
            ..named("docs", "public.*", "t")
        }]);
        let r = p.render(&upd, NOW).unwrap().remove(0);
        assert!(
            r.headers[0].1.ends_with("column(s) `a?b?c?`"),
            "{}",
            r.headers[0].1
        );
    }

    // --- dead-letter topics per route: `<topic>.dlq` by default (2026-10-21) --------------------

    /// Every route's dead-letter topic as described, routes first, `default` last.
    fn dlqs(p: &RoutePlan) -> Vec<Option<String>> {
        p.describe()
            .routes
            .into_iter()
            .map(|r| r.dead_letter_topic)
            .collect()
    }

    fn some(v: &[&str]) -> Vec<Option<String>> {
        v.iter().map(|s| Some(s.to_string())).collect()
    }

    /// A route on `public.<name>` whose `where` reads row data, so it dead-letters `{"n":"x"}`.
    fn failing(name: &str, topic: &str) -> KafkaRoute {
        KafkaRoute {
            filter: Some("row.n > 1".into()),
            ..named(name, &format!("public.{name}"), topic)
        }
    }

    #[test]
    fn each_route_dead_letters_to_its_own_topic_plus_dlq_by_default() {
        let p = RoutePlan::new(
            vec![
                // A static topic.
                named("assets", "sim.assets", "sim.assets.active"),
                // A `{schema}`/`{table}` template: rendered per event, then suffixed.
                named("cdc", "public.*", "cdc.{schema}.{table}"),
                // `{op}` is dropped with one separator: no per-operation dead-letter topic.
                named("ops", "hr.*", "hr.{table}.{op}"),
                // A content-routed template: the row is the problem, so `<topic_fallback>.dlq`.
                content(
                    "orders",
                    "orders.{row.region}",
                    Some("orders.unknown"),
                    None,
                ),
                // An outbox: `<outbox topic_fallback>.dlq`, or its static topic plus `.dlq`.
                outbox_route(outbox_spec()),
                KafkaRoute {
                    name: Some("ob2".into()),
                    pattern: "public.outbox2".into(),
                    outbox: Some(OutboxSpec {
                        topic: "events".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
            // The `default` route follows the same rules.
            Some("all.{schema}.{table}".into()),
        )
        .unwrap();
        assert_eq!(
            dlqs(&p),
            some(&[
                "sim.assets.active.dlq",
                "cdc.{schema}.{table}.dlq",
                "hr.{table}.dlq",
                "orders.unknown.dlq",
                "outbox.unknown.dlq",
                "events.dlq",
                "all.{schema}.{table}.dlq",
            ])
        );
        // No global override.
        assert_eq!(p.describe().dead_letter_topic, None);
        // A template is rendered for the event, like the route's topic, `{op}` left out.
        let p = RoutePlan::new(vec![failing("orders", "cdc.{schema}.{table}.{op}")], None).unwrap();
        for op in [Op::Insert, Op::Update] {
            let out = p.render(&row("orders", op, r#"{"n":"x"}"#), NOW).unwrap();
            assert_eq!(out[0].topic, "cdc.public.orders.dlq");
        }
        // A bad row of a content route goes to its fallback's dead-letter topic.
        let q = RoutePlan::new(
            vec![KafkaRoute {
                filter: Some("row.n > 1".into()),
                ..content(
                    "orders",
                    "orders.{row.region}",
                    Some("orders.unknown"),
                    None,
                )
            }],
            None,
        )
        .unwrap();
        let out = q
            .render(
                &row("orders", Op::Insert, r#"{"n":"x","region":"eu"}"#),
                NOW,
            )
            .unwrap();
        assert_eq!(out[0].topic, "orders.unknown.dlq");
        assert_eq!(out[0].headers[0].0, "aqueduct-error");
        assert_eq!(q.stats().snapshot()[0].counts.errors, 1);
    }

    #[test]
    fn dead_letter_topic_precedence_is_route_then_sink_then_suffix() {
        let own = |name: &str, dlq: &str| KafkaRoute {
            dead_letter_topic: Some(dlq.into()),
            ..failing(name, &format!("{name}.t"))
        };
        let routes = || vec![own("a", "a.errors"), failing("b", "b.t")];
        let p = RoutePlan::new(routes(), Some("rest".into())).unwrap();
        assert_eq!(dlqs(&p), some(&["a.errors", "b.t.dlq", "rest.dlq"]));
        let p = RoutePlan::with_dead_letter(routes(), Some("rest".into()), Some("all.dlq".into()))
            .unwrap();
        assert_eq!(dlqs(&p), some(&["a.errors", "all.dlq", "all.dlq"]));
        assert_eq!(p.describe().dead_letter_topic.as_deref(), Some("all.dlq"));
        // Records follow it.
        for (table, want) in [("a", "a.errors"), ("b", "all.dlq")] {
            let out = p
                .render(&row(table, Op::Insert, r#"{"n":"x"}"#), NOW)
                .unwrap();
            assert_eq!(out[0].topic, want);
        }
        // The fixed ones of routes built to dead-letter are opened at startup, once each.
        assert_eq!(p.dead_letter_topics(), ["a.errors", "all.dlq"]);
        let p = RoutePlan::new(
            vec![
                failing("a", "a.t"),
                own("b", "a.t.dlq"),
                named("plain", "x.*", "plain"),
                failing("tpl", "y.{table}"),
            ],
            None,
        )
        .unwrap();
        // `plain` cannot dead-letter, and `y.{table}.dlq` is created on first use.
        assert_eq!(p.dead_letter_topics(), ["a.t.dlq"]);
        // The inspector reads every one, templates as globs.
        assert_eq!(
            p.topic_globs(),
            [
                "a.t",
                "b.t",
                "plain",
                "y.*",
                "a.t.dlq",
                "plain.dlq",
                "y.*.dlq"
            ]
        );
    }

    #[test]
    fn dead_letter_topics_cannot_be_reached_by_row_values() {
        let build = |routes: Vec<KafkaRoute>, default: Option<&str>, dlq: Option<&str>| {
            RoutePlan::with_dead_letter(routes, default.map(Into::into), dlq.map(Into::into))
                .map(|p| dlqs(&p))
                .map_err(|e| e.to_string())
        };
        let ok = || content("o", "orders.{row.r}", Some("orders.unknown"), None);
        // `orders.{row.r}` reads as the glob `orders.*`, which matches `orders.unknown.dlq` and
        // `orders.dlq`; but a row value is one segment without dots and never `dlq`, so it can
        // render neither: the content route and a static `orders` route keep their defaults.
        assert_eq!(
            build(vec![ok(), failing("s", "orders")], None, None),
            Ok(some(&["orders.unknown.dlq", "orders.dlq"]))
        );
        assert_eq!(
            build(vec![ok()], None, Some("orders.dlq")),
            Ok(some(&["orders.dlq"]))
        );
        // A name a row value, a table or an operation can render is refused, and so are names
        // that break the other rules: as a route's override, the global one, or a default.
        type Case<'a> = (Vec<KafkaRoute>, Option<&'a str>, Option<&'a str>, &'a str);
        let cases: Vec<Case> = vec![
            (
                vec![
                    ok(),
                    KafkaRoute {
                        dead_letter_topic: Some("orders.eu".into()),
                        ..failing("s", "s.t")
                    },
                ],
                None,
                None,
                "sink route `s`: `dead_letter_topic` `orders.eu` is also a topic events are routed to (`orders.{row.r}`)",
            ),
            (
                vec![ok()],
                None,
                Some("orders.eu"),
                "sink.dead_letter_topic `orders.eu` is also a topic events are routed to",
            ),
            // A content template with a fixed `.dlq` suffix renders `orders.x.dlq`, the default
            // of a static `orders.x` route, and `orders.unknown.dlq`, its own fallback's.
            (
                vec![
                    content("o", "orders.{row.r}.dlq", Some("fb"), None),
                    failing("s", "orders.x"),
                ],
                None,
                None,
                "sink route `s`: the dead-letter topic (its `topic` plus `.dlq`) `orders.x.dlq` is also a topic events are routed to (`orders.{row.r}.dlq`); set `dead_letter_topic` on the route or `sink.dead_letter_topic`",
            ),
            (
                vec![KafkaRoute {
                    filter: Some("row.n > 1".into()),
                    ..content("o", "orders.{row.r}.dlq", Some("orders.unknown"), None)
                }],
                None,
                None,
                "the dead-letter topic (its `topic_fallback` plus `.dlq`) `orders.unknown.dlq` is also a topic events are routed to",
            ),
            // `{table}` stands for one name: a table named `dlq` would write into `cdc.dlq`.
            (
                vec![failing("s", "s.t")],
                Some("cdc.{table}"),
                Some("cdc.dlq"),
                "sink.dead_letter_topic `cdc.dlq` is also a topic events are routed to (`cdc.{table}`)",
            ),
            (
                vec![failing("s", "s.t"), named("ins", "i.*", "s.{op}")],
                None,
                Some("s.insert"),
                "is also a topic events are routed to (`s.{op}`)",
            ),
            // A rendered dead-letter template must not overlap a content-routed template.
            (
                vec![
                    content("o", "p.{row.r}.x.dlq", Some("fb"), None),
                    failing("s", "p.{table}.x"),
                ],
                None,
                None,
                "`p.{table}.x.dlq` (as the glob `p.*.x.dlq`) overlaps the content-routed `topic` `p.{row.r}.x.dlq`",
            ),
            (
                vec![KafkaRoute {
                    dead_letter_topic: Some("bad topic".into()),
                    ..failing("s", "s.t")
                }],
                None,
                None,
                "sink route `s`: `dead_letter_topic` `bad topic` is not a valid Kafka topic name",
            ),
            (
                vec![KafkaRoute {
                    dead_letter_topic: Some("{table}.dlq".into()),
                    ..failing("s", "s.t")
                }],
                None,
                None,
                "is not a valid Kafka topic name",
            ),
            (
                vec![failing("s", "_s.t")],
                None,
                None,
                "sink route `s`: the dead-letter topic (its `topic` plus `.dlq`) `_s.t.dlq` must not start with `_`",
            ),
            (
                vec![KafkaRoute {
                    dead_letter_topic: Some("fb".into()),
                    ..content("o", "o.{row.r}", Some("fb"), None)
                }],
                None,
                None,
                "`dead_letter_topic` `fb` is also a route's `topic_fallback`",
            ),
            (
                vec![KafkaRoute {
                    dead_letter_topic: Some("t".into()),
                    ..failing("s", "s.t")
                }],
                Some("t"),
                None,
                "`dead_letter_topic` `t` is also a topic events are routed to (`t`)",
            ),
            (
                vec![failing("s", &format!("s.{}", "x".repeat(244)))],
                None,
                None,
                "is not a valid Kafka topic name",
            ),
        ];
        for (routes, default, dlq, want) in cases {
            let err = build(routes, default, dlq).expect_err(want);
            assert!(err.contains(want), "{err}\nshould contain: {want}");
        }
        // A route that cannot dead-letter gets no dead-letter topic rather than a startup error.
        assert_eq!(
            build(vec![named("p", "p.*", "_p")], None, None),
            Ok(vec![None])
        );
    }

    #[test]
    fn dlq_is_a_reserved_row_value() {
        // `a.{row.x}.{row.y}` would render `a.foo.dlq` for y = "dlq": such rows go to the fallback.
        let p = RoutePlan::new(
            vec![
                content("o", "a.{row.x}.{row.y}", Some("a.unknown"), None),
                failing("s", "a.foo"),
            ],
            None,
        )
        .unwrap();
        assert_eq!(dlqs(&p), some(&["a.unknown.dlq", "a.foo.dlq"]));
        let topic = |json: &str| {
            p.render(&row("orders", Op::Insert, json), NOW).unwrap()[0]
                .topic
                .clone()
        };
        assert_eq!(topic(r#"{"x":"foo","y":"dlq"}"#), "a.unknown");
        assert_eq!(topic(r#"{"x":"dlq","y":"foo"}"#), "a.unknown");
        // Topic names are case-sensitive, so only `dlq` itself is reserved.
        assert_eq!(topic(r#"{"x":"foo","y":"DLQ"}"#), "a.foo.DLQ");
        assert_eq!(p.stats().snapshot()[0].counts.fallback, 2);
        // Listing it in `topic_values` is refused.
        let e = RoutePlan::new(
            vec![content(
                "o",
                "a.{row.x}",
                Some("a.unknown"),
                Some(&["eu", "dlq"]),
            )],
            None,
        )
        .map(|_| ())
        .unwrap_err()
        .to_string();
        assert!(
            e.contains("`topic_values` entry `dlq` can never be used: it is reserved"),
            "{e}"
        );
        // Dead-letter topics already on the broker are not counted as content-routed.
        let q = content_plan(None, 100);
        assert_eq!(
            q.add_existing_topics(["orders.eu", "orders.unknown.dlq", "orders.dlq", "rest.dlq"]),
            1
        );
    }

    #[test]
    fn a_rendered_dead_letter_name_kafka_would_refuse_fails_the_batch() {
        let p = RoutePlan::new(
            vec![KafkaRoute {
                filter: Some("row.n > 1".into()),
                ..named("r", "*", "{schema}.{table}")
            }],
            None,
        )
        .unwrap();
        let mut e = row("orders", Op::Insert, r#"{"n":"x"}"#);
        e.schema = "_internal".into();
        let err = p.render(&e, NOW).unwrap_err().to_string();
        assert!(
            err.contains("its dead-letter topic `_internal.orders.dlq` is not a usable topic name"),
            "{err}"
        );
    }

    #[test]
    fn without_op_drops_the_placeholder_and_one_separator() {
        for (raw, want) in [
            ("cdc.{table}.{op}", "cdc.{table}"),
            ("{op}.cdc.{table}", "cdc.{table}"),
            ("cdc.{op}.{table}", "cdc.{table}"),
            ("cdc_{op}_{table}", "cdc_{table}"),
            ("cdc{op}{table}", "cdc{table}"),
            ("a.{op}.{op}.b", "a.b"),
            ("{op}", "{op}"),
            ("cdc.{table}", "cdc.{table}"),
        ] {
            assert_eq!(without_op(raw), want, "{raw}");
        }
    }
}
