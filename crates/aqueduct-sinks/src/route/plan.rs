//! The compiled routing table and its pure `render` step.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use aqueduct_core::{ChangeEvent, Error, Op, Result};
use serde::Serialize;

use super::expr::{op_name, EventView, Expr, FieldRef, Tri};
use crate::kafka::{glob_match, partition_input, PartitionBy};
use crate::{encode, op_code, record_key, table_key, SourceInfo};

/// Rows of a matched table seen without a field a route references, while the field has never been
/// seen, before the field is reported as unseen (warn-once log, metric, cockpit).
pub const UNSEEN_FIELD_ROWS: u64 = 1000;

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

/// A topic with `{schema}`/`{table}` placeholders; one without is cloned as is.
#[derive(Debug, Clone)]
struct TopicTemplate {
    raw: String,
    dynamic: bool,
}

impl TopicTemplate {
    fn new(raw: &str) -> Self {
        Self {
            dynamic: raw.contains("{schema}") || raw.contains("{table}"),
            raw: raw.to_string(),
        }
    }

    fn fill(&self, schema: &str, table: &str) -> String {
        if self.dynamic {
            self.raw
                .replace("{schema}", schema)
                .replace("{table}", table)
        } else {
            self.raw.clone()
        }
    }

    /// The template as a glob: placeholders turned into `*`.
    fn glob(&self) -> String {
        self.raw.replace("{schema}", "*").replace("{table}", "*")
    }
}

#[derive(Debug, Clone)]
struct CompiledRoute {
    pattern: String,
    topic: TopicTemplate,
    /// `None`: every op passes.
    ops: Option<Vec<Op>>,
    filter: Option<Expr>,
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
    /// Events sent to a route's `topic_fallback` (content routing, a later slice). Always 0 for now.
    fallback: Vec<AtomicU64>,
    errors: Vec<AtomicU64>,
    /// Per route, every field path it references. `where` registers its paths here; templates,
    /// `key`, field operations and outbox columns register theirs the same way.
    fields: Vec<Vec<WatchedField>>,
    /// Per route, whether the first dead-lettered event was logged.
    error_logged: Vec<AtomicBool>,
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
            names,
        }
    }

    fn bump(counter: &[AtomicU64], route: usize) {
        counter[route].fetch_add(1, Ordering::Relaxed);
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
    /// The topic template, placeholders included.
    pub topic: String,
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
            if r.name.is_some() && names.contains(&name) {
                return Err(Error::Config(format!("duplicate sink route name `{name}`")));
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
            fields.push(filter.iter().flat_map(|f| f.fields()).cloned().collect());
            compiled.push(CompiledRoute {
                pattern: r.pattern.clone(),
                topic: TopicTemplate::new(&r.topic),
                ops,
                filter,
            });
            names.push(name);
        }
        if default.is_some() {
            names.push("default".into());
        }
        let default = default.as_deref().map(TopicTemplate::new);
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
        }
        Ok(Self {
            routes: compiled,
            default,
            dead_letter: dead_letter_topic,
            partition_by: PartitionBy::default(),
            source: SourceInfo::default(),
            stats: Arc::new(RouteStats::new(names, fields)),
        })
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

    /// The topic for a table, or `None` when no route matches and there is no default.
    pub fn topic_for(&self, schema: &str, table: &str) -> Option<String> {
        self.resolve(schema, table).map(|(_, topic)| topic)
    }

    /// Index into the stats (routes first, default last) and the topic.
    fn resolve(&self, schema: &str, table: &str) -> Option<(usize, String)> {
        let name = format!("{schema}.{table}");
        if let Some((i, r)) = self
            .routes
            .iter()
            .enumerate()
            .find(|(_, r)| glob_match(&r.pattern, &name))
        {
            return Some((i, r.topic.fill(schema, table)));
        }
        self.default
            .as_ref()
            .map(|t| (self.routes.len(), t.fill(schema, table)))
    }

    /// Globs matching every topic this plan can produce to: each route, the default topic and
    /// the dead-letter topic, with `{schema}`/`{table}` placeholders turned into `*`.
    pub fn topic_globs(&self) -> Vec<String> {
        self.routes
            .iter()
            .map(|r| r.topic.glob())
            .chain(self.default.as_ref().map(TopicTemplate::glob))
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
                topic: topic.to_string(),
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
        let (route, topic) = self.resolve(&event.schema, &event.table).ok_or_else(|| {
            Error::Sink(format!(
                "no kafka route or default topic for {}.{}",
                event.schema, event.table
            ))
        })?;
        RouteStats::bump(&self.stats.matched, route);
        if let Some(r) = self.routes.get(route) {
            if r.ops.as_ref().is_some_and(|ops| !ops.contains(&event.op)) {
                RouteStats::bump(&self.stats.filtered, route);
                return Ok(());
            }
            if let Some(filter) = &r.filter {
                let view = EventView::new(event);
                self.stats.observe(route, &view);
                match filter.eval(&view) {
                    Ok(Tri::True) => {}
                    Ok(Tri::False | Tri::Unknown) => {
                        RouteStats::bump(&self.stats.filtered, route);
                        return Ok(());
                    }
                    Err(e) => {
                        let reason = format!(
                            "route `{}`: where `{}`: {e}",
                            self.stats.names[route],
                            filter.source()
                        );
                        self.dead_letter(route, reason, event, now_us, out)?;
                        RouteStats::bump(&self.stats.errors, route);
                        return Ok(());
                    }
                }
            }
        }
        let key = record_key(event, &self.source);
        out.push(OutRecord {
            topic,
            partition_key: partition_input(event, &key, self.partition_by),
            key,
            value: encode(event, &self.source, now_us)?,
            op: op_code(event.op),
            lsn: event.position.to_checkpoint(),
            commit_ts_micros: event.commit_ts_micros,
            headers: Vec::new(),
        });
        RouteStats::bump(&self.stats.produced, route);
        Ok(())
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
        if !self.stats.error_logged[route].swap(true, Ordering::Relaxed) {
            tracing::warn!(
                route = %self.stats.names[route],
                topic = %topic,
                error = %reason,
                "an event could not be evaluated and went to the dead-letter topic; later ones are only counted under errors"
            );
        }
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
            headers: vec![("aqueduct-error", reason)],
        });
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
        // Unnamed routes label by pattern; a repeated pattern is allowed (the second never matches).
        assert!(RoutePlan::new(vec![route("a.*", "t"), route("a.*", "u")], None).is_ok());
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
            route("public.*", "cdc.{table}"),
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
                     "where": "row.status == 'paid'", "topic": "orders.topic",
                     "counts": {"matched": 1, "produced": 1, "filtered": 0, "fallback": 0, "errors": 0},
                     "unseen_fields": []},
                    {"route": "public.*", "match": "public.*", "ops": null, "where": null,
                     "topic": "cdc.{table}",
                     "counts": {"matched": 0, "produced": 0, "filtered": 0, "fallback": 0, "errors": 0},
                     "unseen_fields": []},
                    {"route": "default", "match": null, "ops": null, "where": null, "topic": "rest",
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
