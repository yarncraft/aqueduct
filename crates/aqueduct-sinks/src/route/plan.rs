//! The compiled routing table and its pure `render` step.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use aqueduct_core::{ChangeEvent, Error, Op, Result};

use crate::kafka::{glob_match, partition_input, PartitionBy};
use crate::{encode, op_code, record_key, SourceInfo};

/// Sends events whose `schema.table` matches `pattern` to `topic`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaRoute {
    /// Label in route stats; defaults to `pattern`.
    pub name: Option<String>,
    /// `schema.table` with `*` wildcards, e.g. `public.*`, `*.audit_log`, `*`.
    pub pattern: String,
    /// Destination topic; `{schema}` and `{table}` are substituted.
    pub topic: String,
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
}

/// Counters per route, shared between the sink (writer) and the admin server (reader).
#[derive(Debug)]
pub struct RouteStats {
    names: Vec<String>,
    matched: Vec<AtomicU64>,
    produced: Vec<AtomicU64>,
}

/// A point-in-time copy of one route's counters. `route` is the route's `name`, else its `match`
/// pattern; the `sink.topic` fallback is `default`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteCount {
    pub route: String,
    /// Events the route claimed (first match wins).
    pub matched: u64,
    /// Records produced from them.
    pub produced: u64,
}

impl RouteStats {
    fn new(names: Vec<String>) -> Self {
        let zeros = || names.iter().map(|_| AtomicU64::new(0)).collect();
        Self {
            matched: zeros(),
            produced: zeros(),
            names,
        }
    }

    pub fn snapshot(&self) -> Vec<RouteCount> {
        self.names
            .iter()
            .zip(self.matched.iter().zip(&self.produced))
            .map(|(name, (m, p))| RouteCount {
                route: name.clone(),
                matched: m.load(Ordering::Relaxed),
                produced: p.load(Ordering::Relaxed),
            })
            .collect()
    }
}

/// Picks the topic for a table (first matching route wins, else the default) and renders the
/// Kafka record. Compiled once from `[[sink.routes]]`.
#[derive(Debug, Clone)]
pub struct RoutePlan {
    routes: Vec<CompiledRoute>,
    default: Option<TopicTemplate>,
    partition_by: PartitionBy,
    source: SourceInfo,
    stats: Arc<RouteStats>,
}

impl RoutePlan {
    pub fn new(routes: Vec<KafkaRoute>, default: Option<String>) -> Result<Self> {
        if routes.is_empty() && default.is_none() {
            return Err(Error::Config(
                "kafka sink needs `topic` and/or at least one route".into(),
            ));
        }
        let mut names: Vec<String> = Vec::with_capacity(routes.len() + 1);
        for r in &routes {
            let name = r.name.clone().unwrap_or_else(|| r.pattern.clone());
            if name.is_empty() {
                return Err(Error::Config("sink route `name` must not be empty".into()));
            }
            if r.name.is_some() && names.contains(&name) {
                return Err(Error::Config(format!("duplicate sink route name `{name}`")));
            }
            names.push(name);
        }
        if default.is_some() {
            names.push("default".into());
        }
        Ok(Self {
            routes: routes
                .iter()
                .map(|r| CompiledRoute {
                    pattern: r.pattern.clone(),
                    topic: TopicTemplate::new(&r.topic),
                })
                .collect(),
            default: default.as_deref().map(TopicTemplate::new),
            partition_by: PartitionBy::default(),
            source: SourceInfo::default(),
            stats: Arc::new(RouteStats::new(names)),
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

    /// Globs matching every topic this plan can produce to: each route and the default topic,
    /// with `{schema}`/`{table}` placeholders turned into `*`.
    pub fn topic_globs(&self) -> Vec<String> {
        self.routes
            .iter()
            .map(|r| r.topic.glob())
            .chain(self.default.as_ref().map(TopicTemplate::glob))
            .collect()
    }

    /// Appends the records for `event` to `out`. Pure: no I/O, and the clock is the caller's
    /// `now_us` (the envelope's top-level `ts_ms`/`ts_us`). Heartbeats produce nothing. An event
    /// no route claims is an error, as before.
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
        self.stats.matched[route].fetch_add(1, Ordering::Relaxed);
        let key = record_key(event, &self.source);
        out.push(OutRecord {
            topic,
            partition_key: partition_input(event, &key, self.partition_by),
            key,
            value: encode(event, &self.source, now_us)?,
            op: op_code(event.op),
            lsn: event.position.to_checkpoint(),
            commit_ts_micros: event.commit_ts_micros,
        });
        self.stats.produced[route].fetch_add(1, Ordering::Relaxed);
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
        assert!(p.stats().snapshot().iter().all(|c| c.matched == 0));
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
            matched: n,
            produced: n,
        };
        assert_eq!(
            p.stats().snapshot(),
            vec![c("users", 2), c("sales.*", 1), c("default", 1)]
        );
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
