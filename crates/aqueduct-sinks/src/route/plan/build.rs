//! Compiling `[[sink.routes]]` into a [`RoutePlan`], with every startup check: patterns,
//! templates, content routing, collisions, dead-letter topics, `otherwise` chains and
//! field operations.

use super::*;

#[derive(Debug, Clone)]
pub(super) struct CompiledRoute {
    pub(super) pattern: String,
    pub(super) topic: TopicTemplate,
    pub(super) filter: Option<Expr>,
    /// `key = [...]`: the columns and the `row.<column>` paths that read them, in config order.
    pub(super) key: Option<RouteKey>,
    /// Where an event goes when its content-routed topic cannot be used. Set exactly when the
    /// topic reads a row field.
    pub(super) topic_fallback: Option<String>,
    /// The values a content-routed topic's row fields may take; `None` allows any valid segment.
    pub(super) topic_values: Option<Vec<String>>,
    /// `outbox = {...}`; its topic template is `topic`.
    pub(super) outbox: Option<Outbox>,
    /// `None`: no field operation, the record is built exactly as without routing.
    pub(super) fields: Option<FieldOps>,
    /// `format`, `deletes`, `tombstones` and `unchanged`; the default builds the envelope.
    pub(super) shape: Shape,
    /// `otherwise`: where the rows `where` rejects are passed on to.
    pub(super) otherwise: Option<PassOn>,
}

/// A route's `key = [...]`.
#[derive(Debug, Clone)]
pub(super) struct RouteKey {
    pub(super) columns: Vec<String>,
    pub(super) fields: Vec<FieldRef>,
}

impl CompiledRoute {
    /// The `otherwise` topic, when `otherwise` names one.
    pub(super) fn otherwise_topic(&self) -> Option<&str> {
        match &self.otherwise {
            Some(PassOn::Topic(t)) => Some(t),
            _ => None,
        }
    }

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
    pub(super) fn expects_dead_letters(&self) -> bool {
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
pub(super) fn parse_key(name: &str, cols: &[String]) -> Result<RouteKey> {
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

/// Whether a content-routed template may substitute `v` for `f`: row fields must be one segment
/// and in `topic_values`; `{schema}`, `{table}` and `{op}` are not checked.
pub(super) fn value_allowed(r: &CompiledRoute, f: &FieldRef, v: &str) -> bool {
    f.column().is_none()
        || (valid_segment(v)
            && v != RESERVED_SEGMENT
            && r.topic_values
                .as_ref()
                .is_none_or(|vals| vals.iter().any(|a| a == v)))
}

/// True when some string matches both globs (`*` matches any run of characters).
pub(super) fn globs_intersect(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    // memo[i][j]: Some(result) for the suffixes a[i..] and b[j..].
    let mut memo = vec![vec![None; b.len() + 1]; a.len() + 1];
    pub(super) fn go(
        a: &[u8],
        b: &[u8],
        i: usize,
        j: usize,
        memo: &mut Vec<Vec<Option<bool>>>,
    ) -> bool {
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

/// Checks a route's content-routing keys (ADR 0002 §5): a template that reads a row field needs
/// a fixed, non-empty prefix and a fixed `topic_fallback`; `topic_values`, if given, lists valid
/// segments. Other topics may not set either key.
pub(super) fn check_content_routing(
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
pub(super) fn check_collisions(
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
        if let Some(t) = r.otherwise_topic() {
            fixed.push((t, format!("route `{}`'s `otherwise` topic", names[i]), None));
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
pub(super) fn check_dead_letter(
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
            if routes
                .iter()
                .any(|r| r.otherwise_topic() == Some(t.as_str()))
            {
                return bad(format!("is also a route's `otherwise` topic{pick}"));
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
/// Resolves each route's `otherwise` to a later route without `match` or a fixed topic, and
/// refuses a route without `match` that no `otherwise` reaches. A row passed on is only ever
/// handed forward, so a chain cannot loop.
pub(super) fn link_otherwise(
    routes: &[KafkaRoute],
    names: &[String],
    compiled: &mut [CompiledRoute],
) -> Result<()> {
    let mut reached = vec![false; routes.len()];
    for (i, r) in routes.iter().enumerate() {
        let Some(next) = &r.otherwise else { continue };
        let name = &names[i];
        let err = |msg: String| Err(Error::Config(format!("sink route `{name}`: {msg}")));
        if r.outbox.is_some() {
            return err("`outbox` and `otherwise` are mutually exclusive".into());
        }
        if r.filter.is_none() {
            return err("`otherwise` needs a `where`: without one no row is ever passed on".into());
        }
        let next = match next {
            Otherwise::Route(next) => next,
            Otherwise::Topic(t) => {
                if !valid_topic_name(t) {
                    return err(format!(
                        "`otherwise = {{ topic = \"{t}\" }}` is not a valid Kafka topic name (1 to 249 of A-Z a-z 0-9 . _ -, no placeholders)"
                    ));
                }
                if t.starts_with('_') {
                    return err(format!(
                        "`otherwise = {{ topic = \"{t}\" }}` must not start with `_` (reserved for internal topics)"
                    ));
                }
                compiled[i].otherwise = Some(PassOn::Topic(t.clone()));
                continue;
            }
        };
        let Some(j) = names.iter().position(|n| n == next) else {
            return err(format!(
                "`otherwise = {{ route = \"{next}\" }}` names no route"
            ));
        };
        if j <= i {
            return err(format!(
                "`otherwise` must name a later route, so a row can never loop; `{next}` is not after it"
            ));
        }
        if !routes[j].pattern.is_empty() {
            return err(format!(
                "`otherwise` route `{next}` has a `match`; a route reached through `otherwise` has none, it takes only the rows passed on to it"
            ));
        }
        compiled[i].otherwise = Some(PassOn::Route(j));
        reached[j] = true;
    }
    if let Some(i) = (0..routes.len()).find(|&i| routes[i].pattern.is_empty() && !reached[i]) {
        return Err(Error::Config(format!(
            "sink route `{}` has no `match`; set one, or name it in an earlier route's `otherwise`",
            names[i]
        )));
    }
    Ok(())
}

pub(super) fn check_field_ops(
    name: &str,
    r: &CompiledRoute,
    hash_key: Option<&HashKey>,
) -> Result<()> {
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
            if r.pattern.is_empty() && r.name.is_none() {
                return Err(Error::Config(
                    "a sink route needs `match`, or a `name` when an earlier route's `otherwise` reaches it".into(),
                ));
            }
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
                otherwise: None,
            };
            check_field_ops(&name, &route, hash_key.as_ref())?;
            compiled.push(route);
            names.push(name);
        }
        link_otherwise(&routes, &names, &mut compiled)?;
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
                        return Err(e);
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
            resolved: Resolved::default(),
        };
        plan.fixed = plan.static_topics();
        // A content-routed name must not collide (`.` vs `_`) with a fixed topic either.
        plan.dynamic
            .exists(plan.fixed.iter().map(|t| (t.as_str(), false)));
        Ok(plan)
    }
}
