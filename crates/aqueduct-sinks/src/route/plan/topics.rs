//! Topic names: templates, the content-routing segment rules, dead-letter names and the
//! bounded set of topics content routing has created.

use super::*;

/// The longest value one placeholder of a content-routed topic may substitute.
pub(super) const MAX_SEGMENT: usize = 64;

/// A topic template (ADR 0002 §3, §5). `{schema}`/`{table}`-only templates are filled by string
/// replacement, exactly as before content routing; templates with `{op}` or a row field are
/// rendered from the event.
#[derive(Debug, Clone)]
pub(super) struct TopicTemplate {
    pub(super) raw: String,
    pub(super) parsed: Template,
    /// Only `{schema}`/`{table}` placeholders, or none.
    pub(super) simple: bool,
    /// Reads a row field: content routing, with its rules and fallback.
    pub(super) reads_row: bool,
}

impl TopicTemplate {
    pub(super) fn new(raw: &str) -> std::result::Result<Self, String> {
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
    pub(super) fn fill(&self, schema: &str, table: &str) -> String {
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
    pub(super) fn render(&self, view: &EventView<'_>) -> String {
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
    pub(super) fn glob(&self) -> String {
        self.parsed.glob()
    }

    /// True when no placeholder is used: one fixed topic name.
    pub(super) fn is_static(&self) -> bool {
        self.parsed.is_static()
    }

    /// True when the template renders exactly `name` for some event. Row fields take the values
    /// content routing allows on route `r` ([`value_allowed`]); `{op}` one of the operation names;
    /// `{schema}` and `{table}` one dot-free name (names with dots, which need quoting in Postgres,
    /// are not considered).
    pub(super) fn produces(&self, r: Option<&CompiledRoute>, name: &str) -> bool {
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
pub(super) fn without_op(raw: &str) -> String {
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
    if s.is_empty() { raw.to_string() } else { s }
}

/// Where a route's dead-letter records go (ADR 0002, "As built (2026-10-21)").
#[derive(Debug, Clone)]
pub(super) enum DeadLetter {
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
    pub(super) fn derived(topic: &TopicTemplate, fallback: Option<&str>) -> Result<Self> {
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
    pub(super) fn name(&self) -> &str {
        match self {
            Self::Fixed(t) => t,
            Self::Template(t) => &t.raw,
        }
    }

    pub(super) fn glob(&self) -> String {
        match self {
            Self::Fixed(t) => t.clone(),
            Self::Template(t) => t.glob(),
        }
    }

    pub(super) fn fixed(&self) -> Option<&str> {
        match self {
            Self::Fixed(t) => Some(t),
            Self::Template(_) => None,
        }
    }
}

/// One substituted value of a content-routed topic: `[A-Za-z0-9_-]{1,64}`.
pub(super) fn valid_segment(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= MAX_SEGMENT
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

/// Kafka refuses a topic whose name equals an existing one once `.` and `_` are made the same
/// (the two collide in its metric names).
pub(super) fn collision_key(topic: &str) -> String {
    topic.replace('.', "_")
}

/// Why a content-routed topic cannot be used.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Admit {
    Yes,
    /// Another topic has the same name up to `.` vs `_`; Kafka would refuse to create this one.
    Collides,
    /// New, and `sink.max_dynamic_topics` is reached.
    Cap,
    /// The broker refused to create it earlier in this run.
    Refused,
}

#[derive(Debug, Default)]
pub(super) struct TopicNames {
    /// Content-routed topics counted against the cap: found on the broker or admitted since.
    pub(super) counted: HashSet<String>,
    /// Topics known to exist (the broker's at connect, the fixed ones, every one opened since),
    /// which are never un-counted.
    pub(super) existing: HashSet<String>,
    /// [`collision_key`] -> the spelling that holds it: every existing and counted topic.
    pub(super) keys: HashMap<String, String>,
    /// Names the broker refused to create for good, so later rows that render them go to the
    /// fallback without another create attempt. Bounded by [`MAX_REFUSED_TOPICS`] (oldest dropped
    /// first) and cleared by a restart, which picks up a pre-created topic or a new ACL.
    pub(super) refused: HashSet<String>,
    pub(super) refused_order: VecDeque<String>,
}

/// How many refused topic names the plan remembers.
pub(super) const MAX_REFUSED_TOPICS: usize = 10_000;

/// The topics content routing created or found, counted against `sink.max_dynamic_topics`, and
/// every topic name known to exist, to refuse a new name Kafka would reject. Shared by every
/// clone of a plan; the sink seeds it from the broker at connect.
#[derive(Debug)]
pub(super) struct DynamicTopics {
    pub(super) cap: AtomicUsize,
    pub(super) names: RwLock<TopicNames>,
    pub(super) warned: AtomicBool,
}

impl DynamicTopics {
    pub(super) fn new() -> Self {
        Self {
            cap: AtomicUsize::new(DEFAULT_MAX_DYNAMIC_TOPICS),
            names: RwLock::new(TopicNames::default()),
            warned: AtomicBool::new(false),
        }
    }

    pub(super) fn write(&self) -> std::sync::RwLockWriteGuard<'_, TopicNames> {
        self.names.write().unwrap_or_else(|e| e.into_inner())
    }

    pub(super) fn len(&self) -> usize {
        self.names
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .counted
            .len()
    }

    /// Notes topics that exist; `counted` ones also count against the cap and stay usable,
    /// even above it.
    pub(super) fn exists<'a>(&self, topics: impl IntoIterator<Item = (&'a str, bool)>) {
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
    pub(super) fn admit(&self, topic: &str) -> Admit {
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
    pub(super) fn release(&self, topic: &str) {
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
    pub(super) fn refuse(&self, topic: &str) {
        self.release(topic);
        let mut w = self.write();
        if w.existing.contains(topic) || !w.refused.insert(topic.to_string()) {
            return;
        }
        w.refused_order.push_back(topic.to_string());
        if w.refused_order.len() > MAX_REFUSED_TOPICS
            && let Some(oldest) = w.refused_order.pop_front()
        {
            w.refused.remove(&oldest);
        }
    }
}

/// `sink.max_dynamic_topics` when unset.
pub const DEFAULT_MAX_DYNAMIC_TOPICS: usize = 100;

/// A Kafka topic name: 1 to 249 of `[A-Za-z0-9._-]`, and not `.` or `..`.
pub(super) fn valid_topic_name(t: &str) -> bool {
    !t.is_empty()
        && t.len() <= 249
        && t != "."
        && t != ".."
        && t.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Parses a topic or topic template. Fixed text must be valid in a Kafka topic name (Kafka would
/// refuse anything else on the first send). The leading-`_` rule is checked by the plan, since it
/// only applies when content routing is in use.
pub(super) fn parse_topic(what: &str, raw: &str) -> Result<TopicTemplate> {
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
