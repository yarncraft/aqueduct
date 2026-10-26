//! The transactional outbox (`outbox = true`): one record per outbox insert, its topic and
//! key from the row.

use super::*;

/// A compiled `outbox = {...}`: the spec as configured and the `after.<col>` paths it reads.
#[derive(Debug, Clone)]
pub(super) struct Outbox {
    pub(super) spec: OutboxSpec,
    pub(super) aggregate_id: FieldRef,
    pub(super) payload: FieldRef,
    pub(super) id: Option<FieldRef>,
    pub(super) kind: Option<FieldRef>,
}

/// Why an outbox column cannot be used for one row; never holds the value.
pub(super) enum OutboxError {
    /// Missing or `null`.
    Missing,
    /// A string with an escape that is not Unicode (a lone surrogate), so it has no UTF-8 form.
    NotUnicode,
}

impl Outbox {
    /// Validates the column names (non-empty, top-level) and the topic template, and compiles
    /// the template with `{aggregate_type}` read from `after.<aggregate_type>`.
    pub(super) fn compile(name: &str, spec: &OutboxSpec) -> Result<(Self, TopicTemplate)> {
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
    pub(super) fn fields(&self) -> impl Iterator<Item = &FieldRef> {
        [&self.aggregate_id, &self.payload]
            .into_iter()
            .chain(self.id.as_ref())
            .chain(self.kind.as_ref())
    }
}

/// The outbox topic placeholder for the `aggregate_type` column.
pub(super) const AGGREGATE_TYPE: &str = "{aggregate_type}";

/// Maps byte `pos` in a template whose [`AGGREGATE_TYPE`] placeholders were each replaced by
/// `replacement_len` bytes back to `written`, the template as configured. A position inside a
/// replacement maps to the start of its placeholder.
pub(super) fn written_pos(written: &str, replacement_len: usize, pos: usize) -> usize {
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
pub(super) fn outbox_text(
    view: &EventView<'_>,
    f: &FieldRef,
) -> std::result::Result<String, OutboxError> {
    let raw = view.raw(f).ok_or(OutboxError::Missing)?;
    let text = raw.get().trim();
    match text.as_bytes().first() {
        None | Some(b'n') => Err(OutboxError::Missing),
        Some(b'"') => serde_json::from_str::<String>(text).map_err(|_| OutboxError::NotUnicode),
        Some(_) => Ok(text.to_string()),
    }
}

impl RoutePlan {
    /// The record for an insert into an outbox table (ADR 0002 §5): topic from the outbox's
    /// template, key = the `aggregate_id` value, value = the `payload` value, headers `id` and
    /// `type` when configured and present. A row without an `aggregate_id` or `payload` goes to
    /// the dead-letter topic, with an `aqueduct-error` header naming the column, never the value.
    pub(super) fn render_outbox(
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
            self.stats
                .claimed(Outcome::Errors, route, op_code(event.op));
            return Ok(());
        }
        let payload = texts.pop().unwrap_or_default();
        let key = texts.pop().unwrap_or_default();
        let (topic, content, fell_back) = self.route_topic(route, r, view);
        out.push(OutRecord {
            topic,
            partition_key: PartitionKey::Key,
            key,
            value: Some(payload),
            op: op_code(event.op),
            lsn: event.position.to_checkpoint(),
            commit_ts_micros: event.commit_ts_micros,
            headers,
            content,
        });
        if fell_back {
            self.stats
                .claimed(Outcome::Fallback, route, op_code(event.op));
        } else {
            self.stats.produced(route, op_code(event.op));
        }
        Ok(())
    }
}
