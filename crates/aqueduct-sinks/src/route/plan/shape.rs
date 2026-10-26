//! How a route shapes a record: `format`, `deletes`, `pass_on`, and the key/value rows after
//! field operations.

use super::*;

/// A compiled `otherwise`.
#[derive(Debug, Clone)]
pub(super) enum PassOn {
    /// The index of the later route.
    Route(usize),
    /// A fixed topic name.
    Topic(String),
}

/// The record value: `format`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Format {
    /// The change event envelope (`before`, `after`, `source`, `op`, ...).
    #[default]
    Envelope,
    /// The row alone: `after`, or for a delete what `deletes` says.
    Row,
}

/// What a delete becomes under `format = "row"`: `deletes`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Deletes {
    /// A null value with the row's key.
    #[default]
    Tombstone,
    /// No record; counted as filtered.
    Drop,
    /// The before-image with `"__deleted": true`.
    Row,
}

impl Deletes {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Tombstone => "tombstone",
            Self::Drop => "drop",
            Self::Row => "row",
        }
    }
}

/// How a route shapes its records (ADR 0002 §5, step 5 and 6 of the fixed order).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Shape {
    pub(super) format: Format,
    pub(super) deletes: Deletes,
    /// Envelope format: a tombstone after every delete.
    pub(super) tombstones: bool,
    /// `unchanged = "dead_letter"`: a row with columns the source left out goes to the
    /// dead-letter topic.
    pub(super) unchanged_dead_letter: bool,
}

impl Shape {
    /// True when the route produces no record for `event` because of its shape: under
    /// `format = "row"`, a truncate (no row), a delete with `deletes = "drop"`, or a change without
    /// the row: no `after`, or an `after` that is only the key (`ChangeEvent::after_is_key`, a
    /// MongoDB update whose document was deleted before the lookup; its delete follows).
    pub(super) fn drops(&self, event: &ChangeEvent) -> bool {
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
    pub(super) fn null_value_on_delete(&self) -> bool {
        self.tombstones || (self.format == Format::Row && self.deletes == Deletes::Tombstone)
    }
}

/// A column name for the `aqueduct-error` header and the log: control characters and backticks
/// (which quote the name) become `?`, so a hostile name cannot forge lines or quoting.
pub(super) fn safe_name(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_control() || c == '`' { '?' } else { c })
        .collect()
}

/// The marker a `deletes = "row"` value ends with.
pub(super) const DELETED_MARK: &str = r#""__deleted":true"#;

/// Matches a string one of `names`, ignoring ASCII case, and says what was expected otherwise.
pub(super) fn parse_choice<T: Copy>(
    name: &str,
    key: &str,
    value: &str,
    names: &[(&str, T)],
) -> Result<T> {
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
pub(super) fn parse_shape(name: &str, r: &KafkaRoute) -> Result<Shape> {
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
            )));
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
pub(super) fn deleted_row(before: Option<&RawValue>) -> String {
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
pub(super) struct Members<'a>(pub(super) Vec<(String, &'a RawValue)>);

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
pub(super) type Rows = (Option<Box<RawValue>>, Option<Box<RawValue>>);

/// A record's parts after the route's field operations, computed before its topic is chosen.
pub(super) struct Rewritten {
    /// The record key from the rewritten key object; `None` with `key = [...]`.
    pub(super) key: Option<String>,
    pub(super) rows: Rows,
    pub(super) headers: Vec<(&'static str, String)>,
}
