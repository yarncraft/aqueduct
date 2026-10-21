//! Field operations (ADR 0002 §5 "Fields"): `include` | `exclude`, `mask`, `hash` and `rename`,
//! applied to the top-level columns of `before`, `after` and the record key's `key` object.
//!
//! A row is rewritten in one pass over its top-level JSON object, straight into a `String`: no
//! `serde_json::Value` tree is built and kept values are copied as written. Nested MongoDB fields
//! are not addressable (a dotted name is a configuration error); a whole nested value can still be
//! dropped, masked, hashed or renamed through its top-level column.
//!
//! - **mask:** a string becomes `"***"`, any other non-null value `null`, `null` stays `null`.
//! - **hash:** HMAC-SHA256 (`ring::hmac`) as lowercase hex over the value's canonical text: the
//!   unescaped content of a string, a number or bool as written, the compact JSON of an object or
//!   array. `null` stays `null`. The key comes from the environment or a file, never the TOML, and
//!   is redacted in `Debug`.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Write as _};

use aqueduct_core::{Error, Result};
use ring::hmac;
use serde::de::{self, Deserializer, MapAccess, Visitor};
use serde::Deserialize;
use serde_json::value::RawValue;

/// Shortest accepted hash key, in bytes (HMAC-SHA256 wants at least the digest size).
pub const MIN_HASH_KEY_BYTES: usize = 32;
/// Longest accepted `hash_key_id`.
const MAX_HASH_KEY_ID: usize = 64;
/// The header that names the hash key on every record of a hashing route.
pub const HASH_KEY_ID_HEADER: &str = "aqueduct-hash-key-id";

/// The HMAC key for `hash`, with the short label consumers join by (`aqueduct-hash-key-id`). The
/// key bytes never appear in `Debug` output or logs.
#[derive(Clone)]
pub struct HashKey {
    key: hmac::Key,
    id: String,
}

impl HashKey {
    /// `secret` must be at least [`MIN_HASH_KEY_BYTES`] bytes. `id` is 1 to 64 of
    /// `[A-Za-z0-9._:-]`. Errors never include the secret.
    pub fn new(secret: &[u8], id: &str) -> Result<Self> {
        if secret.len() < MIN_HASH_KEY_BYTES {
            return Err(Error::Config(format!(
                "the hash key (AQUEDUCT_HASH_KEY or sink.hash_key_file) is shorter than {MIN_HASH_KEY_BYTES} bytes; use a random secret, e.g. `openssl rand -hex 32`"
            )));
        }
        if id.is_empty()
            || id.len() > MAX_HASH_KEY_ID
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
        {
            return Err(Error::Config(format!(
                "sink.hash_key_id must be 1 to {MAX_HASH_KEY_ID} of A-Z a-z 0-9 . _ : -"
            )));
        }
        Ok(Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, secret),
            id: id.to_string(),
        })
    }

    /// The label sent as `aqueduct-hash-key-id`.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Appends the lowercase hex HMAC-SHA256 of `data` to `out`.
    fn digest_hex(&self, data: &[u8], out: &mut String) {
        for b in hmac::sign(&self.key, data).as_ref() {
            let _ = write!(out, "{b:02x}");
        }
    }
}

impl fmt::Debug for HashKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashKey")
            .field("id", &self.id)
            .field("key", &"<redacted>")
            .finish()
    }
}

/// The field operations of one route, as configured (for validation and `/api/routes`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldSpec {
    pub include: Option<Vec<String>>,
    pub exclude: Option<Vec<String>>,
    pub mask: Vec<String>,
    pub hash: Vec<String>,
    pub rename: BTreeMap<String, String>,
}

impl FieldSpec {
    /// True when the route has no field operation, so its records take the unchanged fast path.
    pub fn is_empty(&self) -> bool {
        self.include.is_none()
            && self.exclude.as_ref().is_none_or(Vec::is_empty)
            && self.mask.is_empty()
            && self.hash.is_empty()
            && self.rename.is_empty()
    }

    /// Every column the operations name, for the unseen-field warning.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.include
            .iter()
            .chain(self.exclude.iter())
            .flatten()
            .chain(self.mask.iter())
            .chain(self.hash.iter())
            .chain(self.rename.keys())
            .map(String::as_str)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ValueOp {
    #[default]
    Keep,
    Mask,
    Hash,
}

/// What happens to one column name.
#[derive(Debug, Clone, Default)]
struct Rule {
    drop: bool,
    value: ValueOp,
    rename: Option<String>,
    /// Another column is renamed to this name: a row that also has this column, kept, collides.
    target: bool,
}

/// A rewrite that could not be done. The text names configured columns only, never row values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FieldError {
    /// A row already has a kept column named like a rename target.
    RenameCollision { from: String, to: String },
    /// The row (or key) is not a JSON object.
    NotAnObject,
    /// A string to hash is not valid Unicode (a lone surrogate escape such as `\ud800`).
    Unhashable { column: String },
}

impl fmt::Display for FieldError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldError::RenameCollision { from, to } => write!(
                f,
                "rename `{from}` to `{to}`: the row already has a column `{to}`"
            ),
            FieldError::NotAnObject => {
                f.write_str("field operations: the row is not a JSON object")
            }
            FieldError::Unhashable { column } => write!(
                f,
                "hash `{column}`: the value is a string that is not valid Unicode"
            ),
        }
    }
}

/// Compiled field operations: one hash-map lookup per column.
#[derive(Debug, Clone)]
pub(crate) struct FieldOps {
    spec: FieldSpec,
    rules: HashMap<String, Rule>,
    /// With `include`, a column without a rule is dropped.
    include_only: bool,
    /// Some column is renamed, and the output names are not all known at startup (no `include`),
    /// so a row can collide with a rename target.
    may_collide: bool,
    hashes: bool,
}

fn bad(route: &str, msg: impl fmt::Display) -> Error {
    Error::Config(format!("sink route `{route}`: {msg}"))
}

fn check_name(route: &str, key: &str, name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(bad(route, format!("`{key}` has an empty column name")));
    }
    if name.contains('.') {
        return Err(bad(
            route,
            format!(
                "`{key}` names `{name}`: field operations take top-level column names only, not dotted paths"
            ),
        ));
    }
    Ok(())
}

impl FieldOps {
    /// Validates and compiles `spec`; `None` when it has no operation (the fast path).
    pub(crate) fn compile(route: &str, spec: &FieldSpec) -> Result<Option<Self>> {
        if spec.is_empty() {
            return Ok(None);
        }
        if spec.include.is_some() && spec.exclude.as_ref().is_some_and(|e| !e.is_empty()) {
            return Err(bad(
                route,
                "`include` and `exclude` cannot both be set; list the columns to keep or the ones to drop",
            ));
        }
        if spec.include.as_ref().is_some_and(Vec::is_empty) {
            return Err(bad(
                route,
                "`include` is empty, so every column would be dropped; list the columns to keep",
            ));
        }
        for (key, names) in [
            ("include", spec.include.as_deref().unwrap_or_default()),
            ("exclude", spec.exclude.as_deref().unwrap_or_default()),
            ("mask", &spec.mask[..]),
            ("hash", &spec.hash[..]),
        ] {
            for n in names {
                check_name(route, key, n)?;
            }
        }
        for (from, to) in &spec.rename {
            check_name(route, "rename", from)?;
            check_name(route, "rename", to)?;
        }
        if let Some(c) = spec.mask.iter().find(|c| spec.hash.contains(c)) {
            return Err(bad(
                route,
                format!("`{c}` is both masked and hashed; pick one"),
            ));
        }
        let rewritten = spec
            .mask
            .iter()
            .map(|c| ("mask", c))
            .chain(spec.hash.iter().map(|c| ("hash", c)))
            .chain(spec.rename.keys().map(|c| ("rename", c)));
        for (key, c) in rewritten {
            if spec.exclude.as_ref().is_some_and(|e| e.contains(c)) {
                return Err(bad(
                    route,
                    format!("`{c}` is in `exclude` and in `{key}`; an excluded column is dropped, so `{key}` has no effect"),
                ));
            }
            if spec.include.as_ref().is_some_and(|i| !i.contains(c)) {
                return Err(bad(
                    route,
                    format!("`{c}` is in `{key}` but not in `include`, so it is dropped and `{key}` has no effect"),
                ));
            }
        }
        let mut targets: Vec<&str> = Vec::with_capacity(spec.rename.len());
        for (from, to) in &spec.rename {
            if from == to {
                return Err(bad(route, format!("`rename` maps `{from}` to itself")));
            }
            if spec.rename.contains_key(to) {
                return Err(bad(
                    route,
                    format!("`rename` target `{to}` is also a renamed column; chains and swaps are not supported"),
                ));
            }
            if targets.contains(&to.as_str()) {
                return Err(bad(route, format!("two columns are renamed to `{to}`")));
            }
            if spec.include.as_ref().is_some_and(|i| i.contains(to)) {
                return Err(bad(
                    route,
                    format!("`rename` target `{to}` is also an included column"),
                ));
            }
            // Field operations name source columns. Listing the new name would look protected
            // while the renamed value passes through as is.
            for (key, list) in [
                ("mask", &spec.mask[..]),
                ("hash", &spec.hash[..]),
                ("exclude", spec.exclude.as_deref().unwrap_or_default()),
            ] {
                if list.contains(to) {
                    return Err(bad(
                        route,
                        format!(
                            "`rename` target `{to}` is in `{key}`, but field operations apply to the original column names, so `{from}` would pass through unprotected; put `{from}` in `{key}` instead"
                        ),
                    ));
                }
            }
            targets.push(to);
        }

        let mut rules: HashMap<String, Rule> = HashMap::new();
        let include_only = spec.include.is_some();
        for c in spec.include.iter().flatten() {
            rules.entry(c.clone()).or_default();
        }
        for c in spec.exclude.iter().flatten() {
            rules.entry(c.clone()).or_default().drop = true;
        }
        for c in &spec.mask {
            rules.entry(c.clone()).or_default().value = ValueOp::Mask;
        }
        for c in &spec.hash {
            rules.entry(c.clone()).or_default().value = ValueOp::Hash;
        }
        for (from, to) in &spec.rename {
            rules.entry(from.clone()).or_default().rename = Some(to.clone());
        }
        // With `include`, a target is not an included column (checked above), so a row column of
        // that name is dropped and cannot collide. Without it, any row may have such a column.
        if !include_only {
            for to in spec.rename.values() {
                rules.entry(to.clone()).or_default().target = true;
            }
        }
        Ok(Some(Self {
            may_collide: !include_only && !spec.rename.is_empty(),
            hashes: !spec.hash.is_empty(),
            include_only,
            rules,
            spec: spec.clone(),
        }))
    }

    pub(crate) fn spec(&self) -> &FieldSpec {
        &self.spec
    }

    /// The route hashes a column, so it needs the key and its records carry the key id header.
    pub(crate) fn hashes(&self) -> bool {
        self.hashes
    }

    /// A row can collide with a rename target at runtime (so the route needs a dead-letter topic).
    pub(crate) fn may_collide(&self) -> bool {
        self.may_collide
    }

    /// How the operations protect `column`, if they do: `excluded`, `not included`, `masked` or
    /// `hashed`. Such a column may not reach the record key, a topic or a header unchanged (ADR
    /// 0002 §5 "No row data may bypass the field operations"). A renamed column is not protected.
    pub(crate) fn protection(&self, column: &str) -> Option<&'static str> {
        match self.rules.get(column) {
            Some(r) if r.drop => Some("excluded"),
            Some(r) => match r.value {
                ValueOp::Mask => Some("masked"),
                ValueOp::Hash => Some("hashed"),
                ValueOp::Keep => None,
            },
            None => self.include_only.then_some("not included"),
        }
    }

    /// Rewrites one JSON object into `out`. Returns how many columns were written. `renames`
    /// false keeps the original names (for the dead-letter value of a row whose rename collided).
    pub(crate) fn rewrite(
        &self,
        raw: &str,
        key: Option<&HashKey>,
        renames: bool,
        out: &mut String,
    ) -> std::result::Result<usize, FieldError> {
        let mut error = None;
        let visitor = Rewriter {
            ops: self,
            key,
            renames,
            out,
            error: &mut error,
        };
        let mut de = serde_json::Deserializer::from_str(raw);
        match de
            .deserialize_map(visitor)
            .and_then(|n| de.end().map(|()| n))
        {
            Ok(n) => Ok(n),
            Err(_) => Err(error.unwrap_or(FieldError::NotAnObject)),
        }
    }

    /// [`rewrite`](Self::rewrite) into a new `RawValue`.
    pub(crate) fn rewrite_raw(
        &self,
        raw: &RawValue,
        key: Option<&HashKey>,
        renames: bool,
    ) -> std::result::Result<Box<RawValue>, FieldError> {
        let mut out = String::with_capacity(raw.get().len());
        self.rewrite(raw.get(), key, renames, &mut out)?;
        RawValue::from_string(out).map_err(|_| FieldError::NotAnObject)
    }
}

/// A column name, borrowed when it has no escapes.
#[derive(Deserialize)]
struct Name<'a>(#[serde(borrow)] Cow<'a, str>);

/// Writes the rewritten object while serde walks the input's top-level map.
struct Rewriter<'o, 'k, 's> {
    ops: &'o FieldOps,
    key: Option<&'k HashKey>,
    renames: bool,
    out: &'s mut String,
    error: &'s mut Option<FieldError>,
}

impl<'de> Visitor<'de> for Rewriter<'_, '_, '_> {
    type Value = usize;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<usize, A::Error> {
        let out = self.out;
        out.push('{');
        let mut n = 0usize;
        while let Some(Name(name)) = map.next_key::<Name<'de>>()? {
            let value: &'de RawValue = map.next_value()?;
            let rule = self.ops.rules.get(name.as_ref());
            let (drop, op, rename, target) = match rule {
                Some(r) => (r.drop, r.value, r.rename.as_deref(), r.target),
                None => (self.ops.include_only, ValueOp::Keep, None, false),
            };
            if drop {
                continue;
            }
            if target && self.renames {
                let from = self
                    .ops
                    .spec
                    .rename
                    .iter()
                    .find(|(_, to)| *to == name.as_ref())
                    .map(|(from, _)| from.clone())
                    .unwrap_or_default();
                *self.error = Some(FieldError::RenameCollision {
                    from,
                    to: name.into_owned(),
                });
                return Err(de::Error::custom("rename collision"));
            }
            if n > 0 {
                out.push(',');
            }
            n += 1;
            push_json_str(
                out,
                match rename {
                    Some(to) if self.renames => to,
                    _ => name.as_ref(),
                },
            );
            out.push(':');
            let text = value.get();
            match op {
                ValueOp::Keep => out.push_str(text),
                ValueOp::Mask => out.push_str(mask(text)),
                ValueOp::Hash => match (canonical(text), self.key) {
                    (Err(()), _) => {
                        *self.error = Some(FieldError::Unhashable {
                            column: name.into_owned(),
                        });
                        return Err(de::Error::custom("unhashable string"));
                    }
                    (Ok(None), _) => out.push_str("null"),
                    (Ok(Some(c)), Some(key)) => {
                        out.push('"');
                        key.digest_hex(c.as_bytes(), out);
                        out.push('"');
                    }
                    // The plan refuses to compile a hashing route without a key; never emit
                    // the value unhashed.
                    (Ok(Some(_)), None) => out.push_str("null"),
                },
            }
        }
        out.push('}');
        Ok(n)
    }
}

/// The masked form of a JSON value: `"***"` for a string, `null` for anything else.
fn mask(text: &str) -> &'static str {
    if text.trim_start().starts_with('"') {
        "\"***\""
    } else {
        "null"
    }
}

/// The text a value is hashed from; `None` for `null`. A string's unescaped content, a number or
/// bool as written, an object's or array's compact JSON (whitespace outside strings removed, key
/// order and escapes as written). `Err` for a string that does not decode to valid Unicode (a lone
/// surrogate escape), which must not silently become `null`.
fn canonical(text: &str) -> std::result::Result<Option<Cow<'_, str>>, ()> {
    let t = text.trim();
    Ok(match t.as_bytes().first() {
        None | Some(b'n') => None,
        Some(b'"') => Some(serde_json::from_str::<Name>(t).map_err(|_| ())?.0),
        Some(b'{' | b'[') => Some(compact(t)),
        Some(_) => Some(Cow::Borrowed(t)),
    })
}

/// `text` (valid JSON) without whitespace outside strings.
fn compact(text: &str) -> Cow<'_, str> {
    let ws = |b: u8| matches!(b, b' ' | b'\t' | b'\n' | b'\r');
    let mut in_str = false;
    let mut escaped = false;
    let needs = text.bytes().any(|b| {
        if in_str {
            match (escaped, b) {
                (true, _) => escaped = false,
                (false, b'\\') => escaped = true,
                (false, b'"') => in_str = false,
                _ => {}
            }
            false
        } else if b == b'"' {
            in_str = true;
            false
        } else {
            ws(b)
        }
    });
    if !needs {
        return Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len());
    let (mut in_str, mut escaped) = (false, false);
    for c in text.chars() {
        if in_str {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_str = false,
                _ => {}
            }
            out.push(c);
        } else if c == '"' {
            in_str = true;
            out.push(c);
        } else if !(c.is_ascii() && ws(c as u8)) {
            out.push(c);
        }
    }
    Cow::Owned(out)
}

/// Appends `s` as a JSON string.
fn push_json_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

    fn key() -> HashKey {
        HashKey::new(SECRET, "k1").unwrap()
    }

    fn hex(data: &[u8]) -> String {
        let mut s = String::new();
        key().digest_hex(data, &mut s);
        s
    }

    fn spec(
        include: Option<&[&str]>,
        exclude: Option<&[&str]>,
        mask: &[&str],
        hash: &[&str],
        rename: &[(&str, &str)],
    ) -> FieldSpec {
        let v = |s: &[&str]| s.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        FieldSpec {
            include: include.map(v),
            exclude: exclude.map(v),
            mask: v(mask),
            hash: v(hash),
            rename: rename
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
        }
    }

    fn run(s: &FieldSpec, row: &str) -> std::result::Result<String, FieldError> {
        let ops = FieldOps::compile("r", s).unwrap().unwrap();
        let mut out = String::new();
        ops.rewrite(row, Some(&key()), true, &mut out)?;
        Ok(out)
    }

    #[test]
    fn hmac_matches_the_rfc_4231_vector_shape_and_is_lowercase_hex() {
        // RFC 4231 test case 2 ("Jefe"), checked through ring directly; our wrapper only hex-encodes.
        let k = hmac::Key::new(hmac::HMAC_SHA256, b"Jefe");
        let tag = hmac::sign(&k, b"what do ya want for nothing?");
        let h: String = tag.as_ref().iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            h,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        let d = hex(b"x");
        assert_eq!(d.len(), 64);
        assert!(d
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
    }

    #[test]
    fn ops_table_driven() {
        let h = |s: &str| format!("\"{}\"", hex(s.as_bytes()));
        let cases: Vec<(FieldSpec, &str, String)> = vec![
            // exclude drops, order kept
            (
                spec(None, Some(&["b"]), &[], &[], &[]),
                r#"{"a":1,"b":2,"c":3}"#,
                r#"{"a":1,"c":3}"#.into(),
            ),
            // include keeps only the listed columns, in row order
            (
                spec(Some(&["c", "a"]), None, &[], &[], &[]),
                r#"{"a":1,"b":2,"c":3}"#,
                r#"{"a":1,"c":3}"#.into(),
            ),
            // include of columns the row lacks: empty object
            (
                spec(Some(&["z"]), None, &[], &[], &[]),
                r#"{"a":1}"#,
                "{}".into(),
            ),
            // mask type rules: string, number, bool, object, array, null
            (
                spec(None, None, &["s", "n", "b", "o", "a", "z"], &[], &[]),
                r#"{"s":"secret","n":42,"b":true,"o":{"x":1},"a":[1],"z":null}"#,
                r#"{"s":"***","n":null,"b":null,"o":null,"a":null,"z":null}"#.into(),
            ),
            // hash: string content unescaped, number as written, bool, null kept
            (
                spec(None, None, &[], &["s", "n", "f", "b", "z"], &[]),
                r#"{"s":"a\"bé","n":42,"f":42.0,"b":false,"z":null}"#,
                format!(
                    r#"{{"s":{},"n":{},"f":{},"b":{},"z":null}}"#,
                    h("a\"b\u{e9}"),
                    h("42"),
                    h("42.0"),
                    h("false")
                ),
            ),
            // hash a MongoDB nested value: compact JSON, key order and escapes kept
            (
                spec(None, None, &[], &["addr", "tags", "_id"], &[]),
                r#"{"_id":{"$oid":"6530"},"addr":{ "city" : "Gent", "zip":"9000 x" },"tags":[ "a", 1 ]}"#,
                format!(
                    r#"{{"_id":{},"addr":{},"tags":{}}}"#,
                    h(r#"{"$oid":"6530"}"#),
                    h(r#"{"city":"Gent","zip":"9000 x"}"#),
                    h(r#"["a",1]"#)
                ),
            ),
            // rename, with an escaped target name
            (
                spec(None, None, &[], &[], &[("cust_id", "customer \"id\"")]),
                r#"{"cust_id":7,"n":1}"#,
                r#"{"customer \"id\"":7,"n":1}"#.into(),
            ),
            // all together, with include: masked and hashed and renamed included columns
            (
                spec(
                    Some(&["id", "email", "card", "cust"]),
                    None,
                    &["card"],
                    &["email"],
                    &[("cust", "customer")],
                ),
                r#"{"id":1,"email":"a@b.c","card":"4111","cust":9,"note":"x"}"#,
                format!(
                    r#"{{"id":1,"email":{},"card":"***","customer":9}}"#,
                    h("a@b.c")
                ),
            ),
            // escaped column names in the input are matched unescaped
            (
                spec(None, Some(&["a\"b"]), &[], &[], &[]),
                r#"{"a\"b":1,"c\n":2}"#,
                r#"{"c\n":2}"#.into(),
            ),
        ];
        for (s, row, want) in cases {
            let got = run(&s, row).unwrap();
            assert_eq!(got, want, "{s:?} {row}");
            serde_json::from_str::<serde_json::Value>(&got).expect("valid JSON");
        }
    }

    #[test]
    fn postgres_and_mongodb_ints_hash_alike_but_doubles_do_not() {
        let s = spec(None, None, &[], &["n"], &[]);
        // Postgres int4 42 and MongoDB Int32/Int64 42 (relaxed extended JSON) are both `42`.
        let pg = run(&s, r#"{"n":42}"#).unwrap();
        let mongo = run(&s, r#"{"n": 42}"#).unwrap();
        assert_eq!(pg, mongo);
        // A MongoDB double is `42.0`, a different digest.
        assert_ne!(pg, run(&s, r#"{"n":42.0}"#).unwrap());
        // A string "42" hashes its content, so it matches the number 42.
        assert_eq!(pg, run(&s, r#"{"n":"42"}"#).unwrap());
    }

    #[test]
    fn rename_collision_is_a_row_error_and_can_be_skipped() {
        let s = spec(None, None, &[], &[], &[("a", "b")]);
        assert_eq!(
            run(&s, r#"{"a":1,"b":2}"#),
            Err(FieldError::RenameCollision {
                from: "a".into(),
                to: "b".into()
            })
        );
        // A target listed in `exclude` (or `mask`, `hash`) is rejected at startup: the operations
        // name source columns, so `a`'s value would pass through as `b` (see validation test).
        // Without renames (the dead-letter value), nothing collides.
        let ops = FieldOps::compile("r", &s).unwrap().unwrap();
        let mut out = String::new();
        ops.rewrite(r#"{"a":1,"b":2}"#, None, false, &mut out)
            .unwrap();
        assert_eq!(out, r#"{"a":1,"b":2}"#);
        assert!(ops.may_collide());
        let inc = spec(Some(&["a"]), None, &[], &[], &[("a", "b")]);
        assert!(!FieldOps::compile("r", &inc).unwrap().unwrap().may_collide());
        assert_eq!(run(&inc, r#"{"a":1,"b":2}"#).unwrap(), r#"{"b":1}"#);
        // The error text names columns, never values.
        let e = run(&s, r#"{"a":"secret","b":"other"}"#)
            .unwrap_err()
            .to_string();
        assert!(!e.contains("secret") && !e.contains("other"), "{e}");
    }

    #[test]
    fn a_string_that_is_not_valid_unicode_is_a_row_error_not_null() {
        let s = spec(None, None, &[], &["s"], &[]);
        let e = run(&s, r#"{"s":"\ud800"}"#).unwrap_err();
        assert_eq!(e, FieldError::Unhashable { column: "s".into() });
        assert_eq!(
            e.to_string(),
            "hash `s`: the value is a string that is not valid Unicode"
        );
        // A valid surrogate pair hashes like the character it encodes.
        assert_eq!(
            run(&s, r#"{"s":"\ud83d\ude00"}"#).unwrap(),
            run(&s, "{\"s\":\"\u{1f600}\"}").unwrap()
        );
        // Masking does not decode the string, so it still masks.
        let m = spec(None, None, &["s"], &[], &[]);
        assert_eq!(run(&m, r#"{"s":"\ud800"}"#).unwrap(), r#"{"s":"***"}"#);
    }

    #[test]
    fn non_objects_are_errors() {
        let s = spec(None, Some(&["a"]), &[], &[], &[]);
        for bad in ["[1]", "42", "\"x\"", "null", "{"] {
            assert_eq!(run(&s, bad), Err(FieldError::NotAnObject), "{bad}");
        }
    }

    #[test]
    fn validation_rejects_bad_specs() {
        let err = |s: FieldSpec| FieldOps::compile("r", &s).unwrap_err().to_string();
        let cases: Vec<(FieldSpec, &str)> = vec![
            (
                spec(Some(&["a"]), Some(&["b"]), &[], &[], &[]),
                "cannot both be set",
            ),
            (spec(Some(&[]), None, &[], &[], &[]), "`include` is empty"),
            // A rename target in mask, hash or exclude would leave the renamed value unprotected.
            (
                spec(None, None, &[], &["x"], &[("email", "x")]),
                "`rename` target `x` is in `hash`, but field operations apply to the original column names, so `email` would pass through unprotected; put `email` in `hash` instead",
            ),
            (
                spec(None, None, &["x"], &[], &[("email", "x")]),
                "`rename` target `x` is in `mask`",
            ),
            (
                spec(None, Some(&["x"]), &[], &[], &[("email", "x")]),
                "`rename` target `x` is in `exclude`",
            ),
            (
                spec(None, Some(&["a.b"]), &[], &[], &[]),
                "top-level column names only",
            ),
            (
                spec(None, None, &["addr.city"], &[], &[]),
                "not dotted paths",
            ),
            (spec(None, None, &[], &["x.y"], &[]), "not dotted paths"),
            (
                spec(None, None, &[], &[], &[("a.b", "c")]),
                "not dotted paths",
            ),
            (
                spec(None, None, &[], &[], &[("a", "c.d")]),
                "not dotted paths",
            ),
            (spec(None, None, &[""], &[], &[]), "empty column name"),
            (
                spec(None, None, &["a"], &["a"], &[]),
                "both masked and hashed",
            ),
            (spec(None, Some(&["a"]), &["a"], &[], &[]), "has no effect"),
            (
                spec(None, Some(&["a"]), &[], &[], &[("a", "b")]),
                "has no effect",
            ),
            (
                spec(Some(&["b"]), None, &[], &["a"], &[]),
                "not in `include`",
            ),
            (spec(None, None, &[], &[], &[("a", "a")]), "to itself"),
            (
                spec(None, None, &[], &[], &[("a", "b"), ("b", "c")]),
                "also a renamed column",
            ),
            (
                spec(None, None, &[], &[], &[("a", "x"), ("b", "x")]),
                "two columns are renamed to `x`",
            ),
            (
                spec(Some(&["a", "x"]), None, &[], &[], &[("a", "x")]),
                "also an included column",
            ),
        ];
        for (s, want) in cases {
            let e = err(s.clone());
            assert!(e.contains(want), "{s:?}: {e}");
            assert!(e.contains("sink route `r`"), "{e}");
        }
        // No operation: the fast path.
        assert!(FieldOps::compile("r", &FieldSpec::default())
            .unwrap()
            .is_none());
        assert!(
            FieldOps::compile("r", &spec(None, Some(&[]), &[], &[], &[]))
                .unwrap()
                .is_none()
        );
        // An empty `exclude` is no exclusion, also next to `include`.
        let both = spec(Some(&["a"]), Some(&[]), &[], &[], &[]);
        assert_eq!(run(&both, r#"{"a":1,"b":2}"#).unwrap(), r#"{"a":1}"#);
    }

    #[test]
    fn protection_reports_dropped_masked_and_hashed_columns() {
        let s = spec(None, Some(&["x"]), &["m"], &["h"], &[("r", "s")]);
        let ops = FieldOps::compile("r", &s).unwrap().unwrap();
        for (c, want) in [
            ("x", Some("excluded")),
            ("m", Some("masked")),
            ("h", Some("hashed")),
            ("r", None),
            ("id", None),
        ] {
            assert_eq!(ops.protection(c), want, "{c}");
        }
        let inc = FieldOps::compile("r", &spec(Some(&["id"]), None, &[], &[], &[]))
            .unwrap()
            .unwrap();
        assert_eq!(inc.protection("id"), None);
        assert_eq!(inc.protection("email"), Some("not included"));
    }

    #[test]
    fn hash_key_rules_and_debug_redaction() {
        assert!(HashKey::new(&SECRET[..31], "k").is_err());
        let e = HashKey::new(b"too-short-secret", "k")
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("shorter than 32 bytes") && !e.contains("too-short-secret"),
            "{e}"
        );
        for id in ["", "has space", &"x".repeat(65)] {
            assert!(HashKey::new(SECRET, id).is_err(), "{id:?}");
        }
        let k = HashKey::new(SECRET, "2026-10:a_b.c").unwrap();
        assert_eq!(k.id(), "2026-10:a_b.c");
        let d = format!("{k:?}");
        assert!(
            d.contains("<redacted>") && d.contains("2026-10:a_b.c"),
            "{d}"
        );
        assert!(!d.contains("0123456789abcdef"), "{d}");
    }

    #[test]
    fn compact_keeps_strings_and_order() {
        assert_eq!(compact(r#"{"a":1}"#), Cow::Borrowed(r#"{"a":1}"#));
        assert_eq!(
            compact("{ \"a b\" : [ 1 , \"x \\\" y\" ] }"),
            "{\"a b\":[1,\"x \\\" y\"]}"
        );
    }
}
