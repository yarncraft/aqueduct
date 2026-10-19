//! Pure decoder for the pgoutput logical replication protocol (version 1).

use std::collections::HashMap;

use std::fmt::Write;

use serde_json::value::RawValue;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("unexpected end of message")]
    Eof,
    #[error("unknown message tag {0:#04x}")]
    UnknownTag(u8),
    #[error("unknown tuple kind {0:#04x}")]
    UnknownTupleKind(u8),
    #[error("invalid utf-8 in message")]
    Utf8,
    #[error("data for unknown relation {0}")]
    UnknownRelation(u32),
    #[error("column value is not valid JSON")]
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub type_oid: u32,
    /// Part of the replica identity key.
    pub key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub id: u32,
    pub schema: String,
    pub name: String,
    pub columns: Vec<Column>,
}

/// One column of a tuple as sent on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Datum {
    Null,
    /// TOASTed value that was not changed and is not sent.
    Unchanged,
    Text(String),
    Binary(Vec<u8>),
}

pub type Tuple = Vec<Datum>;

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Begin {
        final_lsn: u64,
        commit_ts_micros: i64,
        xid: u32,
    },
    Commit {
        commit_lsn: u64,
        end_lsn: u64,
        commit_ts_micros: i64,
    },
    Relation(Relation),
    Insert {
        relation: u32,
        new: Tuple,
    },
    Update {
        relation: u32,
        old: Option<Tuple>,
        new: Tuple,
    },
    Delete {
        relation: u32,
        old: Tuple,
    },
    Truncate {
        relations: Vec<u32>,
    },
    /// Origin / Type / logical messages: nothing we emit.
    Ignored,
}

/// Microseconds between the Unix epoch and the Postgres epoch (2000-01-01).
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.0.len() < n {
            return Err(DecodeError::Eof);
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        self.take(N)?.try_into().map_err(|_| DecodeError::Eof)
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(u8::from_be_bytes(self.array()?))
    }
    fn i16(&mut self) -> Result<i16, DecodeError> {
        Ok(i16::from_be_bytes(self.array()?))
    }
    fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_be_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn cstr(&mut self) -> Result<String, DecodeError> {
        let end = self
            .0
            .iter()
            .position(|&b| b == 0)
            .ok_or(DecodeError::Eof)?;
        let s = std::str::from_utf8(&self.0[..end]).map_err(|_| DecodeError::Utf8)?;
        self.0 = &self.0[end + 1..];
        Ok(s.to_owned())
    }
    fn tuple(&mut self) -> Result<Tuple, DecodeError> {
        let n = self.i16()?.max(0) as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(match self.u8()? {
                b'n' => Datum::Null,
                b'u' => Datum::Unchanged,
                kind @ (b't' | b'b') => {
                    let len = self.i32()?.max(0) as usize;
                    let bytes = self.take(len)?;
                    if kind == b't' {
                        Datum::Text(
                            std::str::from_utf8(bytes)
                                .map_err(|_| DecodeError::Utf8)?
                                .to_owned(),
                        )
                    } else {
                        Datum::Binary(bytes.to_vec())
                    }
                }
                other => return Err(DecodeError::UnknownTupleKind(other)),
            });
        }
        Ok(out)
    }
}

/// Decode a single pgoutput message.
pub fn decode(buf: &[u8]) -> Result<Message, DecodeError> {
    let mut r = Reader(buf);
    Ok(match r.u8()? {
        b'B' => Message::Begin {
            final_lsn: r.u64()?,
            commit_ts_micros: r.i64()? + PG_EPOCH_OFFSET_MICROS,
            xid: r.u32()?,
        },
        b'C' => {
            let _flags = r.u8()?;
            Message::Commit {
                commit_lsn: r.u64()?,
                end_lsn: r.u64()?,
                commit_ts_micros: r.i64()? + PG_EPOCH_OFFSET_MICROS,
            }
        }
        b'R' => {
            let id = r.u32()?;
            let schema = r.cstr()?;
            let name = r.cstr()?;
            let _replica_identity = r.u8()?;
            let n = r.i16()?.max(0) as usize;
            let mut columns = Vec::with_capacity(n);
            for _ in 0..n {
                let flags = r.u8()?;
                let name = r.cstr()?;
                let type_oid = r.u32()?;
                let _typmod = r.i32()?;
                columns.push(Column {
                    name,
                    type_oid,
                    key: flags & 1 != 0,
                });
            }
            Message::Relation(Relation {
                id,
                schema,
                name,
                columns,
            })
        }
        b'I' => {
            let relation = r.u32()?;
            let _n = r.u8()?; // 'N'
            Message::Insert {
                relation,
                new: r.tuple()?,
            }
        }
        b'U' => {
            let relation = r.u32()?;
            let mut old = None;
            let mut tag = r.u8()?;
            if tag == b'K' || tag == b'O' {
                old = Some(r.tuple()?);
                tag = r.u8()?; // 'N'
            }
            let _ = tag;
            Message::Update {
                relation,
                old,
                new: r.tuple()?,
            }
        }
        b'D' => {
            let relation = r.u32()?;
            let _k = r.u8()?; // 'K' or 'O'
            Message::Delete {
                relation,
                old: r.tuple()?,
            }
        }
        b'T' => {
            let n = r.i32()?.max(0) as usize;
            let _options = r.u8()?;
            let mut relations = Vec::with_capacity(n);
            for _ in 0..n {
                relations.push(r.u32()?);
            }
            Message::Truncate { relations }
        }
        b'O' | b'Y' | b'M' => Message::Ignored,
        other => return Err(DecodeError::UnknownTag(other)),
    })
}

/// Remembers the latest [`Relation`] definition for each relation id.
#[derive(Debug, Default)]
pub struct RelationCache(HashMap<u32, Relation>);

impl RelationCache {
    pub fn insert(&mut self, rel: Relation) {
        self.0.insert(rel.id, rel);
    }

    pub fn get(&self, id: u32) -> Result<&Relation, DecodeError> {
        self.0.get(&id).ok_or(DecodeError::UnknownRelation(id))
    }

    /// Turn a tuple into a JSON object keyed by column name, written straight into one
    /// string (no intermediate `Value` tree). `Unchanged` TOAST columns are omitted.
    /// `json`/`jsonb` text is passed through verbatim: Postgres only emits valid JSON there.
    pub fn row(&self, id: u32, tuple: &Tuple) -> Result<Box<RawValue>, DecodeError> {
        let rel = self.get(id)?;
        let mut out = String::with_capacity(64 + tuple.len() * 24);
        out.push('{');
        let mut first = true;
        for (col, datum) in rel.columns.iter().zip(tuple) {
            if matches!(datum, Datum::Unchanged) {
                continue;
            }
            if !first {
                out.push(',');
            }
            first = false;
            push_json_str(&mut out, &col.name);
            out.push(':');
            match datum {
                Datum::Unchanged => {}
                Datum::Null => out.push_str("null"),
                Datum::Text(s) => push_text(&mut out, col.type_oid, s),
                Datum::Binary(b) => {
                    out.push('"');
                    for x in b {
                        let _ = write!(out, "{x:02x}");
                    }
                    out.push('"');
                }
            }
        }
        out.push('}');
        RawValue::from_string(out).map_err(|_| DecodeError::Json)
    }

    /// Replica-identity key values of `tuple` joined with the unit separator (0x1f), or `None` when the table
    /// has no key columns. Used to partition by primary key.
    pub fn key(&self, id: u32, tuple: &Tuple) -> Result<Option<String>, DecodeError> {
        let rel = self.get(id)?;
        let mut out = String::new();
        let mut any = false;
        for (col, datum) in rel.columns.iter().zip(tuple) {
            if !col.key {
                continue;
            }
            if any {
                out.push('\u{1f}');
            }
            any = true;
            match datum {
                Datum::Text(s) => out.push_str(s),
                Datum::Binary(b) => {
                    for x in b {
                        let _ = write!(out, "{x:02x}");
                    }
                }
                Datum::Null | Datum::Unchanged => {}
            }
        }
        Ok(any.then_some(out))
    }
}

/// Append `s` as a JSON string literal.
fn push_json_str(out: &mut String, s: &str) {
    out.push('"');
    let mut start = 0;
    for (i, c) in s.char_indices() {
        let esc = match c {
            '"' => "\\\"",
            '\\' => "\\\\",
            '\n' => "\\n",
            '\r' => "\\r",
            '\t' => "\\t",
            c if (c as u32) < 0x20 => {
                out.push_str(&s[start..i]);
                let _ = write!(out, "\\u{:04x}", c as u32);
                start = i + 1;
                continue;
            }
            _ => continue,
        };
        out.push_str(&s[start..i]);
        out.push_str(esc);
        start = i + c.len_utf8();
    }
    out.push_str(&s[start..]);
    out.push('"');
}

/// Append Postgres text output as JSON using well-known type OIDs; anything
/// unrecognised becomes a string.
fn push_text(out: &mut String, oid: u32, s: &str) {
    match oid {
        16 => out.push_str(if s == "t" { "true" } else { "false" }),
        20 | 21 | 23 => match s.parse::<i64>() {
            Ok(n) => {
                let _ = write!(out, "{n}");
            }
            Err(_) => push_json_str(out, s),
        },
        700 | 701 => match s.parse::<f64>().ok().and_then(serde_json::Number::from_f64) {
            Some(n) => {
                let _ = write!(out, "{n}");
            }
            None => push_json_str(out, s), // NaN / Infinity
        },
        114 | 3802 if !s.trim().is_empty() => out.push_str(s),
        _ => push_json_str(out, s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuple_bytes(cols: &[Option<&str>]) -> Vec<u8> {
        let mut v = (cols.len() as i16).to_be_bytes().to_vec();
        for c in cols {
            match c {
                None => v.push(b'n'),
                Some(s) => {
                    v.push(b't');
                    v.extend((s.len() as i32).to_be_bytes());
                    v.extend(s.as_bytes());
                }
            }
        }
        v
    }

    fn relation_bytes() -> Vec<u8> {
        let mut v = vec![b'R'];
        v.extend(42u32.to_be_bytes());
        v.extend(b"public\0users\0");
        v.push(b'd');
        v.extend(2i16.to_be_bytes());
        for (flags, name, oid) in [(1u8, "id", 23u32), (0, "name", 25)] {
            v.push(flags);
            v.extend(name.as_bytes());
            v.push(0);
            v.extend(oid.to_be_bytes());
            v.extend((-1i32).to_be_bytes());
        }
        v
    }

    #[test]
    fn begin_converts_pg_epoch() {
        let mut b = vec![b'B'];
        b.extend(100u64.to_be_bytes());
        b.extend(0i64.to_be_bytes());
        b.extend(7u32.to_be_bytes());
        assert_eq!(
            decode(&b).unwrap(),
            Message::Begin {
                final_lsn: 100,
                commit_ts_micros: PG_EPOCH_OFFSET_MICROS,
                xid: 7
            }
        );
    }

    #[test]
    fn relation_insert_roundtrip_to_json() {
        let Message::Relation(rel) = decode(&relation_bytes()).unwrap() else {
            panic!()
        };
        assert_eq!(rel.schema, "public");
        assert!(rel.columns[0].key && !rel.columns[1].key);
        let mut cache = RelationCache::default();
        cache.insert(rel);

        let mut ins = vec![b'I'];
        ins.extend(42u32.to_be_bytes());
        ins.push(b'N');
        ins.extend(tuple_bytes(&[Some("5"), None]));
        let Message::Insert { relation, new } = decode(&ins).unwrap() else {
            panic!()
        };
        let row = cache.row(relation, &new).unwrap();
        assert_eq!(row.get(), r#"{"id":5,"name":null}"#);
        assert_eq!(cache.key(relation, &new).unwrap().as_deref(), Some("5"));
    }

    #[test]
    fn update_with_old_key_tuple() {
        let mut u = vec![b'U'];
        u.extend(42u32.to_be_bytes());
        u.push(b'K');
        u.extend(tuple_bytes(&[Some("1"), None]));
        u.push(b'N');
        u.extend(tuple_bytes(&[Some("1"), Some("bob")]));
        let Message::Update { old, new, .. } = decode(&u).unwrap() else {
            panic!()
        };
        assert!(old.is_some());
        assert_eq!(new[1], Datum::Text("bob".into()));
    }

    #[test]
    fn update_without_old_tuple() {
        let mut u = vec![b'U'];
        u.extend(42u32.to_be_bytes());
        u.push(b'N');
        u.extend(tuple_bytes(&[Some("1"), Some("x")]));
        assert!(matches!(
            decode(&u).unwrap(),
            Message::Update { old: None, .. }
        ));
    }

    #[test]
    fn delete_and_truncate() {
        let mut d = vec![b'D'];
        d.extend(42u32.to_be_bytes());
        d.push(b'K');
        d.extend(tuple_bytes(&[Some("1"), None]));
        assert!(matches!(
            decode(&d).unwrap(),
            Message::Delete { relation: 42, .. }
        ));

        let mut t = vec![b'T'];
        t.extend(2i32.to_be_bytes());
        t.push(0);
        t.extend(1u32.to_be_bytes());
        t.extend(2u32.to_be_bytes());
        assert_eq!(
            decode(&t).unwrap(),
            Message::Truncate {
                relations: vec![1, 2]
            }
        );
    }

    #[test]
    fn truncated_and_unknown() {
        assert_eq!(decode(&[]), Err(DecodeError::Eof));
        assert_eq!(decode(&[b'I', 0, 0]), Err(DecodeError::Eof));
        assert_eq!(decode(b"?"), Err(DecodeError::UnknownTag(b'?')));
    }

    fn text(oid: u32, s: &str) -> String {
        let mut out = String::new();
        push_text(&mut out, oid, s);
        out
    }

    #[test]
    fn json_and_bool_types() {
        assert_eq!(text(16, "t"), "true");
        assert_eq!(text(3802, r#"{"a":1}"#), r#"{"a":1}"#);
        assert_eq!(text(701, "NaN"), r#""NaN""#);
        assert_eq!(text(23, "-7"), "-7");
        assert_eq!(text(701, "1.5"), "1.5");
    }

    #[test]
    fn strings_are_escaped() {
        let mut out = String::new();
        push_json_str(&mut out, "a\"b\\c\nd\u{1}é");
        assert_eq!(out, r#""a\"b\\c\nd\u0001é""#);
        let v: String = serde_json::from_str(&out).unwrap();
        assert_eq!(v, "a\"b\\c\nd\u{1}é");
    }

    #[test]
    fn truncated_messages_error_instead_of_panicking() {
        let mut begin = vec![b'B'];
        begin.extend(100u64.to_be_bytes());
        begin.extend(0i64.to_be_bytes());
        begin.extend(7u32.to_be_bytes());
        let mut ins = vec![b'I'];
        ins.extend(42u32.to_be_bytes());
        ins.push(b'N');
        ins.extend(tuple_bytes(&[Some("5"), None]));
        let mut upd = vec![b'U'];
        upd.extend(42u32.to_be_bytes());
        upd.push(b'K');
        upd.extend(tuple_bytes(&[Some("1"), None]));
        upd.push(b'N');
        upd.extend(tuple_bytes(&[Some("1"), Some("bob")]));
        let mut del = vec![b'D'];
        del.extend(42u32.to_be_bytes());
        del.push(b'K');
        del.extend(tuple_bytes(&[Some("1"), None]));
        for full in [begin, relation_bytes(), ins, upd, del] {
            for cut in 0..full.len() {
                assert!(decode(&full[..cut]).is_err(), "cut at {cut}");
            }
        }
    }
}
