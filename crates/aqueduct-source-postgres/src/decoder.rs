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
    #[error("negative length or count {0}")]
    NegativeLength(i32),
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
    /// Replica identity byte from the Relation message: `d` default, `n` nothing, `f` full, `i` index.
    pub identity: u8,
}

impl Relation {
    /// `REPLICA IDENTITY FULL` makes pgoutput flag every column as identity, so the "key" would be the
    /// whole row and change on every update.
    pub fn identity_full(&self) -> bool {
        self.identity == b'f'
    }
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
    /// A logical decoding message (`pg_logical_emit_message`). Only sent when the peek asks for
    /// `messages`. `lsn` is the message's own position.
    Logical(Logical),
    /// Origin / Type messages: nothing we emit.
    Ignored,
}

/// The prefix of the messages Aqueduct emits into its own stream (snapshot watermarks).
pub const WATERMARK_PREFIX: &str = "aqueduct";

/// A decoded `M` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Logical {
    /// Emitted inside a transaction, so it is decoded in commit order with the data.
    pub transactional: bool,
    pub lsn: u64,
    pub prefix: String,
    pub content: Vec<u8>,
}

/// Which end of a snapshot chunk a watermark marks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mark {
    Low,
    High,
}

/// A snapshot watermark: `pg_logical_emit_message(true, 'aqueduct', '{"slot":..,"snapshot_id":..,"chunk":..,"mark":..}')`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watermark {
    pub slot: String,
    pub snapshot_id: String,
    pub chunk: u64,
    pub mark: Mark,
}

impl Logical {
    /// The watermark this message carries for `slot`. `None` for any other prefix, for another
    /// slot's watermark (several Aqueduct instances can share a database), for a payload that is
    /// not a watermark, and for a non-transactional message: watermarks must arrive in commit order.
    pub fn watermark_for(&self, slot: &str) -> Option<Watermark> {
        if !self.transactional || self.prefix != WATERMARK_PREFIX {
            return None;
        }
        let v: serde_json::Value = serde_json::from_slice(&self.content).ok()?;
        if v.get("slot")?.as_str()? != slot {
            return None;
        }
        Some(Watermark {
            slot: slot.to_owned(),
            snapshot_id: v.get("snapshot_id")?.as_str()?.to_owned(),
            chunk: v.get("chunk")?.as_u64()?,
            mark: match v.get("mark")?.as_str()? {
                "low" => Mark::Low,
                "high" => Mark::High,
                _ => return None,
            },
        })
    }
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
    /// A non-negative i16 count. A negative one is a malformed message, not an empty value.
    fn count16(&mut self) -> Result<usize, DecodeError> {
        let n = self.i16()?;
        usize::try_from(n).map_err(|_| DecodeError::NegativeLength(i32::from(n)))
    }
    /// A non-negative i32 length or count.
    fn count32(&mut self) -> Result<usize, DecodeError> {
        let n = self.i32()?;
        usize::try_from(n).map_err(|_| DecodeError::NegativeLength(n))
    }
    /// Preallocation bound for a declared element count: every element takes at least one byte,
    /// so a count above the bytes left is malformed and must not drive a huge allocation.
    fn cap(&self, n: usize) -> usize {
        n.min(self.0.len())
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
        let n = self.count16()?;
        let mut out = Vec::with_capacity(self.cap(n));
        for _ in 0..n {
            out.push(match self.u8()? {
                b'n' => Datum::Null,
                b'u' => Datum::Unchanged,
                kind @ (b't' | b'b') => {
                    let len = self.count32()?;
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
            let identity = r.u8()?;
            let n = r.count16()?;
            let mut columns = Vec::with_capacity(r.cap(n));
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
                identity,
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
            let n = r.count32()?;
            let _options = r.u8()?;
            let mut relations = Vec::with_capacity(r.cap(n));
            for _ in 0..n {
                relations.push(r.u32()?);
            }
            Message::Truncate { relations }
        }
        b'M' => {
            // Protocol v1 has no xid here; it only exists for streamed transactions (v2+).
            let flags = r.u8()?;
            let lsn = r.u64()?;
            let prefix = r.cstr()?;
            let len = r.count32()?;
            Message::Logical(Logical {
                transactional: flags & 1 == 1,
                lsn,
                prefix,
                content: r.take(len)?.to_vec(),
            })
        }
        b'O' | b'Y' => Message::Ignored,
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
    /// has no key columns or uses `REPLICA IDENTITY FULL` (where every column is flagged, so the key would
    /// be the whole row and move on every update), or when a key column was not sent (unchanged TOAST) and the update carries no old value for it.
    /// `None` means "fall back to the table key", not "no primary key". Used to partition by primary key.
    pub fn key(&self, id: u32, tuple: &Tuple) -> Result<Option<String>, DecodeError> {
        self.key_with_old(id, tuple, None)
    }

    /// Like [`key`](Self::key), but a key column the new tuple did not send (unchanged TOAST) is taken from
    /// the `old` key/old-row tuple of an update when that has it. Postgres sends the old key whenever an
    /// identity column is stored externally, so the row keeps the key its insert had.
    pub fn key_with_old(
        &self,
        id: u32,
        tuple: &Tuple,
        old: Option<&Tuple>,
    ) -> Result<Option<String>, DecodeError> {
        let rel = self.get(id)?;
        if rel.identity_full() {
            return Ok(None);
        }
        let mut out = String::new();
        let mut any = false;
        for (i, (col, datum)) in rel.columns.iter().zip(tuple).enumerate() {
            if !col.key {
                continue;
            }
            let datum = match (datum, old.and_then(|o| o.get(i))) {
                (Datum::Unchanged, Some(o)) => o,
                _ => datum,
            };
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
                Datum::Null => {}
                // Unsent and no old value: a different key than the insert had, so use the table key.
                Datum::Unchanged => return Ok(None),
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
        relation_bytes_with(b'd')
    }

    fn relation_bytes_with(identity: u8) -> Vec<u8> {
        let mut v = vec![b'R'];
        v.extend(42u32.to_be_bytes());
        v.extend(b"public\0users\0");
        v.push(identity);
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
    fn negative_tuple_column_count_is_an_error() {
        let mut b = vec![b'I'];
        b.extend(42u32.to_be_bytes());
        b.push(b'N');
        b.extend((-1i16).to_be_bytes());
        assert_eq!(decode(&b), Err(DecodeError::NegativeLength(-1)));
    }

    #[test]
    fn negative_value_length_is_an_error() {
        for kind in *b"tb" {
            let mut b = vec![b'I'];
            b.extend(42u32.to_be_bytes());
            b.push(b'N');
            b.extend(1i16.to_be_bytes());
            b.push(kind);
            b.extend((-5i32).to_be_bytes());
            assert_eq!(decode(&b), Err(DecodeError::NegativeLength(-5)));
        }
    }

    #[test]
    fn negative_relation_column_count_is_an_error() {
        let mut b = vec![b'R'];
        b.extend(42u32.to_be_bytes());
        b.extend(b"public\0users\0");
        b.push(b'd');
        b.extend((-2i16).to_be_bytes());
        assert_eq!(decode(&b), Err(DecodeError::NegativeLength(-2)));
    }

    #[test]
    fn huge_truncate_relation_count_is_eof_not_an_allocation() {
        let mut b = vec![b'T'];
        b.extend(i32::MAX.to_be_bytes());
        b.push(0);
        assert_eq!(decode(&b), Err(DecodeError::Eof));
    }

    #[test]
    fn negative_truncate_relation_count_is_an_error() {
        let mut b = vec![b'T'];
        b.extend(i32::MIN.to_be_bytes());
        b.push(0);
        assert_eq!(decode(&b), Err(DecodeError::NegativeLength(i32::MIN)));
    }

    fn key_of(identity: u8, tuple: &[Option<&str>]) -> Option<String> {
        let Message::Relation(rel) = decode(&relation_bytes_with(identity)).unwrap() else {
            panic!()
        };
        let mut cache = RelationCache::default();
        cache.insert(rel);
        cache.key(42, &decode_tuple(tuple)).unwrap()
    }

    fn decode_tuple(cols: &[Option<&str>]) -> Tuple {
        let mut ins = vec![b'I'];
        ins.extend(42u32.to_be_bytes());
        ins.push(b'N');
        ins.extend(tuple_bytes(cols));
        let Message::Insert { new, .. } = decode(&ins).unwrap() else {
            panic!()
        };
        new
    }

    #[test]
    fn full_identity_has_no_key_so_updates_keep_one_partition() {
        // Under FULL pgoutput flags every column, so the old and new row would hash differently.
        let flagged_all = {
            let Message::Relation(mut rel) = decode(&relation_bytes_with(b'f')).unwrap() else {
                panic!()
            };
            for c in &mut rel.columns {
                c.key = true;
            }
            rel
        };
        let mut cache = RelationCache::default();
        cache.insert(flagged_all);
        let old = decode_tuple(&[Some("5"), Some("a")]);
        let new = decode_tuple(&[Some("5"), Some("b")]);
        assert_eq!(cache.key(42, &old).unwrap(), None);
        assert_eq!(cache.key(42, &new).unwrap(), None);
    }

    #[test]
    fn unchanged_toast_key_is_taken_from_the_old_tuple_of_an_update() {
        let Message::Relation(rel) = decode(&relation_bytes()).unwrap() else {
            panic!()
        };
        let mut cache = RelationCache::default();
        cache.insert(rel);
        let new = vec![Datum::Unchanged, Datum::Text("b".into())];
        let old = vec![Datum::Text("5".into()), Datum::Null];
        let insert_key = cache
            .key(42, &decode_tuple(&[Some("5"), Some("a")]))
            .unwrap();
        assert_eq!(
            cache.key_with_old(42, &new, Some(&old)).unwrap(),
            insert_key
        );
        assert_eq!(cache.key_with_old(42, &new, None).unwrap(), None);
    }

    #[test]
    fn nothing_identity_without_key_columns_has_no_key() {
        let Message::Relation(mut rel) = decode(&relation_bytes_with(b'n')).unwrap() else {
            panic!()
        };
        for c in &mut rel.columns {
            c.key = false;
        }
        let mut cache = RelationCache::default();
        cache.insert(rel);
        assert_eq!(
            cache
                .key(42, &decode_tuple(&[Some("5"), Some("a")]))
                .unwrap(),
            None
        );
    }

    #[test]
    fn unchanged_toast_key_column_falls_back_to_the_table_key() {
        let Message::Relation(rel) = decode(&relation_bytes()).unwrap() else {
            panic!()
        };
        let mut cache = RelationCache::default();
        cache.insert(rel);
        let tuple = vec![Datum::Unchanged, Datum::Text("a".into())];
        assert_eq!(cache.key(42, &tuple).unwrap(), None);
    }

    #[test]
    fn index_identity_key_changes_when_the_key_column_changes() {
        // Documented caveat: updating a key column moves the row; pinned, not fixed.
        let a = key_of(b'i', &[Some("5"), Some("x")]);
        let b = key_of(b'i', &[Some("6"), Some("x")]);
        assert_ne!(a, b);
    }

    #[test]
    fn default_identity_without_a_primary_key_falls_back_to_the_table_key() {
        let Message::Relation(mut rel) = decode(&relation_bytes_with(b'd')).unwrap() else {
            panic!()
        };
        for c in &mut rel.columns {
            c.key = false;
        }
        let mut cache = RelationCache::default();
        cache.insert(rel);
        assert_eq!(
            cache
                .key(42, &decode_tuple(&[Some("5"), Some("a")]))
                .unwrap(),
            None
        );
    }

    #[test]
    fn default_and_index_identity_keep_the_key() {
        assert_eq!(key_of(b'd', &[Some("5"), Some("a")]), Some("5".to_string()));
        assert_eq!(key_of(b'i', &[Some("5"), Some("a")]), Some("5".to_string()));
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

    fn logical_bytes(flags: u8, lsn: u64, prefix: &str, content: &[u8]) -> Vec<u8> {
        let mut b = vec![b'M', flags];
        b.extend(lsn.to_be_bytes());
        b.extend(prefix.as_bytes());
        b.push(0);
        b.extend((content.len() as i32).to_be_bytes());
        b.extend(content);
        b
    }

    fn logical(flags: u8, prefix: &str, content: &str) -> Logical {
        match decode(&logical_bytes(
            flags,
            0x016B_37A8,
            prefix,
            content.as_bytes(),
        ))
        .unwrap()
        {
            Message::Logical(l) => l,
            other => panic!("not a logical message: {other:?}"),
        }
    }

    const LOW: &str = r#"{"slot":"s1","snapshot_id":"snap-7","chunk":3,"mark":"low"}"#;

    #[test]
    fn transactional_message_is_decoded() {
        let l = logical(1, "aqueduct", LOW);
        assert!(l.transactional);
        assert_eq!(l.lsn, 0x016B_37A8);
        assert_eq!(l.prefix, "aqueduct");
        assert_eq!(l.content, LOW.as_bytes());
        assert_eq!(
            l.watermark_for("s1"),
            Some(Watermark {
                slot: "s1".into(),
                snapshot_id: "snap-7".into(),
                chunk: 3,
                mark: Mark::Low
            })
        );
    }

    #[test]
    fn high_mark_is_decoded() {
        let l = logical(
            1,
            "aqueduct",
            r#"{"slot":"s1","snapshot_id":"x","chunk":0,"mark":"high"}"#,
        );
        assert_eq!(l.watermark_for("s1").map(|w| w.mark), Some(Mark::High));
    }

    #[test]
    fn non_transactional_message_is_decoded_but_is_not_a_watermark() {
        let l = logical(0, "aqueduct", LOW);
        assert!(!l.transactional);
        assert_eq!(l.watermark_for("s1"), None);
    }

    #[test]
    fn foreign_prefix_is_not_a_watermark() {
        assert_eq!(logical(1, "wal2json", LOW).watermark_for("s1"), None);
        assert_eq!(logical(1, "aqueduct2", LOW).watermark_for("s1"), None);
    }

    #[test]
    fn foreign_slot_is_not_a_watermark() {
        assert_eq!(logical(1, "aqueduct", LOW).watermark_for("other"), None);
    }

    #[test]
    fn malformed_payloads_are_not_watermarks() {
        for body in [
            "",
            "not json",
            "{}",
            r#"{"slot":"s1","snapshot_id":"x","chunk":-1,"mark":"low"}"#,
            r#"{"slot":"s1","snapshot_id":"x","chunk":1,"mark":"middle"}"#,
            r#"{"slot":"s1","snapshot_id":7,"chunk":1,"mark":"low"}"#,
        ] {
            assert_eq!(
                logical(1, "aqueduct", body).watermark_for("s1"),
                None,
                "{body}"
            );
        }
    }

    #[test]
    fn truncated_or_oversized_message_is_an_error() {
        let full = logical_bytes(1, 1, "aqueduct", LOW.as_bytes());
        for n in 1..full.len() {
            assert!(decode(&full[..n]).is_err(), "{n} bytes");
        }
        let mut b = logical_bytes(1, 1, "aqueduct", b"");
        let at = b.len() - 4;
        b[at..].copy_from_slice(&i32::MAX.to_be_bytes());
        assert_eq!(decode(&b), Err(DecodeError::Eof));
        b[at..].copy_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(decode(&b), Err(DecodeError::NegativeLength(-1)));
    }

    #[test]
    fn origin_and_type_messages_are_still_ignored() {
        assert_eq!(decode(b"O"), Ok(Message::Ignored));
        assert_eq!(decode(b"Y"), Ok(Message::Ignored));
    }
}
