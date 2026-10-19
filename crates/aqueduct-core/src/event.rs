use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// Where an event sits in its source's change stream: the unit that is checkpointed and
/// acked. Sources choose the representation; the pipeline only stores and hands it back.
///
/// `Lsn` is the Postgres log sequence number. `Text` is any other source's token (a Kafka
/// offset map, a Mongo resume token, a SQL Server CDC LSN in hex...), kept as opaque text.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Position {
    Lsn(u64),
    Text(String),
}

impl Position {
    /// Durable text form for checkpoint files. Plain decimal digits stay an `Lsn`, so
    /// checkpoints written before `Position` existed still load.
    pub fn to_checkpoint(&self) -> String {
        match self {
            Position::Lsn(n) => n.to_string(),
            Position::Text(t) => format!("~{t}"),
        }
    }

    pub fn from_checkpoint(s: &str) -> Result<Self, String> {
        let s = s.trim();
        match s.strip_prefix('~') {
            Some(t) => Ok(Position::Text(t.to_string())),
            None => s
                .parse::<u64>()
                .map(Position::Lsn)
                .map_err(|e| e.to_string()),
        }
    }
}

impl std::fmt::Display for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Position::Lsn(n) => write!(f, "{:X}/{:X}", n >> 32, n & 0xFFFF_FFFF),
            Position::Text(t) => f.write_str(t),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Op {
    Insert,
    Update,
    Delete,
    Truncate,
    /// A row read by a snapshot, not a change. `before` is null and `after` is the full row; it
    /// reaches consumers as `"read"` in the JSON and in the Kafka `op` header (ADR 0001).
    Read,
    /// Carries only a position, no row. A source emits it to move the checkpoint while it
    /// has nothing to deliver (idle or filtered stream). The pipeline checkpoints it but
    /// never hands it to a sink or counts it in metrics.
    Heartbeat,
}

impl ChangeEvent {
    /// A position-only event, see [`Op::Heartbeat`].
    pub fn heartbeat(position: Position, commit_ts_micros: i64) -> Self {
        Self {
            position,
            xid: 0,
            commit_ts_micros,
            schema: String::new(),
            table: String::new(),
            op: Op::Heartbeat,
            before: None,
            after: None,
            key: None,
            checkpoint: None,
        }
    }
}

/// One row-level change, emitted at transaction commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Serialised as `lsn` to keep the wire format of existing consumers.
    #[serde(rename = "lsn")]
    pub position: Position,
    pub xid: u32,
    /// Commit timestamp, microseconds since the Unix epoch.
    pub commit_ts_micros: i64,
    pub schema: String,
    pub table: String,
    pub op: Op,
    /// Row before the change (update/delete, when replica identity allows).
    ///
    /// Held as pre-serialised JSON: the decoder writes it straight from the wire tuple, so
    /// no `serde_json::Value` tree is built, and the Kafka record embeds the bytes as is.
    pub before: Option<Box<RawValue>>,
    /// Row after the change (insert/update).
    pub after: Option<Box<RawValue>>,
    /// Replica-identity (primary key) values, used to pick a Kafka partition when
    /// `partition_by = "key"`. Not part of the serialised event. `None` means "fall back to the table
    /// key", not "no primary key": it is set for `REPLICA IDENTITY FULL`/`NOTHING` tables, TRUNCATE,
    /// and unchanged-TOAST key columns.
    #[serde(skip)]
    pub key: Option<String>,
    /// What the pipeline saves and acks after delivering this event, when it differs from
    /// `position`. A source whose `position` is not unique or not ordered across the stream
    /// (Postgres: change LSNs are not commit-ordered) puts its resume key here. Not serialised.
    #[serde(skip)]
    pub checkpoint: Option<Position>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(op: Op) -> ChangeEvent {
        ChangeEvent {
            position: Position::Lsn(42),
            xid: 7,
            commit_ts_micros: 1_700_000_000_000_000,
            schema: "public".into(),
            table: "t".into(),
            op,
            before: None,
            after: RawValue::from_string(r#"{"id":1}"#.into()).ok(),
            key: Some("1".into()),
        }
    }

    #[test]
    fn existing_ops_serialise_exactly_as_before() {
        // Pinned wire format: adding `Read` must not change a byte of the others.
        for (op, name) in [
            (Op::Insert, "insert"),
            (Op::Update, "update"),
            (Op::Delete, "delete"),
            (Op::Truncate, "truncate"),
        ] {
            assert_eq!(
                serde_json::to_string(&event(op)).unwrap(),
                format!(
                    r#"{{"lsn":42,"xid":7,"commit_ts_micros":1700000000000000,"schema":"public","table":"t","op":"{name}","before":null,"after":{{"id":1}}}}"#
                )
            );
        }
    }

    #[test]
    fn read_is_serialised_as_read_and_round_trips() {
        let json = serde_json::to_string(&event(Op::Read)).unwrap();
        assert!(json.contains(r#""op":"read""#), "{json}");
        let back: ChangeEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back.op, Op::Read);
    }
}
