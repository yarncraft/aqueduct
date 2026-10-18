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
    /// `partition_by = "key"`. Not part of the serialised event.
    #[serde(skip)]
    pub key: Option<String>,
}
