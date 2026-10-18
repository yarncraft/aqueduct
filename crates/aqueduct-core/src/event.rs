use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Postgres log sequence number.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Lsn(pub u64);

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub lsn: Lsn,
    pub xid: u32,
    /// Commit timestamp, microseconds since the Unix epoch.
    pub commit_ts_micros: i64,
    pub schema: String,
    pub table: String,
    pub op: Op,
    /// Row before the change (update/delete, when replica identity allows).
    pub before: Option<Map<String, Value>>,
    /// Row after the change (insert/update).
    pub after: Option<Map<String, Value>>,
}
