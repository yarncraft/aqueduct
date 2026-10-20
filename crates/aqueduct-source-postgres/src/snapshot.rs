//! Watermark-based snapshots of Postgres tables (ADR 0001, #74).
//!
//! A snapshot copies each table in primary-key chunks while the slot keeps streaming. Per chunk
//! the engine writes a low watermark into the WAL (`pg_logical_emit_message`), reads the chunk
//! with a keyset query, writes a high watermark, and hands the chunk to the [`Window`]. Live
//! changes of the table between the watermarks remove the rows they touch; at the high watermark
//! the surviving rows are emitted as `read` events. Rows are encoded by the decoder's own
//! [`RelationCache::row`] and keyed by [`RelationCache::key`], so a `read` row is byte-identical to
//! an `insert` of the same row and lands on the same Kafka partition.
//!
//! The engine does no streaming itself: `PostgresSource` feeds it the live changes and
//! watermarks it decodes and asks it for the next chunk when none is in flight.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aqueduct_core::{ChunkRow, Mark, Outcome, SkippedTable, SnapState, SnapshotStatus, Window};
use aqueduct_core::{Error, Result};
use serde_json::value::RawValue;
use tokio_postgres::{Client, SimpleQueryMessage};

use crate::catalog::{quote_ident, Catalog, PublishedTable, ReplicaIdentity};
use crate::decoder::{self, Column, Datum, Relation, RelationCache, Tuple, WATERMARK_PREFIX};

/// When a snapshot runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SnapshotMode {
    /// No snapshots, and the slot is read exactly as without this feature.
    #[default]
    Never,
    /// Snapshots run only when requested (admin API); logical messages are on.
    OnDemand,
    /// Snapshot every published table on a start without a checkpoint, and on request.
    Initial,
}

impl std::str::FromStr for SnapshotMode {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s {
            "never" => Ok(Self::Never),
            "on_demand" => Ok(Self::OnDemand),
            "initial" => Ok(Self::Initial),
            other => Err(format!(
                "snapshot = {other:?}: expected \"never\", \"on_demand\" or \"initial\""
            )),
        }
    }
}

/// Snapshot settings of the Postgres source.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    pub mode: SnapshotMode,
    /// Rows per chunk.
    pub chunk_size: u32,
    /// Upper bound on rows read per second; `None` is unthrottled.
    pub max_rows_per_sec: Option<u32>,
    /// Key columns per `schema.table` for tables without a primary key (#91). They must be
    /// published, `NOT NULL` and unique together.
    pub key_columns: HashMap<String, Vec<String>>,
    /// `schema.table` of a published table that carries the watermarks as rows instead of
    /// logical messages (#92), so snapshots work on Postgres 10 to 13. See [`WATERMARK_TABLE_DDL`].
    pub watermark_table: Option<String>,
}

/// The watermark table [`SnapshotConfig::watermark_table`] names, as DEPLOYMENT.md documents it.
pub const WATERMARK_TABLE_DDL: &str = "(slot text NOT NULL, snapshot_id text NOT NULL, \
     chunk bigint NOT NULL, mark text NOT NULL, PRIMARY KEY (slot, chunk, mark))";

/// Default for [`SnapshotConfig::chunk_size`].
pub const DEFAULT_CHUNK_SIZE: u32 = 1024;

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            mode: SnapshotMode::Never,
            chunk_size: DEFAULT_CHUNK_SIZE,
            max_rows_per_sec: None,
            key_columns: HashMap::new(),
            watermark_table: None,
        }
    }
}

/// Logical messages (the watermarks) exist from this version on.
pub const MIN_SERVER_VERSION: u32 = 140_000;

/// `schema.table` as both the stream and the snapshot name a table: one function, so the window
/// never misses a live event over a spelling difference.
pub fn table_id(schema: &str, table: &str) -> String {
    format!("{schema}.{table}")
}

/// Unit separator between key values, as in [`RelationCache::key`].
const SEP: char = '\u{1f}';

/// How to read and key one table.
#[derive(Debug, Clone)]
struct Plan {
    schema: String,
    name: String,
    /// `relation` stands in for the pgoutput Relation message, so [`RelationCache`] encodes chunk
    /// rows exactly like live ones.
    relation: Relation,
    /// Published columns (also the order of the chunk query's output).
    columns: Vec<String>,
    /// Snapshot key columns in key order and their SQL types.
    key: Vec<String>,
    key_types: Vec<String>,
    /// Position of each key column in `columns`.
    key_pos: Vec<usize>,
    /// The replica identity covers every key column (it is the primary key, the whole row, or an
    /// index containing the key). Only then does a delete's old tuple carry the key, and does an
    /// update that changes the key send its old one. Otherwise such live changes are unknown.
    key_in_old: bool,
    row_filter: Option<String>,
    estimated_rows: Option<u64>,
    /// The table's primary key, which keys `read` rows exactly as live events are keyed.
    primary_key: Vec<String>,
}

impl Plan {
    /// Builds the plan, or says why the table cannot be snapshotted.
    fn new(
        t: &PublishedTable,
        key_override: Option<&Vec<String>>,
    ) -> std::result::Result<Self, String> {
        let (key, key_types) = match key_override {
            Some(cols) if !cols.is_empty() => {
                let mut types = Vec::new();
                for c in cols {
                    let i = t
                        .columns
                        .iter()
                        .position(|p| p == c)
                        .ok_or_else(|| format!("key column {c:?} is not published"))?;
                    types.push(t.column_type_names[i].clone());
                }
                // The keyset cursor skips rows that share the boundary value and the window
                // merges rows with equal keys, so the key must identify one row.
                let same =
                    |u: &Vec<String>| u.len() == cols.len() && u.iter().all(|c| cols.contains(c));
                if !t.unique_keys.iter().any(same) {
                    return Err(format!(
                        "snapshot_key_columns {cols:?} is not a unique index of NOT NULL columns"
                    ));
                }
                (cols.clone(), types)
            }
            _ if t.primary_key.is_empty() => {
                return Err("no primary key (set postgres.snapshot_key_columns for it)".into())
            }
            _ => (t.primary_key.clone(), t.primary_key_types.clone()),
        };
        let mut key_pos = Vec::new();
        for k in &key {
            key_pos.push(t.columns.iter().position(|c| c == k).ok_or_else(|| {
                format!("key column {k:?} is not in the publication's column list")
            })?);
        }
        let identity = match t.replica_identity {
            ReplicaIdentity::Default => b'd',
            ReplicaIdentity::Nothing => b'n',
            ReplicaIdentity::Full => b'f',
            ReplicaIdentity::Index => b'i',
        };
        let key_in_old = match t.replica_identity {
            ReplicaIdentity::Full => true,
            ReplicaIdentity::Nothing => false,
            _ => key.iter().all(|k| t.identity_columns.contains(k)),
        };
        let relation = Relation {
            id: t.oid,
            schema: t.schema.clone(),
            name: t.name.clone(),
            columns: t
                .columns
                .iter()
                .zip(&t.column_types)
                // pgoutput flags every column under FULL, the identity columns otherwise.
                .map(|(name, &type_oid)| Column {
                    name: name.clone(),
                    type_oid,
                    key: identity == b'f' || t.identity_columns.contains(name),
                })
                .collect(),
            identity,
        };
        Ok(Self {
            schema: t.schema.clone(),
            name: t.name.clone(),
            relation,
            columns: t.columns.clone(),
            key,
            key_types,
            key_pos,
            key_in_old,
            row_filter: t.row_filter.clone(),
            estimated_rows: t.estimated_rows,
            primary_key: t.primary_key.clone(),
        })
    }

    /// The keyset query for the chunk after `after` (the first chunk when `None`). It runs over
    /// the simple query protocol, so every value comes back as its type's text output, exactly as
    /// pgoutput sends it (a `::text` cast is not that: `true::text` is `true`, pgoutput sends `t`).
    /// Columns are qualified with the alias `t`: an unqualified `ORDER BY` name would bind to an
    /// output column.
    fn chunk_sql(&self, after: Option<&[String]>, limit: u32) -> String {
        let col = |c: &String| format!("t.{}", quote_ident(c));
        let cols = self.columns.iter().map(col).collect::<Vec<_>>().join(", ");
        let key = self.key.iter().map(col).collect::<Vec<_>>().join(", ");
        let table = format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name));
        let mut conds = Vec::new();
        if let Some(after) = after {
            let values = after
                .iter()
                .zip(&self.key_types)
                .map(|(v, t)| format!("{}::{t}", quote_literal(v)))
                .collect::<Vec<_>>()
                .join(", ");
            conds.push(format!("({key}) > ({values})"));
        }
        if let Some(f) = &self.row_filter {
            conds.push(format!("({f})"));
        }
        let filter = if conds.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conds.join(" AND "))
        };
        format!("SELECT {cols} FROM {table} AS t{filter} ORDER BY {key} LIMIT {limit}")
    }

    /// The window key of a tuple of this table's live stream: the key columns' text joined with
    /// the unit separator, looked up by name in `rel` (the Relation the stream sent, which may
    /// list columns in another order after a schema change). `old` supplies key columns the new
    /// tuple did not send (unchanged TOAST). `None` when a key column is missing or not text.
    fn live_key(&self, rel: &Relation, tuple: &Tuple, old: Option<&Tuple>) -> Option<String> {
        let mut out = String::new();
        for (n, k) in self.key.iter().enumerate() {
            let i = rel.columns.iter().position(|c| &c.name == k)?;
            let datum = match (tuple.get(i)?, old.and_then(|o| o.get(i))) {
                (Datum::Unchanged, Some(o)) => o,
                (d, _) => d,
            };
            let Datum::Text(s) = datum else {
                return None;
            };
            if n > 0 {
                out.push(SEP);
            }
            out.push_str(s);
        }
        Some(out)
    }
}

/// `schema.table` as a quoted SQL name.
fn quote_table(id: &str) -> String {
    match id.split_once('.') {
        Some((schema, table)) => format!("{}.{}", quote_ident(schema), quote_ident(table)),
        None => quote_ident(id),
    }
}

/// The watermark a row of the watermark table carries, if it is this slot's (#92).
pub fn table_watermark(rel: &Relation, tuple: &Tuple, slot: &str) -> Option<decoder::Watermark> {
    let text = |name: &str| {
        let i = rel.columns.iter().position(|c| c.name == name)?;
        match tuple.get(i)? {
            Datum::Text(s) => Some(s.clone()),
            _ => None,
        }
    };
    if text("slot")? != slot {
        return None;
    }
    Some(decoder::Watermark {
        slot: slot.to_owned(),
        snapshot_id: text("snapshot_id")?,
        chunk: text("chunk")?.parse::<i64>().ok()?.try_into().ok()?,
        mark: match text("mark")?.as_str() {
            "low" => decoder::Mark::Low,
            "high" => decoder::Mark::High,
            _ => return None,
        },
    })
}

/// `s` as an SQL string literal. The escape-string form keeps backslashes literal whatever
/// `standard_conforming_strings` says.
fn quote_literal(s: &str) -> String {
    if s.contains('\\') {
        format!("E'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
    } else {
        format!("'{}'", s.replace('\'', "''"))
    }
}

/// A live change of a snapshotted table, as the window needs it.
pub enum Live<'a> {
    Insert {
        rel: &'a Relation,
        new: &'a Tuple,
    },
    Update {
        rel: &'a Relation,
        old: Option<&'a Tuple>,
        new: &'a Tuple,
    },
    Delete {
        rel: &'a Relation,
        old: &'a Tuple,
    },
    Truncate,
}

/// A row read by a chunk, encoded like a live insert.
#[derive(Debug, Clone)]
pub struct ReadRow {
    pub schema: String,
    pub table: String,
    pub row: Box<RawValue>,
    /// Kafka key part, exactly as for a live event of the row.
    pub key: Option<String>,
}

/// What closing a chunk produced.
pub struct Closed {
    pub rows: Vec<ReadRow>,
    /// Progress once these rows are delivered; `None` when the snapshot is finished.
    pub state: Option<SnapState>,
}

/// A running snapshot.
pub struct Engine {
    slot: String,
    /// `schema.table` quoted for SQL when watermarks are rows of a table (#92).
    watermark_table: Option<String>,
    state: SnapState,
    plans: HashMap<String, Plan>,
    window: Window<ReadRow>,
    /// Chunk ids must never repeat across restarts (a replayed watermark of an old chunk would
    /// close a new one), and must not be guessable (anyone with EXECUTE on
    /// `pg_logical_emit_message` could close a window early): a random start, counting up.
    next_chunk: u64,
    in_flight: Option<u64>,
    /// Key values of the last row the chunk in flight read: the cursor once it closes. Kept apart
    /// from the window's joined key, which cannot be split back when a value holds 0x1f.
    last_values: Option<Vec<String>>,
    /// Consecutive retries of the current chunk, for the backoff.
    retries: u32,
    chunk_size: u32,
    max_rows_per_sec: Option<u32>,
    not_before: Instant,
    rows: u64,
    skipped: Vec<(String, String)>,
    /// Stop after the chunk in flight (admin `DELETE /api/snapshot`).
    cancelled: bool,
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// 64 random bits from the standard library's per-process hash keys (no extra dependency).
fn random_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(now_micros());
    h.finish()
}

/// A fresh snapshot id.
pub fn new_snapshot_id() -> String {
    format!("s{}", now_micros())
}

impl Engine {
    /// A snapshot of `state.tables` (resuming at `state.table` after `state.after`), planned from
    /// `catalog`. Tables that cannot be snapshotted (no key, key not published, not in the
    /// publication any more) are skipped with a warning. `None` if nothing is left to copy.
    pub fn new(
        slot: &str,
        mut state: SnapState,
        catalog: &Catalog,
        cfg: &SnapshotConfig,
    ) -> Option<Self> {
        let mut plans = HashMap::new();
        let mut skipped = Vec::new();
        let mut kept = Vec::new();
        let by_id: HashMap<String, &PublishedTable> = catalog
            .tables
            .iter()
            .map(|t| (table_id(&t.schema, &t.name), t))
            .collect();
        for (i, id) in state.tables.iter().enumerate() {
            let plan = match by_id.get(id) {
                None => Err("not in the publication".to_string()),
                Some(_) if cfg.watermark_table.as_deref() == Some(id.as_str()) => {
                    Err("it is the watermark table".to_string())
                }
                Some(t) => Plan::new(t, cfg.key_columns.get(id)),
            };
            match plan {
                Ok(p) => {
                    plans.insert(id.clone(), p);
                    kept.push((i, id.clone()));
                }
                Err(why) => {
                    tracing::warn!(table = %id, reason = %why, "snapshot skips this table");
                    skipped.push((id.clone(), why));
                }
            }
        }
        // Keep the table index pointing at the same table after dropping skipped ones; the
        // cursor only belongs to the table it was saved for.
        let cur = state.table;
        let new_index = kept.iter().position(|(i, _)| *i >= cur);
        if new_index.map(|n| kept[n].0) != Some(cur) {
            state.after = None;
        }
        state.tables = kept.into_iter().map(|(_, id)| id).collect();
        state.table = new_index?;
        Some(Self {
            slot: slot.to_owned(),
            watermark_table: cfg.watermark_table.as_deref().map(quote_table),
            state,
            plans,
            window: Window::new(),
            next_chunk: random_u64() >> 1,
            in_flight: None,
            last_values: None,
            retries: 0,
            chunk_size: cfg.chunk_size.max(1),
            max_rows_per_sec: cfg.max_rows_per_sec.filter(|n| *n > 0),
            not_before: Instant::now(),
            rows: 0,
            skipped,
            cancelled: false,
        })
    }

    /// Progress as of the last fully emitted chunk, for the checkpoint of events emitted now.
    pub fn state(&self) -> &SnapState {
        &self.state
    }

    pub fn in_flight(&self) -> bool {
        self.in_flight.is_some()
    }

    /// Progress for the admin API and metrics, in the state `state` (`running`, `finished`,
    /// `cancelled`).
    pub fn status(&self, state: &str) -> SnapshotStatus {
        let running = state == "running";
        SnapshotStatus {
            id: self.state.id.clone(),
            state: state.to_owned(),
            tables: self.state.tables.clone(),
            tables_done: if state == "finished" {
                self.state.tables.len()
            } else {
                self.state.table
            },
            table: running
                .then(|| self.state.tables.get(self.state.table).cloned())
                .flatten(),
            rows: self.rows,
            rows_estimated: self
                .state
                .tables
                .iter()
                .map(|t| self.plans.get(t).and_then(|p| p.estimated_rows))
                .sum(),
            skipped: self
                .skipped
                .iter()
                .map(|(table, reason)| SkippedTable {
                    table: table.clone(),
                    reason: reason.clone(),
                })
                .collect(),
        }
    }

    /// Whether the next chunk may start now: none in flight, not cancelled, and the throttle
    /// allows it.
    pub fn due(&self) -> bool {
        self.in_flight.is_none() && !self.cancelled && Instant::now() >= self.not_before
    }

    /// Stops the snapshot once the chunk in flight (if any) is closed.
    pub fn cancel(&mut self) {
        self.cancelled = true;
    }

    pub fn cancelled(&self) -> bool {
        self.cancelled
    }

    /// The snapshot id.
    pub fn id(&self) -> &str {
        &self.state.id
    }

    /// Appends tables to the running snapshot (an admin request while one runs).
    pub fn append(&mut self, tables: Vec<String>, catalog: &Catalog, cfg: &SnapshotConfig) {
        for id in tables {
            if self.state.tables.contains(&id) {
                continue;
            }
            match catalog
                .tables
                .iter()
                .find(|t| table_id(&t.schema, &t.name) == id)
                .ok_or_else(|| "not in the publication".to_string())
                .and_then(|t| Plan::new(t, cfg.key_columns.get(&id)))
            {
                Ok(p) => {
                    self.plans.insert(id.clone(), p);
                    self.state.tables.push(id);
                }
                Err(why) => {
                    tracing::warn!(table = %id, reason = %why, "snapshot skips this table");
                    self.skipped.push((id, why));
                }
            }
        }
    }

    fn plan(&self) -> Result<&Plan> {
        let id = &self.state.tables[self.state.table];
        self.plans
            .get(id)
            .ok_or_else(|| Error::Source(format!("snapshot has no plan for {id}")))
    }

    async fn emit_watermark(&self, client: &Client, chunk: u64, mark: &str) -> Result<()> {
        if let Some(table) = &self.watermark_table {
            // Its own transaction, like the logical message: the row is decoded in commit order.
            let chunk =
                i64::try_from(chunk).map_err(|_| Error::Source("chunk id overflow".into()))?;
            client
                .execute(
                    &format!(
                        "INSERT INTO {table} (slot, snapshot_id, chunk, mark) VALUES ($1, $2, $3, $4)"
                    ),
                    &[&self.slot, &self.state.id, &chunk, &mark],
                )
                .await
                .map_err(crate::source::src_err)?;
            return Ok(());
        }
        let payload = serde_json::json!({
            "slot": self.slot,
            "snapshot_id": self.state.id,
            "chunk": chunk,
            "mark": mark,
        })
        .to_string();
        client
            .execute(
                "SELECT pg_logical_emit_message(true, $1::text, $2::text)",
                &[&WATERMARK_PREFIX, &payload],
            )
            .await
            .map_err(crate::source::src_err)?;
        Ok(())
    }

    /// Reads the next chunk between a low and a high watermark. Each statement runs in its own
    /// transaction, so the chunk's snapshot is taken after the low watermark committed and
    /// before the high one.
    pub async fn start_chunk(&mut self, client: &Client) -> Result<()> {
        let id = self.next_chunk;
        self.next_chunk += 1;
        let table = self.state.tables[self.state.table].clone();
        self.window
            .expect(id, &table)
            .map_err(|e| Error::Source(format!("snapshot window: {e}")))?;
        self.in_flight = Some(id);
        if let Some(t) = &self.watermark_table {
            // Only one chunk is in flight, so the rows of earlier chunks are no longer needed.
            client
                .execute(&format!("DELETE FROM {t} WHERE slot = $1"), &[&self.slot])
                .await
                .map_err(crate::source::src_err)?;
        }
        self.emit_watermark(client, id, "low").await?;

        let plan = self.plan()?;
        let after = self
            .state
            .after
            .clone()
            .filter(|a| a.len() == plan.key.len());
        let sql = plan.chunk_sql(after.as_deref(), self.chunk_size);
        let messages = client
            .simple_query(&sql)
            .await
            .map_err(crate::source::src_err)?;
        let mut cache = RelationCache::default();
        cache.insert(plan.relation.clone());
        let mut chunk = Vec::new();
        let mut last_values = None;
        for m in &messages {
            let SimpleQueryMessage::Row(r) = m else {
                continue;
            };
            let tuple: Tuple = (0..plan.columns.len())
                .map(|i| match r.get(i) {
                    Some(s) => Datum::Text(s.to_owned()),
                    None => Datum::Null,
                })
                .collect();
            let values = plan
                .key_pos
                .iter()
                .map(|&i| match &tuple[i] {
                    Datum::Text(s) => Ok(s.clone()),
                    _ => Err(Error::Source(format!(
                        "snapshot of {table}: key column {:?} is NULL",
                        plan.columns[i]
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            let window_key = values.join(&SEP.to_string());
            last_values = Some(values);
            let dec = |e: decoder::DecodeError| Error::Source(e.to_string());
            chunk.push(ChunkRow {
                key: window_key,
                row: ReadRow {
                    schema: plan.schema.clone(),
                    table: plan.name.clone(),
                    row: cache.row(plan.relation.id, &tuple).map_err(dec)?,
                    key: cache
                        .key(plan.relation.id, &tuple, Some(&plan.primary_key))
                        .map_err(dec)?,
                },
            });
        }
        let n = chunk.len() as u64;
        self.last_values = last_values;
        self.window
            .set_chunk(chunk)
            .map_err(|e| Error::Source(format!("snapshot window: {e}")))?;
        self.emit_watermark(client, id, "high").await?;
        if let Some(rate) = self.max_rows_per_sec {
            self.not_before = Instant::now() + Duration::from_secs_f64(n as f64 / f64::from(rate));
        }
        Ok(())
    }

    /// A live change of `table` passed on the stream (only changes queued for the first time).
    pub fn observe(&mut self, table: &str, change: Live<'_>) {
        if self.in_flight.is_none() {
            return;
        }
        let Some(plan) = self.plans.get(table) else {
            return;
        };
        let keys = match change {
            Live::Truncate => {
                self.window.live_truncate(table);
                return;
            }
            Live::Insert { rel, new } => vec![plan.live_key(rel, new, None)],
            Live::Update { rel, old, new } => {
                let mut k = vec![plan.live_key(rel, new, old)];
                // An old tuple is only sent when the identity changed or is FULL. If the identity
                // does not cover the key, the key can change with no old tuple, and the row it
                // had cannot be told.
                match old {
                    _ if !plan.key_in_old => k.push(None),
                    Some(o) => k.push(plan.live_key(rel, o, None)),
                    None => {}
                }
                k
            }
            Live::Delete { rel, old } => {
                if plan.key_in_old {
                    vec![plan.live_key(rel, old, None)]
                } else {
                    vec![None]
                }
            }
        };
        for k in keys {
            match k {
                Some(k) => self.window.live_key(table, &k),
                None => self.window.live_unknown(table),
            }
        }
    }

    /// A Relation message of `table` passed on the stream. When its columns differ from the plan
    /// (an `ALTER TABLE` since the plan was made), later chunks read the new columns, and a chunk
    /// in flight, read with the old ones, is read again. A key column that is gone makes the next
    /// chunk query fail, and the restart then skips the table.
    pub fn relation(&mut self, table: &str, rel: &Relation) {
        let Some(plan) = self.plans.get_mut(table) else {
            return;
        };
        let shape = |r: &Relation| {
            r.columns
                .iter()
                .map(|c| (c.name.clone(), c.type_oid))
                .collect::<Vec<_>>()
        };
        if shape(&plan.relation) == shape(rel) {
            return;
        }
        tracing::info!(table = %table, "the table's columns changed during the snapshot");
        plan.relation = Relation {
            id: plan.relation.id,
            ..rel.clone()
        };
        plan.columns = rel.columns.iter().map(|c| c.name.clone()).collect();
        for (pos, k) in plan.key_pos.iter_mut().zip(&plan.key) {
            if let Some(i) = plan.columns.iter().position(|c| c == k) {
                *pos = i;
            }
        }
        plan.key_in_old = rel.identity == b'f'
            || (rel.identity != b'n'
                && plan
                    .key
                    .iter()
                    .all(|k| rel.columns.iter().any(|c| &c.name == k && c.key)));
        self.window.live_unknown(table);
    }

    /// A watermark of this slot passed on the stream. Returns the rows to emit and the new
    /// progress when it closes the chunk in flight.
    pub fn mark(&mut self, wm: &decoder::Watermark) -> Result<Option<Closed>> {
        if wm.snapshot_id != self.state.id {
            return Ok(None);
        }
        let mark = match wm.mark {
            decoder::Mark::Low => Mark::Low(wm.chunk),
            decoder::Mark::High => Mark::High(wm.chunk),
        };
        let outcome = self
            .window
            .mark(mark)
            .map_err(|e| Error::Source(format!("snapshot window: {e}")))?;
        let (rows, last_key) = match outcome {
            Outcome::None => return Ok(None),
            Outcome::Retry => {
                // Back off, so a table that keeps producing unknown keys is not hammered.
                self.retries = self.retries.saturating_add(1);
                let wait = Duration::from_millis(100u64 << self.retries.min(7))
                    .min(Duration::from_secs(10));
                self.not_before = self.not_before.max(Instant::now() + wait);
                tracing::info!(
                    table = %self.state.tables[self.state.table],
                    "a live change in the snapshot window had no readable key; reading the chunk again"
                );
                self.in_flight = None;
                return Ok(Some(Closed {
                    rows: Vec::new(),
                    state: Some(self.state.clone()),
                }));
            }
            Outcome::Emit { rows, last_key } => (rows, last_key),
        };
        self.in_flight = None;
        self.retries = 0;
        let last_values = self.last_values.take();
        match last_key {
            Some(_) => self.state.after = last_values,
            None => {
                // An empty chunk: the table is done.
                self.state.after = None;
                self.state.table += 1;
            }
        }
        self.rows += rows.len() as u64;
        let rows = rows.into_iter().map(|r| r.row).collect();
        let state = (self.state.table < self.state.tables.len()).then(|| self.state.clone());
        Ok(Some(Closed { rows, state }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(identity: ReplicaIdentity) -> PublishedTable {
        PublishedTable {
            schema: "public".into(),
            name: "Orders".into(),
            oid: 42,
            columns: vec!["b".into(), "a".into(), "v".into()],
            column_types: vec![23, 25, 3802],
            column_type_names: vec!["integer".into(), "text".into(), "jsonb".into()],
            primary_key: vec!["a".into(), "b".into()],
            primary_key_types: vec!["text".into(), "integer".into()],
            replica_identity: identity,
            identity_columns: match identity {
                ReplicaIdentity::Default => vec!["b".into(), "a".into()],
                ReplicaIdentity::Index => vec!["b".into()],
                _ => vec![],
            },
            row_filter: None,
            estimated_rows: Some(10),
            unique_keys: vec![vec!["a".into(), "b".into()], vec!["b".into()]],
        }
    }

    fn catalog(tables: Vec<PublishedTable>) -> Catalog {
        Catalog {
            server_version_num: 160_000,
            tables,
        }
    }

    fn state(tables: &[&str]) -> SnapState {
        SnapState {
            id: "s1".into(),
            tables: tables.iter().map(|t| t.to_string()).collect(),
            table: 0,
            after: None,
        }
    }

    #[test]
    fn chunk_query_is_a_keyset_on_the_quoted_key() {
        let p = Plan::new(&table(ReplicaIdentity::Default), None).unwrap();
        assert_eq!(
            p.chunk_sql(None, 100),
            r#"SELECT t."b", t."a", t."v" FROM "public"."Orders" AS t ORDER BY t."a", t."b" LIMIT 100"#
        );
        let mut t = table(ReplicaIdentity::Default);
        t.row_filter = Some("(v IS NOT NULL)".into());
        let p = Plan::new(&t, None).unwrap();
        let after = [r"it's \ x".to_string(), "7".to_string()];
        assert_eq!(
            p.chunk_sql(Some(&after), 5),
            r#"SELECT t."b", t."a", t."v" FROM "public"."Orders" AS t WHERE (t."a", t."b") > (E'it''s \\ x'::text, '7'::integer) AND ((v IS NOT NULL)) ORDER BY t."a", t."b" LIMIT 5"#
        );
    }

    #[test]
    fn tables_without_a_usable_key_are_skipped() {
        let mut nopk = table(ReplicaIdentity::Default);
        nopk.primary_key.clear();
        assert!(Plan::new(&nopk, None)
            .unwrap_err()
            .contains("no primary key"));
        // The key must be published, or live events could not be matched to it.
        let mut hidden = table(ReplicaIdentity::Default);
        hidden.columns = vec!["b".into(), "v".into()];
        hidden.column_types = vec![23, 3802];
        hidden.column_type_names = vec!["integer".into(), "jsonb".into()];
        assert!(Plan::new(&hidden, None)
            .unwrap_err()
            .contains("not in the publication"));
        // #91: an override names published key columns.
        let over = vec!["b".to_string()];
        let p = Plan::new(&nopk, Some(&over)).unwrap();
        assert_eq!(
            (p.key.clone(), p.key_types.clone()),
            (over, vec!["integer".to_string()])
        );
        let bad = vec!["x".to_string()];
        assert!(Plan::new(&nopk, Some(&bad))
            .unwrap_err()
            .contains("not published"));
        // ... that are a unique key of NOT NULL columns, or rows would be skipped or merged.
        let loose = vec!["v".to_string()];
        assert!(Plan::new(&nopk, Some(&loose))
            .unwrap_err()
            .contains("not a unique index"));
    }

    #[test]
    fn engine_skips_unusable_tables_and_keeps_the_cursor_on_its_table() {
        let mut nopk = table(ReplicaIdentity::Default);
        nopk.name = "a_nopk".into();
        nopk.primary_key.clear();
        let mut other = table(ReplicaIdentity::Default);
        other.name = "z".into();
        let cat = catalog(vec![nopk, table(ReplicaIdentity::Default), other]);
        let mut st = state(&["public.a_nopk", "public.Orders", "public.z"]);
        st.table = 1;
        st.after = Some(vec!["k".into(), "1".into()]);
        let e = Engine::new("slot", st, &cat, &SnapshotConfig::default()).unwrap();
        assert_eq!(e.state().tables, ["public.Orders", "public.z"]);
        assert_eq!(e.state().table, 0);
        assert_eq!(e.state().after, Some(vec!["k".into(), "1".into()]));
        assert_eq!(e.status("running").skipped.len(), 1);
        // Nothing usable left: no engine.
        assert!(Engine::new(
            "slot",
            state(&["public.gone"]),
            &cat,
            &SnapshotConfig::default()
        )
        .is_none());
    }

    fn rel(identity: u8) -> Relation {
        // The stream's Relation lists columns in its own order.
        Relation {
            id: 42,
            schema: "public".into(),
            name: "Orders".into(),
            columns: vec![
                Column {
                    name: "a".into(),
                    type_oid: 25,
                    key: true,
                },
                Column {
                    name: "b".into(),
                    type_oid: 23,
                    key: true,
                },
                Column {
                    name: "v".into(),
                    type_oid: 3802,
                    key: false,
                },
            ],
            identity,
        }
    }

    fn tuple(a: &str, b: &str) -> Tuple {
        vec![Datum::Text(a.into()), Datum::Text(b.into()), Datum::Null]
    }

    #[test]
    fn live_keys_are_looked_up_by_column_name() {
        let p = Plan::new(&table(ReplicaIdentity::Default), None).unwrap();
        let r = rel(b'd');
        assert_eq!(
            p.live_key(&r, &tuple("x", "1"), None),
            Some("x\u{1f}1".into())
        );
        let unsent = vec![Datum::Unchanged, Datum::Text("1".into()), Datum::Null];
        assert_eq!(p.live_key(&r, &unsent, None), None);
        assert_eq!(
            p.live_key(&r, &unsent, Some(&tuple("x", "1"))),
            Some("x\u{1f}1".into())
        );
    }

    #[test]
    fn a_delete_whose_old_tuple_lacks_the_key_is_unknown() {
        // USING INDEX on `b` only: a delete's old tuple does not carry `a`.
        assert!(
            !Plan::new(&table(ReplicaIdentity::Index), None)
                .unwrap()
                .key_in_old
        );
        assert!(
            Plan::new(&table(ReplicaIdentity::Default), None)
                .unwrap()
                .key_in_old
        );
        assert!(
            Plan::new(&table(ReplicaIdentity::Full), None)
                .unwrap()
                .key_in_old
        );
    }

    fn wm(chunk: u64, mark: decoder::Mark) -> decoder::Watermark {
        decoder::Watermark {
            slot: "slot".into(),
            snapshot_id: "s1".into(),
            chunk,
            mark,
        }
    }

    fn read(key: &str) -> ChunkRow<ReadRow> {
        ChunkRow {
            key: key.into(),
            row: ReadRow {
                schema: "public".into(),
                table: "Orders".into(),
                row: RawValue::from_string("{}".into()).unwrap(),
                key: None,
            },
        }
    }

    /// Drives the window as `start_chunk` would, without a server.
    fn open(e: &mut Engine, rows: Vec<ChunkRow<ReadRow>>) -> u64 {
        let id = e.next_chunk;
        e.next_chunk += 1;
        let t = e.state.tables[e.state.table].clone();
        e.window.expect(id, &t).unwrap();
        e.in_flight = Some(id);
        e.last_values = rows
            .last()
            .map(|r| r.key.split('\u{1f}').map(str::to_owned).collect());
        e.window.set_chunk(rows).unwrap();
        id
    }

    #[test]
    fn closing_chunks_moves_the_cursor_then_the_table_then_finishes() {
        let cat = catalog(vec![table(ReplicaIdentity::Default)]);
        let mut e = Engine::new(
            "slot",
            state(&["public.Orders"]),
            &cat,
            &SnapshotConfig::default(),
        )
        .unwrap();
        let id = open(&mut e, vec![read("a\u{1f}1"), read("b\u{1f}2")]);
        assert!(e.mark(&wm(id, decoder::Mark::Low)).unwrap().is_none());
        // A live update of b/2 in the window removes that row.
        let r = rel(b'd');
        e.observe(
            "public.Orders",
            Live::Update {
                rel: &r,
                old: None,
                new: &tuple("b", "2"),
            },
        );
        // Another snapshot's watermark is ignored.
        let mut foreign = wm(id, decoder::Mark::High);
        foreign.snapshot_id = "s0".into();
        assert!(e.mark(&foreign).unwrap().is_none());
        let closed = e.mark(&wm(id, decoder::Mark::High)).unwrap().unwrap();
        assert_eq!(closed.rows.len(), 1);
        let st = closed.state.unwrap();
        assert_eq!(
            st.after,
            Some(vec!["b".into(), "2".into()]),
            "the cursor passes removed rows too"
        );
        assert!(!e.in_flight());

        // An unknown key retries: same cursor, nothing emitted.
        let id = open(&mut e, vec![read("c\u{1f}3")]);
        e.mark(&wm(id, decoder::Mark::Low)).unwrap();
        let unsent = vec![Datum::Unchanged, Datum::Text("3".into()), Datum::Null];
        e.observe(
            "public.Orders",
            Live::Update {
                rel: &r,
                old: None,
                new: &unsent,
            },
        );
        let closed = e.mark(&wm(id, decoder::Mark::High)).unwrap().unwrap();
        assert!(closed.rows.is_empty());
        assert_eq!(
            closed.state.unwrap().after,
            Some(vec!["b".into(), "2".into()])
        );

        // An empty chunk ends the table, and with it the snapshot.
        let id = open(&mut e, vec![]);
        e.mark(&wm(id, decoder::Mark::Low)).unwrap();
        let closed = e.mark(&wm(id, decoder::Mark::High)).unwrap().unwrap();
        assert!(closed.rows.is_empty());
        assert!(closed.state.is_none(), "finished");
        assert_eq!(e.status("finished").rows, 1);
        assert_eq!(e.status("finished").tables_done, 1);
    }

    #[test]
    fn modes_parse() {
        assert_eq!("never".parse(), Ok(SnapshotMode::Never));
        assert_eq!("on_demand".parse(), Ok(SnapshotMode::OnDemand));
        assert_eq!("initial".parse(), Ok(SnapshotMode::Initial));
        assert!("always".parse::<SnapshotMode>().is_err());
    }

    #[test]
    fn an_update_the_identity_cannot_place_retries_the_chunk() {
        // USING INDEX on `b` alone: an update may change `a` without an old tuple.
        let cat = catalog(vec![table(ReplicaIdentity::Index)]);
        let mut e = Engine::new(
            "slot",
            state(&["public.Orders"]),
            &cat,
            &SnapshotConfig::default(),
        )
        .unwrap();
        let id = open(&mut e, vec![read("x\u{1f}5")]);
        e.mark(&wm(id, decoder::Mark::Low)).unwrap();
        let r = rel(b'i');
        e.observe(
            "public.Orders",
            Live::Update {
                rel: &r,
                old: None,
                new: &tuple("y", "5"),
            },
        );
        let closed = e.mark(&wm(id, decoder::Mark::High)).unwrap().unwrap();
        assert!(closed.rows.is_empty(), "the chunk is read again");
        assert!(!e.due(), "after a backoff");
    }

    #[test]
    fn the_cursor_keeps_key_values_that_hold_the_separator() {
        let cat = catalog(vec![table(ReplicaIdentity::Default)]);
        let mut e = Engine::new(
            "slot",
            state(&["public.Orders"]),
            &cat,
            &SnapshotConfig::default(),
        )
        .unwrap();
        let id = open(&mut e, vec![read("a\u{1f}b\u{1f}1")]);
        e.last_values = Some(vec!["a\u{1f}b".into(), "1".into()]);
        e.mark(&wm(id, decoder::Mark::Low)).unwrap();
        let closed = e.mark(&wm(id, decoder::Mark::High)).unwrap().unwrap();
        assert_eq!(
            closed.state.unwrap().after,
            Some(vec!["a\u{1f}b".to_string(), "1".to_string()])
        );
    }

    #[test]
    fn a_column_change_replans_and_retries_the_chunk_in_flight() {
        let cat = catalog(vec![table(ReplicaIdentity::Default)]);
        let mut e = Engine::new(
            "slot",
            state(&["public.Orders"]),
            &cat,
            &SnapshotConfig::default(),
        )
        .unwrap();
        let id = open(&mut e, vec![read("a\u{1f}1")]);
        e.mark(&wm(id, decoder::Mark::Low)).unwrap();
        // The same columns again change nothing.
        let mut same = e.plans["public.Orders"].relation.clone();
        e.relation("public.Orders", &same);
        // A new column: the plan reads it from now on, and the chunk in flight is read again.
        same.columns.push(Column {
            name: "w".into(),
            type_oid: 25,
            key: false,
        });
        e.relation("public.Orders", &same);
        assert_eq!(e.plans["public.Orders"].columns, ["b", "a", "v", "w"]);
        assert_eq!(e.plans["public.Orders"].key_pos, [1, 0]);
        let closed = e.mark(&wm(id, decoder::Mark::High)).unwrap().unwrap();
        assert!(closed.rows.is_empty());
    }
}
