//! The window logic of a watermark-based snapshot (ADR 0001 §1): one chunk at a time is read
//! between a low and a high watermark while the live stream keeps flowing, and every chunk row
//! whose key shows up in a live event of that window is dropped.
//!
//! This is a state machine over a scripted stream. The source feeds it what it sees on the
//! stream (watermarks and live changes) and what it read (the chunk), and it says what to emit.

use std::collections::HashSet;

/// Identifies one chunk. Watermarks carry it, so a replayed watermark of an older chunk is
/// recognised and ignored.
///
/// **Ids must never repeat across restarts.** After a crash the stream replays watermarks from
/// before it; if a new chunk reused an old id, a replayed `High(n)` would close the new window
/// early. Allocate them from something persisted, such as the snapshot id plus a counter that
/// resumes from the checkpoint, and never restart at 0.
pub type ChunkId = u64;

/// A watermark as it appears in the change stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mark {
    Low(ChunkId),
    High(ChunkId),
}

/// One row read by the chunk query. `key` is the primary key encoded the same way as the live
/// events' keys, so the two compare equal for the same row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRow<R> {
    pub key: String,
    pub row: R,
}

/// What the source does after a watermark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome<R> {
    /// Nothing to emit: a low watermark opened the window, or the watermark was stale.
    None,
    /// The high watermark closed the window. Emit `rows` in order, then go on with the stream.
    /// `rows` is empty when a `TRUNCATE` or live changes covered the whole chunk.
    ///
    /// `last_key` is the key of the last row the chunk query read, whether or not it survived:
    /// the cursor for the next chunk and for `SnapState.after`. `None` only for an empty chunk,
    /// which means the table is done.
    Emit {
        rows: Vec<ChunkRow<R>>,
        last_key: Option<String>,
    },
    /// The high watermark closed the window, but a live change of the chunk's table could not be
    /// matched to a key (an unsent TOAST key, a delete that does not carry the key). Any chunk row
    /// may be stale, so emit nothing and read the same chunk again; the cursor does not move.
    Retry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowError {
    /// `expect` while another chunk is still in flight.
    Busy(ChunkId),
    /// `set_chunk` with no chunk in flight.
    NoChunk,
    /// `set_chunk` twice for the same chunk.
    ChunkAlreadySet(ChunkId),
    /// The high watermark arrived and the chunk was never set.
    ChunkMissing(ChunkId),
    /// The high watermark of the in-flight chunk arrived before its low watermark.
    HighBeforeLow(ChunkId),
}

impl std::fmt::Display for WindowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy(id) => write!(f, "chunk {id} is still in flight"),
            Self::NoChunk => f.write_str("no chunk is in flight"),
            Self::ChunkAlreadySet(id) => write!(f, "chunk {id} was already read"),
            Self::ChunkMissing(id) => write!(f, "high watermark of chunk {id} before its read"),
            Self::HighBeforeLow(id) => write!(f, "high watermark of chunk {id} before its low"),
        }
    }
}

impl std::error::Error for WindowError {}

struct InFlight<R> {
    id: ChunkId,
    table: String,
    low_seen: bool,
    chunk: Option<Vec<ChunkRow<R>>>,
    touched: HashSet<String>,
    truncated: bool,
    unknown: bool,
}

/// At most one chunk of one table is in flight. Memory is the chunk plus the keys of the live
/// writes to that table inside the window.
pub struct Window<R> {
    cur: Option<InFlight<R>>,
}

impl<R> Default for Window<R> {
    fn default() -> Self {
        Self { cur: None }
    }
}

impl<R> Window<R> {
    pub fn new() -> Self {
        Self::default()
    }

    /// The id of the chunk in flight, if any.
    pub fn in_flight(&self) -> Option<ChunkId> {
        self.cur.as_ref().map(|c| c.id)
    }

    /// Start a chunk of `table`: its low watermark has been written and is awaited on the
    /// stream. Live events of other tables never touch the chunk.
    pub fn expect(&mut self, id: ChunkId, table: &str) -> Result<(), WindowError> {
        if let Some(c) = &self.cur {
            return Err(WindowError::Busy(c.id));
        }
        self.cur = Some(InFlight {
            id,
            table: table.to_owned(),
            low_seen: false,
            chunk: None,
            touched: HashSet::new(),
            truncated: false,
            unknown: false,
        });
        Ok(())
    }

    /// Hand over the rows the chunk query returned. This may happen before the low watermark is
    /// seen on the stream: the read starts after the low watermark commits, which the stream
    /// reports later.
    pub fn set_chunk(&mut self, rows: Vec<ChunkRow<R>>) -> Result<(), WindowError> {
        let c = self.cur.as_mut().ok_or(WindowError::NoChunk)?;
        if c.chunk.is_some() {
            return Err(WindowError::ChunkAlreadySet(c.id));
        }
        c.chunk = Some(rows);
        Ok(())
    }

    /// A live insert, update or delete of `table` passed on the stream. Give every key it
    /// names: the new key, and the old key too when an update changed the primary key. Ignored
    /// outside the window and for any table but the chunk's.
    pub fn live_key(&mut self, table: &str, key: &str) {
        if let Some(c) = self.cur.as_mut().filter(|c| c.low_seen && c.table == table) {
            c.touched.insert(key.to_owned());
        }
    }

    /// A live change of `table` whose key cannot be told passed on the stream. If it is the
    /// chunk's table, the chunk is read again ([`Outcome::Retry`]) unless a `TRUNCATE` voids it.
    pub fn live_unknown(&mut self, table: &str) {
        if let Some(c) = self.cur.as_mut().filter(|c| c.low_seen && c.table == table) {
            c.unknown = true;
        }
    }

    /// A live `TRUNCATE` of `table` passed on the stream. It voids the chunk if it is the
    /// chunk's table.
    pub fn live_truncate(&mut self, table: &str) {
        if let Some(c) = self.cur.as_mut().filter(|c| c.low_seen && c.table == table) {
            c.truncated = true;
        }
    }

    /// A watermark passed on the stream. Watermarks of any chunk but the one in flight are
    /// stale (a replay after a restart) and are ignored.
    pub fn mark(&mut self, mark: Mark) -> Result<Outcome<R>, WindowError> {
        let Some(c) = self.cur.as_mut() else {
            return Ok(Outcome::None);
        };
        match mark {
            Mark::Low(id) if id == c.id => {
                c.low_seen = true;
                Ok(Outcome::None)
            }
            Mark::High(id) if id == c.id => {
                if !c.low_seen {
                    return Err(WindowError::HighBeforeLow(id));
                }
                if c.chunk.is_none() {
                    return Err(WindowError::ChunkMissing(id));
                }
                let Some(InFlight {
                    chunk: Some(rows),
                    touched,
                    truncated,
                    unknown,
                    ..
                }) = self.cur.take()
                else {
                    return Err(WindowError::ChunkMissing(id));
                };
                // After a TRUNCATE no chunk row exists any more, so an unknown key does not matter.
                if unknown && !truncated {
                    return Ok(Outcome::Retry);
                }
                let last_key = rows.last().map(|r| r.key.clone());
                let rows = if truncated {
                    Vec::new()
                } else {
                    rows.into_iter()
                        .filter(|r| !touched.contains(&r.key))
                        .collect()
                };
                Ok(Outcome::Emit { rows, last_key })
            }
            _ => Ok(Outcome::None),
        }
    }
}
