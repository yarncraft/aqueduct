//! Where the source's decoded pgoutput comes from (#74; ADR 0003 T4 adds a second transport).
//!
//! A transport only moves bytes: it returns decoded messages in WAL order and confirms commit
//! end LSNs to the server. Every change gets its resume [`Key`](crate::source::Key) in one place,
//! `PostgresSource::apply`, for every transport. A transport never builds keys or ordinals itself,
//! so the poll and stream transports cannot number the rows of a COPY or a multi-table TRUNCATE
//! differently (the #94 row loss).

use std::sync::Arc;

use aqueduct_core::{Error, Result};
use async_trait::async_trait;
use tokio_postgres::Client;

use crate::decoder::{decode, Message};
use crate::source::{parse_lsn, src_err};

/// Decoded messages, each with the LSN of its WAL record, in stream order.
pub struct Fetched {
    pub messages: Vec<(u64, Message)>,
    /// Everything the server had when the fetch started was returned. Only then may an idle
    /// heartbeat move the slot to the WAL position read before the fetch.
    pub drained: bool,
}

#[async_trait]
pub trait Transport: Send + Sync {
    /// The next messages, starting at a transaction boundary. A transport may return messages
    /// it returned before (a peek restarts at the slot's confirmed position every time); the
    /// caller skips changes whose key it already queued.
    async fn fetch(&mut self) -> Result<Fetched>;

    /// Tells the server that every transaction ending at or before `lsn` is delivered. `lsn` is
    /// always a commit end LSN: confirming a position inside a transaction replays all of it.
    async fn confirm(&mut self, lsn: u64) -> Result<()>;
}

/// The peek query. `proto_version` stays 1 and `streaming` is never set: a streamed in-progress
/// transaction has no commit LSN until it commits, so no key could be built for its changes.
pub const PEEK_SQL: &str = "SELECT lsn::text, data \
     FROM pg_logical_slot_peek_binary_changes($1::text::name, NULL, $2::int4, \
     'proto_version', '1', 'publication_names', $3::text)";

/// [`PEEK_SQL`] plus `messages`, so logical messages (snapshot watermarks) are sent. Postgres 14+.
pub const PEEK_SQL_WITH_MESSAGES: &str = "SELECT lsn::text, data \
     FROM pg_logical_slot_peek_binary_changes($1::text::name, NULL, $2::int4, \
     'proto_version', '1', 'publication_names', $3::text, 'messages', 'true')";

pub fn peek_sql(logical_messages: bool) -> &'static str {
    if logical_messages {
        PEEK_SQL_WITH_MESSAGES
    } else {
        PEEK_SQL
    }
}

/// Polls the slot over a plain SQL connection with `pg_logical_slot_peek_binary_changes`, which
/// does not consume anything, and confirms with `pg_replication_slot_advance`.
pub struct PeekTransport {
    pub client: Arc<Client>,
    pub slot: String,
    pub publication: String,
    pub batch_size: i32,
    pub logical_messages: bool,
}

#[async_trait]
impl Transport for PeekTransport {
    async fn fetch(&mut self) -> Result<Fetched> {
        let rows = self
            .client
            .query(
                peek_sql(self.logical_messages),
                &[&self.slot, &self.batch_size, &self.publication],
            )
            .await
            .map_err(src_err)?;
        // `upto_nchanges` ends a capped peek at the first commit past the limit. Only a peek that
        // returned fewer rows than the cap saw everything. A peek that reached the end can still
        // return `batch_size` rows or more (it stops at a commit boundary); that only delays the
        // jump to the WAL position by one heartbeat, never skips data.
        let drained = i64::try_from(rows.len()).is_ok_and(|n| n < i64::from(self.batch_size));
        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            // Borrow from the result buffer instead of copying each change out of it.
            let lsn_text: &str = row.get(0);
            let data: &[u8] = row.get(1);
            let lsn = parse_lsn(lsn_text)?;
            let msg = decode(data)
                .map_err(|e| Error::Source(format!("pgoutput decode at {lsn_text}: {e}")))?;
            messages.push((lsn, msg));
        }
        Ok(Fetched { messages, drained })
    }

    async fn confirm(&mut self, lsn: u64) -> Result<()> {
        self.client
            .execute(
                "SELECT pg_replication_slot_advance($1::text::name, $2::text::pg_lsn)",
                &[&self.slot, &aqueduct_core::Position::Lsn(lsn).to_string()],
            )
            .await
            .map_err(src_err)?;
        Ok(())
    }
}
