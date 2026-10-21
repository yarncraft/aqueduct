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
use crate::replication::{Replication, ReplicationHandle};
use crate::source::{parse_lsn, src_err};

/// Decoded messages, each with the LSN of its WAL record, in stream order.
pub struct Fetched {
    pub messages: Vec<(u64, Message)>,
    /// Everything the server had when the fetch started was returned. Only then may an idle
    /// heartbeat move the slot to the WAL position read before the fetch.
    pub drained: bool,
    /// Stream transport: the fetch ended on a keepalive with nothing after it, so every
    /// transaction that committed before this WAL position was received.
    pub caught_up: Option<u64>,
    /// Stream transport: the WAL end of the newest keepalive so far, a position the server has
    /// reached (a lower bound of its WAL head).
    pub server_wal_end: Option<u64>,
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
        Ok(Fetched {
            messages,
            drained,
            caught_up: None,
            server_wal_end: None,
        })
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

/// Receives pgoutput from a replication connection (ADR 0003): changes arrive as they commit,
/// nothing is decoded twice, and the slot is confirmed by the connection's status updates.
pub struct StreamTransport {
    handle: ReplicationHandle,
    /// Messages per fetch, so the source keeps turning (heartbeats, snapshot chunks, admin
    /// requests) inside a large transaction.
    batch: usize,
    /// How long a fetch waits for the first message.
    wait: std::time::Duration,
    /// WAL end of the last keepalive with no data after it, across fetches: everything that
    /// committed before it has been received. The server sends one when it is idle and our
    /// confirmed position is behind; the heartbeat's own fetch is often empty.
    caught_up: Option<u64>,
    /// The highest keepalive WAL end seen.
    server_wal_end: Option<u64>,
}

impl StreamTransport {
    pub fn new(handle: ReplicationHandle, batch: usize, wait: std::time::Duration) -> Self {
        Self {
            handle,
            batch,
            wait,
            caught_up: None,
            server_wal_end: None,
        }
    }
}

fn ended() -> Error {
    Error::Source("replication connection: the server ended the stream".into())
}

#[async_trait]
impl Transport for StreamTransport {
    async fn fetch(&mut self) -> Result<Fetched> {
        let mut messages = Vec::new();
        let mut next = match tokio::time::timeout(self.wait, self.handle.recv()).await {
            Ok(m) => m?.ok_or_else(ended)?,
            Err(_) => {
                return Ok(Fetched {
                    messages,
                    drained: false,
                    caught_up: self.caught_up,
                    server_wal_end: self.server_wal_end,
                })
            }
        };
        loop {
            match next {
                Replication::Keepalive { wal_end, .. } => {
                    self.caught_up = Some(wal_end);
                    self.server_wal_end = self.server_wal_end.max(Some(wal_end));
                }
                Replication::XLogData {
                    wal_start, data, ..
                } => {
                    self.caught_up = None;
                    let msg = decode(&data).map_err(|e| {
                        Error::Source(format!(
                            "pgoutput decode at {}: {e}",
                            aqueduct_core::Position::Lsn(wal_start)
                        ))
                    })?;
                    messages.push((wal_start, msg));
                    if messages.len() >= self.batch {
                        break;
                    }
                }
            }
            next = match self.handle.try_recv()? {
                Some(m) => m,
                None if self.handle.is_finished() => return Err(ended()),
                None => break,
            };
        }
        Ok(Fetched {
            messages,
            drained: false,
            caught_up: self.caught_up,
            server_wal_end: self.server_wal_end,
        })
    }

    async fn confirm(&mut self, lsn: u64) -> Result<()> {
        // Reported with the next status update; the task never reports more than this.
        self.handle.ack(lsn);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::{streaming, StreamOptions};
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    fn keepalive(wal_end: u64) -> Vec<u8> {
        let mut k = vec![b'k'];
        k.extend(wal_end.to_be_bytes());
        k.extend(0i64.to_be_bytes());
        k.push(0);
        let mut m = vec![b'd'];
        m.extend(((k.len() + 4) as i32).to_be_bytes());
        m.extend(k);
        m
    }

    /// Keepalives report the server's WAL end, which the source takes as its head (#150); it
    /// only moves forward, and an empty fetch keeps the last one.
    #[tokio::test]
    async fn keepalives_report_the_server_wal_end() {
        let (client, mut server) = tokio::io::duplex(1 << 16);
        let (stream, status) = streaming(Box::new(client));
        let handle = ReplicationHandle::spawn(
            stream,
            status,
            StreamOptions {
                buffer_bytes: 1 << 20,
                status_interval: Duration::from_secs(3600),
            },
        );
        let mut t = StreamTransport::new(handle, 100, Duration::from_millis(200));
        let mut bytes = keepalive(0x500);
        bytes.extend(keepalive(0x900));
        server.write_all(&bytes).await.unwrap();
        let mut seen = None;
        for _ in 0..20 {
            seen = t.fetch().await.unwrap().server_wal_end;
            if seen == Some(0x900) {
                break;
            }
        }
        assert_eq!(seen, Some(0x900));
        // An older WAL end (another walsender's view) never moves it back.
        server.write_all(&keepalive(0x700)).await.unwrap();
        let mut f = t.fetch().await.unwrap();
        for _ in 0..20 {
            if f.caught_up == Some(0x700) {
                break;
            }
            f = t.fetch().await.unwrap();
        }
        assert_eq!(f.caught_up, Some(0x700));
        assert_eq!(f.server_wal_end, Some(0x900));
        // Nothing new: the last head is still reported.
        assert_eq!(t.fetch().await.unwrap().server_wal_end, Some(0x900));
    }
}
