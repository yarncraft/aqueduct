use std::collections::VecDeque;
use std::time::Duration;

use aqueduct_core::{ChangeEvent, Error, Op, Position, Result, Source};
use async_trait::async_trait;
use serde_json::value::RawValue;
use tokio_postgres::{Client, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::tls::{self, SslMode};

use crate::decoder::{decode, Message, RelationCache};

type Row = Box<RawValue>;

#[derive(Debug, Clone)]
pub struct PostgresSourceConfig {
    /// libpq-style connection string / URL.
    pub url: String,
    /// Logical replication slot; created (pgoutput) if missing.
    pub slot: String,
    /// Existing publication to stream.
    pub publication: String,
    /// Sleep between polls when the slot has no new changes.
    pub poll_interval: Duration,
    /// Approximate max changes fetched per poll (whole transactions are never split).
    pub batch_size: i32,
    /// If the slot is missing but a checkpoint exists, changes made while the slot was gone are
    /// lost. By default that is a startup error; set this to accept it and start a fresh slot.
    pub allow_slot_recreate: bool,
    /// TLS policy; overrides `sslmode` in the URL when set. Default: URL, else `verify-full`.
    pub ssl_mode: Option<SslMode>,
    /// PEM CA bundle to verify the server against; overrides `sslrootcert` in the URL.
    pub ssl_root_cert: Option<String>,
}

impl PostgresSourceConfig {
    pub fn new(
        url: impl Into<String>,
        slot: impl Into<String>,
        publication: impl Into<String>,
    ) -> Self {
        Self {
            url: url.into(),
            slot: slot.into(),
            publication: publication.into(),
            poll_interval: Duration::from_millis(500),
            batch_size: 1000,
            allow_slot_recreate: false,
            ssl_mode: None,
            ssl_root_cert: None,
        }
    }

    /// See [`PostgresSourceConfig::allow_slot_recreate`].
    pub fn allow_slot_recreate(mut self, allow: bool) -> Self {
        self.allow_slot_recreate = allow;
        self
    }
}

/// Reads a pgoutput logical slot over a normal SQL connection.
///
/// Changes are fetched with `pg_logical_slot_peek_binary_changes` (which does
/// not consume them) and the slot is advanced in [`Source::ack`] with
/// `pg_replication_slot_advance`, so a crash before ack replays the change.
/// Replays after a restart may re-deliver the remainder of a transaction
/// (at-least-once).
pub struct PostgresSource {
    cfg: PostgresSourceConfig,
    client: Option<Client>,
    buffer: VecDeque<ChangeEvent>,
    /// Highest change LSN already queued; later peeks skip rows at or below it.
    emitted: u64,
    /// Highest LSN the slot has been advanced to.
    acked: u64,
    /// Completed transactions as (last change LSN, commit end LSN). The slot can only
    /// be advanced to a commit end LSN: advancing to a mid-transaction change LSN
    /// leaves `confirmed_flush_lsn` unmoved and the whole transaction is replayed.
    tx_ends: VecDeque<(u64, u64)>,
    /// Highest commit end LSN recorded in `tx_ends`.
    last_end: u64,
}

impl PostgresSource {
    pub fn new(cfg: PostgresSourceConfig) -> Self {
        Self {
            cfg,
            client: None,
            buffer: VecDeque::new(),
            emitted: 0,
            acked: 0,
            tx_ends: VecDeque::new(),
            last_end: 0,
        }
    }

    /// Open the SQL connection according to the TLS policy.
    async fn connect(&self) -> Result<Client> {
        let parsed = tls::parse(&self.cfg.url)?;
        let mode = self.cfg.ssl_mode.or(parsed.url_mode).unwrap_or_default();
        let root = self.cfg.ssl_root_cert.clone().or(parsed.url_root_cert);
        let local = tls::is_local(&parsed.config);
        let mut pg = parsed.config;
        pg.ssl_mode(mode.pg());

        let client = if mode == SslMode::Disable {
            let (client, conn) = pg.connect(NoTls).await.map_err(src_err)?;
            spawn_conn(conn);
            client
        } else {
            let cfg = aqueduct_tls::client_config(root.as_deref(), mode.verify(root.is_some()))
                .map_err(Error::Config)?;
            let (client, conn) = pg
                .connect(MakeRustlsConnect::new((*cfg).clone()))
                .await
                .map_err(src_err)?;
            spawn_conn(conn);
            client
        };

        // Authoritative check of what was negotiated (tokio-postgres hides it).
        let encrypted: bool = client
            .query_one(
                "SELECT COALESCE((SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()), false)",
                &[],
            )
            .await
            .map_err(src_err)?
            .get(0);
        tls::check_negotiated(mode, encrypted, local, root.is_some())?;
        Ok(client)
    }

    fn client(&self) -> Result<&Client> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::Source("source not started".into()))
    }

    /// Advance the slot past every fully acknowledged transaction (all changes <= `lsn`).
    async fn advance_acked(&mut self, lsn: u64) -> Result<()> {
        if let Some(target) = ack_target(&mut self.tx_ends, lsn) {
            self.advance(target).await?;
        }
        Ok(())
    }

    async fn advance(&mut self, lsn: u64) -> Result<()> {
        if lsn <= self.acked {
            return Ok(());
        }
        self.client()?
            .execute(
                "SELECT pg_replication_slot_advance($1::text::name, $2::text::pg_lsn)",
                &[&self.cfg.slot, &Position::Lsn(lsn).to_string()],
            )
            .await
            .map_err(src_err)?;
        self.acked = lsn;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn queue(
        &mut self,
        lsn: u64,
        xid: u32,
        commit_ts_micros: i64,
        (schema, table): (String, String),
        op: Op,
        before: Option<Row>,
        after: Option<Row>,
        key: Option<String>,
    ) {
        if lsn > self.emitted {
            self.emitted = lsn;
            self.buffer.push_back(ChangeEvent {
                position: Position::Lsn(lsn),
                xid,
                commit_ts_micros,
                schema,
                table,
                op,
                before,
                after,
                key,
            });
        }
    }

    async fn poll(&mut self) -> Result<()> {
        let rows = self
            .client()?
            .query(
                "SELECT lsn::text, data \
                 FROM pg_logical_slot_peek_binary_changes($1::text::name, NULL, $2::int4, \
                 'proto_version', '1', 'publication_names', $3::text)",
                &[&self.cfg.slot, &self.cfg.batch_size, &self.cfg.publication],
            )
            .await
            .map_err(src_err)?;

        let mut cache = RelationCache::default();
        let (mut xid, mut ts) = (0u32, 0i64);
        let mut tx_last = 0u64;
        for row in rows {
            // Borrow from the result buffer instead of copying each change out of it.
            let lsn_text: &str = row.get(0);
            let data: &[u8] = row.get(1);
            let lsn = parse_lsn(lsn_text)?;
            let msg = decode(data)
                .map_err(|e| Error::Source(format!("pgoutput decode at {lsn_text}: {e}")))?;
            if matches!(
                msg,
                Message::Insert { .. }
                    | Message::Update { .. }
                    | Message::Delete { .. }
                    | Message::Truncate { .. }
            ) {
                tx_last = tx_last.max(lsn);
            }
            let dec = |e: crate::decoder::DecodeError| Error::Source(e.to_string());
            match msg {
                Message::Begin {
                    xid: x,
                    commit_ts_micros,
                    ..
                } => {
                    xid = x;
                    ts = commit_ts_micros;
                }
                Message::Relation(rel) => cache.insert(rel),
                Message::Insert { relation, new } => {
                    let r = cache.get(relation).map_err(dec)?;
                    let name = (r.schema.clone(), r.name.clone());
                    let after = Some(cache.row(relation, &new).map_err(dec)?);
                    let key = cache.key(relation, &new).map_err(dec)?;
                    self.queue(lsn, xid, ts, name, Op::Insert, None, after, key);
                }
                Message::Update { relation, old, new } => {
                    let r = cache.get(relation).map_err(dec)?;
                    let name = (r.schema.clone(), r.name.clone());
                    let before = old
                        .map(|t| cache.row(relation, &t))
                        .transpose()
                        .map_err(dec)?;
                    let after = Some(cache.row(relation, &new).map_err(dec)?);
                    let key = cache.key(relation, &new).map_err(dec)?;
                    self.queue(lsn, xid, ts, name, Op::Update, before, after, key);
                }
                Message::Delete { relation, old } => {
                    let r = cache.get(relation).map_err(dec)?;
                    let name = (r.schema.clone(), r.name.clone());
                    let before = Some(cache.row(relation, &old).map_err(dec)?);
                    let key = cache.key(relation, &old).map_err(dec)?;
                    self.queue(lsn, xid, ts, name, Op::Delete, before, None, key);
                }
                Message::Truncate { relations } => {
                    for id in relations {
                        let r = cache.get(id).map_err(dec)?;
                        let name = (r.schema.clone(), r.name.clone());
                        self.queue(lsn, xid, ts, name, Op::Truncate, None, None, None);
                    }
                }
                Message::Commit { end_lsn, .. } => {
                    if end_lsn > self.last_end {
                        self.last_end = end_lsn;
                        self.tx_ends.push_back((tx_last, end_lsn));
                    }
                    tx_last = 0;
                }
                Message::Ignored => {}
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Source for PostgresSource {
    async fn start(&mut self, start: Option<Position>) -> Result<()> {
        let client = self.connect().await?;
        self.client = Some(client);

        let slot = self.cfg.slot.clone();
        let mut recreated = false;
        let existing = self
            .client()?
            .query_opt(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .map_err(src_err)?;
        match existing {
            Some(row) => {
                let flushed: Option<String> = row.get(0);
                self.acked = flushed.as_deref().map(parse_lsn).transpose()?.unwrap_or(0);
            }
            None => {
                if let Some(lsn) = &start {
                    if !self.cfg.allow_slot_recreate {
                        return Err(Error::Source(format!(
                            "replication slot {slot:?} does not exist but a checkpoint exists (LSN {lsn}); \
                             changes made while the slot was missing would be lost. Restore the slot or \
                             set postgres.allow_slot_recreate = true to start a fresh stream"
                        )));
                    }
                    tracing::warn!(slot = %slot, checkpoint = %lsn, "slot missing; recreating (allow_slot_recreate)");
                }
                recreated = true;
                self.client()?
                    .execute(
                        "SELECT pg_create_logical_replication_slot($1::text::name, 'pgoutput')",
                        &[&slot],
                    )
                    .await
                    .map_err(src_err)?;
                tracing::info!(slot = %slot, "created replication slot");
            }
        }
        // The checkpoint can be ahead of the slot if we crashed between save and ack, and
        // the slot replays whole transactions: skip changes already delivered.
        // A recreated slot starts a new stream, so the old checkpoint no longer applies.
        if let (Some(pos), false) = (start, recreated) {
            match pos {
                Position::Lsn(l) => self.emitted = l,
                Position::Text(t) => {
                    return Err(Error::Checkpoint(format!(
                        "checkpoint {t:?} is not a Postgres LSN; it belongs to another source type"
                    )))
                }
            }
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<ChangeEvent>> {
        loop {
            if let Some(ev) = self.buffer.pop_front() {
                return Ok(Some(ev));
            }
            self.poll().await?;
            if self.buffer.is_empty() {
                tokio::time::sleep(self.cfg.poll_interval).await;
            }
        }
    }

    fn try_next(&mut self) -> Option<ChangeEvent> {
        self.buffer.pop_front()
    }

    async fn ack(&mut self, position: Position) -> Result<()> {
        match position {
            Position::Lsn(l) => self.advance_acked(l).await,
            Position::Text(t) => Err(Error::Source(format!("cannot ack non-LSN position {t:?}"))),
        }
    }
}

fn src_err(e: tokio_postgres::Error) -> Error {
    Error::Source(e.to_string())
}

/// Parse Postgres `X/Y` hex LSN text.
pub fn parse_lsn(s: &str) -> Result<u64> {
    let bad = || Error::Source(format!("invalid LSN {s:?}"));
    let (hi, lo) = s.split_once('/').ok_or_else(bad)?;
    let hi = u64::from_str_radix(hi, 16).map_err(|_| bad())?;
    let lo = u64::from_str_radix(lo, 16).map_err(|_| bad())?;
    Ok((hi << 32) | lo)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_roundtrip() {
        let l = parse_lsn("16/B374D848").unwrap();
        assert_eq!(l, 0x16_B374_D848);
        assert_eq!(Position::Lsn(l).to_string(), "16/B374D848");
        assert!(parse_lsn("garbage").is_err());
    }
}

/// Pops every transaction whose changes are all <= `lsn` and returns the commit end LSN
/// of the last one: the only position the slot can safely be advanced to.
fn ack_target(tx_ends: &mut VecDeque<(u64, u64)>, lsn: u64) -> Option<u64> {
    let mut target = None;
    while let Some(&(last, end)) = tx_ends.front() {
        if last > lsn {
            break;
        }
        target = Some(end);
        tx_ends.pop_front();
    }
    target
}

#[cfg(test)]
mod ack_tests {
    use super::*;

    #[test]
    fn ack_mid_transaction_does_not_advance() {
        let mut q = VecDeque::from([(100, 120), (200, 220)]);
        assert_eq!(ack_target(&mut q, 150), Some(120));
        // Acking inside the second transaction must not advance past its commit end.
        assert_eq!(ack_target(&mut q, 199), None);
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn ack_last_change_advances_to_commit_end() {
        let mut q = VecDeque::from([(100, 120), (200, 220)]);
        assert_eq!(ack_target(&mut q, 200), Some(220));
        assert!(q.is_empty());
    }
}

fn spawn_conn<S, T>(conn: tokio_postgres::Connection<S, T>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!(error = %e, "postgres connection closed");
        }
    });
}
