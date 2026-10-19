use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

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
    /// While idle, move the slot forward to the server's WAL position at most this often, so
    /// changes to tables outside the publication do not pin WAL on the primary. `None` turns it off.
    pub heartbeat_interval: Option<Duration>,
    /// Optional SQL run before each heartbeat (e.g. an upsert into a tiny published table), for
    /// servers where WAL position alone does not move the slot.
    pub heartbeat_action_query: Option<String>,
}

/// Default for [`PostgresSourceConfig::heartbeat_interval`].
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

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
            heartbeat_interval: Some(DEFAULT_HEARTBEAT_INTERVAL),
            heartbeat_action_query: None,
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
    /// Relations already logged as `REPLICA IDENTITY FULL`; every poll re-sends the Relation messages,
    /// so this lives on the source, not in the per-poll relation cache.
    full_identity_logged: HashSet<(String, String)>,
    /// Highest LSN known to be durably checkpointed: the start checkpoint, then every `ack`.
    checkpointed: u64,
    /// When the last heartbeat was attempted.
    last_heartbeat: Instant,
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
            full_identity_logged: HashSet::new(),
            checkpointed: 0,
            last_heartbeat: Instant::now(),
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

    /// Starts a heartbeat if one is due: runs the optional action query, then reads the server's
    /// WAL flush position. This must happen **before** the peek, so every transaction committed
    /// at or below the returned LSN is visible to that peek.
    async fn heartbeat_begin(&mut self) -> Result<Option<u64>> {
        let Some(every) = self.cfg.heartbeat_interval else {
            return Ok(None);
        };
        if self.last_heartbeat.elapsed() < every {
            return Ok(None);
        }
        self.last_heartbeat = Instant::now();
        if let Some(q) = &self.cfg.heartbeat_action_query {
            self.client()?.batch_execute(q).await.map_err(src_err)?;
        }
        let row = self
            .client()?
            .query_one(
                "SELECT (CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_flush_lsn() END)::text",
                &[],
            )
            .await
            .map_err(src_err)?;
        let text: Option<String> = row.get(0);
        text.as_deref().map(parse_lsn).transpose()
    }

    /// Finishes a heartbeat after the peek was processed. Advances the slot only when nothing
    /// is queued or awaiting a checkpoint; see [`heartbeat_targets`].
    async fn heartbeat_end(&mut self, wal: u64, drained: bool) -> Result<()> {
        let targets = heartbeat_targets(
            &mut self.tx_ends,
            drained && self.buffer.is_empty(),
            self.checkpointed,
            wal,
        );
        for t in targets {
            self.advance(t).await?;
        }
        Ok(())
    }

    async fn poll(&mut self) -> Result<()> {
        let heartbeat_wal = self.heartbeat_begin().await?;
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
        // `upto_nchanges` ends a capped peek at the first commit past the limit, possibly well
        // below `heartbeat_wal`. Only a peek that returned fewer rows than the cap saw everything.
        // A peek that reached the end can still return `batch_size` rows or more (it stops at a
        // commit boundary); that only delays the jump to `wal` by one interval, never skips data.
        let drained = i64::try_from(rows.len()).is_ok_and(|n| n < i64::from(self.cfg.batch_size));

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
                Message::Relation(rel) => {
                    if rel.identity_full()
                        && self
                            .full_identity_logged
                            .insert((rel.schema.clone(), rel.name.clone()))
                    {
                        tracing::info!(
                            table = %format!("{}.{}", rel.schema, rel.name),
                            "REPLICA IDENTITY FULL: with partition_by = key this table uses the table key (per-table order only)"
                        );
                    }
                    cache.insert(rel)
                }
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
                    let key = cache
                        .key_with_old(relation, &new, old.as_ref())
                        .map_err(dec)?;
                    let before = old
                        .map(|t| cache.row(relation, &t))
                        .transpose()
                        .map_err(dec)?;
                    let after = Some(cache.row(relation, &new).map_err(dec)?);
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
        if let Some(wal) = heartbeat_wal {
            self.heartbeat_end(wal, drained).await?;
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
                Position::Lsn(l) => {
                    self.emitted = l;
                    self.checkpointed = l;
                }
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
            Position::Lsn(l) => {
                self.checkpointed = self.checkpointed.max(l);
                self.advance_acked(l).await
            }
            Position::Text(t) => Err(Error::Source(format!("cannot ack non-LSN position {t:?}"))),
        }
    }
}

/// tokio-postgres' `Display` prints only the kind ("error performing TLS handshake"); the cause
/// (`UnknownIssuer`, "server does not support TLS", ...) is only reachable via `source()`.
/// A server error contributes its SQLSTATE and message only: DETAIL/HINT can carry row values
/// and would otherwise reach `last_error`, `/api/metrics` and the logs.
fn src_err(e: tokio_postgres::Error) -> Error {
    if let Some(db) = e.as_db_error() {
        return Error::Source(format!("{e}: {} {}", db.code().code(), db.message()));
    }
    Error::Source(with_causes(e.to_string(), std::error::Error::source(&e)))
}

/// Appends each cause in the `source()` chain to `msg`, skipping text already present.
fn with_causes(mut msg: String, mut cause: Option<&(dyn std::error::Error + 'static)>) -> String {
    while let Some(c) = cause {
        let text = c.to_string();
        if !msg.contains(&text) {
            msg.push_str(": ");
            msg.push_str(&text);
        }
        cause = c.source();
    }
    msg
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

    #[derive(Debug)]
    struct Layer(&'static str, Option<Box<Layer>>);

    impl std::fmt::Display for Layer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for Layer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_deref().map(|e| e as _)
        }
    }

    #[test]
    fn with_causes_appends_chain_and_skips_duplicates() {
        let root = Layer("root", None);
        let middle = Layer("middle", Some(Box::new(root)));
        let chain = Layer("outer", Some(Box::new(middle)));
        let msg = with_causes("handshake".into(), Some(&chain));
        assert_eq!(msg, "handshake: outer: middle: root");
        // A cause whose text is already in the message is not repeated.
        let dup = Layer("UnknownIssuer", None);
        let msg = with_causes("handshake: UnknownIssuer".into(), Some(&dup));
        assert_eq!(msg, "handshake: UnknownIssuer");
        assert_eq!(with_causes("x".into(), None), "x");
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

/// LSNs the slot may be advanced to, in order, for a heartbeat at WAL position `wal`.
///
/// 1. Transactions whose changes are all at or below `checkpointed` count as acknowledged:
///    that covers empty transactions (Postgres < 15 sends them for unpublished tables) and
///    replays of transactions already delivered before a restart. The slot may move to the
///    last such commit end.
/// 2. Only if the peek was drained (returned fewer rows than `batch_size`), nothing is queued and
///    no transaction is still waiting for its checkpoint may the slot move to `wal`. The caller
///    captured `wal` before the peek, so a drained peek contained every transaction committed at
///    or below it, all already acknowledged. A capped peek can end below `wal`, so it only gets
///    step 1.
///
/// An unacknowledged event therefore always holds the slot back (at-least-once).
fn heartbeat_targets(
    tx_ends: &mut VecDeque<(u64, u64)>,
    idle: bool,
    checkpointed: u64,
    wal: u64,
) -> Vec<u64> {
    let mut targets = Vec::new();
    if let Some(t) = ack_target(tx_ends, checkpointed) {
        targets.push(t);
    }
    if idle && tx_ends.is_empty() {
        targets.push(wal);
    }
    targets
}

#[cfg(test)]
mod heartbeat_tests {
    use super::*;

    #[test]
    fn idle_advances_to_wal() {
        let mut q = VecDeque::new();
        assert_eq!(heartbeat_targets(&mut q, true, 0, 500), vec![500]);
    }

    #[test]
    fn capped_peek_only_gets_the_checkpointed_target() {
        // Not idle (capped peek): the slot may reach the last checkpointed commit end, never `wal`.
        let mut q = VecDeque::from([(100, 120), (200, 220)]);
        assert_eq!(heartbeat_targets(&mut q, false, 100, 500), vec![120]);
        assert_eq!(q.len(), 1);
        let mut q = VecDeque::from([(0, 120), (0, 140)]);
        assert_eq!(heartbeat_targets(&mut q, false, 0, 500), vec![140]);
    }

    #[test]
    fn queued_events_hold_the_slot() {
        let mut q = VecDeque::new();
        assert!(heartbeat_targets(&mut q, false, 0, 500).is_empty());
    }

    #[test]
    fn delivered_but_uncheckpointed_transaction_holds_the_slot() {
        let mut q = VecDeque::from([(100, 120)]);
        assert!(heartbeat_targets(&mut q, true, 99, 500).is_empty());
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn checkpointed_transactions_are_acknowledged_first() {
        let mut q = VecDeque::from([(100, 120), (200, 220)]);
        assert_eq!(heartbeat_targets(&mut q, true, 200, 500), vec![220, 500]);
        assert!(q.is_empty());
    }

    #[test]
    fn partially_checkpointed_queue_only_moves_to_the_acked_commit_end() {
        let mut q = VecDeque::from([(100, 120), (200, 220)]);
        assert_eq!(heartbeat_targets(&mut q, true, 150, 500), vec![120]);
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn empty_transactions_do_not_pin_the_slot() {
        // (0, end): a transaction without published changes (Postgres < 15).
        let mut q = VecDeque::from([(0, 120), (0, 140)]);
        assert_eq!(heartbeat_targets(&mut q, true, 0, 500), vec![140, 500]);
    }

    #[test]
    fn empty_transaction_behind_unacked_data_stays_queued() {
        let mut q = VecDeque::from([(100, 120), (0, 140)]);
        assert!(heartbeat_targets(&mut q, true, 0, 500).is_empty());
        assert_eq!(q.len(), 2);
    }
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
