use std::collections::VecDeque;
use std::time::Duration;

use aqueduct_core::{ChangeEvent, Error, Lsn, Op, Result, Source};
use async_trait::async_trait;
use serde_json::{Map, Value};
use tokio_postgres::{Client, NoTls};

use crate::decoder::{decode, Message, RelationCache};

type Row = Map<String, Value>;

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
        }
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
}

impl PostgresSource {
    pub fn new(cfg: PostgresSourceConfig) -> Self {
        Self {
            cfg,
            client: None,
            buffer: VecDeque::new(),
            emitted: 0,
            acked: 0,
        }
    }

    fn client(&self) -> Result<&Client> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::Source("source not started".into()))
    }

    async fn advance(&mut self, lsn: u64) -> Result<()> {
        if lsn <= self.acked {
            return Ok(());
        }
        self.client()?
            .execute(
                "SELECT pg_replication_slot_advance($1::text::name, $2::text::pg_lsn)",
                &[&self.cfg.slot, &Lsn(lsn).to_string()],
            )
            .await
            .map_err(src_err)?;
        self.acked = lsn;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn queue(
        &mut self,
        lsn: Lsn,
        xid: u32,
        commit_ts_micros: i64,
        (schema, table): (String, String),
        op: Op,
        before: Option<Row>,
        after: Option<Row>,
    ) {
        if lsn.0 > self.emitted {
            self.emitted = lsn.0;
            self.buffer.push_back(ChangeEvent {
                lsn,
                xid,
                commit_ts_micros,
                schema,
                table,
                op,
                before,
                after,
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
        for row in rows {
            let lsn_text: String = row.get(0);
            let data: Vec<u8> = row.get(1);
            let lsn = parse_lsn(&lsn_text)?;
            let msg = decode(&data)
                .map_err(|e| Error::Source(format!("pgoutput decode at {lsn_text}: {e}")))?;
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
                    self.queue(lsn, xid, ts, name, Op::Insert, None, after);
                }
                Message::Update { relation, old, new } => {
                    let r = cache.get(relation).map_err(dec)?;
                    let name = (r.schema.clone(), r.name.clone());
                    let before = old
                        .map(|t| cache.row(relation, &t))
                        .transpose()
                        .map_err(dec)?;
                    let after = Some(cache.row(relation, &new).map_err(dec)?);
                    self.queue(lsn, xid, ts, name, Op::Update, before, after);
                }
                Message::Delete { relation, old } => {
                    let r = cache.get(relation).map_err(dec)?;
                    let name = (r.schema.clone(), r.name.clone());
                    let before = Some(cache.row(relation, &old).map_err(dec)?);
                    self.queue(lsn, xid, ts, name, Op::Delete, before, None);
                }
                Message::Truncate { relations } => {
                    for id in relations {
                        let r = cache.get(id).map_err(dec)?;
                        let name = (r.schema.clone(), r.name.clone());
                        self.queue(lsn, xid, ts, name, Op::Truncate, None, None);
                    }
                }
                Message::Commit { .. } | Message::Ignored => {}
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Source for PostgresSource {
    async fn start(&mut self, start: Option<Lsn>) -> Result<()> {
        let (client, conn) = tokio_postgres::connect(&self.cfg.url, NoTls)
            .await
            .map_err(src_err)?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::error!(error = %e, "postgres connection closed");
            }
        });
        self.client = Some(client);

        let slot = self.cfg.slot.clone();
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
                self.acked = flushed
                    .as_deref()
                    .map(parse_lsn)
                    .transpose()?
                    .map_or(0, |l| l.0);
            }
            None => {
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
        // The checkpoint can be ahead of the slot if we crashed between save and ack.
        if let Some(Lsn(l)) = start {
            self.advance(l).await?;
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

    async fn ack(&mut self, lsn: Lsn) -> Result<()> {
        self.advance(lsn.0).await
    }
}

fn src_err(e: tokio_postgres::Error) -> Error {
    Error::Source(e.to_string())
}

/// Parse Postgres `X/Y` hex LSN text.
pub fn parse_lsn(s: &str) -> Result<Lsn> {
    let bad = || Error::Source(format!("invalid LSN {s:?}"));
    let (hi, lo) = s.split_once('/').ok_or_else(bad)?;
    let hi = u64::from_str_radix(hi, 16).map_err(|_| bad())?;
    let lo = u64::from_str_radix(lo, 16).map_err(|_| bad())?;
    Ok(Lsn((hi << 32) | lo))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_roundtrip() {
        let l = parse_lsn("16/B374D848").unwrap();
        assert_eq!(l, Lsn(0x16_B374_D848));
        assert_eq!(l.to_string(), "16/B374D848");
        assert!(parse_lsn("garbage").is_err());
    }
}
