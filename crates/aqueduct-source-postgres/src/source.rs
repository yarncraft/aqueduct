use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aqueduct_core::{ChangeEvent, Error, Metrics, Op, Position, Result, Source};
use async_trait::async_trait;
use serde_json::value::RawValue;
use tokio_postgres::{Client, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;

use aqueduct_core::{Checkpoint, SnapState, SnapshotCommand, SnapshotReceiver, SnapshotStatus};

use crate::azure::{AccessToken, AzureCredential};
use crate::catalog::read_catalog;
use crate::decoder::{Message, RelationCache};
use crate::snapshot::{
    new_snapshot_id, table_id, table_watermark, Closed, Engine, Live, SnapshotConfig, SnapshotMode,
    MIN_SERVER_VERSION,
};
use crate::tls::{self, SslMode};
use crate::transport::{StreamTransport, Transport};

type Row = Box<RawValue>;

/// Resume key of one change: `(commit LSN, change LSN, ord)`.
///
/// Change LSNs alone are neither unique (a COPY's rows and a multi-table TRUNCATE share one WAL
/// record, so one LSN) nor commit-ordered (transactions overlap). Transactions arrive in commit
/// order and changes within one in WAL order, so the stream is strictly increasing in this key.
/// `ord` is the index among the changes of one transaction that share a `change` LSN; decoding the
/// same WAL record again gives the same order. `commit` is `Begin.final_lsn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Key {
    pub commit: u64,
    pub change: u64,
    pub ord: u32,
}

impl Key {
    /// Checkpoint encoding: `<commit>@<change>#<ord>`, decimal (written by [`encode_checkpoint`]).
    #[cfg(test)]
    pub fn encode(&self) -> String {
        encode_checkpoint(*self, None)
    }

    pub fn parse(s: &str) -> Option<Key> {
        let (commit, rest) = s.split_once('@')?;
        let (change, ord) = rest.split_once('#')?;
        Some(Key {
            commit: commit.parse().ok()?,
            change: change.parse().ok()?,
            ord: ord.parse().ok()?,
        })
    }
}

/// The database a checkpoint was written against: the cluster's `system_identifier` (set by
/// `initdb`, so it is the same on physical replicas and on restores of that cluster), its
/// timeline (a new one after every promotion and PITR) and the database OID within it. `system`
/// and `timeline` are `None` with `skip_system_check`; the database is always known, because
/// `current_database()` needs no privilege.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Origin {
    system: Option<u64>,
    timeline: Option<u32>,
    database: u32,
}

/// What a text checkpoint records about its origin. Checkpoints written before #63 have none of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Stamp {
    system: Option<u64>,
    timeline: Option<u32>,
    database: Option<u32>,
}

/// Checkpoint text: the [`Key`], then the `;name=value` fields (`sys`, `db`, `tli`) of the origin
/// that is known.
fn encode_checkpoint(key: Key, origin: Option<Origin>) -> String {
    use std::fmt::Write;
    // Built for every event, so one allocation, sized for the longest form.
    let mut t = String::with_capacity(96);
    let _ = write!(t, "{}@{}#{}", key.commit, key.change, key.ord);
    if let Some(o) = origin {
        if let Some(sys) = o.system {
            let _ = write!(t, ";sys={sys}");
        }
        let _ = write!(t, ";db={}", o.database);
        if let Some(tli) = o.timeline {
            let _ = write!(t, ";tli={tli}");
        }
    }
    t
}

/// Parses checkpoint text. An unknown or repeated field makes the whole checkpoint invalid, so a
/// format this version does not understand is refused rather than half-read.
fn parse_checkpoint(t: &str) -> Option<(Key, Stamp)> {
    let mut parts = t.split(';');
    let key = Key::parse(parts.next()?)?;
    let mut stamp = Stamp::default();
    for field in parts {
        let (name, value) = field.split_once('=')?;
        match name {
            "sys" if stamp.system.is_none() => stamp.system = Some(value.parse().ok()?),
            "db" if stamp.database.is_none() => stamp.database = Some(value.parse().ok()?),
            "tli" if stamp.timeline.is_none() => stamp.timeline = Some(value.parse().ok()?),
            _ => return None,
        }
    }
    Some((key, stamp))
}

/// Parses a text checkpoint; anything that is not a Postgres checkpoint belongs to another source
/// type (or to a newer Aqueduct version).
fn key_from_checkpoint(t: &str) -> Result<(Key, Stamp)> {
    parse_checkpoint(t).ok_or_else(|| {
        Error::Checkpoint(format!(
            "checkpoint {t:?} is not a Postgres checkpoint; it belongs to another source type"
        ))
    })
}

/// Splits checkpoint text into the resume key, its origin stamp and the snapshot progress it
/// carries (ADR 0001 §4: `{"pos": <key text>, "snap": {...}}` while a snapshot runs).
fn split_checkpoint(t: &str) -> Result<(Key, Stamp, Option<SnapState>)> {
    let c = Checkpoint::decode(t).map_err(Error::Checkpoint)?;
    let (key, stamp) = key_from_checkpoint(&c.pos)?;
    Ok((key, stamp, c.snap))
}

/// What to do after a restore, PITR or failover of the same cluster, which cannot be told apart.
const RESYNC: &str = "The cluster was restored, recovered to a point in time (PITR) or failed \
     over since the checkpoint was written, so changes between the server's WAL end and the \
     checkpoint may never reach Kafka. Resync the consumers: remove the checkpoint and take a \
     snapshot. If you have verified that nothing was lost, for example after a planned \
     switchover with no lag, delete only the `;tli=` field from the checkpoint";

/// Refuses a checkpoint written against another cluster (a different `initdb`: a re-pointed URL
/// or a rebuilt primary), another timeline of the same cluster (a promotion or PITR since) or
/// another database of the same cluster. A restore that stays on the same timeline is only
/// caught by the checkpoint-ahead-of-WAL check in `start`.
///
/// With `skip_system_check` the server's cluster and timeline are unknown and only the database
/// is compared.
fn check_origin(saved: Stamp, server: Origin) -> Result<()> {
    if let (Some(sys), Some(server_sys)) = (saved.system, server.system) {
        if sys != server_sys {
            return Err(Error::Checkpoint(format!(
                "checkpoint was written against Postgres cluster {sys}, but this server is cluster {server_sys}. \
                 The URL points at a different or rebuilt cluster (pg_upgrade to a new major \
                 version also makes a new one). Point it back, or remove the checkpoint to start over"
            )));
        }
        if let (Some(tli), Some(server_tli)) = (saved.timeline, server.timeline) {
            if tli != server_tli {
                return Err(Error::Checkpoint(format!(
                    "checkpoint was written on timeline {tli} of this cluster, but the server is \
                     on timeline {server_tli}. {RESYNC}"
                )));
            }
        }
    }
    if let Some(db) = saved.database {
        if db != server.database {
            return Err(Error::Checkpoint(format!(
                "checkpoint was written against database OID {db}, but this connection is to \
                 database OID {} on the same cluster. Point the URL back at that database, or \
                 remove the checkpoint to start over",
                server.database
            )));
        }
    }
    Ok(())
}

/// A checkpoint ahead of the server's WAL. Treating it as acknowledged would move the slot past
/// data that was never delivered, so it is refused. `same_cluster` is true when the checkpoint
/// records this cluster's `system_identifier`: then only a restore, PITR or failover explains it.
fn ahead_of_wal(lsn: u64, wal: u64, same_cluster: bool) -> Error {
    let cause = if same_cluster {
        RESYNC
    } else {
        "It belongs to another cluster, or this cluster was restored, recovered to a point in \
         time or failed over since, and then changes after the server's WAL end may never reach \
         Kafka. Remove the checkpoint to start over, and resync the consumers"
    };
    Error::Checkpoint(format!(
        "checkpoint LSN {} is ahead of this server's WAL position {}. {cause}",
        Position::Lsn(lsn),
        Position::Lsn(wal)
    ))
}

/// The WARN logged when a checkpoint lacks part of the origin and the next save will record this
/// server's: nothing has verified that this is the server the checkpoint came from, so the first
/// start after an upgrade is protected only by the checkpoint-ahead-of-WAL check. `None` when the
/// checkpoint already records everything that is known.
fn stamp_warning(saved: Stamp, server: Origin) -> Option<String> {
    let new_sys = server.system.filter(|_| saved.system.is_none());
    let new_db = saved.database.is_none();
    let new_tli = server.timeline.filter(|_| saved.timeline.is_none());
    if new_sys.is_none() && !new_db && new_tli.is_none() {
        return None;
    }
    let mut fields = Vec::new();
    if let Some(sys) = new_sys {
        fields.push(format!("sys={sys}"));
    }
    if new_db {
        fields.push(format!("db={}", server.database));
    }
    if let Some(tli) = new_tli {
        fields.push(format!("tli={tli}"));
    }
    Some(format!(
        "checkpoint does not record which cluster or database it belongs to (written before this \
         check existed, or with checkpoint_skip_system_check); the next checkpoint records {} as \
         its origin. Only the checkpoint-ahead-of-WAL check confirmed this is the right server; \
         if the URL was changed, stop now and remove the checkpoint",
        fields.join(" ")
    ))
}

/// Refuses an existing slot of the configured name that does not belong to the connected
/// database (a logical slot of another database, or a physical slot). Such a slot is someone
/// else's: it is never used, dropped or recreated, whatever `allow_slot_recreate` says.
fn check_slot_database(slot: &str, slot_db: Option<&str>, current: &str) -> Result<()> {
    match slot_db {
        Some(db) if db == current => Ok(()),
        Some(db) => Err(Error::Source(format!(
            "replication slot {slot:?} belongs to database {db:?}, not {current:?}. It was left \
             untouched; use another slot name, or point the URL at {db:?}"
        ))),
        None => Err(Error::Source(format!(
            "replication slot {slot:?} is a physical slot, not a logical slot of database \
             {current:?}. It was left untouched; use another slot name"
        ))),
    }
}

/// What decoding carries from one fetch to the next. A fetch can end in the middle of a
/// transaction, so the relation cache, the transaction's keys and its xid and commit time live on
/// the source.
#[derive(Default)]
struct DecodeState {
    cache: RelationCache,
    xid: u32,
    ts: i64,
    keys: TxKeys,
}

/// Assigns [`Key`]s to the changes of the transaction being decoded.
#[derive(Default)]
struct TxKeys {
    commit: u64,
    last: Option<Key>,
}

impl TxKeys {
    fn begin(&mut self, commit: u64) {
        *self = TxKeys { commit, last: None };
    }

    fn next(&mut self, change: u64) -> Key {
        let ord = match self.last {
            Some(k) if k.change == change => k.ord + 1,
            _ => 0,
        };
        let key = Key {
            commit: self.commit,
            change,
            ord,
        };
        self.last = Some(key);
        key
    }

    /// Key of the transaction's last change; `None` for a transaction without published changes.
    fn end_key(&self) -> Option<Key> {
        self.last
    }
}

#[derive(Clone)]
pub struct PostgresSourceConfig {
    /// libpq-style connection string / URL.
    pub url: String,
    /// Logical replication slot; created (pgoutput) if missing.
    pub slot: String,
    /// Existing publication to stream.
    pub publication: String,
    /// How long a fetch waits for the first change before the source turns over (heartbeats,
    /// snapshot chunks, admin requests).
    pub poll_interval: Duration,
    /// Max messages taken per fetch.
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
    /// Skip reading, recording and checking the cluster's `system_identifier` and timeline. Only for servers
    /// that refuse `pg_control_system()`; without it such a server fails at startup. The database
    /// check stays on.
    pub skip_system_check: bool,
    /// Ask the slot for logical messages (`'messages', 'true'`), which snapshots use as watermarks.
    /// Needs Postgres 14+. Off by default; any snapshot mode but `Never` turns it on.
    pub logical_messages: bool,
    /// Snapshots of existing rows (ADR 0001). Off by default.
    pub snapshot: SnapshotConfig,
    /// Bytes of decoded-but-not-taken messages to hold before reading pauses.
    pub stream_buffer_bytes: usize,
    /// How every connection authenticates. Default: the URL's password.
    pub auth: SourceAuth,
}

/// The URL's password is redacted: `Debug` output can end up in logs.
impl std::fmt::Debug for PostgresSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSourceConfig")
            .field("url", &aqueduct_core::redact_password(&self.url))
            .field("slot", &self.slot)
            .field("publication", &self.publication)
            .field("poll_interval", &self.poll_interval)
            .field("batch_size", &self.batch_size)
            .field("allow_slot_recreate", &self.allow_slot_recreate)
            .field("ssl_mode", &self.ssl_mode)
            .field("ssl_root_cert", &self.ssl_root_cert)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("heartbeat_action_query", &self.heartbeat_action_query)
            .field("skip_system_check", &self.skip_system_check)
            .field("logical_messages", &self.logical_messages)
            .field("snapshot", &self.snapshot)
            .field("stream_buffer_bytes", &self.stream_buffer_bytes)
            .field("auth", &self.auth)
            .finish()
    }
}

/// How the source signs in to Postgres.
#[derive(Debug, Clone, Default)]
pub enum SourceAuth {
    /// The password in the URL, if any.
    #[default]
    Password,
    /// A Microsoft Entra access token from an Azure managed identity is the password of every
    /// connection, the SQL session and the replication connection alike (#140).
    Azure(AzureCredential),
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
            skip_system_check: false,
            logical_messages: false,
            snapshot: SnapshotConfig::default(),
            stream_buffer_bytes: crate::replication::DEFAULT_STREAM_BUFFER_BYTES,
            auth: SourceAuth::Password,
        }
    }

    /// Fails closed for managed identity sign-in: the URL must name the identity's role and
    /// carry no password, and the token is only sent under `verify-full` (the default): a
    /// verified chain without the hostname check would hand the bearer token to anyone with a
    /// certificate from a trusted CA. Errors never show the URL.
    pub fn check_auth(&self) -> Result<()> {
        let SourceAuth::Azure(cred) = &self.auth else {
            return Ok(());
        };
        let name = cred.kind().name();
        let parsed = tls::parse(&self.url)?;
        if parsed.config.get_password().is_some() {
            return Err(Error::Config(format!(
                "postgres auth = \"{name}\" signs in with a Microsoft Entra token as the \
                 password, but the postgres url also carries a password; remove it from the url"
            )));
        }
        if parsed.config.get_user().is_none() {
            return Err(Error::Config(format!(
                "postgres auth = \"{name}\" needs the url user: the Postgres role of the \
                 managed identity (its name)"
            )));
        }
        let mode = self.ssl_mode.or(parsed.url_mode).unwrap_or_default();
        if mode != SslMode::VerifyFull {
            return Err(Error::Config(format!(
                "postgres auth = \"{name}\" sends an access token as the password, so the \
                 server certificate and hostname must be verified; ssl_mode = \"{}\" does not. \
                 Use \"verify-full\" (the default)",
                mode.as_str(),
            )));
        }
        Ok(())
    }

    /// See [`PostgresSourceConfig::allow_slot_recreate`].
    pub fn allow_slot_recreate(mut self, allow: bool) -> Self {
        self.allow_slot_recreate = allow;
        self
    }
}

/// Reads a pgoutput logical slot over a replication connection (`START_REPLICATION`, ADR 0003).
///
/// Changes arrive as they commit, and the slot is confirmed in [`Source::ack`] by the
/// connection's status updates, so a crash before ack replays the change. Replays after a
/// restart may re-deliver the remainder of a transaction (at-least-once).
pub struct PostgresSource {
    cfg: PostgresSourceConfig,
    client: Option<Arc<Client>>,
    /// Where decoded changes come from; set by `start`.
    transport: Option<Box<dyn Transport>>,
    /// The running snapshot, if any.
    snapshot: Option<Engine>,
    /// Snapshot requests from the admin API.
    control: Option<SnapshotReceiver>,
    /// Decoding state carried across fetches.
    decode: DecodeState,
    /// Status of the last snapshot that ended, kept for the admin API.
    last_status: Option<SnapshotStatus>,
    buffer: VecDeque<ChangeEvent>,
    /// Highest key already queued; a replay (after a reconnect) skips changes at or below it.
    emitted: Key,
    /// Highest LSN the slot has been advanced to.
    acked: u64,
    /// Completed transactions as (key of the last change, commit end LSN), in commit order. The
    /// slot can only be advanced to a commit end LSN: advancing to a mid-transaction change LSN
    /// leaves `confirmed_flush_lsn` unmoved and the whole transaction is replayed.
    tx_ends: VecDeque<(Key, u64)>,
    /// Highest commit end LSN recorded in `tx_ends`.
    last_end: u64,
    /// Relations already logged as `REPLICA IDENTITY FULL`; every poll re-sends the Relation messages,
    /// so this lives on the source, not in the per-poll relation cache.
    full_identity_logged: HashSet<(String, String)>,
    /// Primary-key columns per published `schema.table` (empty without one), from the
    /// catalog: the record key, as Debezium keys records, whatever the replica identity.
    pks: HashMap<String, Vec<String>>,
    /// Highest key known to be durably checkpointed: the start checkpoint, then every `ack`.
    checkpointed: Key,
    /// When the last heartbeat was attempted.
    last_heartbeat: Instant,
    /// Where heartbeats and slot lag are reported, if the host wants them.
    metrics: Option<Arc<Metrics>>,
    /// Reads the slot stats and the WAL head once a second, independently of the pipeline loop,
    /// so the cockpit sees the head move while the sink holds the pipeline up (#150).
    stats_task: Option<tokio::task::JoinHandle<()>>,
    /// The connected cluster and database, stamped on every checkpoint. `None` before `start`.
    origin: Option<Origin>,
}

/// The slot's `confirmed_flush_lsn`, its lag behind the WAL head and the WAL head (write
/// position). On a standby the head is the replay position. The head row comes back even without
/// the slot.
const SLOT_STATS_SQL: &str = "SELECT s.confirmed_flush_lsn::text, \
     pg_wal_lsn_diff(h.lsn, s.confirmed_flush_lsn)::int8, h.lsn::text \
     FROM (SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() \
     ELSE pg_current_wal_lsn() END AS lsn) h \
     LEFT JOIN pg_replication_slots s ON s.slot_name = $1";

/// How long a transport waits for a slot another connection still holds.
pub(crate) const SLOT_BUSY_RETRY: Duration = Duration::from_secs(15);

/// How often the slot lag and the WAL head are read (one cheap query on the SQL connection).
/// Once a second, so the cockpit's backlog does not trail a fast catch-up by seconds.
const SLOT_STATS_EVERY: Duration = Duration::from_secs(1);

/// One read of [`SLOT_STATS_SQL`] into `metrics`. Telemetry only: a failure clears the slot
/// values and never stops the stream. The query is a single statement, and nothing on the SQL
/// connection runs inside an explicit transaction, so it can share the connection with the source.
async fn read_slot_stats(client: &Client, slot: &str, metrics: &Metrics) {
    match client.query_one(SLOT_STATS_SQL, &[&slot]).await {
        Ok(row) => {
            let flush: Option<String> = row.get(0);
            let lag: Option<i64> = row.get(1);
            let lsn = |i: usize| {
                row.get::<_, Option<String>>(i)
                    .as_deref()
                    .and_then(|t| parse_lsn(t).ok())
            };
            metrics.set_slot(lag.map(|n| n.max(0) as u64), flush);
            if let Some(h) = lsn(2) {
                metrics.report_source_head_lsn(h);
            }
        }
        Err(e) => {
            tracing::debug!(error = %e, "slot lag query failed");
            metrics.set_slot(None, None);
        }
    }
}

impl Drop for PostgresSource {
    fn drop(&mut self) {
        if let Some(t) = self.stats_task.take() {
            t.abort();
        }
    }
}

impl PostgresSource {
    pub fn new(cfg: PostgresSourceConfig) -> Self {
        Self {
            cfg,
            client: None,
            transport: None,
            snapshot: None,
            control: None,
            decode: DecodeState::default(),
            last_status: None,
            buffer: VecDeque::new(),
            emitted: Key::default(),
            acked: 0,
            tx_ends: VecDeque::new(),
            last_end: 0,
            full_identity_logged: HashSet::new(),
            pks: HashMap::new(),
            checkpointed: Key::default(),
            last_heartbeat: Instant::now(),
            metrics: None,
            stats_task: None,
            origin: None,
        }
    }

    /// The current database's OID and, unless `skip_system_check`, the connected cluster's
    /// `system_identifier`.
    async fn read_origin(&self) -> Result<Origin> {
        let database: i64 = self
            .client()?
            .query_one(
                "SELECT oid::int8 FROM pg_database WHERE datname = current_database()",
                &[],
            )
            .await
            .map_err(src_err)?
            .get(0);
        let database = u32::try_from(database)
            .map_err(|_| Error::Source(format!("invalid database OID {database}")))?;
        if self.cfg.skip_system_check {
            return Ok(Origin {
                system: None,
                timeline: None,
                database,
            });
        }
        let row = self
            .client()?
            .query_one(
                "SELECT s.system_identifier::text, c.timeline_id::int8 \
                 FROM pg_control_system() s, pg_control_checkpoint() c",
                &[],
            )
            .await
            .map_err(|e| {
                Error::Source(format!(
                    "{}. Aqueduct reads pg_control_system() and pg_control_checkpoint() to tell \
                     which cluster and timeline a checkpoint belongs to; grant them, or set \
                     postgres.checkpoint_skip_system_check = true",
                    src_err(e)
                ))
            })?;
        let system: String = row.get(0);
        let timeline: i64 = row.get(1);
        let system = system
            .parse()
            .map_err(|_| Error::Source(format!("invalid system_identifier {system:?}")))?;
        let timeline = u32::try_from(timeline)
            .map_err(|_| Error::Source(format!("invalid timeline {timeline}")))?;
        Ok(Origin {
            system: Some(system),
            timeline: Some(timeline),
            database,
        })
    }

    /// Take snapshot requests (start, cancel) from the admin API.
    pub fn with_snapshot_control(mut self, control: SnapshotReceiver) -> Self {
        self.control = Some(control);
        self
    }

    /// Reports the running or last snapshot to the metrics.
    fn publish_status(&self) {
        if let Some(m) = &self.metrics {
            let status = match &self.snapshot {
                Some(e) => Some(e.status("running")),
                None => self.last_status.clone(),
            };
            m.set_snapshot(status);
        }
    }

    /// Answers the admin API's snapshot requests. Runs between batches, never while a chunk is
    /// half set up.
    async fn handle_requests(&mut self) -> Result<()> {
        loop {
            let Some(req) = self.control.as_mut().and_then(|c| c.try_recv().ok()) else {
                return Ok(());
            };
            let answer = match req.command {
                SnapshotCommand::Start(tables) => self.request_snapshot(tables).await,
                SnapshotCommand::Cancel => self.cancel_snapshot(),
            };
            let _ = req.reply.send(answer);
            self.publish_status();
        }
    }

    /// Starts a snapshot of `tables`, or appends them to the running one. The answer is the
    /// snapshot id. The request is durable only once an event carrying the snapshot is
    /// checkpointed; a crash before that forgets it.
    async fn request_snapshot(
        &mut self,
        tables: Vec<String>,
    ) -> std::result::Result<String, String> {
        if self.cfg.snapshot.mode == SnapshotMode::Never {
            return Err(
                "snapshots are off; set postgres.snapshot = \"on_demand\" or \"initial\"".into(),
            );
        }
        let client = self.client().map_err(|e| e.to_string())?;
        let catalog = read_catalog(client, &self.cfg.publication)
            .await
            .map_err(|e| e.to_string())?;
        let published: Vec<String> = catalog
            .tables
            .iter()
            .map(|t| table_id(&t.schema, &t.name))
            .collect();
        let unknown: Vec<&str> = tables
            .iter()
            .filter(|t| {
                !published.contains(t) || self.cfg.snapshot.watermark_table.as_ref() == Some(t)
            })
            .map(String::as_str)
            .collect();
        if !unknown.is_empty() {
            return Err(format!(
                "not in publication {:?}: {}",
                self.cfg.publication,
                unknown.join(", ")
            ));
        }
        if let Some(e) = self.snapshot.as_mut() {
            if e.cancelled() {
                // Its last chunk is still in flight; replacing it now would drop those rows,
                // which the cancel promised to deliver.
                return Err(
                    "a cancelled snapshot is finishing its last chunk; try again in a moment"
                        .into(),
                );
            }
            e.append(tables, &catalog, &self.cfg.snapshot);
            return Ok(e.id().to_owned());
        }
        let id = new_snapshot_id();
        let state = SnapState {
            id: id.clone(),
            tables,
            table: 0,
            after: None,
        };
        match Engine::new(&self.cfg.slot, state, &catalog, &self.cfg.snapshot) {
            Some(e) => {
                tracing::info!(snapshot = %id, tables = e.state().tables.len(), "snapshot requested");
                self.snapshot = Some(e);
                Ok(id)
            }
            None => Err("none of these tables can be snapshotted (see the log for why)".into()),
        }
    }

    /// Stops the running snapshot after the chunk in flight.
    fn cancel_snapshot(&mut self) -> std::result::Result<String, String> {
        let Some(e) = self.snapshot.as_mut() else {
            return Err("no snapshot is running".into());
        };
        e.cancel();
        let id = e.id().to_owned();
        if !e.in_flight() {
            // Nothing in flight: end now, and return the checkpoint to the plain form.
            let checkpoint = self.checkpoint_text(self.emitted, None);
            let mut hb = ChangeEvent::heartbeat(Position::Lsn(self.emitted.change), 0);
            hb.checkpoint = Some(Position::Text(checkpoint));
            self.buffer.push_back(hb);
            self.end_snapshot();
        }
        Ok(id)
    }

    /// Forgets the running snapshot, keeping its final status for the admin API.
    fn end_snapshot(&mut self) {
        if let Some(e) = self.snapshot.take() {
            let state = if e.cancelled() {
                "cancelled"
            } else {
                "finished"
            };
            let st = e.status(state);
            tracing::info!(snapshot = %st.id, rows = st.rows, "snapshot {state}");
            self.last_status = Some(st);
        }
        self.publish_status();
    }

    /// Opens the replication connection for the stream transport, after the slot exists. It
    /// must reach the same cluster as the SQL connection (the URL could resolve elsewhere with
    /// several hosts), and it starts at the slot's confirmed position. Status updates go out
    /// well within the server's `wal_sender_timeout`, also while the sink holds up reading.
    async fn open_stream(&mut self) -> Result<StreamTransport> {
        use crate::replication::{
            ConnectOptions, ReplicationClient, StartReplication, StreamOptions,
        };
        let timeout_ms: i64 = self
            .client()?
            .query_one(
                "SELECT setting::int8 FROM pg_settings WHERE name = 'wal_sender_timeout'",
                &[],
            )
            .await
            .map_err(src_err)?
            .get(0);
        // A previous process's walsender can hold the slot for a moment after it went away
        // (55006, "slot is active"): try again for a while before giving up.
        let deadline = Instant::now() + SLOT_BUSY_RETRY;
        let (encrypted, stream, status) = loop {
            let mut client = ReplicationClient::connect(
                &self.cfg.url,
                &ConnectOptions {
                    ssl_mode: self.cfg.ssl_mode,
                    ssl_root_cert: self.cfg.ssl_root_cert.clone(),
                    password: self.access_token().await?,
                },
            )
            .await?;
            let encrypted = client.encrypted();
            let system = client.identify_system().await?;
            if let Some(sys) = self.origin.and_then(|o| o.system) {
                if system.system_id != sys.to_string() {
                    return Err(Error::Config(format!(
                        "the replication connection reached cluster {}, the SQL connection cluster {}; {}",
                        system.system_id, sys, "point the URL at one server"
                    )));
                }
            }
            let started = client
                .start_replication(&StartReplication {
                    slot: &self.cfg.slot,
                    start_lsn: 0,
                    publication: &self.cfg.publication,
                    logical_messages: self.cfg.logical_messages,
                })
                .await;
            match started {
                Ok((stream, status)) => break (encrypted, stream, status),
                Err(e) if e.to_string().contains("(55006)") && Instant::now() < deadline => {
                    tracing::info!(slot = %self.cfg.slot, "slot still held by a previous connection; retrying");
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Err(e) => return Err(e),
            }
        };
        let opts = StreamOptions {
            buffer_bytes: self.cfg.stream_buffer_bytes,
            status_interval: status_interval(timeout_ms),
        };
        tracing::info!(
            slot = %self.cfg.slot,
            encrypted,
            status_every_ms = opts.status_interval.as_millis() as u64,
            "streaming from the replication connection"
        );
        Ok(StreamTransport::new(
            crate::replication::ReplicationHandle::spawn(stream, status, opts),
            usize::try_from(self.cfg.batch_size).unwrap_or(1000).max(1),
            self.cfg.poll_interval,
        ))
    }

    /// Report heartbeats and slot lag to `metrics`.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn skip_heartbeat(&self, reason: String) {
        tracing::warn!("{reason}; skipping this heartbeat");
        if let Some(m) = &self.metrics {
            m.record_heartbeat_skip(reason);
        }
    }

    /// Starts (or restarts) the task that reads the slot stats and the WAL head every
    /// [`SLOT_STATS_EVERY`], when metrics are wanted.
    fn spawn_stats(&mut self) {
        if let Some(t) = self.stats_task.take() {
            t.abort();
        }
        let (Some(metrics), Some(client)) = (self.metrics.clone(), self.client.clone()) else {
            return;
        };
        let slot = self.cfg.slot.clone();
        self.stats_task = Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(SLOT_STATS_EVERY);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                read_slot_stats(&client, &slot, &metrics).await;
            }
        }));
    }

    /// Everything up to `lsn` was delivered, or holds nothing to deliver (#150).
    fn report_delivered(&self, lsn: u64) {
        if let (Some(m), true) = (&self.metrics, lsn > 0) {
            m.report_source_delivered_lsn(lsn);
        }
    }

    /// The password for a new connection when it is not the URL's: a managed identity's
    /// access token, cached until shortly before it expires.
    async fn access_token(&self) -> Result<Option<AccessToken>> {
        match &self.cfg.auth {
            SourceAuth::Password => Ok(None),
            SourceAuth::Azure(cred) => cred.token().await.map(Some),
        }
    }

    /// Open the SQL connection according to the TLS policy.
    async fn connect(&self) -> Result<Client> {
        let parsed = tls::parse(&self.cfg.url)?;
        let mode = self.cfg.ssl_mode.or(parsed.url_mode).unwrap_or_default();
        let root = self.cfg.ssl_root_cert.clone().or(parsed.url_root_cert);
        let local = tls::is_local(&parsed.config);
        let mut pg = parsed.config;
        tls::apply_connection_defaults(&mut pg);
        pg.ssl_mode(tls::dial_mode(mode, local).pg());
        if let Some(token) = self.access_token().await? {
            pg.password(token.secret());
        }

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

        // Time values are converted from their text output, which must be in ISO style; the
        // snapshot chunk reads run in this session.
        client
            .batch_execute("SET DateStyle TO ISO")
            .await
            .map_err(src_err)?;
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
            .as_deref()
            .ok_or_else(|| Error::Source("source not started".into()))
    }

    fn transport(&mut self) -> Result<&mut Box<dyn Transport>> {
        self.transport
            .as_mut()
            .ok_or_else(|| Error::Source("source not started".into()))
    }

    /// Advance the slot past transactions that produced no event, or only events that are already
    /// checkpointed: empty ones (Postgres < 15 sends them for unpublished tables), ones holding only
    /// another tool's logical messages or our watermarks, and replays after a restart. Nothing
    /// else would release them before the next ack or heartbeat; with heartbeats off the slot
    /// would never move again.
    /// This is heartbeat step 1 ([`heartbeat_targets`]), so an unacknowledged event still holds
    /// the slot.
    async fn advance_settled(&mut self) -> Result<()> {
        self.advance_acked(self.checkpointed).await
    }

    /// Advance the slot past every fully acknowledged transaction (all changes <= `key`).
    async fn advance_acked(&mut self, key: Key) -> Result<()> {
        if let Some(target) = ack_target(&mut self.tx_ends, key) {
            self.advance(target).await?;
        }
        Ok(())
    }

    async fn advance(&mut self, lsn: u64) -> Result<()> {
        if lsn <= self.acked {
            return Ok(());
        }
        self.transport()?.confirm(lsn).await?;
        self.acked = lsn;
        self.report_delivered(lsn);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn queue(
        &mut self,
        key: Key,
        xid: u32,
        commit_ts_micros: i64,
        (schema, table): (String, String),
        op: Op,
        before: Option<Row>,
        after: Option<Row>,
        row_key: Option<String>,
        unchanged: Vec<String>,
    ) -> bool {
        let snap = self.snapshot.as_ref().map(|e| e.state().clone());
        let checkpoint = self.checkpoint_text(key, snap);
        self.push(
            key,
            ChangeEvent {
                position: Position::Lsn(key.change),
                xid,
                commit_ts_micros,
                schema,
                table,
                op,
                before,
                after,
                key: row_key,
                checkpoint: Some(Position::Text(checkpoint)),
                unchanged,
                after_is_key: false,
            },
        )
    }

    /// Queues `ev` unless a change with this key was queued before (a reconnected stream replays
    /// from the slot's confirmed position). Returns whether it was queued.
    fn push(&mut self, key: Key, ev: ChangeEvent) -> bool {
        if key <= self.emitted {
            return false;
        }
        self.emitted = key;
        self.buffer.push_back(ev);
        true
    }

    /// Checkpoint text for `key`: the key with its origin, wrapped with the snapshot progress
    /// while a snapshot runs, so both are saved in one atomic write.
    fn checkpoint_text(&self, key: Key, snap: Option<SnapState>) -> String {
        Checkpoint {
            pos: encode_checkpoint(key, self.origin),
            snap,
        }
        .encode()
    }

    /// Queues what closing a snapshot chunk produced, inside the high watermark's transaction:
    /// each row gets the next key at the watermark's LSN, so the rows sort after every live change
    /// of the window and the transaction's commit end covers them. Rows before the last carry the
    /// progress from before the chunk (a crash part-way reads the chunk again: duplicates only),
    /// the last row the new progress. A chunk without rows saves its progress with a heartbeat.
    fn queue_closed(
        &mut self,
        mut closed: Closed,
        before: SnapState,
        lsn: u64,
        keys: &mut TxKeys,
        xid: u32,
        ts: i64,
    ) {
        // A cancelled snapshot ends with this chunk: its rows are delivered, then the checkpoint
        // returns to the plain form.
        if self.snapshot.as_ref().is_some_and(|e| e.cancelled()) {
            closed.state = None;
        }
        let n = closed.rows.len();
        for (i, r) in closed.rows.into_iter().enumerate() {
            let key = keys.next(lsn);
            let snap = if i + 1 == n {
                closed.state.clone()
            } else {
                Some(before.clone())
            };
            let checkpoint = self.checkpoint_text(key, snap);
            self.push(
                key,
                ChangeEvent {
                    position: Position::Lsn(lsn),
                    xid,
                    commit_ts_micros: ts,
                    schema: r.schema,
                    table: r.table,
                    op: Op::Read,
                    before: None,
                    after: Some(r.row),
                    key: r.key,
                    checkpoint: Some(Position::Text(checkpoint)),
                    // A snapshot reads whole rows: nothing is left out.
                    unchanged: Vec::new(),
                    after_is_key: false,
                },
            );
        }
        if n == 0 && closed.state.as_ref() != Some(&before) {
            let checkpoint = self.checkpoint_text(self.emitted, closed.state.clone());
            let mut hb = ChangeEvent::heartbeat(Position::Lsn(lsn), ts);
            hb.checkpoint = Some(Position::Text(checkpoint));
            self.buffer.push_back(hb);
        }
        if closed.state.is_none() {
            self.end_snapshot();
        } else {
            self.publish_status();
        }
    }

    /// Starts a heartbeat if one is due: runs the optional action query, then reads the server's
    /// WAL flush position. `None` when no heartbeat is due or this one was skipped.
    async fn heartbeat_begin(&mut self) -> Result<Option<u64>> {
        let Some(every) = self.cfg.heartbeat_interval else {
            return Ok(None);
        };
        if self.last_heartbeat.elapsed() < every {
            return Ok(None);
        }
        self.last_heartbeat = Instant::now();
        // A heartbeat only saves WAL; a failure (lock timeout, revoked grant) must not halt the
        // stream, which would pin WAL harder than having no heartbeat. Skip this beat.
        if let Some(q) = self.cfg.heartbeat_action_query.clone() {
            let ran = match self.client() {
                Ok(c) => c.batch_execute(&q).await.map_err(src_err),
                Err(e) => Err(e),
            };
            if let Err(e) = ran {
                self.skip_heartbeat(format!("heartbeat action query failed: {e}"));
                return Ok(None);
            }
        }
        match self.wal_position().await {
            Ok(wal) => Ok(wal),
            Err(e) => {
                self.skip_heartbeat(format!("heartbeat WAL position query failed: {e}"));
                Ok(None)
            }
        }
    }

    /// The server's WAL flush position (replay position on a standby).
    async fn wal_position(&self) -> Result<Option<u64>> {
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
        // A chunk in flight counts as queued: its watermarks and rows are not delivered yet.
        let in_flight = self.snapshot.as_ref().is_some_and(|e| e.in_flight());
        let targets = heartbeat_targets(
            &mut self.tx_ends,
            drained && self.buffer.is_empty() && !in_flight,
            self.checkpointed,
            wal,
        );
        for t in targets {
            self.advance(t).await?;
        }
        if let Some(m) = &self.metrics {
            m.record_heartbeat();
        }
        Ok(())
    }

    async fn poll(&mut self) -> Result<()> {
        self.poll_with(true).await
    }

    /// One fetch. It waits up to `poll_interval` for data only when `wait`.
    async fn poll_with(&mut self, wait: bool) -> Result<()> {
        // The fetch waits for data, so the heartbeat runs after it: a heartbeat started before a
        // fetch that is then cancelled would never finish. The idle target is the last
        // keepalive's position W, not the server's WAL position: W proves receipt of everything
        // committed below it, while the WAL position on a busy cluster (or after the heartbeat
        // action query) is always past W, and the slot would never move.
        let t = self.transport()?;
        let fetched = if wait {
            t.fetch().await?
        } else {
            t.fetch_ready().await?
        };
        self.learn_keys(&fetched.messages).await?;
        self.apply(fetched.messages)?;
        self.advance_settled().await?;
        self.report_stream_positions(fetched.caught_up, fetched.server_wal_end);
        if self.heartbeat_begin().await?.is_some() {
            match fetched.caught_up {
                Some(w) => self.heartbeat_end(w, true).await?,
                None => self.heartbeat_end(0, false).await?,
            }
        }
        Ok(())
    }

    /// Stream transport: the newest keepalive's WAL end is a position the server has reached, and
    /// a fetch that ended on a keepalive with everything handed on has nothing left below it.
    fn report_stream_positions(&self, caught_up: Option<u64>, server_wal_end: Option<u64>) {
        let Some(m) = &self.metrics else { return };
        if let Some(w) = server_wal_end {
            m.report_source_head_lsn(w);
        }
        if let (Some(w), true) = (caught_up, self.buffer.is_empty()) {
            self.report_delivered(w);
        }
    }

    /// Reads the primary keys of the published tables from the catalog.
    async fn refresh_keys(&mut self) -> Result<()> {
        let catalog = read_catalog(self.client()?, &self.cfg.publication).await?;
        self.pks = catalog
            .tables
            .into_iter()
            .map(|t| (table_id(&t.schema, &t.name), t.primary_key))
            .collect();
        Ok(())
    }

    /// A Relation message for a table not seen before (added to the publication since the
    /// catalog was read) reloads the primary keys before its rows are keyed.
    async fn learn_keys(&mut self, messages: &[(u64, Message)]) -> Result<()> {
        let unknown: Vec<String> = messages
            .iter()
            .filter_map(|(_, m)| match m {
                Message::Relation(r) => Some(table_id(&r.schema, &r.name)),
                _ => None,
            })
            .filter(|id| !self.pks.contains_key(id))
            .collect();
        if unknown.is_empty() {
            return Ok(());
        }
        self.refresh_keys().await?;
        for id in unknown {
            // Still unknown (not in the publication's catalog): key by replica identity.
            self.pks.entry(id).or_default();
        }
        Ok(())
    }

    /// Whether `(schema, table)` is the watermark table (#92): its rows are never events.
    fn is_watermark_table(&self, (schema, table): &(String, String)) -> bool {
        self.cfg
            .snapshot
            .watermark_table
            .as_deref()
            .is_some_and(|w| w == table_id(schema, table))
    }

    /// A watermark of this slot at `lsn`, from a logical message or a watermark-table row.
    fn on_watermark(
        &mut self,
        wm: &crate::decoder::Watermark,
        lsn: u64,
        keys: &mut TxKeys,
        xid: u32,
        ts: i64,
    ) -> Result<()> {
        let Some(e) = self.snapshot.as_mut() else {
            return Ok(());
        };
        let before = e.state().clone();
        if let Some(closed) = e.mark(wm)? {
            self.queue_closed(closed, before, lsn, keys, xid, ts);
        }
        Ok(())
    }

    /// Turns decoded messages into queued events. This is the one place where changes get their
    /// [`Key`], for every transport.
    fn apply(&mut self, messages: Vec<(u64, Message)>) -> Result<()> {
        // Taken out while decoding (relations are borrowed across `queue` calls) and put back at
        // the end. An error ends the stream, so a state lost on the error path is never used.
        let DecodeState {
            mut cache,
            mut xid,
            mut ts,
            mut keys,
        } = std::mem::take(&mut self.decode);
        for (lsn, msg) in messages {
            let dec = |e: crate::decoder::DecodeError| Error::Source(e.to_string());
            match msg {
                Message::Begin {
                    xid: x,
                    commit_ts_micros,
                    final_lsn,
                } => {
                    xid = x;
                    ts = commit_ts_micros;
                    keys.begin(final_lsn);
                }
                Message::Relation(rel) => {
                    let no_pk = self
                        .pks
                        .get(&table_id(&rel.schema, &rel.name))
                        .is_none_or(Vec::is_empty);
                    if rel.identity_full()
                        && no_pk
                        && self
                            .full_identity_logged
                            .insert((rel.schema.clone(), rel.name.clone()))
                    {
                        tracing::info!(
                            table = %format!("{}.{}", rel.schema, rel.name),
                            "REPLICA IDENTITY FULL without a primary key: records of this table have a null key"
                        );
                    }
                    if let Some(e) = self.snapshot.as_mut() {
                        e.relation(&table_id(&rel.schema, &rel.name), &rel);
                    }
                    cache.insert(rel)
                }
                Message::Insert { relation, new } => {
                    let r = cache.get(relation).map_err(dec)?;
                    let name = (r.schema.clone(), r.name.clone());
                    if self.is_watermark_table(&name) {
                        if let Some(wm) = table_watermark(r, &new, &self.cfg.slot) {
                            self.on_watermark(&wm, lsn, &mut keys, xid, ts)?;
                        }
                        continue;
                    }
                    let after = Some(cache.row(relation, &new).map_err(dec)?);
                    let id = table_id(&name.0, &name.1);
                    let pk = self.pks.get(&id).map(Vec::as_slice);
                    let row_key = cache.key(relation, &new, pk).map_err(dec)?;
                    let key = keys.next(lsn);
                    let none = Vec::new();
                    if self.queue(key, xid, ts, name, Op::Insert, None, after, row_key, none) {
                        if let Some(e) = self.snapshot.as_mut() {
                            e.observe(&id, Live::Insert { rel: r, new: &new });
                        }
                    }
                }
                Message::Update { relation, old, new } => {
                    let r = cache.get(relation).map_err(dec)?;
                    let name = (r.schema.clone(), r.name.clone());
                    if self.is_watermark_table(&name) {
                        continue;
                    }
                    let new = cache
                        .fill_unchanged(relation, new, old.as_ref())
                        .map_err(dec)?;
                    let id = table_id(&name.0, &name.1);
                    let pk = self.pks.get(&id).map(Vec::as_slice);
                    let row_key = cache
                        .key_with_old(relation, &new, old.as_ref(), pk)
                        .map_err(dec)?;
                    let before = old
                        .as_ref()
                        .map(|t| cache.row(relation, t))
                        .transpose()
                        .map_err(dec)?;
                    let after = Some(cache.row(relation, &new).map_err(dec)?);
                    // Unchanged TOAST columns `row` left out of `after`, for `unchanged` routes.
                    let unchanged = cache.unchanged(relation, &new).map_err(dec)?;
                    let key = keys.next(lsn);
                    if self.queue(
                        key,
                        xid,
                        ts,
                        name,
                        Op::Update,
                        before,
                        after,
                        row_key,
                        unchanged,
                    ) {
                        if let Some(e) = self.snapshot.as_mut() {
                            e.observe(
                                &id,
                                Live::Update {
                                    rel: r,
                                    old: old.as_ref(),
                                    new: &new,
                                },
                            );
                        }
                    }
                }
                Message::Delete { relation, old } => {
                    let r = cache.get(relation).map_err(dec)?;
                    let name = (r.schema.clone(), r.name.clone());
                    if self.is_watermark_table(&name) {
                        continue;
                    }
                    let before = Some(cache.row(relation, &old).map_err(dec)?);
                    let id = table_id(&name.0, &name.1);
                    let pk = self.pks.get(&id).map(Vec::as_slice);
                    let row_key = cache.key(relation, &old, pk).map_err(dec)?;
                    let key = keys.next(lsn);
                    let none = Vec::new();
                    if self.queue(key, xid, ts, name, Op::Delete, before, None, row_key, none) {
                        if let Some(e) = self.snapshot.as_mut() {
                            e.observe(&id, Live::Delete { rel: r, old: &old });
                        }
                    }
                }
                Message::Truncate { relations } => {
                    for rel_id in relations {
                        let r = cache.get(rel_id).map_err(dec)?;
                        let name = (r.schema.clone(), r.name.clone());
                        if self.is_watermark_table(&name) {
                            continue;
                        }
                        let key = keys.next(lsn);
                        let id = table_id(&name.0, &name.1);
                        let none = Vec::new();
                        if self.queue(key, xid, ts, name, Op::Truncate, None, None, None, none) {
                            if let Some(e) = self.snapshot.as_mut() {
                                e.observe(&id, Live::Truncate);
                            }
                        }
                    }
                }
                Message::Commit { end_lsn, .. } => {
                    if end_lsn > self.last_end {
                        self.last_end = end_lsn;
                        // An empty transaction (Postgres < 15 sends them for unpublished tables) has no key of
                        // its own. It ends at the highest key queued so far, so it counts as
                        // acknowledged exactly when everything before it is.
                        let end = keys.end_key().unwrap_or(self.emitted);
                        self.tx_ends.push_back((end, end_lsn));
                    }
                }
                Message::Logical(l) => {
                    // Watermarks of this slot close snapshot chunks; any other message (another
                    // slot's, another tool's, a non-transactional one) is not ours.
                    if let Some(wm) = l.watermark_for(&self.cfg.slot) {
                        self.on_watermark(&wm, l.lsn, &mut keys, xid, ts)?;
                    }
                }
                Message::Ignored => {}
            }
        }
        self.decode = DecodeState {
            cache,
            xid,
            ts,
            keys,
        };
        Ok(())
    }

    /// Sets up the snapshot this start needs: resumes the one the checkpoint carries, or starts
    /// the initial one when there is no checkpoint. Fails when snapshots are on but the server
    /// cannot send watermarks; that is checked here, never in the poll loop.
    async fn start_snapshot(&mut self, resume: Option<SnapState>, first_start: bool) -> Result<()> {
        let cfg = self.cfg.snapshot.clone();
        if cfg.mode == SnapshotMode::Never {
            if let Some(s) = resume {
                tracing::warn!(
                    snapshot = %s.id,
                    "the checkpoint carries an unfinished snapshot, but snapshot = \"never\"; \
                     it is abandoned (rows not copied yet are not delivered)"
                );
            }
            return Ok(());
        }
        let catalog = read_catalog(self.client()?, &self.cfg.publication).await?;
        match &cfg.watermark_table {
            Some(table) => {
                // #92: watermarks are rows of a published table, for servers without (or instead
                // of) logical messages. It must be published and writable.
                if !catalog
                    .tables
                    .iter()
                    .any(|t| &table_id(&t.schema, &t.name) == table)
                {
                    return Err(Error::Config(format!(
                        "postgres.snapshot_watermark_table {table:?} is not in publication {:?}; \
                         create it as {table} {} and add it to the publication",
                        self.cfg.publication,
                        crate::snapshot::WATERMARK_TABLE_DDL
                    )));
                }
                let allowed: bool = self
                    .client()?
                    .query_one(
                        "SELECT has_table_privilege($1::text, 'INSERT') AND has_table_privilege($1::text, 'DELETE')",
                        &[table],
                    )
                    .await
                    .map_err(src_err)?
                    .get(0);
                if !allowed {
                    return Err(Error::Config(format!(
                        "snapshots write watermarks into {table}; grant INSERT and DELETE on it to the Aqueduct role"
                    )));
                }
            }
            None => {
                if catalog.server_version_num < MIN_SERVER_VERSION {
                    return Err(Error::Config(format!(
                        "postgres.snapshot = {:?} needs Postgres 14 or newer for its watermarks (this \
                         server is {}); on older servers set postgres.snapshot_watermark_table",
                        cfg.mode, catalog.server_version_num
                    )));
                }
                let allowed: bool = self
                    .client()?
                    .query_one(
                        // By OID, not signature: PG17 added a fourth argument (`flush`).
                        "SELECT COALESCE(bool_or(has_function_privilege(p.oid, 'EXECUTE')), false) \
                         FROM pg_proc p WHERE p.proname = 'pg_logical_emit_message' \
                         AND p.pronamespace = 'pg_catalog'::regnamespace \
                         AND p.proargtypes[1] = 'text'::regtype",
                        &[],
                    )
                    .await
                    .map_err(src_err)?
                    .get(0);
                if !allowed {
                    return Err(Error::Config(
                        "snapshots write watermarks with pg_logical_emit_message(); grant EXECUTE on it \
                         to the Aqueduct role, or set postgres.snapshot = \"never\""
                            .into(),
                    ));
                }
                self.cfg.logical_messages = true;
            }
        }
        let state = match resume {
            Some(s) => Some(s),
            None if first_start && cfg.mode == SnapshotMode::Initial => Some(SnapState {
                id: new_snapshot_id(),
                tables: catalog
                    .tables
                    .iter()
                    .map(|t| table_id(&t.schema, &t.name))
                    .filter(|t| cfg.watermark_table.as_ref() != Some(t))
                    .collect(),
                table: 0,
                after: None,
            }),
            None => None,
        };
        if let Some(state) = state {
            let id = state.id.clone();
            self.snapshot = Engine::new(&self.cfg.slot, state, &catalog, &cfg);
            match &self.snapshot {
                Some(e) => {
                    tracing::info!(snapshot = %id, tables = e.state().tables.len(), "snapshot running")
                }
                None => tracing::info!(snapshot = %id, "nothing to snapshot"),
            }
        }
        self.publish_status();
        Ok(())
    }
}

#[async_trait]
impl Source for PostgresSource {
    async fn start(&mut self, start: Option<Position>) -> Result<()> {
        // A new stream: nothing decoded or queued before belongs to it.
        self.decode = DecodeState::default();
        self.tx_ends.clear();
        self.last_end = 0;
        self.buffer.clear();
        self.cfg.check_auth()?;
        let client = self.connect().await?;
        self.client = Some(Arc::new(client));
        if let Some(m) = &self.metrics {
            m.heartbeat_configured(self.cfg.heartbeat_interval);
        }

        // Which cluster and database this is, checked against the checkpoint before the slot is
        // looked up or created, so a checkpoint is never applied to (and no slot is created on)
        // the wrong database. `allow_slot_recreate` does not bypass this. `skip_system_check`
        // only drops the cluster part; the database is always compared.
        if self.cfg.skip_system_check {
            tracing::warn!(
                "checkpoint_skip_system_check is set: a checkpoint from another cluster is not \
                 detected (one from another database of the same cluster still is)"
            );
        }
        let origin = self.read_origin().await?;
        self.origin = Some(origin);
        let parsed = match &start {
            Some(Position::Text(t)) => Some(split_checkpoint(t)?),
            // A plain-LSN checkpoint predates keys and records no origin.
            Some(Position::Lsn(_)) => None,
            None => None,
        };
        let stamp = match &start {
            Some(Position::Text(_)) => parsed.as_ref().map(|p| p.1),
            Some(Position::Lsn(_)) => Some(Stamp::default()),
            None => None,
        };
        if let Some(stamp) = stamp {
            check_origin(stamp, origin)?;
            if let Some(msg) = stamp_warning(stamp, origin) {
                tracing::warn!("{msg}");
            }
        }

        let slot = self.cfg.slot.clone();
        let mut recreated = false;
        let existing = self
            .client()?
            .query_opt(
                "SELECT confirmed_flush_lsn::text, database::text, current_database()::text \
                 FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .map_err(src_err)?;
        match existing {
            Some(row) => {
                // Checked before the slot is used; a slot of another database is never touched.
                let slot_db: Option<String> = row.get(1);
                let current: String = row.get(2);
                check_slot_database(&slot, slot_db.as_deref(), &current)?;
                let flushed: Option<String> = row.get(0);
                self.acked = flushed.as_deref().map(parse_lsn).transpose()?.unwrap_or(0);
                // The slot's confirmed position: everything before it was delivered earlier.
                self.report_delivered(self.acked);
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
        let first_start = start.is_none();
        if let (Some(pos), false) = (start, recreated) {
            match pos {
                Position::Lsn(l) => {
                    // A plain-LSN checkpoint (written before commit-order keys) cannot be mapped
                    // to a key, so local dedupe is off for this start: the slot replays from its
                    // `confirmed_flush_lsn`, giving duplicates but never loss.
                    // A checkpoint ahead of this server's WAL comes from another cluster (restored
                    // dump, copied state volume). Treating it as acknowledged would move the slot
                    // past data that was never delivered, so refuse it.
                    if let Some(wal) = self.wal_position().await? {
                        if l > wal {
                            return Err(ahead_of_wal(l, wal, false));
                        }
                    }
                    tracing::warn!(
                        checkpoint = %Position::Lsn(l),
                        "plain-LSN checkpoint: replaying from the slot without local dedupe; \
                         some changes may be delivered again"
                    );
                }
                Position::Text(t) => {
                    let (k, stamp, _) = split_checkpoint(&t)?;
                    // Same guard as for a plain LSN, on the commit LSN. `check_origin` already
                    // refused another cluster, so a recorded `sys` means a restore of this one.
                    if let Some(wal) = self.wal_position().await? {
                        if k.commit > wal {
                            let known = stamp.system.is_some()
                                && stamp.system == self.origin.and_then(|o| o.system);
                            return Err(ahead_of_wal(k.commit, wal, known));
                        }
                    }
                    self.emitted = k;
                    self.checkpointed = k;
                }
            }
        }
        self.refresh_keys().await?;
        // A recreated slot starts a new stream, so an old snapshot's progress no longer applies.
        let resume = parsed.and_then(|p| p.2).filter(|_| !recreated);
        self.start_snapshot(resume, first_start).await?;
        self.transport = Some(Box::new(self.open_stream().await?));
        self.spawn_stats();
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<ChangeEvent>> {
        loop {
            if let Some(ev) = self.buffer.pop_front() {
                return Ok(Some(ev));
            }
            self.handle_requests().await?;
            if let Some(ev) = self.buffer.pop_front() {
                return Ok(Some(ev));
            }
            // The next chunk starts only when everything before it is handed on, so memory stays
            // at one chunk.
            if let (Some(e), Some(c)) = (self.snapshot.as_mut(), self.client.as_deref()) {
                if e.due() {
                    e.start_chunk(c).await?;
                }
            }
            // The fetch waits up to `poll_interval` for data, so an idle loop does not spin.
            self.poll().await?;
        }
    }

    fn try_next(&mut self) -> Option<ChangeEvent> {
        self.buffer.pop_front()
    }

    /// `next` without the idle pause: one poll (a stream fetch that does not wait), so the
    /// next batch is read while earlier ones are still being delivered.
    async fn read_ahead(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            return Ok(());
        }
        self.handle_requests().await?;
        if !self.buffer.is_empty() {
            return Ok(());
        }
        if let (Some(e), Some(c)) = (self.snapshot.as_mut(), self.client.as_deref()) {
            if e.due() {
                e.start_chunk(c).await?;
            }
        }
        self.poll_with(false).await
    }

    async fn ack(&mut self, position: Position) -> Result<()> {
        // Events carry their key in `checkpoint`, and the pipeline acks that. A bare change LSN
        // cannot be placed in the commit order, so acking one would be a guess.
        match position {
            Position::Text(t) => {
                let (k, _) = Checkpoint::decode(&t)
                    .ok()
                    .and_then(|c| parse_checkpoint(&c.pos))
                    .ok_or_else(|| Error::Source(format!("cannot ack position {t:?}")))?;
                self.checkpointed = self.checkpointed.max(k);
                // Delivered up to this change; the slot itself moves only at commit ends.
                self.report_delivered(k.change);
                self.advance_acked(k).await
            }
            Position::Lsn(l) => Err(Error::Source(format!(
                "cannot ack plain LSN {}; ack the event's checkpoint",
                Position::Lsn(l)
            ))),
        }
    }
}

/// tokio-postgres' `Display` prints only the kind ("error performing TLS handshake"); the cause
/// (`UnknownIssuer`, "server does not support TLS", ...) is only reachable via `source()`.
/// A server error contributes its SQLSTATE and message only: DETAIL/HINT can carry row values
/// and would otherwise reach `last_error`, `/api/metrics` and the logs.
pub(crate) fn src_err(e: tokio_postgres::Error) -> Error {
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

/// Status update interval for a server whose `wal_sender_timeout` is `timeout_ms` (0 = off):
/// a third of it, so two updates can be lost before the server gives up, and at most the
/// default 10 s.
fn status_interval(timeout_ms: i64) -> Duration {
    let default = crate::replication::DEFAULT_STATUS_INTERVAL;
    match u64::try_from(timeout_ms) {
        Ok(ms) if ms > 0 => Duration::from_millis((ms / 3).max(100)).min(default),
        _ => default,
    }
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

    #[test]
    fn multi_table_truncate_emits_one_event_per_table() {
        let mut src = PostgresSource::new(PostgresSourceConfig::new("postgres://x", "s", "p"));
        let mut keys = TxKeys::default();
        let run = |src: &mut PostgresSource, keys: &mut TxKeys| {
            keys.begin(500);
            for (schema, table) in [("public", "a"), ("public", "b"), ("app", "c")] {
                let key = keys.next(100);
                src.queue(
                    key,
                    7,
                    0,
                    (schema.into(), table.into()),
                    Op::Truncate,
                    None,
                    None,
                    None,
                    Vec::new(),
                );
            }
        };
        run(&mut src, &mut keys);
        let got: Vec<_> = src
            .buffer
            .iter()
            .map(|e| (e.schema.as_str(), e.table.as_str(), e.position.clone()))
            .collect();
        // One WAL message, one LSN, one event per table.
        assert_eq!(
            got,
            vec![
                ("public", "a", Position::Lsn(100)),
                ("public", "b", Position::Lsn(100)),
                ("app", "c", Position::Lsn(100)),
            ]
        );
        // A replay re-sends the same message: it must not be queued twice.
        run(&mut src, &mut keys);
        assert_eq!(src.buffer.len(), 3);
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

/// Pops every transaction whose changes are all <= `key` and returns the commit end LSN
/// of the last one: the only position the slot can safely be advanced to.
///
/// Keys are compared whole, so change LSNs of different transactions are never compared with
/// each other: a transaction that committed later has a larger commit LSN and stays queued.
fn ack_target(tx_ends: &mut VecDeque<(Key, u64)>, key: Key) -> Option<u64> {
    let mut target = None;
    while let Some(&(last, end)) = tx_ends.front() {
        if last > key {
            break;
        }
        target = Some(end);
        tx_ends.pop_front();
    }
    target
}

/// LSNs the slot may be advanced to, in order, for a heartbeat at WAL position `wal`.
///
/// 1. Transactions whose changes are all at or below the `checkpointed` key count as acknowledged:
///    that covers empty transactions (Postgres < 15 sends them for unpublished tables) and
///    replays of transactions already delivered before a restart. The slot may move to the
///    last such commit end.
/// 2. Only if the stream is caught up to `wal` (the WAL end of a keepalive with nothing after
///    it), nothing is queued and no transaction is still waiting for its checkpoint may the slot
///    move to `wal`: every transaction committed below it was received and acknowledged. A fetch
///    that is not caught up only gets step 1.
///
/// An unacknowledged event therefore always holds the slot back (at-least-once).
fn heartbeat_targets(
    tx_ends: &mut VecDeque<(Key, u64)>,
    idle: bool,
    checkpointed: Key,
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

    fn k(commit: u64, change: u64) -> Key {
        Key {
            commit,
            change,
            ord: 0,
        }
    }

    #[test]
    fn idle_advances_to_wal() {
        let mut q = VecDeque::new();
        assert_eq!(
            heartbeat_targets(&mut q, true, Key::default(), 500),
            vec![500]
        );
    }

    #[test]
    fn not_caught_up_only_gets_the_checkpointed_target() {
        // Not idle (not caught up): the slot may reach the last checkpointed commit end, never `wal`.
        let mut q = VecDeque::from([(k(120, 100), 120), (k(220, 200), 220)]);
        assert_eq!(
            heartbeat_targets(&mut q, false, k(120, 100), 500),
            vec![120]
        );
        assert_eq!(q.len(), 1);
        let mut q = VecDeque::from([(k(120, 0), 120), (k(140, 0), 140)]);
        assert_eq!(heartbeat_targets(&mut q, false, k(140, 0), 500), vec![140]);
    }

    #[test]
    fn queued_events_hold_the_slot() {
        let mut q = VecDeque::new();
        assert!(heartbeat_targets(&mut q, false, Key::default(), 500).is_empty());
    }

    #[test]
    fn delivered_but_uncheckpointed_transaction_holds_the_slot() {
        let mut q = VecDeque::from([(k(120, 100), 120)]);
        assert!(heartbeat_targets(&mut q, true, k(120, 99), 500).is_empty());
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn checkpointed_transactions_are_acknowledged_first() {
        let mut q = VecDeque::from([(k(120, 100), 120), (k(220, 200), 220)]);
        assert_eq!(
            heartbeat_targets(&mut q, true, k(220, 200), 500),
            vec![220, 500]
        );
        assert!(q.is_empty());
    }

    #[test]
    fn partially_checkpointed_queue_only_moves_to_the_acked_commit_end() {
        let mut q = VecDeque::from([(k(120, 100), 120), (k(220, 200), 220)]);
        assert_eq!(heartbeat_targets(&mut q, true, k(120, 100), 500), vec![120]);
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn empty_transactions_do_not_pin_the_slot() {
        // A transaction without published changes (Postgres < 15) ends at the key before it.
        let mut q = VecDeque::from([(Key::default(), 120), (Key::default(), 140)]);
        assert_eq!(
            heartbeat_targets(&mut q, true, Key::default(), 500),
            vec![140, 500]
        );
    }

    #[test]
    fn empty_transaction_behind_unacked_data_stays_queued() {
        let mut q = VecDeque::from([(k(120, 100), 120), (k(120, 100), 140)]);
        assert!(heartbeat_targets(&mut q, true, Key::default(), 500).is_empty());
        assert_eq!(q.len(), 2);
    }
}

#[cfg(test)]
mod ack_tests {
    use super::*;

    fn k(commit: u64, change: u64, ord: u32) -> Key {
        Key {
            commit,
            change,
            ord,
        }
    }

    #[test]
    fn ack_mid_transaction_does_not_advance() {
        let mut q = VecDeque::from([(k(120, 100, 0), 120), (k(220, 200, 1), 220)]);
        assert_eq!(ack_target(&mut q, k(120, 100, 0)), Some(120));
        // Acking inside the second transaction must not advance past its commit end.
        assert_eq!(ack_target(&mut q, k(220, 200, 0)), None);
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn ack_last_change_advances_to_commit_end() {
        let mut q = VecDeque::from([(k(120, 100, 0), 120), (k(220, 200, 0), 220)]);
        assert_eq!(ack_target(&mut q, k(220, 200, 0)), Some(220));
        assert!(q.is_empty());
    }

    /// Ta commits first (end 0xFD8) with its last change at a high LSN. Tb's only change has a
    /// lower change LSN and its commit LSN equals Ta's end. Acking all of Ta must target exactly
    /// Ta's end and keep Tb queued: Postgres skips a transaction only if its commit LSN is below
    /// the slot's start, so exactly 0xFD8 still replays Tb. Do not "optimise" this target upward.
    #[test]
    fn ack_boundary_is_exactly_the_commit_end_of_the_acked_transaction() {
        let ta = k(0xFD8, 0xF80, 1);
        let tb = k(0xFD8 + 0x10, 0xF00, 0);
        let mut q = VecDeque::from([(ta, 0xFD8), (tb, 0xFE8)]);
        assert_eq!(ack_target(&mut q, ta), Some(0xFD8));
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].0, tb);
    }

    /// Changes sharing one change LSN are ordered by `ord`: acking row 0 of a COPY must not
    /// complete the transaction while the later rows are undelivered.
    #[test]
    fn ack_inside_a_same_lsn_group_does_not_advance() {
        let mut q = VecDeque::from([(k(300, 200, 2), 300)]);
        assert_eq!(ack_target(&mut q, k(300, 200, 0)), None);
        assert_eq!(ack_target(&mut q, k(300, 200, 1)), None);
        assert_eq!(ack_target(&mut q, k(300, 200, 2)), Some(300));
    }
}

#[cfg(test)]
mod key_tests {
    use super::*;

    #[test]
    fn key_roundtrips_and_orders_lexicographically() {
        let a = Key {
            commit: 10,
            change: 99,
            ord: 3,
        };
        assert_eq!(a.encode(), "10@99#3");
        assert_eq!(Key::parse("10@99#3"), Some(a));
        for bad in ["", "10", "10@99", "10@99#", "x@1#1", "1@y#1", "1@1#z"] {
            assert_eq!(Key::parse(bad), None, "{bad:?}");
        }
        // Commit order dominates change LSN; ord breaks ties within one LSN.
        let later_tx_lower_change = Key {
            commit: 11,
            change: 5,
            ord: 0,
        };
        assert!(a < later_tx_lower_change);
        assert!(Key { ord: 4, ..a } > a);
    }

    #[test]
    fn same_lsn_changes_get_distinct_increasing_keys() {
        let mut t = TxKeys::default();
        t.begin(500);
        let ks = [t.next(100), t.next(100), t.next(100), t.next(200)];
        assert_eq!(
            ks.map(|k| (k.change, k.ord)),
            [(100, 0), (100, 1), (100, 2), (200, 0)]
        );
        assert!(ks.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(t.end_key(), Some(ks[3]));
        // A new transaction restarts ord and takes its own commit LSN.
        t.begin(600);
        assert_eq!(
            t.next(100),
            Key {
                commit: 600,
                change: 100,
                ord: 0
            }
        );
    }

    #[test]
    fn empty_transaction_has_no_key() {
        let mut t = TxKeys::default();
        t.begin(500);
        assert_eq!(t.end_key(), None);
    }

    /// Rollback guard. A binary that predates `Key` refuses every `Position::Text` checkpoint
    /// (main's `start` arm), so downgrading after this change fails loudly instead of being read
    /// as "no checkpoint". The current parser likewise refuses text that is not a key.
    #[test]
    fn foreign_text_checkpoint_is_refused() {
        assert_eq!(key_from_checkpoint("10@99#3").unwrap().0.commit, 10);
        for foreign in ["{\"_data\":\"82\"}", "16/B374D848", "10@99"] {
            assert!(
                matches!(key_from_checkpoint(foreign), Err(Error::Checkpoint(_))),
                "{foreign:?}"
            );
        }
    }

    const HERE: Origin = Origin {
        system: Some(7_312_345_678_901_234_567),
        timeline: Some(1),
        database: 16_384,
    };
    /// `HERE` as seen with `skip_system_check`.
    const HERE_NO_SYS: Origin = Origin {
        system: None,
        timeline: None,
        database: 16_384,
    };

    #[test]
    fn checkpoint_carries_its_origin() {
        let k = Key {
            commit: 10,
            change: 99,
            ord: 3,
        };
        let text = encode_checkpoint(k, Some(HERE));
        assert_eq!(text, "10@99#3;sys=7312345678901234567;db=16384;tli=1");
        let (key, stamp) = parse_checkpoint(&text).unwrap();
        assert_eq!(key, k);
        assert_eq!(stamp.system, HERE.system);
        assert_eq!(stamp.timeline, HERE.timeline);
        assert_eq!(stamp.database, Some(HERE.database));
        // Written by the first version of this check, before the timeline was recorded.
        let (_, old) = parse_checkpoint("10@99#3;sys=7312345678901234567;db=16384").unwrap();
        assert_eq!(old.timeline, None);
        // With skip_system_check only the database is recorded.
        assert_eq!(encode_checkpoint(k, Some(HERE_NO_SYS)), "10@99#3;db=16384");
        // Before start there is no origin, and the text is the bare key, as before #63.
        assert_eq!(encode_checkpoint(k, None), "10@99#3");
        assert_eq!(parse_checkpoint("10@99#3"), Some((k, Stamp::default())));
    }

    #[test]
    fn unknown_or_malformed_checkpoint_fields_are_refused() {
        for bad in [
            "10@99#3;",
            "10@99#3;sys",
            "10@99#3;sys=x",
            "10@99#3;db=-1",
            "10@99#3;sys=1;sys=1",
            "10@99#3;tli=2;tli=2",
            "10@99#3;tli=-1",
            "10@99#3;sys=1;db=2;x=3",
        ] {
            assert!(
                matches!(key_from_checkpoint(bad), Err(Error::Checkpoint(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn checkpoint_from_another_cluster_or_database_is_refused() {
        let same = Stamp {
            system: HERE.system,
            timeline: HERE.timeline,
            database: Some(HERE.database),
        };
        assert!(check_origin(same, HERE).is_ok());
        // Legacy checkpoints carry no origin and are accepted (backfilled on the next save).
        assert!(check_origin(Stamp::default(), HERE).is_ok());

        let other_cluster = Stamp {
            system: Some(HERE.system.unwrap() + 1),
            ..same
        };
        let err = check_origin(other_cluster, HERE).unwrap_err().to_string();
        assert!(err.contains("different or rebuilt cluster"), "{err}");
        assert!(
            !err.contains("backup"),
            "must not promise to catch restores: {err}"
        );

        let other_db = Stamp {
            database: Some(HERE.database + 1),
            ..same
        };
        let err = check_origin(other_db, HERE).unwrap_err().to_string();
        assert!(err.contains("same cluster"), "{err}");

        // Same cluster, new timeline: a promotion or PITR since, which may have lost changes.
        let old_timeline = Stamp {
            timeline: Some(2),
            ..same
        };
        let on_three = Origin {
            timeline: Some(3),
            ..HERE
        };
        let err = check_origin(old_timeline, on_three)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("timeline 2")
                && err.contains("restored, recovered to a point in time (PITR) or failed over"),
            "{err}"
        );
        assert!(
            err.contains("may never reach Kafka") && err.contains(";tli="),
            "{err}"
        );
        // A checkpoint without a timeline is accepted and gets one on the next save.
        let no_timeline = Stamp {
            timeline: None,
            ..same
        };
        assert!(check_origin(no_timeline, on_three).is_ok());

        // skip_system_check drops the cluster and timeline comparisons: the database is still compared.
        assert!(check_origin(other_cluster, HERE_NO_SYS).is_ok());
        assert!(check_origin(old_timeline, HERE_NO_SYS).is_ok());
        let err = check_origin(other_db, HERE_NO_SYS).unwrap_err().to_string();
        assert!(err.contains("same cluster"), "{err}");
    }

    #[test]
    fn stamping_a_new_origin_warns_with_what_it_records() {
        // Legacy checkpoint: both fields are new, and the warning names them.
        let msg = stamp_warning(Stamp::default(), HERE).expect("legacy stamp warns");
        assert!(
            msg.contains("sys=7312345678901234567 db=16384 tli=1"),
            "{msg}"
        );
        assert!(msg.contains("checkpoint-ahead-of-WAL"), "{msg}");
        // Written with skip_system_check, now checked: only sys is new.
        let db_only = Stamp {
            system: None,
            timeline: None,
            database: Some(HERE.database),
        };
        let msg = stamp_warning(db_only, HERE).expect("adding sys warns");
        assert!(
            msg.contains("sys=7312345678901234567") && !msg.contains("db="),
            "{msg}"
        );
        // Nothing new to record: no warning.
        assert_eq!(stamp_warning(db_only, HERE_NO_SYS), None);
        let full = Stamp {
            system: HERE.system,
            timeline: HERE.timeline,
            ..db_only
        };
        assert_eq!(stamp_warning(full, HERE), None);
        // Written before the timeline was recorded: only tli is new.
        let msg = stamp_warning(
            Stamp {
                timeline: None,
                ..full
            },
            HERE,
        )
        .expect("adding tli warns");
        assert!(msg.contains("tli=1") && !msg.contains("sys="), "{msg}");
    }

    #[test]
    fn checkpoint_ahead_of_wal_names_every_possible_cause() {
        let same = ahead_of_wal(0x20, 0x10, true).to_string();
        assert!(
            same.contains("restored, recovered to a point in time (PITR) or failed over"),
            "{same}"
        );
        assert!(!same.contains("another cluster"), "{same}");
        let unknown = ahead_of_wal(0x20, 0x10, false).to_string();
        assert!(
            unknown.contains("another cluster") && unknown.contains("failed over"),
            "{unknown}"
        );
    }

    #[test]
    fn slot_of_another_database_is_refused() {
        assert!(check_slot_database("s", Some("app"), "app").is_ok());
        let err = check_slot_database("s", Some("other"), "app")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("belongs to database \"other\"") && err.contains("untouched"),
            "{err}"
        );
        let err = check_slot_database("s", None, "app")
            .unwrap_err()
            .to_string();
        assert!(err.contains("physical slot"), "{err}");
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

#[cfg(test)]
mod auth_tests {
    use super::*;

    fn azure(url: &str, mode: Option<SslMode>, root: Option<&str>) -> PostgresSourceConfig {
        let mut c = PostgresSourceConfig::new(url, "s", "p");
        c.ssl_mode = mode;
        c.ssl_root_cert = root.map(str::to_string);
        c.auth = SourceAuth::Azure(
            AzureCredential::container_apps("http://localhost:42356/msi/token", "h", None).unwrap(),
        );
        c
    }

    #[test]
    fn password_auth_is_unchecked() {
        let mut c = azure("postgres://u:pw@db/app?sslmode=disable", None, None);
        c.auth = SourceAuth::Password;
        assert!(c.check_auth().is_ok());
    }

    #[test]
    fn azure_auth_needs_verified_tls() {
        for (url, mode, root) in [
            ("postgres://id@db/app", None, None),
            ("postgres://id@db/app?sslmode=verify-full", None, None),
            (
                "postgres://id@db/app",
                Some(SslMode::VerifyFull),
                Some("/ca.pem"),
            ),
            ("postgres://id@db/app?sslrootcert=/ca.pem", None, None),
            // The config override wins over a weaker URL.
            (
                "postgres://id@db/app?sslmode=require",
                Some(SslMode::VerifyFull),
                None,
            ),
        ] {
            assert!(
                azure(url, mode, root).check_auth().is_ok(),
                "{url} {mode:?}"
            );
        }
        for (url, mode, root, needle) in [
            (
                "postgres://id@db/app?sslmode=disable",
                None,
                None,
                "\"disable\"",
            ),
            (
                "postgres://id@db/app",
                Some(SslMode::Disable),
                None,
                "\"disable\"",
            ),
            (
                "postgres://id@db/app",
                Some(SslMode::Prefer),
                None,
                "\"prefer\"",
            ),
            (
                "postgres://id@db/app?sslmode=require",
                None,
                None,
                "\"require\"",
            ),
            // A verified chain without the hostname check is not enough, with or without a CA.
            (
                "postgres://id@db/app?sslmode=verify-ca",
                None,
                None,
                "\"verify-ca\"",
            ),
            (
                "postgres://id@db/app",
                Some(SslMode::VerifyCa),
                Some("/ca.pem"),
                "\"verify-ca\"",
            ),
            (
                "postgres://id@db/app?sslmode=require&sslrootcert=/ca.pem",
                None,
                None,
                "\"require\"",
            ),
            (
                "postgres://id@db/app",
                Some(SslMode::Require),
                Some("/ca.pem"),
                "\"require\"",
            ),
            // The config override wins over a verifying URL.
            (
                "postgres://id@db/app?sslmode=verify-full",
                Some(SslMode::Disable),
                None,
                "\"disable\"",
            ),
        ] {
            let e = azure(url, mode, root).check_auth().unwrap_err().to_string();
            assert!(
                e.contains(needle) && e.contains("azure-container-apps"),
                "{url}: {e}"
            );
        }
    }

    #[test]
    fn azure_auth_refuses_a_url_password_without_showing_it() {
        let e = azure("postgres://id:hunter2@db/app", None, None)
            .check_auth()
            .unwrap_err()
            .to_string();
        assert!(e.contains("password") && !e.contains("hunter2"), "{e}");
        let e = azure("host=db dbname=app", None, None)
            .check_auth()
            .unwrap_err()
            .to_string();
        assert!(e.contains("url user"), "{e}");
    }
}
