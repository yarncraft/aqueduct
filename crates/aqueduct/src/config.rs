use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;

/// A credential that never shows up in `Debug` output (and so not in logs).
// Parsed once at startup; boxing the Postgres variant would only add noise.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn expose(&self) -> String {
        self.0.clone()
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Shorthand for `[source] type = "postgres"`; kept so existing configs keep working.
    #[serde(default)]
    pub postgres: Option<PostgresConfig>,
    /// The change source. Omit it to use the top-level `[postgres]` table.
    #[serde(default)]
    pub source: Option<SourceConfig>,
    /// The Kafka sink (the only sink).
    pub sink: KafkaConfig,
    #[serde(default = "default_health_addr")]
    pub health_addr: String,
    /// Protects `/api/*` on the admin cockpit when set. Also read from
    /// `AQUEDUCT_ADMIN_TOKEN`.
    #[serde(default)]
    pub admin_token: Option<String>,
    /// Explicit opt-out: serve the cockpit and `/api/*` without a token on a non-loopback
    /// `health_addr`. Off by default; startup refuses that combination otherwise.
    #[serde(default)]
    pub admin_insecure_no_auth: bool,
    /// Serve the cockpit's event inspector, which reads recent events back from
    /// Kafka. It exposes row data, so it also needs `admin_token`.
    #[serde(default = "default_true")]
    pub inspector: bool,
    #[serde(default = "default_checkpoint_path")]
    pub checkpoint_path: String,
}

/// Which system changes are read from, picked by `type`: postgres or mongodb.
// Parsed once at startup; boxing the Postgres variant would only add noise.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SourceConfig {
    Postgres(PostgresConfig),
    Mongodb(aqueduct_source_mongodb::MongoConfig),
}

impl SourceConfig {
    pub fn type_name(&self) -> &'static str {
        match self {
            SourceConfig::Postgres(_) => "postgres",
            SourceConfig::Mongodb(_) => "mongodb",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    pub url: String,
    #[serde(default = "default_slot")]
    pub slot: String,
    #[serde(default = "default_publication")]
    pub publication: String,
    /// Start a fresh slot even if a checkpoint exists (data between is lost). Default: refuse.
    #[serde(default)]
    pub allow_slot_recreate: bool,
    /// `disable | prefer | require | verify-ca | verify-full`. Overrides `sslmode`
    /// in the URL; default is the URL's, else `verify-full`.
    #[serde(default)]
    pub ssl_mode: Option<String>,
    /// PEM CA bundle used to verify the server (overrides `sslrootcert` in the URL).
    #[serde(default)]
    pub ssl_root_cert: Option<String>,
    /// While idle, advance the slot to the server's WAL position this often (seconds), so writes
    /// to unpublished tables do not pin WAL. `0` turns it off. Default 10.
    #[serde(default = "default_heartbeat_interval_secs")]
    pub heartbeat_interval_secs: u64,
    /// Optional SQL run before each heartbeat, e.g. an upsert into a small published table.
    #[serde(default)]
    pub heartbeat_action_query: Option<String>,
    /// Do not read, record or check the cluster's `system_identifier` and timeline. Only for servers that refuse
    /// `pg_control_system()`. The database check stays on. Default: check, and fail at startup if
    /// it cannot be read.
    #[serde(default)]
    pub checkpoint_skip_system_check: bool,
    /// `never | on_demand | initial`: whether existing rows are copied (ADR 0001). Default never.
    #[serde(default)]
    pub snapshot: Option<String>,
    /// Rows per snapshot chunk. Default 1024.
    #[serde(default)]
    pub snapshot_chunk_size: Option<u32>,
    /// Upper bound on snapshot rows read per second. Default unthrottled.
    #[serde(default)]
    pub snapshot_max_rows_per_sec: Option<u32>,
    /// Snapshot key columns for tables without a primary key, by `schema.table`.
    #[serde(default)]
    pub snapshot_key_columns: HashMap<String, Vec<String>>,
    /// `schema.table` of a published table that carries snapshot watermarks as rows (Postgres
    /// 10 to 13, or wherever `pg_logical_emit_message` is not allowed).
    #[serde(default)]
    pub snapshot_watermark_table: Option<String>,
    /// `poll | stream`: read the slot by polling over SQL (default) or over a replication
    /// connection (ADR 0003).
    #[serde(default)]
    pub transport: Option<String>,
    /// Stream transport: bytes of received messages buffered ahead of the sink. Default 64 MiB.
    #[serde(default)]
    pub stream_buffer_bytes: Option<usize>,
    /// `password` (default): the URL's password. `azure-workload-identity` (AKS) or
    /// `azure-container-apps`: a Microsoft Entra token from the managed identity is the password.
    #[serde(default)]
    pub auth: Option<String>,
    /// Managed identity client ID: overrides `AZURE_CLIENT_ID` on AKS, and picks a user-assigned
    /// identity on Container Apps.
    #[serde(default)]
    pub azure_client_id: Option<String>,
}

fn default_heartbeat_interval_secs() -> u64 {
    aqueduct_source_postgres::DEFAULT_HEARTBEAT_INTERVAL.as_secs()
}

impl PostgresConfig {
    pub fn source_config(&self) -> Result<aqueduct_source_postgres::PostgresSourceConfig, String> {
        self.source_config_with_env(&|k: &str| std::env::var(k).ok().filter(|v| !v.is_empty()))
    }

    /// [`Self::source_config`] with the managed identity variables read through `env`.
    pub fn source_config_with_env(
        &self,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<aqueduct_source_postgres::PostgresSourceConfig, String> {
        let mut c = aqueduct_source_postgres::PostgresSourceConfig::new(
            self.url.clone(),
            self.slot.clone(),
            self.publication.clone(),
        );
        c.ssl_mode = self.ssl_mode.as_deref().map(str::parse).transpose()?;
        c.ssl_root_cert = self.ssl_root_cert.clone();
        c.allow_slot_recreate = self.allow_slot_recreate;
        c.heartbeat_interval = (self.heartbeat_interval_secs > 0)
            .then(|| Duration::from_secs(self.heartbeat_interval_secs));
        c.heartbeat_action_query = self
            .heartbeat_action_query
            .clone()
            .filter(|q| !q.trim().is_empty());
        c.skip_system_check = self.checkpoint_skip_system_check;
        if let Some(mode) = &self.snapshot {
            c.snapshot.mode = mode.parse().map_err(|e| format!("postgres.{e}"))?;
        }
        if let Some(n) = self.snapshot_chunk_size {
            if n == 0 {
                return Err("postgres.snapshot_chunk_size must be at least 1".into());
            }
            c.snapshot.chunk_size = n;
        }
        c.snapshot.max_rows_per_sec = self.snapshot_max_rows_per_sec.filter(|n| *n > 0);
        for (table, cols) in &self.snapshot_key_columns {
            if !table.contains('.') || cols.is_empty() {
                return Err(format!(
                    "postgres.snapshot_key_columns: {table:?} needs the form \"schema.table\" and at least one column"
                ));
            }
        }
        c.snapshot.key_columns = self.snapshot_key_columns.clone();
        if let Some(t) = self.snapshot_watermark_table.as_deref() {
            if t.split('.').count() != 2 || t.split('.').any(str::is_empty) {
                return Err(format!(
                    "postgres.snapshot_watermark_table: {t:?} needs the form \"schema.table\""
                ));
            }
        }
        c.snapshot.watermark_table = self.snapshot_watermark_table.clone();
        if let Some(t) = &self.transport {
            c.transport = t.parse().map_err(|e| format!("postgres.{e}"))?;
        }
        if let Some(n) = self.stream_buffer_bytes {
            if n < 64 * 1024 {
                return Err("postgres.stream_buffer_bytes must be at least 65536".into());
            }
            c.stream_buffer_bytes = n;
        }
        let client_id = self.azure_client_id.as_deref().filter(|s| !s.is_empty());
        let auth = self.auth.as_deref().unwrap_or("password");
        if auth == "password" && client_id.is_some() {
            return Err(
                "postgres.azure_client_id is set but postgres.auth is \"password\"; set auth to \
                 \"azure-workload-identity\" or \"azure-container-apps\""
                    .into(),
            );
        }
        c.auth = match auth {
            "password" => aqueduct_source_postgres::SourceAuth::Password,
            other => {
                let kind = other
                    .parse::<aqueduct_source_postgres::AzureIdentity>()
                    .map_err(|e| format!("postgres.{e}"))?;
                aqueduct_source_postgres::SourceAuth::Azure(
                    aqueduct_source_postgres::AzureCredential::from_env(kind, client_id, env)
                        .map_err(|e| e.to_string())?,
                )
            }
        };
        c.check_auth().map_err(|e| e.to_string())?;
        Ok(c)
    }
}

/// Produce events to Kafka. Keyed by `schema.table` so all changes to a table
/// stay ordered within one partition. Each event goes to the first matching
/// `routes` entry, else to `topic`.
#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    /// Fallback topic for tables no route matches; may use `{schema}`/`{table}`.
    #[serde(default)]
    pub topic: Option<String>,
    /// Partitions to create topics with if they do not exist.
    #[serde(default = "default_partitions")]
    pub partitions: i32,
    /// Replicas for topics created if they do not exist. Unset: 3, or 1 (with a warning) on a
    /// cluster with fewer than 3 brokers.
    #[serde(default)]
    pub replication_factor: Option<i16>,
    /// `table` (default): key `schema.table`, per-table ordering. `key`: key also includes the
    /// primary-key values, spreading a table over partitions with per-row ordering.
    #[serde(default)]
    pub partition_by: Option<String>,
    #[serde(default)]
    pub routes: Vec<KafkaRouteConfig>,
    /// Where events go that a route cannot evaluate (a type error in `where`), for every route
    /// without its own `dead_letter_topic`. Unset: each route's topic plus `.dlq`.
    #[serde(default)]
    pub dead_letter_topic: Option<String>,
    /// Never accepted: the key for `hash` comes from `AQUEDUCT_HASH_KEY` or `hash_key_file`
    /// only. Read here so that a key pasted into the TOML fails startup instead of being ignored.
    #[serde(default)]
    pub hash_key: Option<Secret>,
    /// File holding the key for `hash` (one trailing newline is ignored). Or `AQUEDUCT_HASH_KEY`.
    #[serde(default)]
    pub hash_key_file: Option<String>,
    /// Short label of the hash key, sent as the `aqueduct-hash-key-id` header on every record of
    /// a hashing route. Required with a key.
    #[serde(default)]
    pub hash_key_id: Option<String>,
    /// How many topics content routing (a route `topic` that reads a row field) may create,
    /// counted from the broker at startup; beyond it events go to the route's `topic_fallback`.
    #[serde(default = "default_max_dynamic_topics")]
    pub max_dynamic_topics: usize,
    /// Encrypt with TLS and verify the broker certificate.
    #[serde(default)]
    pub tls: bool,
    /// PEM CA bundle for brokers with a private CA.
    #[serde(default)]
    pub tls_ca_file: Option<String>,
    /// `plain | scram-sha-256 | scram-sha-512`; requires username and password.
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    /// Or `AQUEDUCT_KAFKA_SASL_USERNAME`.
    #[serde(default)]
    pub sasl_username: Option<String>,
    /// Or `AQUEDUCT_KAFKA_SASL_PASSWORD`.
    #[serde(default)]
    pub sasl_password: Option<Secret>,
}

impl KafkaConfig {
    /// An explicit `replication_factor` must be at least 1.
    pub fn check_replication_factor(&self) -> Result<(), String> {
        match self.replication_factor {
            Some(r) if r < 1 => Err(format!(
                "sink.replication_factor must be at least 1, got {r}"
            )),
            _ => Ok(()),
        }
    }

    /// The key for `hash`, from `env` (the value of `AQUEDUCT_HASH_KEY`) or `hash_key_file`.
    /// `None` when neither is set; the route plan then refuses any route that hashes. Errors
    /// never contain the key.
    pub fn hash_key(&self, env: Option<String>) -> Result<Option<aqueduct_sinks::HashKey>, String> {
        if self.hash_key.is_some() {
            return Err(
                "sink.hash_key is not allowed in the config file, so the key never sits next to the config; set AQUEDUCT_HASH_KEY or sink.hash_key_file"
                    .into(),
            );
        }
        let env = env.filter(|v| !v.is_empty());
        let secret = match (env, &self.hash_key_file) {
            (Some(_), Some(_)) => {
                return Err("both AQUEDUCT_HASH_KEY and sink.hash_key_file are set; use one".into())
            }
            (Some(v), None) => v.into_bytes(),
            (None, Some(path)) => {
                std::fs::read(path).map_err(|e| format!("sink.hash_key_file {path}: {e}"))?
            }
            (None, None) if self.hash_key_id.is_some() => {
                return Err(
                    "sink.hash_key_id is set but there is no hash key; set AQUEDUCT_HASH_KEY or sink.hash_key_file, or remove hash_key_id"
                        .into(),
                )
            }
            (None, None) => return Ok(None),
        };
        // The same secret gives the same digests from the environment or a file: one trailing
        // newline (`\n` or `\r\n`, as `echo` or an editor leaves it) is not part of the key.
        let mut secret = secret;
        if secret.ends_with(b"\n") {
            secret.pop();
            if secret.ends_with(b"\r") {
                secret.pop();
            }
        }
        let id = self.hash_key_id.as_deref().ok_or(
            "sink.hash_key_id is required with a hash key: a short label consumers see in the aqueduct-hash-key-id header, changed whenever the key is",
        )?;
        aqueduct_sinks::HashKey::new(&secret, id)
            .map(Some)
            .map_err(|e| e.to_string())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaRouteConfig {
    /// Label for this route in per-route stats; defaults to `match`. Unique.
    #[serde(default)]
    pub name: Option<String>,
    /// `schema.table` with `*` wildcards, e.g. `public.orders`, `public.*`, `*.audit_*`.
    #[serde(rename = "match")]
    pub pattern: String,
    /// Destination topic template: `{schema}`, `{table}`, `{op}` and row fields such as
    /// `{row.region}` (content routing, which needs `topic_fallback`). Required, except with
    /// `outbox`, which sets the topic itself.
    #[serde(default)]
    pub topic: Option<String>,
    /// Where events go whose content-routed `topic` cannot be used (null, missing or disallowed
    /// value, or `max_dynamic_topics` reached). Required when `topic` reads a row field.
    #[serde(default)]
    pub topic_fallback: Option<String>,
    /// The only values a content-routed `topic` may substitute; others go to `topic_fallback`.
    #[serde(default)]
    pub topic_values: Option<Vec<String>>,
    /// Removed before release: filter operations in `where` (`op in ['insert', 'update']`).
    /// Still read, so a config that sets it fails startup with the `where` to use instead.
    #[serde(default)]
    pub ops: Option<toml::Value>,
    /// Predicate over the event (ADR 0002 §3); only events it is true for pass. Filters
    /// operations too: `op in ['insert', 'update']`.
    #[serde(default, rename = "where")]
    pub filter: Option<String>,
    /// Top-level columns of the row (`after`, else `before`) that make the record key, in order.
    /// Overrides `partition_by` for this route: the partition is hashed from the whole key.
    #[serde(default)]
    pub key: Option<Vec<String>>,
    /// Keep only these top-level columns. Exclusive with `exclude`.
    #[serde(default)]
    pub include: Option<Vec<String>>,
    /// Drop these top-level columns.
    #[serde(default)]
    pub exclude: Option<Vec<String>>,
    /// Strings become `"***"`, other values `null`.
    #[serde(default)]
    pub mask: Vec<String>,
    /// HMAC-SHA256 hex with the key from `AQUEDUCT_HASH_KEY` or `sink.hash_key_file`.
    #[serde(default)]
    pub hash: Vec<String>,
    /// `{ old = "new" }` column renames.
    #[serde(default)]
    pub rename: std::collections::BTreeMap<String, String>,
    /// `envelope` (default) or `row`: the record value is the change event or the row alone.
    #[serde(default)]
    pub format: Option<String>,
    /// With `format = "row"`: `tombstone` (default), `drop` or `row` (before + `__deleted`).
    #[serde(default)]
    pub deletes: Option<String>,
    /// Envelope format: a null-value record with the same key after every delete.
    #[serde(default)]
    pub tombstones: bool,
    /// `omit` (default) or `dead_letter`: rows lacking unchanged TOAST columns.
    #[serde(default)]
    pub unchanged: Option<String>,
    /// This route's dead-letter topic, a fixed name. Unset: `sink.dead_letter_topic`, else the
    /// route's topic plus `.dlq` (`<topic_fallback>.dlq` for a content-routed topic).
    #[serde(default)]
    pub dead_letter_topic: Option<String>,
    /// Publish the rows of an outbox table as their own events: inserts (and snapshot rows when
    /// `where` is true for them, e.g. `op in ['insert', 'read']`), key = the `aggregate_id`
    /// column, value = the `payload` column.
    /// Excludes `topic` and `key`.
    #[serde(default)]
    pub outbox: Option<OutboxConfig>,
}

/// `outbox = {...}` on a route. Unset keys take Debezium's outbox event router defaults
/// (`aqueduct_sinks::OutboxSpec::default`). Unknown keys fail startup, so a misspelled key never
/// silently falls back to a default column.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxConfig {
    #[serde(default)]
    pub aggregate_type: Option<String>,
    #[serde(default)]
    pub aggregate_id: Option<String>,
    #[serde(default)]
    pub payload: Option<String>,
    /// Topic template over the outbox columns, e.g. `outbox.{aggregate_type}`.
    #[serde(default)]
    pub topic: Option<String>,
    /// Column for the `id` header.
    #[serde(default)]
    pub id: Option<String>,
    /// Column for the `type` header.
    #[serde(default, rename = "type")]
    pub kind: Option<String>,
}

impl OutboxConfig {
    pub fn spec(&self) -> aqueduct_sinks::OutboxSpec {
        let d = aqueduct_sinks::OutboxSpec::default();
        aqueduct_sinks::OutboxSpec {
            aggregate_type: self.aggregate_type.clone().unwrap_or(d.aggregate_type),
            aggregate_id: self.aggregate_id.clone().unwrap_or(d.aggregate_id),
            payload: self.payload.clone().unwrap_or(d.payload),
            topic: self.topic.clone().unwrap_or(d.topic),
            id: self.id.clone(),
            kind: self.kind.clone(),
        }
    }
}

fn default_max_dynamic_topics() -> usize {
    aqueduct_sinks::DEFAULT_MAX_DYNAMIC_TOPICS
}

fn default_true() -> bool {
    true
}

fn default_partitions() -> i32 {
    1
}

fn default_slot() -> String {
    "aqueduct".into()
}
fn default_publication() -> String {
    "aqueduct".into()
}
/// True only when the host part is a literal loopback IP or `localhost`.
fn is_loopback_addr(addr: &str) -> bool {
    if let Ok(sa) = addr.parse::<std::net::SocketAddr>() {
        return sa.ip().is_loopback();
    }
    addr.rsplit_once(':')
        .is_some_and(|(h, _)| h.eq_ignore_ascii_case("localhost"))
}

fn default_health_addr() -> String {
    "0.0.0.0:8080".into()
}
fn default_checkpoint_path() -> String {
    "/var/lib/aqueduct/checkpoint".into()
}

/// Shortest admin token accepted. `openssl rand -hex 32` gives 64.
const MIN_ADMIN_TOKEN_LEN: usize = 16;

/// Placeholders that were published in this repo (the compose default until #30, docs samples).
const KNOWN_WEAK_ADMIN_TOKENS: &[&str] = &["dev-token-change-me", "change-me"];

impl Config {
    /// Reject an admin token that is obviously not a secret. An empty value still means "unset".
    /// A literal `${` is an unexpanded compose or shell placeholder, which would make the
    /// published placeholder text the token.
    pub fn check_admin_token(&self) -> Result<(), String> {
        let Some(t) = self.admin_token.as_deref().filter(|t| !t.is_empty()) else {
            return Ok(());
        };
        if t.trim().is_empty() {
            return Err(
                "admin_token is only whitespace; set a real secret (e.g. `openssl rand -hex 32`)"
                    .into(),
            );
        }
        if t != t.trim() {
            return Err(
                "admin_token has leading or trailing whitespace (a secrets file ending in a newline?); it could never match a Bearer header, so strip it"
                    .into(),
            );
        }
        if t.contains("${") {
            return Err(
                "admin_token contains `${`: an unexpanded variable placeholder, not a secret; check how AQUEDUCT_ADMIN_TOKEN is passed"
                    .into(),
            );
        }
        if KNOWN_WEAK_ADMIN_TOKENS
            .iter()
            .any(|w| t.eq_ignore_ascii_case(w))
        {
            return Err(
                "admin_token is a well-known placeholder (it was a published default); set a random secret (e.g. `openssl rand -hex 32`)"
                    .into(),
            );
        }
        if t.chars().count() < MIN_ADMIN_TOKEN_LEN {
            return Err(format!(
                "admin_token is shorter than {MIN_ADMIN_TOKEN_LEN} characters; use a random secret (e.g. `openssl rand -hex 32`)"
            ));
        }
        Ok(())
    }

    /// Refuse to expose the admin API without a token on a non-loopback bind, unless
    /// `admin_insecure_no_auth = true`. Returns whether the opt-out is in effect (caller logs it).
    pub fn check_admin_exposure(&self) -> Result<bool, String> {
        self.check_admin_token()?;
        let has_token = self.admin_token.as_deref().is_some_and(|t| !t.is_empty());
        if has_token || is_loopback_addr(&self.health_addr) {
            return Ok(false);
        }
        if self.admin_insecure_no_auth {
            return Ok(true);
        }
        Err(format!(
            "health_addr {} is not loopback and no admin_token is set; set admin_token (or AQUEDUCT_ADMIN_TOKEN), bind to 127.0.0.1, or set admin_insecure_no_auth = true",
            self.health_addr
        ))
    }

    /// Load TOML from `AQUEDUCT_CONFIG` (default `/etc/aqueduct/aqueduct.toml`),
    /// then let `AQUEDUCT_PG_URL` override the Postgres URL.
    pub fn load() -> Result<Self, String> {
        let path = std::env::var("AQUEDUCT_CONFIG")
            .unwrap_or_else(|_| "/etc/aqueduct/aqueduct.toml".into());
        let text = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))?;
        let mut cfg: Config = toml::from_str(&text).map_err(|e| format!("{path}: {e}"))?;
        if let Ok(t) = std::env::var("AQUEDUCT_ADMIN_TOKEN") {
            cfg.admin_token = Some(t);
        }
        if let Ok(url) = std::env::var("AQUEDUCT_PG_URL") {
            match (&mut cfg.postgres, &mut cfg.source) {
                (Some(p), _) => p.url = url,
                (None, Some(SourceConfig::Postgres(p))) => p.url = url,
                _ => {}
            }
        }
        cfg.apply_secret_env();
        Ok(cfg)
    }

    /// The configured source: `[source]`, or the legacy top-level `[postgres]` table.
    pub fn source(&self) -> Result<SourceConfig, String> {
        match (&self.source, &self.postgres) {
            (Some(s), None) => Ok(s.clone()),
            (None, Some(p)) => Ok(SourceConfig::Postgres(p.clone())),
            (Some(_), Some(_)) => Err("set either [source] or [postgres], not both".into()),
            (None, None) => Err("no source configured: add a [source] table".into()),
        }
    }

    /// Credentials may come from the environment so they stay out of config files.
    fn apply_secret_env(&mut self) {
        let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        if let Some(v) = env("AQUEDUCT_KAFKA_SASL_USERNAME") {
            self.sink.sasl_username = Some(v);
        }
        if let Some(v) = env("AQUEDUCT_KAFKA_SASL_PASSWORD") {
            self.sink.sasl_password = Some(Secret(v));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin_cfg(extra: &str) -> Config {
        toml::from_str(&format!(
            "{extra}
[postgres]
url = \"postgres://u@db/app\"
[sink]
brokers = [\"b:9093\"]
topic = \"t\"
"
        ))
        .unwrap()
    }

    #[test]
    fn replication_factor_is_optional_and_configurable() {
        assert_eq!(admin_cfg("").sink.replication_factor, None);
        let c: Config = toml::from_str(
            "[postgres]
url = \"postgres://u@db/app\"
[sink]
brokers = [\"b:9093\"]
topic = \"t\"
replication_factor = 2
",
        )
        .unwrap();
        assert_eq!(c.sink.replication_factor, Some(2));
        assert!(c.sink.check_replication_factor().is_ok());
        for bad in [0, -1] {
            let mut c = c.clone();
            c.sink.replication_factor = Some(bad);
            assert!(c.sink.check_replication_factor().is_err(), "{bad}");
        }
    }

    #[test]
    fn admin_exposure_loopback_without_token_ok() {
        for a in ["127.0.0.1:8080", "[::1]:8080", "localhost:8080"] {
            let c = admin_cfg(&format!("health_addr = \"{a}\""));
            assert_eq!(c.check_admin_exposure(), Ok(false), "{a}");
        }
    }

    #[test]
    fn admin_exposure_non_loopback_without_token_rejected() {
        for a in [
            "0.0.0.0:8080",
            "10.0.0.5:8080",
            "[::]:8080",
            "example.com:8080",
        ] {
            let c = admin_cfg(&format!("health_addr = \"{a}\""));
            assert!(c.check_admin_exposure().is_err(), "{a}");
        }
        // Default bind is 0.0.0.0, so a bare config must be rejected.
        assert!(admin_cfg("").check_admin_exposure().is_err());
        // An empty token is no token.
        assert!(admin_cfg("admin_token = \"\"")
            .check_admin_exposure()
            .is_err());
    }

    #[test]
    fn admin_exposure_token_or_explicit_opt_out() {
        let c = admin_cfg(
            "health_addr = \"0.0.0.0:8080\"
admin_token = \"0123456789abcdef\"",
        );
        assert_eq!(c.check_admin_exposure(), Ok(false));
        let c = admin_cfg(
            "health_addr = \"0.0.0.0:8080\"
admin_insecure_no_auth = true",
        );
        assert_eq!(c.check_admin_exposure(), Ok(true));
    }

    #[test]
    fn weak_admin_tokens_are_rejected_on_any_bind() {
        // (token, substring that identifies the rule that must fire)
        let cases: [(&str, &str); 11] = [
            ("k", "shorter than"),
            ("change-me", "well-known placeholder"),
            ("0123456789abcde", "shorter than"),     // 15 chars
            ("                ", "only whitespace"), // long enough, still whitespace
            (" ", "only whitespace"),
            ("dev-token-change-me", "well-known placeholder"),
            ("Dev-Token-Change-Me", "well-known placeholder"),
            ("0123456789abcdef\n", "leading or trailing whitespace"), // secrets file ending in a newline
            (" 0123456789abcdef", "leading or trailing whitespace"),
            (
                "${AQUEDUCT_ADMIN_TOKEN:?set AQUEDUCT_ADMIN_TOKEN to a secret value}",
                "unexpanded variable placeholder",
            ),
            ("prefix-${X}-0123456789", "unexpanded variable placeholder"),
        ];
        for bind in ["127.0.0.1:8080", "0.0.0.0:8080"] {
            for (t, rule) in cases {
                let c = admin_cfg(&format!("health_addr = \"{bind}\"\nadmin_token = {t:?}"));
                let err = c
                    .check_admin_exposure()
                    .expect_err(&format!("{bind} {t:?} should be rejected"));
                assert!(err.contains(rule), "{bind} {t:?}: {err}");
            }
        }
    }

    #[test]
    fn strong_admin_token_and_unset_token_pass_the_token_check() {
        let c = admin_cfg("admin_token = \"0123456789abcdef\"");
        assert_eq!(c.check_admin_token(), Ok(()));
        // Empty stays "unset": loopback without a token is still fine.
        let c = admin_cfg("health_addr = \"127.0.0.1:8080\"\nadmin_token = \"\"");
        assert_eq!(c.check_admin_exposure(), Ok(false));
    }

    #[test]
    fn secrets_are_redacted_in_debug() {
        let cfg: Config = toml::from_str(
            r#"
            [postgres]
            url = "postgres://u@db/app"
            [sink]
            brokers = ["b:9093"]
            topic = "t"
            tls = true
            sasl_mechanism = "scram-sha-512"
            sasl_username = "svc"
            sasl_password = "hunter2"
            "#,
        )
        .unwrap();
        let d = format!("{cfg:?}");
        assert!(!d.contains("hunter2") && d.contains("svc"));
    }

    fn sink_cfg(extra: &str) -> KafkaConfig {
        toml::from_str(&format!("brokers = ['b:9092']\ntopic = 't'\n{extra}")).unwrap()
    }

    const KEY: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn hash_key_comes_from_env_or_file_never_the_toml() {
        // In the TOML: rejected, and redacted in Debug like other secrets.
        let c = sink_cfg(&format!("hash_key = '{KEY}'\nhash_key_id = 'k1'"));
        let e = c.hash_key(None).unwrap_err();
        assert!(e.contains("sink.hash_key is not allowed"), "{e}");
        assert!(!e.contains(KEY));
        assert!(!format!("{c:?}").contains(KEY));

        // Neither: no key (a hashing route then fails to compile).
        assert!(sink_cfg("").hash_key(None).unwrap().is_none());
        assert!(sink_cfg("")
            .hash_key(Some(String::new()))
            .unwrap()
            .is_none());
        // A key id without a key fails closed.
        let e = sink_cfg("hash_key_id = 'k1'").hash_key(None).unwrap_err();
        assert!(
            e.contains("sink.hash_key_id is set but there is no hash key"),
            "{e}"
        );

        // From the environment, with its id.
        let c = sink_cfg("hash_key_id = 'k1'");
        let k = c.hash_key(Some(KEY.into())).unwrap().unwrap();
        assert_eq!(k.id(), "k1");
        assert!(!format!("{k:?}").contains(KEY));
        // A missing id, a short key.
        let e = sink_cfg("").hash_key(Some(KEY.into())).unwrap_err();
        assert!(e.contains("sink.hash_key_id is required"), "{e}");
        let e = c.hash_key(Some(KEY[..31].into())).unwrap_err();
        assert!(
            e.contains("shorter than 32 bytes") && !e.contains(&KEY[..31]),
            "{e}"
        );

        // From a file: one trailing newline is not part of the key.
        let dir = std::env::temp_dir().join(format!(
            "aqueduct-hash-key-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file = |name: &str, body: &str| {
            let p = dir.join(name);
            std::fs::write(&p, body).unwrap();
            sink_cfg(&format!(
                "hash_key_file = {:?}\nhash_key_id = 'k1'",
                p.to_str().unwrap()
            ))
        };
        let ok = file("ok", &format!("{KEY}\n"));
        assert!(ok.hash_key(None).unwrap().is_some());
        // A trailing newline is trimmed from the environment too: the same secret gives the same
        // digest either way, and `{KEY}\n` with a 31-byte key is still too short.
        let env_nl = c.hash_key(Some(format!("{KEY}\r\n"))).unwrap().unwrap();
        let file_key = ok.hash_key(None).unwrap().unwrap();
        let digest = |k: &aqueduct_sinks::HashKey| {
            let mut p = aqueduct_sinks::RoutePlan::build(
                vec![aqueduct_sinks::KafkaRoute {
                    pattern: "*".into(),
                    topic: "t".into(),
                    fields: aqueduct_sinks::FieldSpec {
                        hash: vec!["e".into()],
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                None,
                None,
                Some(k.clone()),
            )
            .unwrap();
            p.set_source(aqueduct_sinks::SourceInfo::default());
            let mut ev = aqueduct_core::ChangeEvent::heartbeat(aqueduct_core::Position::Lsn(1), 0);
            ev.op = aqueduct_core::Op::Insert;
            ev.schema = "s".into();
            ev.table = "t".into();
            ev.after = serde_json::value::RawValue::from_string(r#"{"e":"a@b.c"}"#.into()).ok();
            p.render(&ev, 0).unwrap().remove(0).value
        };
        assert_eq!(digest(&env_nl), digest(&file_key));
        assert!(c.hash_key(Some(format!("{}\n", &KEY[..31]))).is_err());
        let e = file("short", &format!("{}\r\n", &KEY[..31]))
            .hash_key(None)
            .unwrap_err();
        assert!(e.contains("shorter than 32 bytes"), "{e}");
        // Both sources: ambiguous, rejected.
        let e = ok.hash_key(Some(KEY.into())).unwrap_err();
        assert!(e.contains("use one"), "{e}");
        let missing = sink_cfg("hash_key_file = '/nonexistent/aqueduct-key'\nhash_key_id = 'k'");
        assert!(missing
            .hash_key(None)
            .unwrap_err()
            .contains("sink.hash_key_file /nonexistent/aqueduct-key"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn route_field_ops_parse() {
        let c = sink_cfg(
            "[[routes]]
match = 'public.users'
topic = 'users'
exclude = ['note']
mask = ['card']
hash = ['email']
rename = { cust_id = 'customer_id' }
",
        );
        let r = &c.routes[0];
        assert_eq!(r.exclude.as_deref(), Some(&["note".to_string()][..]));
        assert_eq!(r.mask, ["card"]);
        assert_eq!(r.hash, ["email"]);
        assert_eq!(r.rename["cust_id"], "customer_id");
        assert!(r.include.is_none());
    }

    const SINK: &str = "[sink]
brokers = ['b:9092']
topic = 't'
";

    #[test]
    fn source_table_and_legacy_postgres_both_work() {
        let legacy: Config = toml::from_str(&format!(
            "[postgres]
url='postgres://u@db/a'
{SINK}"
        ))
        .unwrap();
        assert_eq!(legacy.source().unwrap().type_name(), "postgres");
        let new: Config = toml::from_str(&format!(
            "[source]
type='postgres'
url='postgres://u@db/a'
{SINK}"
        ))
        .unwrap();
        assert_eq!(new.source().unwrap().type_name(), "postgres");
    }

    #[test]
    fn mongodb_source_is_typed() {
        let c: Config = toml::from_str(&format!(
            "[source]
type='mongodb'
uri='mongodb://h/?replicaSet=rs0'
database='app'
collections=['users']
{SINK}"
        ))
        .unwrap();
        match c.source().unwrap() {
            SourceConfig::Mongodb(m) => {
                assert_eq!(m.collections, ["users"]);
                assert_eq!(m.snapshot, aqueduct_source_mongodb::SnapshotMode::Never);
            }
            _ => panic!("expected mongodb"),
        }
        let c: Config = toml::from_str(&format!(
            "[source]
type='mongodb'
uri='mongodb://h/?replicaSet=rs0'
database='app'
snapshot='initial'
snapshot_chunk_size=10
snapshot_max_rows_per_sec=500
{SINK}"
        ))
        .unwrap();
        match c.source().unwrap() {
            SourceConfig::Mongodb(m) => {
                assert_eq!(m.snapshot, aqueduct_source_mongodb::SnapshotMode::Initial);
                assert_eq!(
                    (m.snapshot_chunk_size, m.snapshot_max_rows_per_sec),
                    (10, Some(500))
                );
            }
            _ => panic!("expected mongodb"),
        }
    }

    #[test]
    fn source_requires_exactly_one_definition() {
        let none: Config = toml::from_str(SINK).unwrap();
        assert!(none.source().is_err());
        let both: Config = toml::from_str(&format!(
            "[postgres]
url='x'
[source]
type='postgres'
url='y'
{SINK}"
        ))
        .unwrap();
        assert!(both.source().is_err());
        assert!(toml::from_str::<Config>(&format!(
            "[source]
type='redis'
{SINK}"
        ))
        .is_err());
    }

    #[test]
    fn snapshot_settings_default_off_and_parse() {
        use aqueduct_source_postgres::SnapshotMode;
        let pg = |extra: &str| -> Result<aqueduct_source_postgres::PostgresSourceConfig, String> {
            toml::from_str::<PostgresConfig>(&format!(
                "url = 'postgres://u@db/app'
ssl_mode = 'disable'
{extra}"
            ))
            .unwrap()
            .source_config()
        };
        let c = pg("").unwrap();
        assert_eq!(c.snapshot.mode, SnapshotMode::Never);
        assert_eq!(c.snapshot.chunk_size, 1024);
        let c = pg("snapshot = 'initial'
snapshot_chunk_size = 10
snapshot_max_rows_per_sec = 500
snapshot_key_columns = { 'public.events' = ['day', 'seq'] }")
        .unwrap();
        assert_eq!(c.snapshot.mode, SnapshotMode::Initial);
        assert_eq!(c.snapshot.chunk_size, 10);
        assert_eq!(c.snapshot.max_rows_per_sec, Some(500));
        assert_eq!(c.snapshot.key_columns["public.events"], ["day", "seq"]);
        assert!(pg("snapshot = 'always'").unwrap_err().contains("on_demand"));
        assert!(pg("snapshot_chunk_size = 0").is_err());
        assert!(pg("snapshot_key_columns = { events = ['a'] }").is_err());
        let c = pg("snapshot_watermark_table = 'public.aqueduct_watermarks'").unwrap();
        assert_eq!(
            c.snapshot.watermark_table.as_deref(),
            Some("public.aqueduct_watermarks")
        );
        assert!(pg("snapshot_watermark_table = 'aqueduct_watermarks'").is_err());
        use aqueduct_source_postgres::TransportKind;
        assert_eq!(pg("").unwrap().transport, TransportKind::Poll);
        let c = pg("transport = 'stream'
stream_buffer_bytes = 1048576")
        .unwrap();
        assert_eq!(
            (c.transport, c.stream_buffer_bytes),
            (TransportKind::Stream, 1_048_576)
        );
        assert!(pg("transport = 'copy'").unwrap_err().contains("poll"));
        assert!(pg("stream_buffer_bytes = 10").is_err());
    }

    #[test]
    fn heartbeat_defaults_and_overrides() {
        let pg = |extra: &str| -> PostgresConfig {
            toml::from_str(&format!(
                "url = 'postgres://u@db/app'
{extra}"
            ))
            .unwrap()
        };
        let c = pg("").source_config().unwrap();
        assert_eq!(c.heartbeat_interval, Some(Duration::from_secs(10)));
        assert_eq!(c.heartbeat_action_query, None);
        let c = pg("heartbeat_interval_secs = 0").source_config().unwrap();
        assert_eq!(c.heartbeat_interval, None);
        let c = pg("heartbeat_interval_secs = 3
heartbeat_action_query = 'SELECT 1'")
        .source_config()
        .unwrap();
        assert_eq!(c.heartbeat_interval, Some(Duration::from_secs(3)));
        assert_eq!(c.heartbeat_action_query.as_deref(), Some("SELECT 1"));
        // A blank query is no query.
        let c = pg("heartbeat_action_query = '  '").source_config().unwrap();
        assert_eq!(c.heartbeat_action_query, None);
    }

    #[test]
    fn rejects_bad_ssl_mode() {
        let mut p = PostgresConfig {
            url: "postgres://u@db/app".into(),
            slot: "s".into(),
            publication: "p".into(),
            ssl_mode: Some("sometimes".into()),
            ssl_root_cert: None,
            allow_slot_recreate: false,
            heartbeat_interval_secs: 10,
            heartbeat_action_query: None,
            checkpoint_skip_system_check: false,
            snapshot: None,
            snapshot_chunk_size: None,
            snapshot_max_rows_per_sec: None,
            snapshot_key_columns: HashMap::new(),
            snapshot_watermark_table: None,
            transport: None,
            stream_buffer_bytes: None,
            auth: None,
            azure_client_id: None,
        };
        assert!(p.source_config().is_err());
        p.ssl_mode = Some("verify-full".into());
        assert!(p.source_config().is_ok());
    }

    #[test]
    fn azure_managed_identity_auth() {
        use aqueduct_source_postgres::{AzureIdentity, SourceAuth};
        let pg = |extra: &str| -> PostgresConfig {
            toml::from_str(&format!(
                "url = 'postgres://aqueduct-mi@db.postgres.database.azure.com/app'
{extra}"
            ))
            .unwrap()
        };
        let wi = |k: &str| -> Option<String> {
            match k {
                "AZURE_CLIENT_ID" => Some("cid".into()),
                "AZURE_TENANT_ID" => Some("tid".into()),
                "AZURE_FEDERATED_TOKEN_FILE" => Some("/var/run/secrets/token".into()),
                _ => None,
            }
        };
        let aca = |k: &str| -> Option<String> {
            match k {
                "IDENTITY_ENDPOINT" => Some("http://localhost:42356/msi/token".into()),
                "IDENTITY_HEADER" => Some("hdr-s3cret".into()),
                _ => None,
            }
        };
        let none = |_: &str| -> Option<String> { None };
        // Default: the URL's password, no environment needed.
        assert!(matches!(
            pg("").source_config_with_env(&none).unwrap().auth,
            SourceAuth::Password
        ));
        assert!(matches!(
            pg("auth = 'password'")
                .source_config_with_env(&none)
                .unwrap()
                .auth,
            SourceAuth::Password
        ));
        match pg("auth = 'azure-workload-identity'")
            .source_config_with_env(&wi)
            .unwrap()
            .auth
        {
            SourceAuth::Azure(c) => assert_eq!(c.kind(), AzureIdentity::WorkloadIdentity),
            other => panic!("{other:?}"),
        }
        match pg("auth = 'azure-container-apps'
azure_client_id = 'uami'")
        .source_config_with_env(&aca)
        .unwrap()
        .auth
        {
            SourceAuth::Azure(c) => {
                assert_eq!(c.kind(), AzureIdentity::ContainerApps);
                assert!(!format!("{c:?}").contains("hdr-s3cret"));
            }
            other => panic!("{other:?}"),
        }
        // (extra config, env, a substring the error must contain)
        type Env<'a> = &'a dyn Fn(&str) -> Option<String>;
        let cases: [(&str, Env, &str); 7] = [
            ("auth = 'azure'", &wi, "expected \"password\""),
            ("auth = 'azure-workload-identity'", &none, "AZURE_CLIENT_ID"),
            ("auth = 'azure-container-apps'", &none, "IDENTITY_ENDPOINT"),
            ("auth = 'azure-container-apps'", &wi, "IDENTITY_ENDPOINT"),
            ("azure_client_id = 'x'", &wi, "azure_client_id is set"),
            (
                "auth = 'azure-workload-identity'
ssl_mode = 'disable'",
                &wi,
                "must be verified",
            ),
            (
                "auth = 'azure-container-apps'
ssl_mode = 'verify-ca'
ssl_root_cert = '/ca.pem'",
                &aca,
                "\"verify-ca\"",
            ),
        ];
        for (extra, env, needle) in cases {
            let e = pg(extra).source_config_with_env(env).unwrap_err();
            assert!(e.contains(needle), "{extra}: {e}");
            assert!(!e.contains("hdr-s3cret"), "{extra}: {e}");
        }
        let e = toml::from_str::<PostgresConfig>(
            "url = 'postgres://mi:pw@db/app'
auth = 'azure-container-apps'",
        )
        .unwrap()
        .source_config_with_env(&aca)
        .unwrap_err();
        assert!(e.contains("also carries a password"), "{e}");
    }

    #[test]
    fn example_config_is_kafka_only() {
        let cfg: Config =
            toml::from_str(include_str!("../../../config/aqueduct.example.toml")).unwrap();
        assert_eq!(cfg.sink.brokers, vec!["kafka:9092"]);
        assert!(cfg.inspector);
        // Other sink types are gone: a stdout sink has no brokers, so it is rejected.
        assert!(toml::from_str::<Config>(
            "[postgres]
url='x'
[sink]
type='stdout'
"
        )
        .is_err());
    }
}
