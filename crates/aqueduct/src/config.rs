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
}

fn default_heartbeat_interval_secs() -> u64 {
    aqueduct_source_postgres::DEFAULT_HEARTBEAT_INTERVAL.as_secs()
}

impl PostgresConfig {
    pub fn source_config(&self) -> Result<aqueduct_source_postgres::PostgresSourceConfig, String> {
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaRouteConfig {
    /// `schema.table` with `*` wildcards, e.g. `public.orders`, `public.*`, `*.audit_*`.
    #[serde(rename = "match")]
    pub pattern: String,
    /// Destination topic; `{schema}` and `{table}` are substituted.
    pub topic: String,
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
        };
        assert!(p.source_config().is_err());
        p.ssl_mode = Some("verify-full".into());
        assert!(p.source_config().is_ok());
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
