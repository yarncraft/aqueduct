use serde::Deserialize;

/// A credential that never shows up in `Debug` output (and so not in logs).
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
    /// Serve the cockpit's event inspector, which reads recent events back from
    /// Kafka. It exposes row data, so it also needs `admin_token`.
    #[serde(default = "default_true")]
    pub inspector: bool,
    #[serde(default = "default_checkpoint_path")]
    pub checkpoint_path: String,
}

/// Which system changes are read from, picked by `type`: postgres, kafka or mongodb.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SourceConfig {
    Postgres(PostgresConfig),
    Kafka(KafkaSourceConfig),
    Mongodb(aqueduct_source_mongodb::MongoConfig),
}

impl SourceConfig {
    pub fn type_name(&self) -> &'static str {
        match self {
            SourceConfig::Postgres(_) => "postgres",
            SourceConfig::Kafka(_) => "kafka",
            SourceConfig::Mongodb(_) => "mongodb",
        }
    }
}

/// Consume Kafka topics as the change source. Offsets are kept in Aqueduct's checkpoint, not in
/// a consumer group.
#[derive(Debug, Clone, Deserialize)]
pub struct KafkaSourceConfig {
    pub brokers: Vec<String>,
    /// Topic names to read; `*` matches any run of characters.
    pub topics: Vec<String>,
    /// Where partitions without a checkpoint begin: `earliest` (default) or `latest`.
    #[serde(default)]
    pub start_at: Option<String>,
    /// `auto` (default): Aqueduct events pass through, other values are wrapped. `raw`: always wrap.
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub tls: bool,
    #[serde(default)]
    pub tls_ca_file: Option<String>,
    /// `plain | scram-sha-256 | scram-sha-512`; requires username and password.
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    /// Or `AQUEDUCT_KAFKA_SOURCE_SASL_USERNAME`.
    #[serde(default)]
    pub sasl_username: Option<String>,
    /// Or `AQUEDUCT_KAFKA_SOURCE_SASL_PASSWORD`.
    #[serde(default)]
    pub sasl_password: Option<Secret>,
}

impl KafkaSourceConfig {
    pub fn source_config(&self) -> Result<aqueduct_source_kafka::KafkaSourceConfig, String> {
        if self.brokers.is_empty() || self.topics.is_empty() {
            return Err("kafka source needs brokers and topics".into());
        }
        let sasl = match (
            &self.sasl_mechanism,
            &self.sasl_username,
            &self.sasl_password,
        ) {
            (None, None, None) => None,
            (Some(m), Some(u), Some(p)) => Some(aqueduct_sinks::KafkaSasl {
                mechanism: m.parse()?,
                username: u.clone(),
                password: p.expose(),
            }),
            _ => {
                return Err(
                    "kafka sasl needs sasl_mechanism, sasl_username and sasl_password together"
                        .into(),
                )
            }
        };
        Ok(aqueduct_source_kafka::KafkaSourceConfig {
            brokers: self.brokers.clone(),
            topics: self.topics.clone(),
            security: aqueduct_sinks::KafkaSecurity {
                tls: self.tls,
                tls_ca_file: self.tls_ca_file.clone(),
                sasl,
            },
            start_at: self
                .start_at
                .as_deref()
                .map(str::parse)
                .transpose()?
                .unwrap_or_default(),
            format: self
                .format
                .as_deref()
                .map(str::parse)
                .transpose()?
                .unwrap_or_default(),
        })
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
    /// in the URL; default is the URL's, else `prefer`.
    #[serde(default)]
    pub ssl_mode: Option<String>,
    /// PEM CA bundle used to verify the server (overrides `sslrootcert` in the URL).
    #[serde(default)]
    pub ssl_root_cert: Option<String>,
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
fn default_health_addr() -> String {
    "0.0.0.0:8080".into()
}
fn default_checkpoint_path() -> String {
    "/var/lib/aqueduct/checkpoint".into()
}

impl Config {
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
        if let Some(SourceConfig::Kafka(k)) = &mut self.source {
            if let Some(v) = env("AQUEDUCT_KAFKA_SOURCE_SASL_USERNAME") {
                k.sasl_username = Some(v);
            }
            if let Some(v) = env("AQUEDUCT_KAFKA_SOURCE_SASL_PASSWORD") {
                k.sasl_password = Some(Secret(v));
            }
        }
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
        let k: Config = toml::from_str(&format!(
            "[source]
type='kafka'
brokers=['k:9092']
topics=['in.*']
{SINK}"
        ))
        .unwrap();
        assert_eq!(k.source().unwrap().type_name(), "kafka");
        let SourceConfig::Kafka(kc) = k.source().unwrap() else {
            panic!("kafka source")
        };
        assert!(kc.source_config().is_ok());
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
            SourceConfig::Mongodb(m) => assert_eq!(m.collections, ["users"]),
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
    fn rejects_bad_ssl_mode() {
        let mut p = PostgresConfig {
            url: "postgres://u@db/app".into(),
            slot: "s".into(),
            publication: "p".into(),
            ssl_mode: Some("sometimes".into()),
            ssl_root_cert: None,
            allow_slot_recreate: false,
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
