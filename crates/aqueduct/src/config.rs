use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub postgres: PostgresConfig,
    #[serde(default)]
    pub sink: SinkConfig,
    #[serde(default = "default_health_addr")]
    pub health_addr: String,
    /// Protects `/api/*` on the admin cockpit when set. Also read from
    /// `AQUEDUCT_ADMIN_TOKEN`.
    #[serde(default)]
    pub admin_token: Option<String>,
    /// How many recent events the cockpit's event inspector keeps in memory
    /// (`0` disables it; events contain row data, so protect the port).
    #[serde(default = "default_inspector_buffer")]
    pub inspector_buffer: usize,
    #[serde(default = "default_checkpoint_path")]
    pub checkpoint_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    pub url: String,
    #[serde(default = "default_slot")]
    pub slot: String,
    #[serde(default = "default_publication")]
    pub publication: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SinkConfig {
    #[default]
    Stdout,
    File {
        path: String,
    },
    Webhook {
        url: String,
    },
    /// Produce events to Kafka. Keyed by `schema.table` so all changes to a
    /// table stay ordered within one partition. Each event goes to the first
    /// matching `routes` entry, else to `topic`.
    Kafka {
        brokers: Vec<String>,
        /// Fallback topic for tables no route matches; may use `{schema}`/`{table}`.
        #[serde(default)]
        topic: Option<String>,
        /// Partitions to create topics with if they do not exist.
        #[serde(default = "default_partitions")]
        partitions: i32,
        #[serde(default)]
        routes: Vec<KafkaRouteConfig>,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaRouteConfig {
    /// `schema.table` with `*` wildcards, e.g. `public.orders`, `public.*`, `*.audit_*`.
    #[serde(rename = "match")]
    pub pattern: String,
    /// Destination topic; `{schema}` and `{table}` are substituted.
    pub topic: String,
}

fn default_inspector_buffer() -> usize {
    aqueduct_core::DEFAULT_RECENT_EVENTS
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
            cfg.postgres.url = url;
        }
        Ok(cfg)
    }
}
