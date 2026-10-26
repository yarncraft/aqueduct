//! Postgres logical-replication source for Aqueduct.
//!
//! [`decoder`] is a pure pgoutput (protocol v1) parser; [`PostgresSource`]
//! streams a logical replication slot over a replication connection and
//! implements [`aqueduct_core::Source`].

pub mod azure;
pub mod catalog;
pub mod decoder;
pub mod replication;
pub mod snapshot;
mod source;
mod tls;
pub mod transport;

pub use azure::{AccessToken, AzureCredential, AzureIdentity};
pub use snapshot::{SnapshotConfig, SnapshotMode};
pub use source::{
    DEFAULT_HEARTBEAT_INTERVAL, PostgresSource, PostgresSourceConfig, SourceAuth, parse_lsn,
};
pub use tls::SslMode;

/// The database a connection URL names (libpq's default: the user name), for the `db` in every
/// record's `source` block and key.
pub fn database_name(url: &str) -> Option<String> {
    let parsed = tls::parse(url).ok()?;
    parsed
        .config
        .get_dbname()
        .or(parsed.config.get_user())
        .map(str::to_owned)
}
