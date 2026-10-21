//! Postgres logical-replication source for Aqueduct.
//!
//! [`decoder`] is a pure pgoutput (protocol v1) parser; [`PostgresSource`]
//! drives a logical replication slot over a regular SQL connection and
//! implements [`aqueduct_core::Source`].
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

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
    parse_lsn, PostgresSource, PostgresSourceConfig, SourceAuth, TransportKind,
    DEFAULT_HEARTBEAT_INTERVAL,
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
