//! Postgres logical-replication source for Aqueduct.
//!
//! [`decoder`] is a pure pgoutput (protocol v1) parser; [`PostgresSource`]
//! drives a logical replication slot over a regular SQL connection and
//! implements [`aqueduct_core::Source`].
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod catalog;
pub mod decoder;
pub mod replication;
pub mod snapshot;
mod source;
mod tls;
pub mod transport;

pub use snapshot::{SnapshotConfig, SnapshotMode};
pub use source::{
    parse_lsn, PostgresSource, PostgresSourceConfig, TransportKind, DEFAULT_HEARTBEAT_INTERVAL,
};
pub use tls::SslMode;
