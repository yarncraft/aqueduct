//! Postgres logical-replication source for Aqueduct.
//!
//! [`decoder`] is a pure pgoutput (protocol v1) parser; [`PostgresSource`]
//! drives a logical replication slot over a regular SQL connection and
//! implements [`aqueduct_core::Source`].
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod decoder;
mod source;
mod tls;

pub use source::{parse_lsn, PostgresSource, PostgresSourceConfig, DEFAULT_HEARTBEAT_INTERVAL};
pub use tls::SslMode;
