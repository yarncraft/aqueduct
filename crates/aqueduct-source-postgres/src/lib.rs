//! Postgres logical-replication source for Aqueduct.
//!
//! [`decoder`] is a pure pgoutput (protocol v1) parser; [`PostgresSource`]
//! drives a logical replication slot over a regular SQL connection and
//! implements [`aqueduct_core::Source`].

pub mod decoder;
mod source;

pub use source::{parse_lsn, PostgresSource, PostgresSourceConfig};
