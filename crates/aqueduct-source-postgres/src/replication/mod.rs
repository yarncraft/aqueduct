//! Streaming replication client: a `replication=database` connection that receives pgoutput
//! messages as they are written (ADR 0003).
//!
//! Nothing here is wired into [`crate::PostgresSource`] yet, which keeps polling the slot over
//! SQL. The connection uses the same TLS policy as the SQL connection ([`ConnectOptions`]) and
//! authenticates with trust, cleartext, md5 or SCRAM-SHA-256 (`-PLUS` over TLS).

mod auth;
mod client;
pub mod frame;
mod secure;

pub use auth::AuthMethod;
pub use client::{
    ConnectOptions, ReplicationClient, ReplicationStream, StartReplication, StatusSender,
    SystemInfo,
};
pub use frame::Replication;
