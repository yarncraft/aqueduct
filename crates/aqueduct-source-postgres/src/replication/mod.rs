//! Streaming replication client: a `replication=database` connection that receives pgoutput
//! messages as they are written (ADR 0003).
//!
//! This slice only frames and receives. Nothing here is wired into [`crate::PostgresSource`],
//! which keeps polling the slot over SQL. TLS and password authentication come in a later slice,
//! so the client accepts trust authentication only and says so when the server asks for more.

mod client;
pub mod frame;

pub use client::{ReplicationClient, SystemInfo};
pub use frame::Replication;
