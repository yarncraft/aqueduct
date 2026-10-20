//! Streaming replication client: a `replication=database` connection that receives pgoutput
//! messages as they are written (ADR 0003).
//!
//! Nothing here is wired into [`crate::PostgresSource`] yet, which keeps polling the slot over
//! SQL. The connection uses the same TLS policy as the SQL connection ([`ConnectOptions`]) and
//! authenticates with trust, cleartext, md5 or SCRAM-SHA-256 (`-PLUS` over TLS). After
//! `START_REPLICATION`, [`ReplicationHandle`] runs the session in its own task.

mod auth;
mod client;
pub mod frame;
mod secure;
mod task;

pub use auth::AuthMethod;
pub use client::{
    ConnectOptions, ReplicationClient, ReplicationStream, StartReplication, StatusSender,
    SystemInfo,
};
pub use frame::Replication;
#[doc(hidden)]
pub use task::fuzz_stream;
pub use task::{
    ReplicationHandle, StreamOptions, DEFAULT_STATUS_INTERVAL, DEFAULT_STREAM_BUFFER_BYTES,
};
