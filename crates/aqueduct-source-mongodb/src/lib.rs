//! MongoDB change-stream source. The resume token is the checkpoint position.
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
pub mod config;
pub mod mapping;
pub mod snapshot;
pub mod source;

pub use config::{MongoConfig, SnapshotMode};
pub use source::MongoSource;
