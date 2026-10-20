//! Pure building blocks for watermark-based snapshots (ADR 0001): the window logic that merges a
//! chunk read with the live stream, and the composite checkpoint that makes snapshot progress
//! durable in the same write as the stream position.
//!
//! Nothing here does I/O or knows about Postgres or MongoDB, so both sources share it. It is not
//! wired into any source yet.

mod checkpoint;
mod window;
#[cfg(test)]
mod window_tests;

pub use checkpoint::{Checkpoint, SnapState};
pub use window::{ChunkId, ChunkRow, Mark, Outcome, Window, WindowError};
