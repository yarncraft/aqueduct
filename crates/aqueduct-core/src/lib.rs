//! Core types and traits shared by every Aqueduct crate.
//!
//! A [`Source`] produces [`ChangeEvent`]s (e.g. from Postgres logical
//! replication). The pipeline hands each event to a [`Sink`] and, only after
//! the sink succeeds, acknowledges the event's [`Position`] back to the source
//! (at-least-once delivery).
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod control;
mod event;
mod metrics;
mod pipeline;
mod snapshot;

pub use control::{
    snapshot_channel, SnapshotCommand, SnapshotReceiver, SnapshotRequest, SnapshotSender,
};
pub use event::{ChangeEvent, Op, Position};
pub use metrics::{
    HeartbeatSnapshot, Metrics, SkippedTable, Snapshot, SnapshotStatus, State, OP_SLOTS,
};
pub use pipeline::{
    run_pipeline, run_pipeline_with_metrics, CheckpointStore, Error, Result, Sink, Source,
    MAX_BATCH,
};
pub use snapshot::{Checkpoint, ChunkId, ChunkRow, Mark, Outcome, SnapState, Window, WindowError};
