//! Core types and traits shared by every Aqueduct crate.
//!
//! A [`Source`] produces [`ChangeEvent`]s (e.g. from Postgres logical
//! replication). The pipeline hands each event to a [`Sink`] and, only after
//! the sink succeeds, acknowledges the event's [`Position`] back to the source
//! (at-least-once delivery).
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod control;
mod event;
mod lag;
mod metrics;
mod pipeline;
mod redact;
mod snapshot;

pub use control::{
    snapshot_channel, SnapshotCommand, SnapshotReceiver, SnapshotRequest, SnapshotSender,
};
pub use event::{ChangeEvent, Op, Position};
pub use lag::{
    LagState, LagThreshold, SnapshotRate, SourceLag, CAUGHT_UP_BYTES, CAUGHT_UP_MS, HEAD_STALE_MS,
    LAG_WINDOW, STALLED_AFTER_SECS,
};
pub use metrics::{
    HeartbeatSnapshot, Metrics, SkippedTable, Snapshot, SnapshotStatus, State, TableMark, OP_SLOTS,
};
pub use pipeline::{
    run_pipeline, run_pipeline_with_metrics, CheckpointStore, Error, Result, Sink, Source,
    MAX_BATCH,
};
pub use redact::redact_password;
pub use snapshot::{Checkpoint, ChunkId, ChunkRow, Mark, Outcome, SnapState, Window, WindowError};
