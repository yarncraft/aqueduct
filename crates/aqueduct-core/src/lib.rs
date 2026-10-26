//! Core types and traits shared by every Aqueduct crate.
//!
//! A [`Source`] produces [`ChangeEvent`]s (e.g. from Postgres logical
//! replication). The pipeline hands each event to a [`Sink`] and, only after
//! the sink succeeds, acknowledges the event's [`Position`] back to the source
//! (at-least-once delivery).

mod control;
mod event;
mod lag;
mod metrics;
mod pipeline;
mod redact;
mod snapshot;

pub use control::{
    SnapshotCommand, SnapshotReceiver, SnapshotRequest, SnapshotSender, snapshot_channel,
};
pub use event::{ChangeEvent, Op, Position};
pub use lag::{
    CAUGHT_UP_BYTES, CAUGHT_UP_MS, HEAD_STALE_MS, LAG_WINDOW, LagState, LagThreshold,
    STALLED_AFTER_SECS, SnapshotRate, SourceLag,
};
pub use metrics::{
    Delivered, HeartbeatSnapshot, Metrics, OP_SLOTS, SkippedTable, Snapshot, SnapshotStatus, State,
    TableMark,
};
pub use pipeline::{
    CheckpointStore, Delivery, Error, MAX_BATCH, Result, Sink, Source, run_pipeline,
    run_pipeline_with_metrics,
};
pub use redact::redact_password;
pub use snapshot::{Checkpoint, ChunkId, ChunkRow, Mark, Outcome, SnapState, Window, WindowError};
