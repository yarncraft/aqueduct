//! Core types and traits shared by every Aqueduct crate.
//!
//! A [`Source`] produces [`ChangeEvent`]s (e.g. from Postgres logical
//! replication). The pipeline hands each event to a [`Sink`] and, only after
//! the sink succeeds, acknowledges the event's [`Position`] back to the source
//! (at-least-once delivery).

mod event;
mod metrics;
mod pipeline;

pub use event::{ChangeEvent, Op, Position};
pub use metrics::{Metrics, Snapshot, State};
pub use pipeline::{
    run_pipeline, run_pipeline_with_metrics, CheckpointStore, Error, Result, Sink, Source,
    MAX_BATCH,
};
