//! Core types and traits shared by every Aqueduct crate.
//!
//! A [`Source`] produces [`ChangeEvent`]s (e.g. from Postgres logical
//! replication). The pipeline hands each event to a [`Sink`] and, only after
//! the sink succeeds, acknowledges the event's [`Lsn`] back to the source
//! (at-least-once delivery).

mod event;
mod metrics;
mod pipeline;

pub use event::{ChangeEvent, Lsn, Op};
pub use metrics::{Metrics, RecordedEvent, Snapshot, State, DEFAULT_RECENT_EVENTS};
pub use pipeline::{
    run_pipeline, run_pipeline_with_metrics, CheckpointStore, Error, Result, Sink, Source,
};
