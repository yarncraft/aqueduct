//! A Kafka [`Source`](aqueduct_core::Source): consume topics and feed the records through the
//! pipeline, e.g. to re-key or fan out events into other topics.
//!
//! Offsets are not committed to the broker (there is no consumer group). The position is the
//! next offset to read per `topic:partition`, kept as JSON text in Aqueduct's own checkpoint,
//! so it is saved only after the sink has the events (at-least-once, like every source).

mod source;

pub use source::{Format, KafkaSource, KafkaSourceConfig, StartAt};
