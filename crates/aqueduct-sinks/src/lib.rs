//! The Kafka [`Sink`](aqueduct_core::Sink) (the only sink) and the Kafka-backed
//! event inspector. Events are encoded as one JSON object per record.
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod inspect;
mod kafka;
pub mod route;

pub use inspect::{InspectedEvent, Inspection, KafkaInspector};
pub use kafka::{
    glob_match, KafkaRoute, KafkaSasl, KafkaSecurity, KafkaSink, PartitionBy, SaslMechanism,
    TopicRouter,
};

use aqueduct_core::{ChangeEvent, Error, Result};

pub(crate) fn encode(event: &ChangeEvent) -> Result<String> {
    serde_json::to_string(event).map_err(|e| Error::Sink(format!("encode: {e}")))
}

#[cfg(test)]
pub(crate) fn sample() -> ChangeEvent {
    use aqueduct_core::{Op, Position};
    ChangeEvent {
        position: Position::Lsn(0x16_B374_D848),
        xid: 7,
        commit_ts_micros: 1_700_000_000_000_000,
        schema: "public".into(),
        table: "users".into(),
        op: Op::Insert,
        before: None,
        after: serde_json::value::RawValue::from_string(r#"{"id":1,"name":"ada"}"#.into()).ok(),
        key: Some("1".into()),
    }
}
