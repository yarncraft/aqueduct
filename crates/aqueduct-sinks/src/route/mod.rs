//! Route plan: how change events become Kafka records, with filters, topic templates and field
//! operations applied inside the Kafka sink (ADR 0002). Everything here is pure: no I/O and no
//! Kafka types, so it is unit-testable and benchmarkable without a broker.

pub mod expr;
mod fields;
mod plan;

pub use fields::{FieldSpec, HASH_KEY_ID_HEADER, HashKey, MIN_HASH_KEY_BYTES};
pub use plan::{
    BatchCounts, ContentTopic, DEFAULT_MAX_DYNAMIC_TOPICS, KafkaRoute, Otherwise, OutRecord,
    OutboxSpec, PartitionKey, RouteCount, RouteCounts, RouteDescription, RoutePlan, RouteStats,
    RoutesDescription, UNSEEN_FIELD_ROWS,
};
