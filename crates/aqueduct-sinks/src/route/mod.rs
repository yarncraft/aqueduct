//! Route plan: how change events become Kafka records, with filters, topic templates and field
//! operations applied inside the Kafka sink (ADR 0002). Everything here is pure: no I/O and no
//! Kafka types, so it is unit-testable and benchmarkable without a broker.

pub mod expr;
mod plan;

pub use plan::{KafkaRoute, OutRecord, RouteCount, RoutePlan, RouteStats};
