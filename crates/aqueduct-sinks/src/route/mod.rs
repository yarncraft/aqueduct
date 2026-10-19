//! Route plan: how change events become Kafka records (ADR 0002). Everything here is pure: no
//! I/O and no Kafka types, so it is unit-testable and benchmarkable without a broker.

mod plan;

pub use plan::{KafkaRoute, OutRecord, RouteCount, RoutePlan, RouteStats};
