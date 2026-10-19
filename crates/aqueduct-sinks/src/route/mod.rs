//! Route plan: filters, topic templates and field operations applied inside the Kafka sink
//! (ADR 0002). Everything here is pure: no I/O, no Kafka types.

pub mod expr;
