//! Built-in [`Sink`](aqueduct_core::Sink) implementations: stdout, JSON-lines
//! file, and HTTP webhook. All encode events as one JSON object per event.

mod file;
mod stdout;
mod webhook;

pub use file::FileSink;
pub use stdout::StdoutSink;
pub use webhook::WebhookSink;

use aqueduct_core::{ChangeEvent, Error, Result};

pub(crate) fn encode(event: &ChangeEvent) -> Result<String> {
    serde_json::to_string(event).map_err(|e| Error::Sink(format!("encode: {e}")))
}

#[cfg(test)]
pub(crate) fn sample() -> ChangeEvent {
    use aqueduct_core::{Lsn, Op};
    ChangeEvent {
        lsn: Lsn(0x16_B374_D848),
        xid: 7,
        commit_ts_micros: 1_700_000_000_000_000,
        schema: "public".into(),
        table: "users".into(),
        op: Op::Insert,
        before: None,
        after: serde_json::json!({"id": 1, "name": "ada"})
            .as_object()
            .cloned(),
    }
}
