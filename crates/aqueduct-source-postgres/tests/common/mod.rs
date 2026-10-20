//! Shared by the integration tests.

use aqueduct_source_postgres::TransportKind;

/// The transport the suite runs with: `AQUEDUCT_TEST_PG_TRANSPORT=stream` runs every source in
/// stream mode (ADR 0003 T4 asks for the whole suite in both modes); unset or `poll` polls.
pub fn transport() -> TransportKind {
    match std::env::var("AQUEDUCT_TEST_PG_TRANSPORT").as_deref() {
        Ok("stream") => TransportKind::Stream,
        Ok("poll") | Err(_) => TransportKind::Poll,
        Ok(other) => panic!("AQUEDUCT_TEST_PG_TRANSPORT={other:?}: expected poll or stream"),
    }
}
