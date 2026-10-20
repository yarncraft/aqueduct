//! `CopyData` payloads after `START_REPLICATION`: `XLogData` and keepalive parsing.
#![no_main]

use aqueduct_source_postgres::replication::frame::parse_copy_data;
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = parse_copy_data(Bytes::copy_from_slice(data));
});
