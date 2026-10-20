//! The replication connection task over arbitrary server bytes after `START_REPLICATION`:
//! `XLogData` and keepalive parsing, reply requests and the byte-bounded channel. It must end
//! with the stream's end or an error, never panic or hang.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    aqueduct_source_postgres::replication::fuzz_stream(data);
});
