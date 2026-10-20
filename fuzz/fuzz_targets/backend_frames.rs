//! Backend message framing on a replication connection: arbitrary bytes, fed in chunks, must
//! decode to messages or a protocol error, never panic or allocate past what was fed.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    aqueduct_source_postgres::replication::frame::fuzz_frames(data);
});
