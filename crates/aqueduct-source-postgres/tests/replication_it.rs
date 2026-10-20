//! Streaming replication client against a real server. Needs a Postgres with
//! `wal_level=logical` that accepts the `postgres` user **without a password** on replication
//! connections (`trust`), because the client does not do password authentication yet:
//!
//! ```text
//! podman run -d -p 5435:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16 -c wal_level=logical
//! AQUEDUCT_TEST_PG_TRUST=localhost:5435 cargo test -p aqueduct-source-postgres \
//!     --test replication_it -- --ignored
//! ```
//!
//! `AQUEDUCT_TEST_PG_TRUST` must be set: the test fails when it is not, so a run that skips
//! the server cannot report green.

use std::time::Duration;

use aqueduct_source_postgres::parse_lsn;
use aqueduct_source_postgres::replication::{Replication, ReplicationClient, StartReplication};
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "needs a trust-auth Postgres server, see the module docs"]
async fn streamed_transaction_matches_the_peeked_one() {
    let addr = std::env::var("AQUEDUCT_TEST_PG_TRUST")
        .expect("AQUEDUCT_TEST_PG_TRUST must name a trust-auth Postgres, see the module docs");
    let url = format!("postgres://postgres@{addr}/postgres?sslmode=disable");
    let (db, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = format!("{}_{}", std::process::id(), nanos % 1_000_000_000);
    // A mixed-case publication name: both transports must read the same publication from the
    // same text (`publication_names` is case-folded by pgoutput unless quoted inside).
    let (table, publication) = (format!("repl_{id}"), format!("Aq_Repl_Pub_{id}"));
    let (stream_slot, peek_slot) = (format!("aq_repl_s_{id}"), format!("aq_repl_p_{id}"));
    db.batch_execute(&format!(
        "CREATE TABLE {table} (k int PRIMARY KEY, v text NOT NULL);
         CREATE PUBLICATION \"{publication}\" FOR TABLE {table};"
    ))
    .await
    .unwrap();
    for slot in [&stream_slot, &peek_slot] {
        db.execute(
            "SELECT pg_create_logical_replication_slot($1::text, 'pgoutput')",
            &[slot],
        )
        .await
        .unwrap();
    }

    let mut client = ReplicationClient::connect(&url).await.unwrap();
    let info = client.identify_system().await.unwrap();
    assert!(info.xlogpos > 0 && info.timeline >= 1);
    let quoted = format!("\"{publication}\"");
    let (mut stream, mut status) = client
        .start_replication(&StartReplication {
            slot: &stream_slot,
            start_lsn: 0,
            publication: &quoted,
            logical_messages: false,
        })
        .await
        .unwrap();

    db.batch_execute(&format!(
        "BEGIN; INSERT INTO {table} VALUES (1, 'a'), (2, 'b'); \
         UPDATE {table} SET v = 'c' WHERE k = 1; COMMIT;"
    ))
    .await
    .unwrap();

    // Every pgoutput message of the transaction, Begin to Commit, as the stream delivers it.
    let streamed: Vec<(u64, Vec<u8>)> = tokio::time::timeout(Duration::from_secs(20), async {
        let mut msgs = Vec::new();
        loop {
            match stream.next_message().await.unwrap().expect("stream ended") {
                Replication::XLogData {
                    wal_start, data, ..
                } => {
                    let commit = data.first() == Some(&b'C');
                    msgs.push((wal_start, data.to_vec()));
                    if commit {
                        break msgs;
                    }
                }
                Replication::Keepalive {
                    reply_requested: true,
                    ..
                } => status.send_status(0, false).await.unwrap(),
                Replication::Keepalive { .. } => {}
            }
        }
    })
    .await
    .expect("no complete transaction arrived over the stream");

    // The same transaction as a peek returns it from the other slot, with the same text for
    // `publication_names`.
    let rows = db
        .query(
            "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes($1::text::name, NULL, NULL, \
             'proto_version', '1', 'publication_names', $2::text)",
            &[&peek_slot, &quoted],
        )
        .await
        .unwrap();
    let peeked: Vec<(u64, Vec<u8>)> = rows
        .iter()
        .map(|r| {
            let lsn: String = r.get(0);
            (parse_lsn(&lsn).unwrap(), r.get(1))
        })
        .collect();

    status.close().await;
    drop(stream);
    for slot in [&stream_slot, &peek_slot] {
        // The walsender can hold the slot for a moment after the connection closes.
        for _ in 0..50 {
            if db
                .execute("SELECT pg_drop_replication_slot($1::text)", &[slot])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    db.batch_execute(&format!(
        "DROP PUBLICATION \"{publication}\"; DROP TABLE {table};"
    ))
    .await
    .unwrap();

    let tags = |m: &[(u64, Vec<u8>)]| m.iter().map(|(_, d)| d[0] as char).collect::<String>();
    assert_eq!(tags(&streamed), "BRIIUC", "streamed messages");
    assert_eq!(tags(&peeked), "BRIIUC", "peeked messages");
    for (i, (s, p)) in streamed.iter().zip(&peeked).enumerate() {
        assert_eq!(s.1, p.1, "pgoutput bytes of message {i} differ");
        if s.1[0] == b'R' {
            // The walsender sends every write but a callback's last one with LSN 0 ("can't have
            // sync rep confused by sending the same LSN several times", walsender.c
            // WalSndPrepareWrite). A Relation message is such a write; the peek reports the
            // change's LSN for it. Nothing positions on a Relation message.
            assert_eq!(s.0, 0, "streamed Relation message LSN");
        } else {
            assert_eq!(s.0, p.0, "LSN of message {i} differs");
        }
    }
}
