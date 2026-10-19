# ADR 0003: Streaming replication for the Postgres source

- Status: proposed
- Issue: #58 (related: #57, #47, #63, ADR 0001)
- Deciders: Lucas (approval), Megatron (review), Perceptor (author)
- Date: 2026-10-19

## Context

The Postgres source polls: `pg_logical_slot_peek_binary_changes` over a plain SQL connection, then
`pg_replication_slot_advance` on ack (`aqueduct-source-postgres/src/source.rs`, main `e996192`). #58 lists what
that costs:

- **Latency:** up to `poll_interval` (500 ms) per change when idle.
- **Re-decoding:** each peek starts a new decoding context at `restart_lsn` and decodes again everything that is
  peeked but not yet acked. Relation messages are re-sent on every poll.
- **Memory (#57):** a peek returns whole transactions as one result set. tokio-postgres holds all of it, and the
  decoder fills `buffer` from it. A 10M-row `UPDATE` is an OOM crash loop.

The standard alternative (pg_recvlogical, Debezium) is a **replication connection**: `START_REPLICATION SLOT ..
LOGICAL ..` in `CopyBoth` mode, with the client reporting its flushed LSN in Standby Status Update messages.

Constraints:

- CLAUDE.md: "Postgres URLs are plain SQL connections. Do not add `replication=database`; tokio-postgres rejects
  it." tokio-postgres 0.7.18 (our version) has no `CopyBoth` API. So a replication client is a dependency decision.
- At-least-once. The slot may only be confirmed to a checkpointed commit-end LSN (`ack_target`), and #47's rule
  that we never confirm past the checkpoint holds.
- The TLS policy must carry over unchanged: `verify-full` by default, and fail closed on plaintext to a
  non-loopback host (`tls.rs`, `aqueduct-tls`).
- **The pipeline is sequential** (`aqueduct-core/src/pipeline.rs::drive`): `next()`, then `send_batch` + `flush`
  (up to `SINK_TIMEOUT` = 60 s), then `save`, then `ack`. Nothing reads from the source while the sink is busy.
  The server's `wal_sender_timeout` defaults to **60 s** and kills a walsender that hears nothing from its client
  for that long. So a design that reads and replies only inside `next()` is killed by a slow sink.

## Decision

### 1. A second transport, opt-in first

`[postgres] transport = "poll" | "stream"`, default **`poll`** in the release that ships it. The default flips to
`stream` in a later release, once the bench numbers (§8) and a soak period support it. `poll` stays as a supported
fallback for setups where a replication connection is not available: PgBouncer and other poolers,
`max_wal_senders = 0`, or a `pg_hba.conf` that the operator can't change.

Both transports use **the same slot, the same decoder and the same positions**. A checkpoint written in one mode
is valid in the other, so switching needs no migration (§5).

### 2. Client: a small hand-written replication client on crates already in the tree

New module `aqueduct-source-postgres/src/replication/`, built on:

- `postgres-protocol` 0.6 (already in `Cargo.lock`, under tokio-postgres) for message framing, md5, and
  SCRAM-SHA-256 including channel binding;
- `tokio-rustls` 0.26 (already in `Cargo.lock`) plus `aqueduct_tls::client_config` for TLS.

It does exactly this: the startup message with `replication=database`, `SSLRequest` and the TLS upgrade,
authentication (trust, cleartext, md5, SCRAM-SHA-256, SCRAM-SHA-256-PLUS), the simple-query protocol for
`IDENTIFY_SYSTEM` and `START_REPLICATION`, and `CopyBoth` framing (`XLogData` `w`, keepalive `k`, status update
`r`). The pgoutput payload goes to the existing `decoder.rs` unchanged. The estimate is 600 to 900 lines plus
tests. **No new crate enters the dependency tree.**

Alternatives considered:

| Option | Why not |
|---|---|
| Materialize's fork of rust-postgres (`CopyBothDuplex`) | A git dependency, not on crates.io. It replaces tokio-postgres for the whole crate (or we carry two copies), and we would follow a fork's rebase schedule for our main database driver. |
| `pgwire-replication` (crates.io) | Fits the problem. But it is young with a small maintainer base, it brings its own TLS and auth stack that we would have to re-audit against our `verify-full` and loopback rules, and it includes a pgoutput decoder we don't need. We can revisit it if our client grows. |
| Keep polling and tune it (larger batches, `upto_lsn` windows) | It doesn't remove re-decoding or the idle latency, and it is only the #57 fix for poll mode (see §7). |

The SQL URL stays a plain SQL connection for tokio-postgres. The replication client reads host, port, user,
password and dbname from the same parsed `tokio_postgres::Config` (`tls::parse`), and adds
`replication=database` itself. In stream mode a URL with more than one host is a validation error in v1.

### 3. Two connections, with a dedicated reader task

- **Control connection** (tokio-postgres, unchanged): slot creation, the checkpoint-vs-WAL check, #63's
  `system_identifier`, ADR 0001's catalog queries and `pg_logical_emit_message` watermarks, and the optional
  `heartbeat_action_query`.
- **Replication connection**, owned by a **spawned task** that runs whether or not the pipeline is in `next()`:
  - It reads `CopyData` and forwards `(lsn, payload)` into a **bounded channel**, bounded by bytes
    (`stream_buffer_bytes`, default 64 MiB; a single message larger than the bound is still admitted alone).
    When the channel is full, the task stops reading the socket. TCP flow control then stalls the walsender, so
    the server holds the backlog (it spills to disk past `logical_decoding_work_mem`), not our heap.
  - It sends a Standby Status Update **every `status_interval` (10 s)** from a timer, and immediately when a
    keepalive asks for a reply. Writes don't depend on reads, so a full channel or a 60 s sink stall never
    trips `wal_sender_timeout`. T3's slow-sink test covers this.
  - The flush LSN it reports comes from a `watch<u64>` that `ack()` sets. The task never decides on its own what
    is safe to confirm.
- `next()` / `try_next()` drain the channel, decode, and `queue` into `buffer` exactly as `poll` does today.
  `RelationCache` lives for the **connection** rather than for one poll, because Relation messages are sent once
  per session and again on schema change. A new connection starts with an empty cache.

### 4. What we confirm, and when

- **`write` = `flush` = `apply` = the `ack_target` result**, i.e. the highest commit-end LSN whose changes are all
  checkpointed. It is the same function and the same rule as today, so mid-transaction acks still don't move the
  slot.
- **Idle progress replaces #47's peek-time heartbeat in stream mode.** A keepalive `k` carries the walsender's
  sent position. Because the socket is ordered, every message before that keepalive has already reached the
  channel. Pgoutput v1 only sends committed transactions, so between two transactions nothing is half-delivered.
  So when the source pops a keepalive from the channel, it emits an **`Op::Heartbeat` event at that position**,
  at most once per `heartbeat_interval`. That event goes through the pipeline in order behind every real event.
  Once it is saved and acked, everything before it has been delivered, and `ack` may confirm to it. No WAL query
  and no `drained` flag are needed: ordering gives the guarantee that `heartbeat_targets` reconstructs in poll mode.
  The MongoDB source already uses `Op::Heartbeat` this way.
  - **To verify in implementation:** that a logical walsender's keepalive position is `sentPtr` (walsender.c
    `WalSndKeepalive`). pg_recvlogical confirms to that position the same way when it has no pending output. If
    a supported version differs, fall back to a WAL-position heartbeat on the control connection under §3's
    rules. #47's `heartbeat_it` suite must pass in both modes either way.
  - On PG 10–14 the walsender sends empty BEGIN/COMMIT pairs for unpublished transactions. They arrive as normal
    commits in `tx_ends` and advance the slot as they do today.
- `heartbeat_action_query` keeps its meaning (a timer on the control connection) for servers where the WAL never
  moves without it.

### 5. Start position and replay

- `START_REPLICATION SLOT <slot> LOGICAL <confirmed_flush_lsn> (proto_version '1', publication_names '<pub>')`,
  plus `messages 'true'` when ADR 0001 snapshots are enabled.
- We request the slot's **own `confirmed_flush_lsn`**, never the checkpoint. A requested start above
  `confirmed_flush_lsn` makes the server skip transactions that commit before it. A checkpoint can be ahead of
  the slot (a crash between `save` and `ack`), so requesting from the checkpoint would rely on that skip being
  exact. Instead the local `emitted` filter drops replayed changes, the same as `poll` does after a restart.
- The `lsn` of each `XLogData` is the same write location that the peek's `lsn` column reports (both come from
  `ctx->write_location`). **Positions are identical in both modes.** T3's equivalence test checks it.
- `IDENTIFY_SYSTEM` returns `systemid`. In stream mode, #63 can compare it without an extra query. #63's SQL check
  stays the source of truth for both modes.

### 6. TLS and authentication

- `SSLRequest` first, unless `ssl_mode = "disable"`. On `S`, the connection is upgraded with
  `aqueduct_tls::client_config(root, mode.verify(..))`, the same config and the same `SslMode` semantics as the
  SQL connection. On `N`, only `prefer` may continue in plaintext. Then `tls::check_negotiated(mode, encrypted,
  local, has_root)` runs unchanged. In stream mode `encrypted` is known directly rather than read from
  `pg_stat_ssl`.
- With TLS, SCRAM uses channel binding (`tls-server-end-point`) when the server offers `SCRAM-SHA-256-PLUS`.
  Cleartext password auth is refused over plaintext to a non-loopback host, so the password never goes over the
  network unencrypted.
- Not in v1, which matches the SQL path today: GSSAPI/SSPI and client certificates (`tls::parse` handles only
  `sslmode` and `sslrootcert`).
- The role needs nothing new. It already has `REPLICATION`. The server needs `max_wal_senders` greater than the
  number of logical walsenders in use (DEPLOYMENT.md already sets 4). It also needs a `pg_hba.conf` line that
  allows the role on **the database** (logical replication connections match the database name, not the
  `replication` keyword).

### 7. #57 (large transactions)

In stream mode, client memory is bounded by `stream_buffer_bytes` plus one pipeline batch, whatever the
transaction size. Changes reach the pipeline as they arrive, mid-transaction positions are checkpointed, and the
slot only moves at the commit end. That closes #57 for `stream`.

**`poll` still needs its own #57 fix**, because it remains the fallback. It is the smaller fix from #57's option
2: a server-side cursor over the peek (`DECLARE .. CURSOR FOR SELECT .. FROM pg_logical_slot_peek_binary_changes
.. ; FETCH n`). #57 stays scheduled after #63 as planned, with a narrower scope.

`proto_version '2'` with `streaming 'on'` (PG14+, in-progress transactions) is **not** in v1. It delivers changes
of transactions that may later abort, which would need a second buffer or abort handling downstream. Its only
gain is less server-side spill. It is a possible follow-up.

### 8. Measuring before flipping the default

`aqueduct-bench` gets a Postgres source mode with `--transport poll|stream` against the CI Postgres. The default
flips only when, on the same machine:

- stream throughput is at least poll throughput;
- p99 end-to-end latency for a single-row insert on an idle stream is under 50 ms (poll is bounded by
  `poll_interval`);
- peak RSS for a 1M-row transaction stays under a fixed bound (#57's acceptance).

### 9. Failure handling

A broken replication connection (EOF, TLS error, `wal_sender_timeout` on the server, a protocol error) becomes
`Error::Source` from `next()`. The pipeline fails and the container restarts, which is today's behaviour for a
failed peek. On restart, §5's replay rules apply. Reconnecting in-process with backoff is a possible follow-up,
not v1: it would duplicate the supervisor and hide failures from `/healthz`.

## Consequences

- Positive: no polling latency, no repeated decoding, memory bounded independently of transaction size, and
  less load on the primary. No new third-party crate. Positions and checkpoints carry over between modes, so
  switching back is a config change.
- Negative: about 1k lines of protocol and auth code that we own, plus an auth/TLS test matrix that has to run
  against a real server. A replication connection uses a walsender slot and a `pg_hba` line, so it doesn't work
  through a pooler, which is why `poll` stays. The SQL-only rule in CLAUDE.md becomes "SQL for the control
  connection, our own client for replication".
- Breaking: none. Stream mode is opt-in, and `poll` is unchanged.

## Implementation issues (after approval)

1. **T1. Wire client:** startup, `SSLRequest` and rustls using the shared policy, auth (trust, cleartext, md5,
   SCRAM ± PLUS), simple query, `CopyBoth` framing. AC: integration tests against the TLS Postgres fixture (#18)
   for every `ssl_mode` and every auth method, including plaintext to non-loopback being refused and a bad
   certificate being refused under `verify-full`. A unit test checks that framing errors are errors, not panics.
2. **T2. Replication task:** `IDENTIFY_SYSTEM`, `START_REPLICATION`, `XLogData` and keepalive parsing, timer and
   on-request status updates, the byte-bounded channel, and the `watch` for flush. AC: unit tests over a scripted
   byte stream (keepalive with a reply request, a full channel still sends status updates, an oversize message).
3. **T3. `transport = "stream"` in `PostgresSource`:** config and validation, the per-connection relation cache,
   `emitted` replay, ack to feedback through `ack_target`, and keepalive heartbeats. AC: the whole Postgres
   integration suite (including #47's `heartbeat_it` and Megatron's capped-peek restart test) runs in both modes.
   An **equivalence test** shows the same workload yields the same events with the same positions in both
   modes. A **slow-sink test** blocks the sink for 20 s with `wal_sender_timeout = 5s` and the connection
   survives. A 1M-row transaction keeps RSS under a bound with every row delivered.
4. **T4. Bench:** `aqueduct-bench` Postgres source with `--transport`, with the §8 numbers posted.
5. **T5. Docs:** DEPLOYMENT.md (`transport`, `max_wal_senders`, `pg_hba`, poolers, `stream_buffer_bytes`),
   CLAUDE.md's connection invariant, README diagram, CHANGELOG.
6. **Later:** flip the default to `stream` after T4 and a soak; in-process reconnect; `proto_version '2'`
   streaming.

Order: T1 → T2 → T3 → T4 → T5 (docs ship with T3 where behaviour is user-visible). T1 and T2 do not touch
`source.rs`, so they can run alongside #57 and #63. T3 goes after #63.

## Open questions for approval

1. Hand-written client on `postgres-protocol` + `tokio-rustls` instead of `pgwire-replication` or a fork?
   Recommendation: hand-written (no new dependency, the TLS policy stays in one place).
2. Ship `stream` opt-in and flip the default only after the §8 numbers and a soak? Recommendation: yes.
3. Keep `poll` permanently as the fallback, which means #57 still gets the cursor fix for `poll`?
   Recommendation: yes, because poolers and `max_wal_senders = 0` setups exist.
