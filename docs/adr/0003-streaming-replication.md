# ADR 0003: Streaming replication for the Postgres source

- Status: proposed (revised after Megatron's review of f1614cc; Lucas accepted the three recommendations)
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
is valid in the other, so switching needs no migration (§4, §5).

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
    (`stream_buffer_bytes`, default 64 MiB). A single message larger than that bound is admitted on its own, but
    **no frame may exceed 1 GiB** (Postgres' own message limit, `MaxAllocSize`). A larger length header is a
    protocol error that closes the connection before anything is allocated, so a hostile or MITM'd server can't
    make us allocate 4 GiB.
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

### 4. Positions: a commit-ordered key, not the change LSN

Megatron's review found, and I reproduced on PG16 (thread, 2026-10-19), that **change LSNs are not monotonic
across transactions**. Transactions are decoded in commit order, so a transaction that started earlier but
commits later has changes with LSNs **below** changes already delivered. Main has two losses from this: `queue`
drops those changes as `<= emitted`, and `ack_target` pops a later-committing transaction whose last change LSN
is below the acked change LSN, advancing the slot past it before it was delivered. Megatron then found a third:
**`change_lsn` is not unique inside a transaction.** COPY is decoded from `heap_multi_insert`, and every tuple
of one WAL record (up to 1000) carries that record's LSN. A multi-table `TRUNCATE` is one message that becomes
one event per table. `lsn > emitted` keeps the first and drops the rest. The P0 fix (#94, Ironhide) and this ADR
use one model:

- **Key K = `(commit_lsn, change_lsn, ord)`.** `commit_lsn` is `Begin.final_lsn`, which equals the Commit
  message's `commit_lsn` and is known at BEGIN, before any change. `ord` is the event's index among the events
  with the same `change_lsn` in that transaction, counted per **event**, not per message: each COPY tuple and
  each table of a TRUNCATE gets the next `ord`, starting at 0. Decoding the same WAL record again yields the
  same events in the same order, so `ord` is stable across re-peeks, restarts and both transports. Transactions
  arrive in commit order and change LSNs are non-decreasing inside one, so the stream is **strictly increasing**
  in K, compared lexicographically, and every event has a unique K. Both transports report the same K for the
  same change.
- **Batches can split anywhere.** Because K is unique per event, a batch boundary inside a same-LSN group (a
  1000-row COPY record against `MAX_BATCH` = 1000) is safe. No carry rule is needed for correctness.
- **Checkpoint = K.** The P0 fix pulls ADR 0001 S1's `ChangeEvent.checkpoint` forward, and Postgres sets it to
  `Position::Text("<commit>@<change>#<ord>")`. The pipeline saves and acks that value. The wire `lsn` stays the change
  LSN, and DEPLOYMENT.md says it is not monotonic across transactions.
- **Dedupe:** the K high-water mark replaces `emitted`, and it applies on the live path (re-peeks) as well as
  after a restart. Skip a transaction with `commit_lsn < mark.commit` (decided at BEGIN). For
  `commit_lsn == mark.commit`, skip events with `(change_lsn, ord) <= (mark.change, mark.ord)`. Deliver
  everything with `commit_lsn > mark.commit`. An old plain-LSN checkpoint has no commit part:
  that first start skips nothing and replays from the slot (duplicates only, logged once).
- **Feedback** (`write` = `flush` = `apply`): `tx_ends` holds `(commit_lsn, (last_change, last_ord), end)` in
  commit order. `ack(k)` pops while `commit_lsn < k.commit`, or while `commit_lsn == k.commit &&
  (last_change, last_ord) <= (k.change, k.ord)`, and reports the last popped `end`. An empty transaction is
  `(commit_lsn, (0, 0), end)`. Change LSNs are never compared across transactions.
- **The ack boundary is exact.** A later transaction's `commit_lsn` can **equal** an earlier one's commit end
  (in the trace, Tb's `final_lsn` 0/1567FD8 = Ta's end). Postgres skips a transaction only when its commit LSN is
  **below** the slot's confirmed position, so confirming exactly Ta's end keeps Tb. A unit test pins that
  equality, so nobody later "optimises" the target past it. Mid-transaction acks still
  don't move the slot.

### 5. Idle progress, start position and replay

- **Keepalives become ordered heartbeats.** A keepalive `k` carries the walsender's sent position. When the
  source pops one from the channel between transactions, it emits an **`Op::Heartbeat` event with checkpoint
  K = `(walEnd, 0, 0)`**, at most once per `heartbeat_interval`. It means "every transaction with
  `commit_lsn < walEnd` is delivered". The event goes through the pipeline behind every real event, so once it is
  saved and acked that is true, and `ack` may confirm to `walEnd`. A transaction that began before `walEnd` and
  commits after it has `commit_lsn > walEnd`, so a restart from that checkpoint does not skip it (Megatron's
  case 1). That is the reason the key is commit-ordered.
- **The `sentPtr` premise, verified in the source before approval:**
  - `WalSndKeepalive` writes `sentPtr` on REL_14_STABLE (walsender.c:3552). On REL_16_STABLE it writes
    `writePtr` when given and `sentPtr` otherwise (walsender.c:3799). The only caller that passes `writePtr`
    is `WalSndUpdateProgress` for a skipped empty transaction under synchronous replication, with that
    transaction's end LSN (walsender.c:1554).
  - For logical decoding `sentPtr` is `reader->EndRecPtr` after each record is decoded (walsender.c:3171).
    Decoded output is written to the socket before the next record is read, so every transaction whose commit
    record ends at or below `sentPtr` precedes the keepalive on the wire.
  - Confirming to `sentPtr` while a transaction is still open is safe. The slot's `restart_lsn` stays behind the
    open transaction, and the server re-sends every transaction that commits after `confirmed_flush_lsn`.
  - REL_10–13 have the REL_14 form. T3 still runs #47's `heartbeat_it` suite in both modes.
- On PG 10–14 the walsender sends empty BEGIN/COMMIT pairs for unpublished transactions. They arrive as normal
  commits in `tx_ends` and advance the slot as today. `heartbeat_action_query` keeps its meaning, as a timer on
  the control connection.
- **Start:** `START_REPLICATION SLOT <slot> LOGICAL <confirmed_flush_lsn> (proto_version '1', publication_names
  '<pub>')`, plus `messages 'true'` when ADR 0001 snapshots are enabled. We request the slot's own
  `confirmed_flush_lsn`, never the checkpoint, because a requested start above it makes the server skip
  transactions that commit before it. Replayed transactions are dropped by the K dedupe above.
- The `lsn` of each `XLogData` is the same write location the peek reports (both come from
  `ctx->write_location`), and `final_lsn` comes from the same Begin message. **K is identical in both modes.**
  T3's equivalence test checks this with overlapping transactions.
- `IDENTIFY_SYSTEM` returns `systemid`. In stream mode, #63 can compare it without an extra query. #63's SQL check
  stays the source of truth for both modes.

### 6. TLS and authentication

- `SSLRequest` first, unless `ssl_mode = "disable"`. On `S`, the connection is upgraded with
  `aqueduct_tls::client_config(root, mode.verify(..))`, the same config and the same `SslMode` semantics as the
  SQL connection. On `N`, only `prefer` may continue in plaintext. Then `tls::check_negotiated(mode, encrypted,
  local, has_root)` runs unchanged. In stream mode `encrypted` is known directly rather than read from
  `pg_stat_ssl`.
- **Authentication requirements:**
  - **No hand-written crypto.** SCRAM-SHA-256 and the md5 hash come from `postgres-protocol`
    (`authentication::sasl::ScramSha256`, `authentication::md5_hash`). We only frame the messages.
  - **Cleartext and md5 are both refused** on a plaintext connection to a non-loopback host. md5 is replayable
    by anyone who records the exchange, so it gets the same rule as cleartext.
  - With TLS, SCRAM uses channel binding (`tls-server-end-point`) when the server offers `SCRAM-SHA-256-PLUS`.
    **Under `verify-full`, if TLS is up and the server offered `-PLUS`, falling back to plain SCRAM is
    refused.** A downgrade there can only mean tampering.
  - The server's SCRAM final message is verified (`ScramSha256::finish`) before we treat authentication as done,
    so a server that doesn't know the password can't impersonate one that does.
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

`aqueduct-bench` gets a Postgres source mode with `--transport poll|stream` against a local Postgres. The default
flips only when, on the same machine:

- stream throughput is at least poll throughput;
- p99 end-to-end latency for a single-row insert on an idle stream is under 50 ms (poll is bounded by
  `poll_interval`);
- peak RSS for a 1M-row transaction stays under a fixed bound (#57's acceptance).

### 9. Failure handling

A broken replication connection (EOF, TLS error, `wal_sender_timeout` on the server, a protocol error) becomes
`Error::Source` from `next()`. The pipeline fails and the container restarts, which is today's behaviour for a
failed peek. On restart, the K dedupe in §4 applies. Reconnecting in-process with backoff is a possible follow-up,
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
   SCRAM ± PLUS through `postgres-protocol`), simple query, `CopyBoth` framing with the 1 GiB frame cap. AC: integration tests against the TLS Postgres fixture (#18)
   for every `ssl_mode` and every auth method, including plaintext to non-loopback being refused and a bad
   certificate being refused under `verify-full`. Also: cleartext **and md5** refused over plaintext to
   non-loopback; no `-PLUS` downgrade under `verify-full`; a bad SCRAM server signature rejected; an over-cap
   length header rejected before allocation. A **fuzz target** over the backend message and `CopyBoth` frame
   parser runs briefly locally (command and output posted on the PR) and shows errors, never a panic or an unbounded allocation.
2. **T2. Replication task:** `IDENTIFY_SYSTEM`, `START_REPLICATION`, `XLogData` and keepalive parsing, timer and
   on-request status updates, the byte-bounded channel, and the `watch` for flush. AC: unit tests over a scripted
   byte stream (keepalive with a reply request, a full channel still sends status updates, an oversize message, an over-cap frame). A **fuzz target** over `XLogData` and keepalive parsing.
3. **T3. `transport = "stream"` in `PostgresSource`:** config and validation, the per-connection relation cache,
   K-based dedupe and feedback (§4), and keepalive heartbeats with K = `(walEnd, 0, 0)`. It reuses the
   P0 fix's K code rather than duplicating it, so it lands after that fix. AC: the whole Postgres
   integration suite (including #47's `heartbeat_it` and Megatron's capped-peek restart test) runs in both modes.
   An **equivalence test** shows the same workload yields the same events with the same K in both modes,
   and the workload **includes overlapping transactions** (an early-starting, late-committing transaction across
   a batch boundary and across a keepalive heartbeat, then a restart), a multi-row **COPY** cut by a batch
   boundary, and a multi-table **TRUNCATE**. A **slow-sink test** blocks the sink for 20 s with `wal_sender_timeout = 5s` and the connection
   survives. A 1M-row transaction keeps RSS under a bound with every row delivered.
4. **T4. Bench:** `aqueduct-bench` Postgres source with `--transport`, with the §8 numbers posted.
5. **T5. Docs:** DEPLOYMENT.md (`transport`, `max_wal_senders`, `pg_hba`, poolers, `stream_buffer_bytes`),
   CLAUDE.md's connection invariant, README diagram, CHANGELOG.
6. **Later:** flip the default to `stream` after T4 and a soak; in-process reconnect; `proto_version '2'`
   streaming.

Order: T1 → T2 → T3 → T4 → T5 (docs ship with T3 where behaviour is user-visible). T1 and T2 do not touch
`source.rs`, so they can run alongside #57 and #63. T3 goes after the P0 position fix and #63.

## Decisions (Lucas, 2026-10-19)

1. Hand-written client on `postgres-protocol` + `tokio-rustls`, not `pgwire-replication` or a fork: **accepted**,
   with the frame cap, the fuzzing and the auth requirements in §3, §6 and T1/T2.
2. `stream` ships opt-in, and the default flips after the §8 numbers and a soak period: **accepted**.
3. `poll` stays permanently, so #57 gets the cursor fix for `poll`: **accepted**.
