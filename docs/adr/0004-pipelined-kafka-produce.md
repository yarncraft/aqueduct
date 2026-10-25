# ADR 0004: Pipelined Kafka produce

- Status: implemented (2026-10-25)
- Deciders: Lucas (approval), Megatron (review)
- Date: 2026-10-25

## Context

The pipeline delivered one batch at a time (`aqueduct-core/src/pipeline.rs::drive`): gather up to `MAX_BATCH` =
1000 events, `send_batch` + `flush`, wait for the broker's ack, save the checkpoint, ack the source, and only then
read the next batch. The Kafka sink sent each partition's records of a batch in requests of at most 900 KiB, one
after another, all partitions in parallel.

Profiling the bench (`aqueduct-bench --sink kafka`, industrial profile, ~4 KB events, local single-broker Kafka)
put the ceiling at about **27 MB/s**, and showed where it comes from:

- Each produce request costs a fixed round trip of about **35 ms** on that broker, whatever its size up to the
  900 KiB budget. One batch in flight means one request per partition per round trip: about 900 KiB / 35 ms.
- It is not Nagle (rskafka never sets `TCP_NODELAY`; a patched copy that does made no difference) and not the
  broker: with requests overlapping, the same broker takes over 500 MB/s.
- The bench events had no key, so every record hashed to one partition and `--partitions` changed nothing.

So the limit is latency, not bandwidth: only overlapping requests raise it. Constraints:

- **Per-partition order.** A row's changes (`partition_by = "key"`) or a table's (`"table"`) must reach their
  partition in commit order.
- **At-least-once, monotonic acks.** A position may be checkpointed only once it and everything before it are
  acknowledged by the broker. Postgres advances the slot to the acked position; acking past an unwritten batch
  loses it.
- **rskafka 0.6** has no idempotent producer (no `InitProducerId`; `producer_id = -1` is hard-coded), and
  `PartitionClient::produce` retries internally on I/O errors, `NotLeaderOrFollower`, `LeaderNotAvailable` and
  `UnknownTopicOrPartition`. Two requests in flight to one partition can therefore be reordered by a retry.
- Errors stay fatal: the process exits and the orchestrator restarts it from the checkpoint.

## Decision

### 1. One lane per partition

The Kafka sink keeps one **lane** per (topic, partition), a task fed by a channel (`aqueduct-sinks/src/lane.rs`).
A lane has **exactly one produce request in flight**. While it waits, the records later batches hand it queue up;
the next request takes all of them, cut into requests of at most 900 KiB, in order. Partitions write in parallel,
and a busy partition sends full requests instead of one small request per batch.

One request per partition keeps rskafka's internal retries harmless: nothing can overtake a request that is being
retried. It also needs no idempotent producer.

A lane **stops at its first failed request**. That request's records, everything queued behind it and everything
handed to it later fail with the same error, so no record of a later batch reaches the partition ahead of an
earlier one that was never written. The error reaches the pipeline, which exits as before.

### 2. Submit returns a delivery

`Sink` gains `submit(batch) -> Delivery` and `in_flight() -> usize`. For Kafka, `submit` renders the batch, opens
its topics, hands each partition's records to its lane and returns a future that resolves when every part of the
batch is acknowledged. The batch's route counts (`RoutePlan::take_counts`) travel with that future and are
committed when it succeeds; a failed batch's counts are dropped, so the counters still show delivered events only.
Per-topic counts are applied per partition once that partition's part is acknowledged, as before.

The default `submit` is `send_batch` + `flush`, delivered before it returns, and the default `in_flight` is 1, so
other sinks behave exactly as before.

### 3. The pipeline completes in submit order

`drive` keeps up to `in_flight()` deliveries in a queue in submit order. Only the front may **land** (metrics,
`store.save`, `source.ack`), so a later batch acknowledged first waits for every earlier one, and the acks never go
backwards. On each turn the pipeline lands every front delivery that is already done, waits for the front when the
queue is full, and otherwise reads the next batch.

- **Reading ahead.** `try_next` only pops what the source has buffered, so `Source` gains `read_ahead()`: one
  poll, no idle sleep. Postgres implements it as `next()` without the pause, and the stream transport's fetch does
  not wait for data there (`fetch_ready`). When it finds nothing, the pipeline waits for the oldest delivery. The
  default reads nothing (MongoDB keeps it for now), so such a source overlaps deliveries only while its buffer
  lasts.
- A **heartbeat-only batch** becomes an already-resolved delivery: it still lands in order and moves the checkpoint.
- **`SINK_TIMEOUT`** (60 s) runs per batch, from submit to its acknowledgement.
- **Shutdown** stops reading, lands every submitted batch, then flushes.

### 4. Configuration

`[sink] in_flight_batches`, `1` to `16`, default **4**. `1` is the old behaviour: one batch at a time.

### 5. Bench

Bench events carry a primary key (`asset_id` for the industrial profile, 10 000 distinct `id`s for the simple
one), so `--partitions` spreads them like a real table, and `--in-flight K` sets `in_flight_batches`.

## Alternatives considered

| Option | Why not |
|---|---|
| Several requests in flight per partition | rskafka's retries could reorder them, and rskafka has no idempotent producer to stop that. |
| Bigger batches (`MAX_BATCH` above 1000) | Raises the bytes per round trip only while one partition's records fill a request, keeps one round trip per batch, and makes every checkpoint coarser. |
| Switching to librdkafka (`rdkafka`) | Idempotent producer and pipelining built in, but a C dependency and build chain, and a second delivery model to reason about against the checkpoint. |

## Consequences

- Throughput on a fast broker rises with `in_flight_batches` until the source or the broker is the limit (numbers
  below).
- At-least-once is unchanged, but a crash can now replay up to `in_flight_batches` batches instead of one.
- Sink latency in the cockpit and `aqueduct_sink_latency_seconds` is submit to acknowledgement, which includes
  waiting behind earlier batches.
- The `poll` transport re-decodes the unacked tail of the slot on every peek, and reading ahead means more of it
  is unacked; `stream` has no such cost.
- A single hot partition is still limited to one request of up to 900 KiB per round trip; spreading it needs more
  partitions.

## As built (2026-10-25)

`aqueduct-bench --sink kafka`, 200 000 events, local single-broker Kafka (`apache/kafka`, Podman), dev laptop. Median
of 3 runs, MB/s of event JSON. `main` is 7c6abf4 (events unkeyed, so all on one partition); `K` is
`in_flight_batches`.

| Profile | Partitions | main | K = 1 | K = 4 |
|---|---|---|---|---|
| industrial (~4 KB events) | 1 | 25.4 | 26.1 | 28.0 |
| industrial | 6 | 26.0 | 72.4 | 115.6 |
| simple (~300 B events) | 1 | 8.2 | 8.5 | 21.5 |
| simple | 6 | 7.9 | 12.3 | 18.3 |

- **K = 1 matches main** on one partition: it is the old one-batch-at-a-time path.
- **Six partitions, K = 1** is the keying fix alone: a 4 MB batch now spreads over six requests in parallel instead of
  five in a row on one partition.
- **K = 4** overlaps batches: 4.4x main for industrial events on six partitions, 2.6x for small events on one.
- **One partition, large events** stays at ~27 MB/s: one 900 KiB request per round trip is that partition's limit,
  and main already filled it.
- Small events on six partitions gain less than on one: each batch becomes six small requests, and the round trip,
  not the bytes, is what each request costs.
