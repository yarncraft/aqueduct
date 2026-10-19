# ADR 0002: Routing and transforms

- Status: proposed
- Issue: #48
- Deciders: Lucas (approval), Megatron (review), Perceptor (author). Implementer: Wheeljack, after #45
- Date: 2026-10-19

## Context

Routing today is `[[sink.routes]]`: the first glob match on `schema.table` picks a topic template with
`{schema}`/`{table}` (`aqueduct-sinks/src/kafka.rs::TopicRouter`). There are no transforms. #48 asks for
filters, content-based routing, projection/masking, renames, unwrap, outbox, tombstones and per-route
keys, declared in TOML and fast, with no plugin system.

Constraints as of `e57a7b3`:

- Kafka is the only sink by design, and routing already lives in it. Every output is a Kafka record:
  topic, key, value (possibly null), headers.
- At-least-once: the pipeline checkpoints the batch's last position after `send_batch` + `flush`.
- Rows arrive as pre-serialised JSON (`Box<RawValue>`). No `Value` tree is built on the hot path.
- The inspector reads back "the topics the router routes to" and parses values as the event envelope.
- `aqueduct-bench --sink null` bypasses the Kafka sink entirely.

## Decision

### 1. Where it runs: in the sink, as a pure "route plan"

Transforms run **inside the Kafka sink**, not in `run_pipeline`. A new module
`aqueduct-sinks/src/route/` compiles the config once into a `RoutePlan` with one pure function:

```rust
fn render(&self, event: &ChangeEvent) -> Result<Rendered>  // 0..n OutRecord { topic, key, value: Option<String>, headers }
```

`send_batch` calls `render`, then groups by (topic, partition) exactly as today.

Why not between batch and sink in core: half the capabilities (unwrap, outbox, tombstones, keys, topics)
produce Kafka records, not `ChangeEvent`s, so a core stage would have to invent a record type that only
Kafka uses. Running in the sink leaves core and the at-least-once loop untouched. **Filtered events still
ack for free**: they stay in the batch, produce zero records, and the batch's last position is
checkpointed as before.

### 2. Config shape: route keys applied in a fixed order (not an ordered `transforms = [...]` list)

Every capability is an optional key on a `[[sink.routes]]` entry. They apply in this **fixed, documented
order**:

1. **match:** the `match` glob on `schema.table`, first match wins (unchanged). The matched route owns the
   event: if its filter drops it, it is dropped, with no fall-through.
2. **filter:** `ops` and `where`, both evaluated on the **original** event.
3. **address:** `topic` (template) and `key`, also evaluated on the original event, so you can route or
   key by a column you then remove.
4. **fields:** `include` | `exclude`, `mask`, `hash`, `rename`, applied to `before` and `after`.
5. **shape:** `format = "envelope"` (default) | `"row"`, or `outbox = {...}`.
6. **tombstone:** the extra null-value record after a delete.

Rejected: a Debezium-style ordered SMT chain. These capabilities have one sensible order, and an
arbitrary order mostly adds ways to be wrong (masking after unwrap, routing on a dropped column) and
more to document. A fixed order also lets the plan pick the fast path statically.

Example:

```toml
[[sink.routes]]
match = "public.orders"
ops = ["insert", "update", "delete"]
where = "row.status == 'paid' or op == 'delete'"
topic = "orders.{row.region}"
topic_fallback = "orders.unknown"
key = ["id"]
exclude = ["internal_note"]
mask = ["card_last4"]                 # -> "***"
hash = ["email"]                      # HMAC-SHA256 hex, key from sink.hash_key
rename = { cust_id = "customer_id" }
format = "row"                        # value = the row, not the envelope
deletes = "tombstone"                 # row format: tombstone | drop | row (before + "__deleted": true)

[[sink.routes]]
match = "public.outbox"
outbox = { aggregate_type = "aggregatetype", aggregate_id = "aggregateid", payload = "payload", topic = "outbox.{aggregate_type}" }
```

### 3. Expression language: closed and minimal

- **Fields:** `op`, `schema`, `table`, `before.<path>`, `after.<path>`, `row.<path>` (= `after`, else
  `before`). `<path>` is a column name, dotted for nested MongoDB documents (`after.address.city`).
- **Operators:** `==` `!=` `<` `<=` `>` `>=`, `in [..]`, `is null`, `is not null`, `and`, `or`, `not`,
  and parentheses. Literals: strings, numbers, `true`/`false`, `null`.
- A missing field is `null`. Comparisons with `null` are false, except `is null`. Type mismatch is false.
- **No** functions, arithmetic, regex or scripting in v1. A hand-written parser with no new dependency;
  the expression is compiled at startup, so a bad expression fails config validation, not at runtime.
- **Templates** (`topic`, outbox `topic`) use `{schema}`, `{table}`, `{op}` and any field path.

### 4. Cost

- A route with no transforms keeps today's path byte for byte: encode the event, key it as
  `partition_by` says. The plan decides this statically.
- Rows are parsed **lazily and borrowed**, only when a route reads or rewrites a field: a top-level
  `HashMap<&str, &RawValue>` over the existing JSON, with nested parsing only for dotted paths.
  Rewritten values are written straight into a `String`, the same way the decoder writes rows.
- Bench: `render` is pure, so `aqueduct-bench` gets a `--sink render` mode that runs the plan without
  Kafka. Acceptance (from #48): no measurable regression with no transforms (within run-to-run noise),
  and the cost of each transform class is reported in its PR.

### 5. Semantics per capability

- **Filter:** `ops` is a list of insert, update, delete, truncate, read (read is from ADR 0001). `where`
  is a predicate. A dropped event counts as `filtered`.
- **Content routing:** if a template field is null or missing, or the rendered name is not a legal
  Kafka topic (`[A-Za-z0-9._-]`, max 249 characters), the event goes to `topic_fallback`. With no
  fallback, the pipeline fails (fail closed, as an unrouted event does today).
  New guard: `sink.max_dynamic_topics` (default 100) caps how many distinct topics templates may create,
  so a high-cardinality column cannot auto-create thousands of topics. Going over it fails the pipeline
  with a clear error.
- **Key:** `key = ["col", ...]` sets the record key to `schema.table:<values joined 0x1f>` (the
  `partition_by = "key"` format) and partitions by it, overriding `partition_by` for that route. It
  reads `row`, so deletes key by the before-image. This is the explicit escape hatch for #45: a route can
  pin the key to immutable PK columns whatever the replica identity is.
- **Fields:** `include` and `exclude` are mutually exclusive. `mask` replaces the value with `"***"`
  (null stays null). `hash` is **HMAC-SHA256** with `sink.hash_key` (env `AQUEDUCT_HASH_KEY`, never logged,
  required when any route hashes), giving lowercase hex. A plain SHA-256 of low-entropy PII (emails,
  phone numbers) can be reversed with a dictionary. `rename` maps old names to new ones; table renames
  are done with `topic`.
- **`format = "row"` (unwrap):** the value is `after` (insert, update, read). Deletes follow `deletes`:
  `tombstone` (null value, the default), `drop`, or `row` (before plus `"__deleted": true`). Truncates
  are dropped and counted. `op` and `lsn` stay in the headers, so nothing is lost for consumers that need
  them.
- **Tombstones (envelope format):** `tombstones = true` emits a null-value record with the same key after
  every delete. Config validation **rejects** it unless the route's key is per row (`key = [...]` or
  `partition_by = "key"`): a tombstone keyed `schema.table` would erase the whole table from a compacted
  topic.
- **Outbox:** only inserts are forwarded; updates and deletes of outbox rows are dropped and counted, as
  in Debezium. topic = template over the outbox columns, key = `aggregate_id`, value = the `payload`
  column (`json`/`jsonb` passes through raw; text is sent as is), headers `id` and `type` plus `op` and
  `lsn`. Mutually exclusive with `format`, the field keys and `key`.

### 6. Ordering

The partition is still FNV-1a of the record key, and the key and topic come from step 3. Order holds per
key within one topic. If a row's topic field or key columns change, its events can move topic or
partition, and order is lost across them. This is the same caveat as `partition_by = "key"`, documented
next to it. Tombstones follow their delete on the same partition.

### 7. Visibility

- Per-route counters in a `RouteStats` that the sink shares with the admin server: `matched`,
  `produced`, `filtered`, `fallback`, `errors`. They show in `/api/metrics` (`routes: [...]`) and in
  Prometheus as `aqueduct_route_records_total{route, outcome}`. `route` is the `match` string, or
  an optional `name` on the route.
- Inspector: dynamic topics are discovered by listing broker topics that match the template read as a
  glob (`orders.{row.region}` becomes `orders.*`). Values that are not the envelope (row format, outbox,
  tombstones) are shown raw with their headers, instead of failing to parse.
- Cockpit: per-route counts and filtered counts (Bumblebee).

## Consequences

- Positive: no change to core or the at-least-once loop. Zero cost when unused. One place (the plan) to
  test, as a pure function. The per-route `key` makes #45-style ordering problems fixable in config.
- Negative: transforms are Kafka-specific (consistent with "Kafka is the only sink"). The fixed order
  can't express exotic chains. Content routing adds a topic-explosion risk, capped by `max_dynamic_topics`.
- Breaking: none. Existing routes behave exactly as today. New keys are additive, and `format = "row"`,
  outbox and tombstones change the value only on routes that opt in.

## Implementation issues (after approval)

1. **Plan + config:** `RoutePlan` compiled from `[[sink.routes]]`, validation (mutually exclusive keys,
   tombstones need a per-row key, `hash_key` when hashing), wired into `send_batch` with the no-transform
   fast path. Includes the `--sink render` bench mode and a baseline.
2. **Expression engine:** field paths, lazy borrowed row view, predicate parser and evaluator, template
   rendering. Table-driven unit tests and a parser fuzz test.
3. **Filter:** `ops` and `where`, plus `RouteStats` and admin/Prometheus exposure.
4. **Content routing:** templated topics, `topic_fallback`, topic-name validation, `max_dynamic_topics`,
   inspector topic discovery.
5. **Per-route key** (coordinate with #45).
6. **Fields:** include/exclude, mask, HMAC hash, rename.
7. **Shapes:** `format = "row"` with `deletes`, envelope `tombstones`, raw display in the inspector.
8. **Outbox router.**
9. **Docs:** DEPLOYMENT.md route reference, README example, CHANGELOG (additive).
10. **Cockpit:** per-route counts (Bumblebee).

Priority: 1 → 2 → 3, 4, 5 (the most-asked: filter, content routing, keys), then 6 (PII), 7, 8. Each one
ships with unit tests, docs, and the bench number from issue 1's harness.

## Open questions for approval

1. Fixed transform order instead of an ordered chain? Recommendation: fixed.
2. First match wins, with no fan-out of one event to several routes in v1? Recommendation: yes. Fan-out can
   come later as `continue = true` on a route.
3. Is `max_dynamic_topics = 100` the right default, and should exceeding it fail the pipeline (my
   recommendation) or send to the fallback?
