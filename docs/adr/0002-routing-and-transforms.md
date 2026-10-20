# ADR 0002: Routing and transforms

- Status: proposed (revised after Megatron's reviews of a22306f and 40ce8ef)
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
topic_fallback = "orders.unknown"     # required for templates that read a row field
key = ["id"]
exclude = ["internal_note"]
mask = ["card_last4"]                 # -> "***"
hash = ["email"]                      # HMAC-SHA256 hex, key from AQUEDUCT_HASH_KEY
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
- **Three-valued logic, as in SQL `WHERE`.** A missing field is `null`. A comparison with `null` is
  **unknown**, and only `is null` / `is not null` test for it. Unknown propagates: `not unknown` is unknown,
  `false and unknown` is false, `true or unknown` is true, and anything else involving unknown stays unknown.
  An event whose `where` ends **unknown is filtered**, so it fails closed. Example: `not (row.region == 'eu')`
  with a missing or TOAST-omitted `region` is unknown, and the row is dropped, not passed. Write
  `row.region is null or not (row.region == 'eu')` to keep such rows.
- **Comparisons are typed by the literal, and a type mismatch is an error, never `false`.** The decoder writes
  only int2/4/8, float4/8 and bool as JSON numbers or booleans (`decoder.rs::push_text`). `numeric`, `uuid`,
  dates, timestamps and the rest arrive as JSON strings. So:
  - A **number literal** compares numerically against a JSON number **or a string that parses as a
    decimal** (`numeric` text). The comparison is exact decimal on the normalised digits, not `f64`. Any other
    value is a type error, including numeric `NaN` and `±Infinity` (as text or as float).
  - A **string literal** allows only `==`, `!=` and `in`. Ordering a string (`<` on a timestamp) is rejected
    at **startup**, because Postgres timestamp text depends on the session `TimeZone` and does not sort reliably.
  - A **bool literal** matches a JSON bool only.
  - A type error **propagates through `not`, `and` and `or`** and wins over true, false and unknown, with no
    short-circuit: `false and <error>` is still an error. It is never coerced to true or false, so
    `not (...)` cannot let a mis-typed row through. The event then follows the error policy (§5a).
  - Numeric `NaN` and `±Infinity` are type errors against a number literal, never unknown.
- **No** functions, arithmetic, regex or scripting in v1. The parser is hand-written and adds no new
  dependency. It is limited to 4 KiB of input and nesting depth 32, so a hostile or accidental config cannot
  overflow the stack at startup. Expressions are compiled at startup, so a syntax error fails config validation.
- **Templates** (`topic`, outbox `topic`) use `{schema}`, `{table}`, `{op}` and any field path, under the
  rules in §5 "Content routing".

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
  is a predicate. A dropped event counts as `filtered`. **Rows leaving the filter:** `where` sees only the
  event's own row, so an update `status: paid → refunded` under `where = "row.status == 'paid'"` is dropped,
  and the consumer keeps the row as "paid". DEPLOYMENT.md says this next to `where`. A follow-up issue adds
  `on_leave = "drop" | "delete" | "tombstone"`: it evaluates `where` on `before` too (this needs before-images,
  i.e. `REPLICA IDENTITY FULL`) and emits a delete or a tombstone when a row leaves the filter.
- **Content routing.** Row data must not be able to choose an arbitrary topic. Validation enforces:
  - Every dynamic template starts with a **static, non-empty prefix** that does not start with `_`
    (`orders.{row.region}` is allowed, `{row.tenant}` is rejected). So no value reaches `__consumer_offsets`,
    another internal topic, or a bare topic of its own choosing.
  - Each substituted value is **one segment**, `[A-Za-z0-9_-]{1,64}`. A `.` inside a value is not allowed, so
    `eu` is fine and `x.payments` is not.
  - **No static topic** anywhere in the config (route `topic`, `sink.topic`, other fallbacks,
    `sink.dead_letter_topic`) may match the template's glob. No static topic, including the dead-letter
    topic, may start with `_`. So `orders.{row.region}` cannot be pointed at another route's `orders.eu`.
  - An optional `topic_values = ["eu", "us"]` allow-list.
  - **`topic_fallback` is required** on every template that reads a row field. A null or missing field, a
    value that breaks the rules, a value not in `topic_values`, or the cap below sends the event to the
    fallback and counts it under `fallback`. So there is no runtime "no topic" failure on dynamic routes:
    one bad row can never stall the pipeline, pin the slot and fill the primary's disk.
  - **`sink.max_dynamic_topics`** (default 100) is counted **from the broker**: at startup, the topics that
    match each template's glob, plus the ones created since. The count therefore survives restarts, and a
    crash loop cannot create 100 more topics on every start. When the cap is reached, the event goes to the
    fallback and is counted under `fallback`, with a warn-once log. It never fails the pipeline.
- **Key:** `key = ["col", ...]` sets the record key to `schema.table:<values joined 0x1f>` (the
  `partition_by = "key"` format) and partitions by it, overriding `partition_by` for that route. It
  reads `row`, so deletes key by the before-image. This is the explicit escape hatch for #45: a route can
  pin the key to immutable PK columns whatever the replica identity is.
- **Fields:** `include` and `exclude` are mutually exclusive. They take **top-level column names only**,
  so a dotted path is a validation error (nested MongoDB fields are not supported by field operations in v1),
  and a `rename` target that collides with an existing or another renamed column is also rejected.
  - **`mask`:** strings become `"***"`. Every other non-null type becomes `null`, so the JSON type is kept or
    goes null, and an int never turns into a string. `null` stays `null`.
  - **`hash`:** **HMAC-SHA256** (`ring::hmac`, which `aqueduct-tls` already uses), giving lowercase hex. The
    input is the **canonical text** of the value: the unescaped content of a JSON string, the JSON text of a
    number or bool as written (`42`, `true`), or the compact JSON of an object or array (MongoDB `ObjectId`
    is its relaxed extended-JSON form). `null` is not hashed. Postgres `int4 42` and MongoDB int `42` give
    the same digest, so consumers can join. MongoDB doubles (`42.0`) do not match Postgres ints, and the
    docs say so.
  - **The key comes from `AQUEDUCT_HASH_KEY` or `sink.hash_key_file` only.** A `hash_key` in the TOML is
    rejected, the key is redacted from the config's `Debug`, and it is never logged. `sink.hash_key_id`
    (a short label) goes on every record of a hashing route as the header `aqueduct-hash-key-id`. Rotation
    means a new key and a new id, and consumers join per id.
  - **A configured field that never appears** (a typo like `card_last_4`) cannot be caught at startup,
    because columns are only known when rows arrive. The plan counts, per route and field, how many rows of a
    matched table were seen without that column. After 1000 such rows it logs a warn-once and raises
    `aqueduct_route_field_unseen{route, field}`, which the cockpit shows as a warning. This covers **every
    field path the route references**: field ops, every path in `where` (`row.stauts` would otherwise just
    read null and filter everything), topic templates, `key` and the outbox columns. For a nested path the
    check is on the full path.
- **No row data may bypass the field operations.** Steps 2–3 read the original row, so validation
  **rejects** a route where any `exclude`d, `mask`ed or `hash`ed column (or, with `include`, any column that
  is not included) appears in `key`, in a topic template, in `topic_values`, or in the outbox `aggregate_id`
  or `topic`. A route with field operations **must declare `key = [...]` explicitly** when
  `partition_by = "key"` applies. The sink cannot see which columns the opaque replica-identity key holds,
  so it would otherwise risk putting a hashed `email` PK in the record key in plaintext. Headers only ever
  carry `op`, `lsn` and `aqueduct-hash-key-id`, never row data.
- **`format = "row"` (unwrap):** the value is `after` (insert, update, read). Deletes follow `deletes`:
  `tombstone` (null value, the default), `drop`, or `row` (before plus `"__deleted": true`). Truncates
  are dropped and counted. `op` and `lsn` stay in the headers, so nothing is lost for consumers that need
  them.
- **TOAST.** The decoder omits `Unchanged` TOAST columns (`decoder.rs::row`). So in `format = "row"` an
  update can lack a large column, and a compacted consumer that replaces whole rows loses it. A `where` on
  that column sees `null`. Two answers:
  - **Route option** `unchanged = "omit" | "dead_letter"` (default `omit`, documented): `dead_letter` sends
    any row with an omitted column to the dead-letter topic instead of producing an incomplete row.
  - **Decoder follow-up:** under `REPLICA IDENTITY FULL` the old tuple is logged in full, so the decoder can
    fill `Unchanged` columns of `after` from `before`. That makes FULL a real workaround, for live events too.
    It needs a separate issue and a test (it reuses #50's TOAST fixture).
- **Tombstones (envelope format):** `tombstones = true` emits a null-value record with the same key after
  every delete. Config validation **rejects** it unless the route's key is per row (`key = [...]` or
  `partition_by = "key"`): a tombstone keyed `schema.table` would erase the whole table from a compacted
  topic.
- **Outbox:** only inserts are forwarded; updates and deletes of outbox rows are dropped and counted, as
  in Debezium. topic = template over the outbox columns, key = `aggregate_id`, value = the `payload`
  column (`json`/`jsonb` passes through raw; text is sent as is), headers `id` and `type` plus `op` and
  `lsn`. Mutually exclusive with `format`, the field keys and `key`.

#### 5a. Error policy: never silent, never a stall from one row

A **row-level evaluation error** has two sources: a type error in `where`, or an outbox row missing its
`aggregate_id` / `payload`. Neither drops the event nor guesses an outcome.

- The event goes to `sink.dead_letter_topic`. The value is the envelope **with the route's
  field operations applied**, so the DLQ never holds data the route would have masked. The key is
  `schema.table`, and an `aqueduct-error` header holds the reason (the expression and the type, no values).
  The event is counted under `errors`, and the batch acks normally.
- **`sink.dead_letter_topic` is required** (a validation error, not a warning) as soon as any route uses
  `where`, outbox or `unchanged = "dead_letter"`. Type errors are per row, not per column type: a `text` or
  `jsonb` column holding `'abc'`, numeric `NaN`, or any MongoDB field (schemaless). If such a row failed the
  pipeline, it would pin the Postgres slot until the primary's disk fills, or let the oplog roll past the
  Mongo resume token. So no row-level error can stop the pipeline.
- An error in route R sends the event to the DLQ. It does **not** fall through to the next matching route,
  which could be less restrictive.

Dynamic topics never reach this path (§5 "Content routing" always has a fallback). An unknown `where`
(three-valued logic, §3) is not an error: the event is `filtered`.

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
  can't express exotic chains. Content routing is deliberately constrained (static prefix, one segment,
  required fallback, broker-counted cap). Ordering comparisons on timestamps are not supported in v1.
  A misspelled field name is only caught at runtime, by the unseen-field warning. Any route with `where` or
  outbox needs a dead-letter topic.
- Breaking: none. Existing routes behave exactly as today. New keys are additive, and `format = "row"`,
  outbox and tombstones change the value only on routes that opt in.

## Implementation issues (after approval)

1. **Plan + config:** `RoutePlan` compiled from `[[sink.routes]]`, with all validation: mutually exclusive
   keys, tombstones need a per-row key, the hash key from env or file only, no masked/hashed/excluded column
   in key/topic/outbox, explicit `key` with field ops under `partition_by = "key"`, the template rules, the
   rename collision check, and the dead-letter checks (`dead_letter_topic` required with `where`, outbox or
   `unchanged = "dead_letter"`; not matching any template glob; no leading `_`). Wired into `send_batch` with the no-transform fast
   path. Includes the `--sink render` bench mode and a baseline.
2. **Expression engine:** field paths, lazy borrowed row view, a typed three-valued evaluator
   (decimal-string numerics, `NaN`/`±Infinity` as errors, null as unknown, errors win), the parser with its depth and size limits, template rendering. Table-driven unit
   tests, plus a parser fuzz test that includes deep nesting.
3. **Filter + error policy:** `ops` and `where`, `sink.dead_letter_topic`, `RouteStats` and admin/Prometheus
   exposure.
4. **Content routing:** templated topics with the prefix/segment/collision rules, `topic_values`, the
   required `topic_fallback`, the broker-counted `max_dynamic_topics`, inspector topic discovery.
5. **Per-route key** (coordinate with #45).
6. **Fields:** include/exclude, mask, HMAC hash (canonical input, `hash_key_id` header), rename, and the
   unseen-field warning and metric over every field path the route references (`where`, templates, `key`,
   outbox columns, field ops).
7. **Shapes:** `format = "row"` with `deletes` and `unchanged`, envelope `tombstones`, raw display in the
   inspector.
8. **Outbox router.**
9. **Docs:** DEPLOYMENT.md route reference (including the rows-leaving-the-filter, TOAST and mask-type
   caveats), README example, CHANGELOG (additive).
10. **Cockpit:** per-route counts, filtered/fallback/error counts, unseen-field warnings (Bumblebee).
11. **Follow-ups, not v1:** `on_leave` for rows leaving a filter; the decoder filling `Unchanged` TOAST
    columns from `before` under FULL; fan-out (`continue = true`).

Priority: 1 → 2 → 3, 4, 5 (the most-asked: filter, content routing, keys), then 6 (PII), 7, 8. Each one
ships with unit tests, docs, and the bench number from issue 1's harness.

## Open questions for approval

1. Fixed transform order instead of an ordered chain? Recommendation: fixed.
2. First match wins, with no fan-out of one event to several routes in v1? Recommendation: yes. Fan-out can
   come later as `continue = true` on a route.
3. ~~Should exceeding `max_dynamic_topics` fail?~~ Resolved in review: it goes to the fallback, the cap is
   counted from the broker, and the fallback is required (Megatron: failing turns one row into a stall that
   fills the primary's disk).

## As built (R3)

Filter and error policy (#81). Where the code differs from the text above, which was written before #138 changed the
event format:

- **Dead-letter key.** Since #138 record keys are JSON (`{"db", "schema", "table", "key": {...}}`, MongoDB `db` and
  `collection`), not `schema.table`. The dead-letter key is that JSON key with `"key": null`: the table only, no row
  values. The partition is hashed from it.
- **Times in `where`.** Since #138 `timestamp` values are JSON numbers (microseconds since the epoch) and `date`
  values JSON numbers (days since the epoch), so number literals compare with them (§3 "Ordering comparisons on
  timestamps are not supported" no longer holds for these two types). `timestamptz` stays an ISO-8601 string in UTC,
  so it allows only `==`, `!=` and `in`. Values without a number form (`infinity`, out-of-range dates) stay strings,
  and a number literal against them is a type error (dead-letter topic).
- **Unseen fields** are reported only while the field has never been seen in a row that had the row the path reads,
  so a delete's key-only before-image does not trigger the warning.
- **Route labels** must be unique, and `default` is reserved for the `sink.topic` fallback.

## As built (R5)

Per-route key (#83):

- **Key format.** Since #138 the record key is JSON, so `key = [...]` does not produce `schema.table:<values joined
  0x1f>` (§5 "Key"). It produces the usual JSON key with `"key": {"col": value, ...}`: the listed top-level columns
  of `row` in config order, each value copied as raw JSON, a missing column `null`. The partition is the FNV-1a hash
  of that whole key (what `partition_by = "key"` hashes), overriding `partition_by` for the route.
- **Truncates** have no row and get the table-only key (`"key": null`), like dead-letter records.
- **Validation:** a non-empty list of distinct top-level column names; a dotted path is rejected. The key columns
  are registered with the unseen-field counter as `row.<column>`, on routes with or without `where`.

## As built (R6)

Field operations (#84). Where the code differs from the text above:

- **The record key gets the field operations.** Since #138 the record key always carries the primary key as a
  `"key"` object (`{"db", "schema", "table", "key": {...}}`), whatever `partition_by` says, so "declare `key`
  explicitly under `partition_by = "key"`" no longer protects anything. Instead, a route with field operations and
  no `key = [...]` applies the same operations to the key object's columns: excluded or not-included columns are
  removed (an object left empty becomes `"key": null`), masked ones masked, hashed ones HMAC'd with the same key as
  the value (so per-row partitioning keeps working and the key matches the value), renamed ones renamed. The
  partition is hashed from the rewritten key. A route with `key = [...]` keeps that key, and validation rejects a key
  column the route excludes, masks, hashes or leaves out of `include`. A renamed column may be a key column: it is
  not protected, and the key reads the original row, so it keeps its original name there.
- **Rename collisions are checked twice.** At startup: a target may not be another renamed column, another target,
  or an included column. With `include` the output columns are known, so that is complete. Without it, a row that
  already has a column named like a target is a row-level error: it goes to `sink.dead_letter_topic` (required for
  `rename` without `include`) with an `aqueduct-error` header naming the columns, never values. Its dead-letter
  value has the other operations applied under the original names.
- **Also rejected at startup:** an empty `include`; a column both masked and hashed; a column masked, hashed or
  renamed that `exclude` drops or `include` leaves out (the operation would have no effect).
- **Dead-letter values** get the route's field operations (§5a), with the `aqueduct-hash-key-id` header when the
  route hashes. A row that is not a JSON object cannot be rewritten; its dead-letter value has `before`/`after`
  `null` rather than the unprotected row.
- **Canonical hash input** for objects and arrays is the JSON as the source wrote it with whitespace outside strings
  removed (key order and escapes kept), not a re-serialisation.
- **Hash key.** `AQUEDUCT_HASH_KEY` or `sink.hash_key_file` (one trailing newline trimmed), not both; at least 32
  bytes; `sink.hash_key_id` (1 to 64 of `A-Z a-z 0-9 . _ : -`) is required with a key.
- **Unseen fields:** every column a field operation names is watched as `row.<column>`. Since R5, `observe` runs on
  every route, with or without `where`.
- **Content routing (R4) is not merged yet.** Its side of "no row data may bypass the field operations" (topic
  templates and `topic_values` may not read a protected column) is a TODO in `RoutePlan::build` against
  `FieldOps::protection`; R4 adds it.
- **Cost** (release, 10-column row updated with before and after, 2-column key, one route): no field operations
  541 ns/event; include 1.8 µs, exclude 2.2 µs, mask 2.3 µs, rename 2.4 µs, hash of 2 columns 4.5 µs, all together
  5.4 µs.
