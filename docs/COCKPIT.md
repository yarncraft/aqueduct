# Admin cockpit

Open `http://host:8080/` (or `/#token=<admin_token>` when a token is set; the fragment is never sent to the server). It shows whether Aqueduct is behind its source
(backlog, rates and time to catch up), throughput, totals, lag, sink latency, errors, the
routing graph, per-table high-water marks and an event inspector. The inspector is a pop-up window that opens only from
a topic's detail panel in the routing graph (**Open in inspector**), filtered to that topic; it reads nothing from
Kafka while closed, and Close, Esc or a click outside closes it.
Routes: `/` and `/admin.css` (page), `/healthz` (liveness), `/readyz` (`200` only while streaming, else `503`), `/metrics` (Prometheus), `/api/metrics`, `/api/stream` (SSE), `/api/events?topic=<t>&limit=<n>` (read from Kafka), `/api/tables` (per table high-water marks), `/api/routes` (the route plan with per-route counts), `/api/routes/history` (per-route and per-topic rates, also per operation, over the last 120 s), `/api/snapshot` (`GET`/`POST`/`DELETE`, see [Snapshots](DEPLOYMENT.md#snapshots)). Only `/api/snapshot` accepts methods other than `GET`.

The inspector shows row data. `admin_token` is required on a non-loopback bind (see `admin_insecure_no_auth`); also publish the port only on a trusted network or behind a reverse proxy
with TLS and authentication, or set `inspector = false`.

Without `admin_token` (a loopback bind, or `admin_insecure_no_auth`), any web page open in the operator's browser can
send requests to the port, so `/metrics` and `/api/*` then refuse what only a browser on another site sends: a `Host`
that is a name other than `localhost` or the host of `health_addr` (DNS rebinding; IP addresses are accepted), an
`Origin` other than the server's own, and a `POST` without `Content-Type: application/json` (`403`, `403` and `415`).
Open the cockpit by IP address or `localhost`, and send snapshot requests with `-H 'Content-Type: application/json'`.
With a token none of this is checked: a page cannot attach the token.

The server takes at most 64 connections at a time. A request head must arrive within 2 s and the whole request within
5 s, or the connection is closed. Past 64, a small reserve (part of it only for loopback peers, so the image
`HEALTHCHECK` always gets through) still answers `/healthz` and `/readyz` and gives other routes `503`; past that,
connections are dropped. At most 16 `/api/stream` streams run at once.

The inspector adds no work to the pipeline: nothing is buffered in memory. It shows a sample, not a feed: the newest
100 events, read once when it opens, when its topic filter changes and on **Resample**. Each sample (shared across tabs
for 2 s) makes a separate Kafka read of up to `limit` records per partition from the topics the sink
routes to, using its own connection (created on first use), and keeps only the newest `limit` of them. One read runs
at a time; a read that takes over 10 s answers `504` (Kafka down or stalled) and other routes are not held up. A
closed tab costs nothing.

## Behind source

The **Behind source** card, below the top tiles, answers "is Aqueduct behind its source, and when does it catch up?".
It compares two positions the source reports:

| | Postgres | MongoDB |
|---|---|---|
| Head | The server's WAL head: `pg_current_wal_lsn()` (`pg_last_wal_replay_lsn()` on a standby), read once a second with the slot stats by a task of its own on the SQL connection; with `transport = "stream"` also the WAL end of each keepalive, whichever is newer | The newest majority-committed write, `lastWrite.majorityOpTime` from `hello`, run once a second by a task of its own (falls back to `lastWrite.opTime`, then the reply's `operationTime`) |
| Delivered | The change LSN of the newest acked event, the commit end the slot was advanced to (also by idle heartbeats), and the WAL flush position (`pg_current_wal_flush_lsn()`, read with the slot stats) as of before a peek that drained the slot, or the WAL end of a keepalive that ended a stream fetch, once everything was handed on | The cluster time of the newest acked event's resume token, and of the postBatchResumeToken of an empty change-stream batch once everything was handed on |

The head is read independently of the pipeline loop, so it keeps moving while the sink holds the pipeline up (a Kafka
outage, up to the 60 s sink timeout). On Postgres the stats query shares the SQL connection, so it waits behind a peek
in flight.

Once a second the admin server takes a sample of both; the card is derived from the last 60 samples (one minute):

- **Status.** `caught up` (green) when the backlog is within the threshold: **1 MiB of WAL** for Postgres, **2 s of
  cluster time** for MongoDB. It is never exactly 0: heartbeat writes, `pg_current_wal_lsn()` running ahead of the
  delivered commit end, and the batch in flight leave a little, and MongoDB times have one-second resolution with an
  idle replica set moving its clock only with the 10 s no-op writer. `catching up` (blue) when the delivered position
  moves faster than the head over the window; `falling behind` (amber) when it does not; `stalled` (red) when there is
  a backlog over the threshold and nothing was delivered for **30 s** of it while the pipeline is streaming (quiet
  time before the backlog appeared does not count). `unknown` (grey) until the source has reported both positions
  (Postgres: the head within a second of start, delivered at the first ack or drained poll; MongoDB: the head within a
  second of start, delivered at the first ack or empty change-stream batch), while the head was last read more than
  **3 s** ago (the card then says how long), and while a backlog has less than 2 s of samples to tell its direction.
- **Backlog.** Postgres: WAL bytes between head and delivered, and the time behind: while there is a backlog, now
  minus the later of the newest delivered change's commit time and the moment the backlog crossed the threshold (when
  that happened inside the window), so a burst after a quiet hour reads seconds, not an hour; 0 when caught up.
  MongoDB: the head's cluster time minus the newest delivered one. A sparkline shows the backlog over the minute (at
  most 60 values).
- **Source vs delivered.** How fast the head and the delivered position move over the window: WAL bytes per second,
  or for MongoDB seconds of cluster time per second (about 1.0 for the source; above 1.0 delivered means catching
  up). Rates need 2 s of samples.
- **Time to catch up.** Backlog divided by (delivered rate minus source rate), only while the backlog shrinks; 0 when
  caught up.
- **Snapshot row**, while a snapshot runs: rows per second over the minute, rows left (the planner estimate of all its
  tables minus the rows this process copied, so after a restart it overstates), the ETA (*estimate exceeded* when
  rows left reaches 0 while the snapshot still runs), and a *throttled by
  snapshot_max_rows_per_sec* chip when the rate is at 90 % or more of that limit. The live stream's status sits next
  to it, so a snapshot that makes the stream fall behind shows.

The **End-to-end lag** tile reads `idle` when the status is caught up and nothing was delivered for 10 s, instead of
counting the idle time up. The Kafka HWM in a table's detail panel is sink-side (what reached Kafka); it does not say whether
Aqueduct is behind the source.

`/api/metrics` and `/api/stream` carry the card as `source_lag`, plus `source_head_lsn` (Postgres, `X/Y`) and
`source_head_time_ms` (MongoDB):

```json
"source_lag": {"state": "catching_up", "backlog_bytes": 10485760, "behind_secs": 30.0,
  "ingest_rate": 1048576.0, "process_rate": 3145728.0, "rate_unit": "bytes/s", "eta_secs": 5.0,
  "window_secs": 60.0, "head_age_secs": 0.4, "threshold": {"backlog_bytes": 1048576, "behind_secs": null, "stalled_after_secs": 30},
  "history": [10485760, ...],
  "snapshot": {"rows_per_sec": 480.0, "rows_left": 95200, "rows_left_scope": "all_tables", "eta_secs": 198.3,
               "throttled": true, "max_rows_per_sec": 500}}
```

`state` is `unknown`, `caught_up`, `catching_up`, `falling_behind` or `stalled`. `head_age_secs` is how long ago the
head was read; past 3 s the state is `unknown` and the numbers are `null`. `backlog_bytes` is `null` for
MongoDB; the rates are `null` for the first 2 s; `eta_secs` is `null` unless the backlog shrinks; `snapshot` is
`null` unless one runs. Prometheus has `aqueduct_source_backlog_bytes`, `aqueduct_source_behind_seconds`,
`aqueduct_source_catch_up_eta_seconds`, `aqueduct_snapshot_rows_per_second` and `aqueduct_snapshot_eta_seconds`, each
left out while unknown. Like the rest of `/api/*` and `/metrics` they need the admin token; none of them carry row
data. Samples, rates and the window start over when the process restarts.

`/api/events` returns each record's `topic`, `partition`, `offset`, `timestamp_ms`, `key`, `headers` (as text) and
`event`: the value parsed as JSON, or its text when it is not JSON, and `null` for a tombstone. A value that is not a
change-event envelope (an outbox payload, a `format = "row"` row, a tombstone) is shown raw, with its op and lsn taken
from the headers; the table of a row or tombstone comes from its JSON record key.

## Per-table high-water marks

Click a table (or collection) in the routing graph: its detail panel shows how far it has got on the source side
since the process started (nothing Kafka-side):

| Field | Source |
|--------|--------|
| Events, ops | Events delivered by this process, per operation, and the table's share of all events |
| Last LSN | LSN of the newest delivered change (`–` for MongoDB, whose resume tokens are not shown per table) |
| Last commit | How long ago that change committed. Amber when the table is more than 1 minute behind the freshest table, red past 10 minutes; only while the pipeline is streaming. Measured against the freshest table rather than the clock, so a database that is idle as a whole is not flagged; a table that is simply written rarely is |
| Delivered | How long ago the sink acknowledged it |

The panel also shows the last checkpointed position, which is global, not per table. While a snapshot runs it is the
stream position followed by `(snapshot: <table> <i>/<n>)`; the snapshot cursor (the last chunk's primary-key values)
stays in the checkpoint file and is never shown. Pipeline numbers refresh every second
with the stream.

The Kafka side is in a topic's detail panel (click the topic): its **Kafka HWM**, the sum over the topic's partitions
of the next offset Kafka will assign, with each partition's offset, and when the offsets were read. Kafka offsets come
from `GET /api/tables`, polled every 5 s: one offsets request per partition of
the routed topics (no records are read), with the inspector's connection and TLS/SASL settings, shared across tabs
for 5 s and abandoned after 3 s, so a Kafka outage leaves the offsets empty (the topic panel shows the reason) but never blocks the cockpit. Offsets need
the inspector (`admin_token` and `inspector = true`); without it the panel says so. The routing graph also takes each
table's topic from `/api/tables`.

Events and Delivered count every event the source delivered for the table, including events a route filtered
(`where`) or sent to the dead-letter topic. A table whose events are all filtered therefore shows events and
recent deliveries while its topic's Kafka HWM does not move; the per-route counts in `/api/routes` show why.
A table whose route picks the topic per event (`{op}` or a row field in `topic`) has no single topic to read
offsets from: in `/api/tables` its topic is the template and `kafka` is null with `content-routed (see Routing)`.

`GET /api/tables` (Bearer token) returns:

```json
{"state": "streaming", "checkpoint": "0/1A2B3C8", "inspector": true, "kafka_read_ms": 1760965426000,
 "tables": [{"table": "public.orders", "topic": "cdc.public.orders", "events": 1200,
             "ops": {"c": 800, "u": 350, "d": 40, "t": 0, "r": 10},
             "last_lsn": "0/1A2B3C0", "last_commit_ms": 1760965425210, "last_delivered_ms": 1760965425250,
             "kafka": {"hwm_total": 5310, "partitions": [{"partition": 0, "hwm": 2650}, {"partition": 1, "hwm": 2660}],
                       "tables_on_topic": 1}}]}
```

`kafka` is `null` with an `error` string when the offsets could not be read (inspector off, Kafka unreachable or slow,
topic missing). Counts and marks start from zero when the process restarts. Prometheus has the commit time per table
as `aqueduct_table_last_commit_timestamp_seconds{table="schema.table"}`; Kafka offsets are not exported there.

## Routing graph

The **Routing** card shows where each table's events go:

- **Tables** (left): every table that delivered events, with its count, grouped by the route that claims it. A route
  with more than 6 tables shows its 5 busiest and a "+N more" node; click it to expand. A table no route matches
  while `sink.topic` is unset is listed in red under "no route" (its events fail the pipeline).
- **Routes** (middle): `[[sink.routes]]` in match order, numbered, then `default` (`sink.topic`, marked `else`). A
  table belongs to the first route whose `match` fits `schema.table` (`*` matches any run of characters, dots
  included), the same rule the sink uses. Each route card has the same five rows, keyword on the left, in this order;
  a clause the route leaves unset shows its effective default, dimmed:

  | Row | Shows | Default |
  |-----|-------|---------|
  | `SELECT` | What the record value holds: `include` as a column list, `exclude` as `* EXCEPT (a, b)`, then `mask(col)`, `hash(col)` and `col AS new`. `format = "row"` adds `ROW` (`SELECT ROW *`); the delete mode, `tombstones` and `unchanged` are small tags after it. An outbox route shows its `payload` column | `*`, the change-event envelope |
  | `WHERE` | The `where` text, operation filter included. Long text is cut; hover for it all, or click the route | `true` |
  | `FROM` | The `match` glob, then the tables it claimed, ` / `-separated, the first three and `+N` | `* (unmatched)` on `default` |
  | `INTO` | The topic template; with content routing, `ELSE` the `topic_fallback` and a tag with the number of `topic_values` (hover for them). The dead-letter topic is in the route's panel, not on the card | `sink.topic` on `default` |
  | `PARTITIONED-BY` | `key (a, b)` for a route `key`; the `aggregate_id` column for an outbox | `primary key` (`partition_by = "key"`) or `table` |

  The step number and route name are on top, above the rows. Below them, four metrics, each in the colour of its line
  in the card's step chart: **processed** (blue, every event the route took), **matched** (green, processed minus
  filtered: the `where` held; a row the `where` could not evaluate also counts here, and under errors), **fallback**
  (violet, sent to `topic_fallback`) and **errors** (red, sent to the dead-letter topic). Produced is in the panel. An amber **never seen** box lists fields its `where` reads that no row has had after enough rows
  (`aqueduct_route_field_unseen`); that is usually a misspelt column, and the route then filters everything.
- **Topics** (right): only real topics, never a template: the topics a template rendered for the tables seen, and
  under **dead letter**, in red, one card per distinct dead-letter topic that has received events (a
  `{schema}`/`{table}` dead-letter template shows the dead-letter topics it rendered), with the errors of the routes
  that use it; a dead-letter topic with no events is not drawn. A route whose template has not rendered a topic yet has no topic edge. The `default` route
  (`sink.topic`) is left out until an event reaches it. Beyond
  six, the quietest ones are behind "+N more". A template that reads the event (`{op}`, `{row.x}`) lists the topics the
  sink has produced to since start (not its dead-letter topics), and its `topic_fallback`.

A line's width follows its count (table events in, the route's produced count out, errors to the dead-letter
topic); lines with traffic in the last 3 s carry moving dots, which stand still when the browser asks for reduced
motion. The line from a route to its dead-letter topic is solid red (only the moving dots are striped) and appears once
that topic has events. A route that
picks its topic per row (content routing) animates its lines to the topics it writes whenever it produces. Hover a
table to highlight its path. The plan is read once from `GET /api/routes` when the page loads; counts update every
second from the stream. Without any `[[sink.routes]]` the graph is all tables → `default` → their topics.

**Operation shares and rate.** Each table and topic card has a thin stacked bar of its counts per operation since
start, as shares (insert green, update blue, delete red, truncate violet, read amber; hover it for the counts and
percentages); every card, routes included, has a step line of its rate over the last 120 s (a route: one line, processed;
its detail chart has four, processed, matched, fallback and errors). The bar counts:

- a table: events the source delivered for it (`table_ops` in `/api/metrics` and every stream frame);
- a topic (dead-letter topics have no bar, only their step line): records Kafka acknowledged, counted by the sink after the broker acknowledged
  the produce, so a failed produce never counts and a retried batch counts when it succeeds. A tombstone carries its
  delete's `op` header and counts as a delete. Topic cards also show the acknowledged count;
- a "+N more" group: the sum over its tables or topics.

Table, route and topic cards keep their counts (events, the route's produced/filtered/fallback/errors, acknowledged
records, a dead-letter topic's errors).

**Rates.** Click a table, a route or a topic for a panel with one step line over the last 120 s, one sample per
second (nothing animates), with its current, average and maximum rate: a table's events, a route's produced events or
a topic's acknowledged records per second. A table's and a topic's chart draw one line per operation, in the op colours, for the operations toggled on with the
buttons above it; only operations that have happened for that table or topic get a button (insert, update, delete,
truncate, read); the selection is shared by the two. A route's chart has no
buttons: four lines, processed, matched, fallback and errors, from the route's own counters. A table's panel names the route that claims it. A route's panel shows only what its card does not: the matched count, its produced events per
operation, the full `topic_fallback` and `topic_values`, the topics a template wrote, its dead-letter topic (click to
select it), the full table list when the card's `FROM` is cut short, and any other config keys.  A topic's panel lists its acknowledged records per
operation, the routes that write to it (by template or as their `topic_fallback`), **dead letter**: the dead-letter
topic of each of those routes that can dead-letter a row (a `{table}` template filled in for this topic,
`cdc.public.orders.dlq`; click one to select it in the graph), and an **Open in inspector** button (shown when the
inspector is on). A dead-letter topic's panel works the same (its line and per-op counts sum every topic a `{table}`
template rendered) and shows **dead letter for**, the routes it serves and their topics; a `{table}` template's panel
lists the dead-letter topics it has rendered, each opening the inspector.

The history is kept by the server, so the charts are full when the page opens: `GET /api/routes/history` returns
the window, and each stream frame carries only the newest second (`rates`: `seq`, every route's outcomes in plan
order as `routes` and its produced records per op as `route_ops`, the topics with records in that second as `topics`
and per op as `topic_ops`). Per-op rows are `[insert, update, delete, truncate, read]`. The window adds `ops` per route
(produced per op and second), `topic_ops` (acknowledged per op and second) and `topic_totals` (acknowledged per op
since start, up to `seq`). It needs the admin token like every `/api/*` route. Memory is bounded:
routes are fixed at startup, and the sink counts at most 500 topics by name and the rest as `(other)`. Topic names can
contain row values under content routing; they are shown only behind the token, as in the graph. Rates start from
zero when the process restarts.

```json
{"seq": 431, "interval_ms": 1000, "len": 120,
 "routes": [{"route": "paid-orders", "produced": [3, 0, 5], "filtered": [1, 0, 2], "fallback": [0, 0, 0], "errors": [0, 0, 0],
             "ops": [[2, 1, 0, 0, 0], [0, 0, 0, 0, 0], [3, 0, 2, 0, 0]]}],
 "topics": {"orders-paid": [3, 0, 7], "orders-paid.dlq": [0, 0, 0]},
 "topic_ops": {"orders-paid": [[2, 1, 0, 0, 0], [0, 0, 0, 0, 0], [3, 0, 4, 0, 0]],
               "orders-paid.dlq": [[0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]]},
 "topic_totals": {"orders-paid": {"insert": 610, "update": 95, "delete": 40, "truncate": 0, "read": 0},
                  "orders-paid.dlq": {"insert": 1, "update": 0, "delete": 0, "truncate": 0, "read": 0}}}
```

## Routing plan

`GET /api/routes` describes the compiled routing for the cockpit's routing graph: each route in match order, then
the `sink.topic` fallback as a route named `default` with `"match": null`, with live counts. It never contains row data.
Later routing options add keys to each route, so readers should ignore keys they do not know. A route with field
operations also has `include`, `exclude`, `mask` and `hash` (column lists) and `rename` (the map), each left out when
unset; with a hash key the top level has `hash_key_id`. The hash key itself is never shown. An outbox route has an
`outbox` object with its column names and topic template (`aggregate_type`, `aggregate_id`, `payload`, `topic`, and
`id` and `type` when set); other routes leave the key out. A route with `format = "row"` has `"format": "row"` and
its `deletes` (`tombstone`, `drop` or `row`, the default included), a route with `tombstones = true` has
`"tombstones": true`, and a route with `unchanged = "dead_letter"` has `"unchanged": "dead_letter"`; each is left out
otherwise. `counts` also has `tombstones_skipped`, which is not an outcome, and `ops`: the produced events per
operation (`insert`, `update`, `delete`, `truncate`, `read`; they sum to `produced`, and a delete's tombstone is not
counted again). Every route has its resolved
`dead_letter_topic` (a `{schema}`/`{table}` template for one rendered per event, `null` only if the default name is
unusable on a route that cannot dead-letter); the top-level `dead_letter_topic` is `sink.dead_letter_topic`, `null`
when unset.

```json
{"default_topic": "cdc.{schema}.{table}", "dead_letter_topic": null, "partition_by": "key",
 "routes": [
   {"route": "paid-orders", "match": "public.orders",
    "where": "op in ['insert', 'update'] and row.status == 'paid' or op == 'delete'", "key": ["order_id"], "topic": "orders-paid",
    "topic_fallback": null, "topic_values": null, "dead_letter_topic": "orders-paid.dlq",
    "counts": {"matched": 120, "produced": 80, "filtered": 39, "fallback": 0, "errors": 1, "tombstones_skipped": 0,
               "ops": {"insert": 50, "update": 20, "delete": 10, "truncate": 0, "read": 0}},
    "unseen_fields": []},
   {"route": "regional", "match": "public.customers", "where": null, "key": null,
    "topic": "customers.{row.region}", "topic_fallback": "customers.unknown", "topic_values": ["eu", "us"],
    "format": "row", "deletes": "tombstone", "dead_letter_topic": "customers.unknown.dlq",
    "counts": {"matched": 300, "produced": 290, "filtered": 0, "fallback": 10, "errors": 0, "tombstones_skipped": 0,
               "ops": {"insert": 200, "update": 85, "delete": 5, "truncate": 0, "read": 0}},
    "unseen_fields": []},
   {"route": "default", "match": null, "where": null, "key": null, "topic": "cdc.{schema}.{table}",
    "topic_fallback": null, "topic_values": null, "dead_letter_topic": "cdc.{schema}.{table}.dlq",
    "counts": {"matched": 5000, "produced": 5000, "filtered": 0, "fallback": 0, "errors": 0, "tombstones_skipped": 0,
               "ops": {"insert": 3000, "update": 1500, "delete": 500, "truncate": 0, "read": 0}},
    "unseen_fields": []}]}
```

`/api/metrics` and `/api/stream` carry the same counts flat, as
`routes: [{"route", "matched", "produced", "filtered", "fallback", "errors", "tombstones_skipped", "ops", "unseen_fields"}]`.

The cockpit smooths throughput, peak and recent sink latency as trailing averages over the last 15 one-second snapshots (`SMOOTH_WINDOW` in `admin.html`). Counters, totals, lag and max latency are exact.
