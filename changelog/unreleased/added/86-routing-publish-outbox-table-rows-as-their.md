- **Routing: publish outbox-table rows as their own events** (#86, ADR 0002). `outbox = { aggregate_type,
  aggregate_id, payload, topic, id, type }` on a route turns each insert into the outbox table into one record: topic
  from a template such as `outbox.{aggregate_type}` (the content-routing rules apply, with `topic_fallback`), key = the
  raw `aggregate_id` value (a string's content, not the JSON table key; the partition is hashed from it), value = the
  `payload` column as is (`jsonb` as JSON text, text as its content), headers `id` and `type` from the configured
  columns besides `op` and `lsn`. Unset keys take Debezium's defaults (`aggregatetype`, `aggregateid`, `payload`,
  `outbox.event.{aggregate_type}`). Updates, deletes and truncates of the outbox are filtered; snapshot rows too,
  unless the route has a `where` that is true for them, such as `where = "op in ['insert', 'read']"` (then rows still
  in the table are published again on every snapshot). A `where` on `before.*` fails startup. Differences from Debezium's outbox
  router: a `jsonb` payload that is a bare string loses its quotes, a `text` payload holding JSON reads as JSON, and
  `bytea` arrives as hex text, so use a `jsonb` object or array, or `text`, as the payload. A row
  without an `aggregate_id` or `payload` goes to the route's dead-letter topic (`<topic_fallback>.dlq` by default),
  with an `aqueduct-error` header naming the column. `outbox` excludes `topic`, `key` and the field operations on the same route. The route's
  `topic` key is now optional in the config when `outbox` is set. `/api/routes` shows an `outbox` object, the cockpit
  the outbox columns on the route card, and `/api/events` now returns each record's headers (only `op`, `lsn`, `aqueduct-error`,
  `aqueduct-hash-key-id`, and the outbox `id`/`type`) and shows non-envelope values raw.
