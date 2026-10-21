- **Routing: set the Kafka message key per route** (#83, ADR 0002). `key = ["col", ...]` on a `[[sink.routes]]`
  entry builds the record key from those columns of the row (`after`, else `before`, so deletes key by their
  before-image): `{"db", "schema", "table", "key": {"col": value, ...}}`, values copied as JSON, a missing column
  `null`. The partition is hashed from that whole key, overriding `partition_by` for the route. Truncates keep the
  table-only key. `GET /api/routes` shows each route's `key`. Additive: routes without `key` are unchanged.
