- **Routing: include, exclude, mask, hash and rename fields to protect personal data** (#84, ADR 0002).
  `[[sink.routes]]` takes `include` or `exclude` (top-level columns), `mask` (strings become `"***"`, other values
  `null`), `hash` (HMAC-SHA256 hex over the value's canonical text, so a Postgres `42` and a MongoDB `42` give the
  same digest) and `rename`. They apply to `before` and `after`, to the route's dead-letter records and, unless the
  route sets `key`, to the primary-key object in the record key, which the partition is then hashed from. The hash
  key comes only from `AQUEDUCT_HASH_KEY` or the new `[sink] hash_key_file` (at least 32 bytes; a `hash_key` in the
  TOML fails startup), and the new `hash_key_id` is sent as the `aqueduct-hash-key-id` header. A `key` column, or
  a column a content-routed `topic` reads, may not be excluded, masked, hashed or left out of `include`, and a rename target may not be listed in `mask`, `hash`
  or `exclude`. A row whose renamed column collides goes to the route's dead-letter topic. `/api/routes`
  lists each route's field operations and the key id, never the key. Additive: routes without these keys are
  unchanged.
