- **Routing: reshape event output (row format, deletes, tombstones)** (#85, ADR 0002). New route keys, all off by
  default, so existing routes produce the same records byte for byte:
  - `format = "row"`: the value is the row (`after`, after field operations) instead of the envelope; `op` and `lsn`
    stay in the headers. Truncates produce nothing (counted `filtered`).
  - `deletes` (row format only): `tombstone` (default, a null value with the row's key), `drop` (counted `filtered`)
    or `row` (the rewritten before-image with `"__deleted": true`).
  - `tombstones = true` (envelope only): a null-value record with the delete's rewritten key after every delete, on
    the same partition.
  - No null value under a shared key: tombstones and row-format `deletes = "tombstone"` deletes need `key = [...]` or
    `partition_by = "key"` at startup, and each delete is checked. A key that is null (no primary key), has a null or
    missing `key = [...]` column, or loses or masks a key column to field operations gets no tombstone (a row-format
    delete is then dropped as `filtered`), logged once per route and counted as `tombstones_skipped` (`/api/routes`,
    `/api/metrics`, Prometheus `aqueduct_route_tombstones_skipped_total`).
  - A MongoDB update whose document was deleted before the lookup keeps its key in the envelope's `after` as before;
    row format drops it instead of publishing the key as the document.
  - `unchanged = "omit" | "dead_letter"`: `dead_letter` sends a row the source sent without unchanged TOAST columns to
    the route's dead-letter topic. The Postgres decoder now names those columns on the event (not
    serialised). TOAST caveat: in row format an update can lack a large column, and a compacted consumer that replaces
    whole rows loses it.
  - Exclusive with `outbox`. `/api/routes` shows `format`, `deletes`, `tombstones` and `unchanged`; the cockpit and
    `/api/events` show row-format values raw and tombstones as `null`, with the table taken from the record key.
    See DEPLOYMENT.md, "Shaping the output".
