- **MongoDB snapshots: copy documents that already exist while changes stream** (#77, ADR 0001). New MongoDB
  `[source]` keys `snapshot` (`never` by default, `on_demand`, `initial`), `snapshot_chunk_size` and
  `snapshot_max_rows_per_sec`, the same as for Postgres, and `/api/snapshot` accepts `database.collection`. Documents
  arrive as `"op": "r"`, encoded like an `insert` of the same document, in `_id` order (mixed `_id` types work).
  With snapshots on, Aqueduct writes watermark documents into `<database>.aqueduct_watermarks` and needs `insert`,
  `remove` and `createIndex` on that collection; startup checks it. See DEPLOYMENT.md, "MongoDB snapshots".
  - While a MongoDB snapshot runs the checkpoint file holds JSON (`{"pos": <resume token>, "snap": {...}}`); a
    version without MongoDB snapshots cannot resume from it. Let the snapshot finish before rolling back, or delete
    the checkpoint.
  - An older Aqueduct watching the same whole database (no `collections` list) forwards the inserts and deletes of
    `aqueduct_watermarks` as ordinary events. Upgrade every instance on that database first, or give the old ones a
    `collections` list.
