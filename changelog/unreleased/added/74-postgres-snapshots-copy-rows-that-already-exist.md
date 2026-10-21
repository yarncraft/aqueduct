- **Postgres snapshots: copy rows that already exist while changes stream** (#74, ADR 0001). New `[postgres]` keys
  `snapshot` (`never` by default, `on_demand`, `initial`), `snapshot_chunk_size`, `snapshot_max_rows_per_sec` and
  `snapshot_key_columns`. Copied rows arrive as `"op": "r"`, encoded like an `insert` of the same row. Needs
  Postgres 14+ and `EXECUTE` on `pg_logical_emit_message`. See DEPLOYMENT.md, "Snapshots".
  - **New op value `r` (snapshot rows): consumers that decode `op` as a closed enum must accept it.**
