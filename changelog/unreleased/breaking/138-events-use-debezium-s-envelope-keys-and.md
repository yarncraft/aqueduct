- **Events use Debezium's envelope, keys and time encodings** (Postgres and MongoDB). Every consumer must be updated:
  - Value: `{"before", "after", "source": {"version", "connector", "ts_ms", "ts_us", "snapshot", "db", "schema",
    "table", "txId", "lsn"}, "op", "ts_ms", "ts_us"}` (MongoDB: `db` and `collection` in `source`). The old top-level
    `lsn`, `xid`, `commit_ts_micros`, `schema` and `table` moved into `source` (`lsn`, `txId`, `ts_us`, `schema`,
    `table`).
  - `op` is a letter: `c`, `u`, `d`, `t`, `r` (was `insert`, `update`, `delete`, `truncate`, `read`); the Kafka header
    `op` too.
  - `timestamptz` values are ISO-8601 in UTC (`2026-10-20T13:23:46.992107Z`, was Postgres text with an offset),
    `timestamp` values microseconds since the epoch, `date` values days since the epoch (both were text).
  - Record key: `{"db": "app", "schema": "public", "table": "orders", "key": {"id": 42}}` (was `public.orders` or
    `public.orders:42`); `key` is the primary key, also for `REPLICA IDENTITY FULL` tables.
  - Partitioning: `partition_by` now defaults to `key` (per-row order, a table spreads over partitions). Set
    `partition_by = "table"` under `[sink]` to keep total per-table order. Records of a table can land on other
    partitions than before, so let consumers drain the topics before upgrading if they rely on order across the
    switch.
  - Upgrade: deploy consumers that read the new format first (or a new topic per route), then upgrade Aqueduct.
