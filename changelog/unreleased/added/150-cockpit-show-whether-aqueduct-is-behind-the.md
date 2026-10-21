- **Cockpit: show whether Aqueduct is behind the source, with backlog and time to catch up** (#150). A new
  **Behind source** card compares the source's head (Postgres WAL head from the slot stats query and stream
  keepalives; MongoDB's newest majority-committed cluster time from `hello` every second; both read by a task of
  their own, independently of the pipeline loop) with the delivered position.
  It shows caught up / catching up / falling behind / stalled, the backlog in bytes and time with a sparkline, the
  source and delivered rates, the time to catch up, and for a running snapshot rows/s, rows left, ETA and whether
  `snapshot_max_rows_per_sec` throttles it. Caught up means within 1 MiB of WAL (Postgres) or 2 s (MongoDB); stalled
  means a backlog and no progress for 30 s while over the threshold; `unknown` while the head is more than 3 s old. The End-to-end lag tile reads `idle` instead of counting up while caught
  up. New `source_lag`, `source_head_lsn` and `source_head_time_ms` on `/api/metrics` and `/api/stream`, and
  Prometheus gauges `aqueduct_source_backlog_bytes`, `aqueduct_source_behind_seconds`,
  `aqueduct_source_catch_up_eta_seconds`, `aqueduct_snapshot_rows_per_second` and `aqueduct_snapshot_eta_seconds`.
  The Postgres slot stats query now runs every second (instead of every 5 s) in its own task on the SQL
  connection, and also returns the WAL head and flush position.
