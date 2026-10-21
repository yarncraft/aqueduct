- **Cockpit: per-table high-water marks.** The per-table card now shows, for each table, its topic, counts per
  operation, the newest delivered LSN, how long ago it committed and was delivered (amber/red when it falls behind
  the freshest table while streaming), and the topic's Kafka high-water mark per partition. New route
  `GET /api/tables` (Bearer token) and Prometheus series `aqueduct_table_last_commit_timestamp_seconds{table}`;
  `/api/metrics` and `/api/stream` gain `table_marks` and `checkpoint`. See DEPLOYMENT.md, "Per-table high-water marks".
