- **Slot lag and heartbeat state in the metrics** (#54, backend). `/api/metrics` and `/api/stream` gain `slot_lag_bytes`,
  `confirmed_flush_lsn` and a `heartbeat` object; `/metrics` gains `aqueduct_slot_lag_bytes`,
  `aqueduct_heartbeat_skipped_total` and `aqueduct_heartbeat_last_timestamp_seconds`. See DEPLOYMENT.md, Idle heartbeat.
