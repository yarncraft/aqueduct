- **Cockpit replication-health tiles** (#54, UI half). Slot lag, last-heartbeat age and skipped heartbeats, read from
  `slot_lag_bytes`, `confirmed_flush_lsn` and `heartbeat` in the `/api/metrics` and `/api/stream` snapshots. A tile
  shows `–` while the server does not report its field.
