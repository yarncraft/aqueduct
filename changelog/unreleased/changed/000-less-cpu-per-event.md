- **Less CPU per event on the Postgres-to-Kafka path** (#000). JSON strings and `timestamptz` values are written
  straight into the row buffer, the checkpoint text and the record key are built in one allocation each, and the
  pipeline records a delivered batch's metrics under one lock instead of one per event. Records, keys and
  checkpoints are byte-for-byte unchanged. No config change.
