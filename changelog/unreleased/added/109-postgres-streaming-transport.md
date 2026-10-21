- **Postgres streaming transport** (#109, ADR 0003): `[postgres] transport = "stream"` receives changes over a
  replication connection as they commit instead of polling the slot every 500 ms. Same events, keys and checkpoints
  as `poll`, which stays the default. Needs one more walsender per instance (`max_wal_senders`), a `pg_hba.conf` line
  for the role on the database, and a direct connection (no pooler). `stream_buffer_bytes` (64 MiB) bounds what is
  buffered ahead of the sink. See DEPLOYMENT.md, "Streaming transport".
