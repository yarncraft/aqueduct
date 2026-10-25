- **The Postgres poll transport is removed** (#251). Changes always arrive over a replication connection
  (`START_REPLICATION`), which drained a backlog about 10x faster than the SQL peeks of `poll` (500k small rows:
  81k against 6.3k events/s) and holds memory flat on large transactions. `transport = "poll"`, the old default, now
  fails startup; `transport = "stream"` is still accepted. On upgrade: delete `transport = "poll"` if set, give the
  role the `REPLICATION` attribute, allow its replication connection in `pg_hba.conf` (a line for the database, e.g.
  `host app aqueduct 10.0.0.0/8 scram-sha-256`), keep a free walsender (`max_wal_senders`), and point the URL at
  Postgres directly rather than PgBouncer. The slot and checkpoint carry over unchanged. While Aqueduct runs, the slot
  shows `active = true`.
