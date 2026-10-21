- **The supported Postgres versions are 12 and newer**, not 10 as the docs said. Postgres 10 has no
  `pg_replication_slot_advance`, and on 11 repeated slot peeks over one connection fail with
  `out of relcache_callback_list slots`; neither ever worked. Nothing changes on 12 and newer.
