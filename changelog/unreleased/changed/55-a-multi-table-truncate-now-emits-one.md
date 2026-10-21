- **A multi-table `TRUNCATE` now emits one event per table** (#55). `TRUNCATE a, b` is one WAL message, and only the
  first table's event was emitted, so consumers of the others never saw the truncate. No config change; expect extra
  `truncate` events (all with the same `lsn`) for statements that name several tables.
