- **Routing matches a table once, not every event** (#260). The sink remembered nothing between events: each one built
  `schema.table` and tried every route's pattern in turn, and the partition input was a fresh copy of the key or the
  table name. A table's route and name are now worked out on its first event and reused (up to 4096 tables; past that,
  matched per event as before), and the partition input points at the key or the shared name. Output and partitioning
  are unchanged. No action needed.
