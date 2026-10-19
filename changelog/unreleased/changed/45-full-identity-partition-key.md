- **`partition_by = "key"` no longer hashes the whole row on `REPLICA IDENTITY FULL` tables** (#45). Under FULL every
  column is flagged as identity, so each update produced a new key and could move the row to another partition. Those
  tables now use the `schema.table` key in `key` mode (per-table order, as with `partition_by = "table"`). A key column
  stored as unchanged TOAST is now read from the update's old key, so the row keeps its insert key; if an update carries
  no old value for it, that event uses the `schema.table` key. Other identities are unchanged. **Upgrade:** events of
  FULL tables that are still unconsumed sit on their old hash partitions while new ones go to the `schema.table`
  partition, so a consumer reading partitions in parallel can see one reorder window at the cutover. Drain the topic
  first, or accept it. The same window opens whenever you run `ALTER TABLE ... REPLICA IDENTITY` on a streaming table.
  An `info` log names each FULL table the first time its Relation message is seen (once per process); it only matters if
  you use `partition_by = "key"`.
