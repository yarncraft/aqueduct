- **Sim: one `events` table** (#247). `asset_readings` and `maintenance_events` are replaced by `events`, with a
  `type` (`asset`, `maintenance`, `system`), `kind`, `severity` and a `detail` object; maintenance events carry their
  `work_order_id`. An existing sim volume keeps the old schema: run `down -v` on the sim project once.
