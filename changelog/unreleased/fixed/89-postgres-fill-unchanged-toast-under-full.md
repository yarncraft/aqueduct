- **Postgres source fills unchanged large (TOAST) columns under `REPLICA IDENTITY FULL`** (#89). An update that left an
  out-of-line column alone used to emit `after` without it. The source now takes it from the old row, so consumers get
  the complete row. Tables with another replica identity are unchanged: Postgres does not send the old value there.
  On a FULL table the `unchanged` list is now always empty, so a route with `unchanged = "dead_letter"` no longer
  dead-letters those updates, and the key is computed from the filled row.
