- **Routing: chain rules on one table with `otherwise`** (#247, ADR 0002). `otherwise = "<name>"` on a route passes
  the rows its `where` rejects (false or unknown) on to a later route that has no `match`, whose own `where` decides
  next; a type error still dead-letters. Startup refuses an `otherwise` without a `where`, to an earlier, missing or
  matching route, or next to `outbox`, and a route without `match` that nothing names. A passed-on row counts as
  `filtered` and `passed_on` at its route and `matched` at the next (`/api/routes`, `/api/metrics`,
  `aqueduct_route_passed_on_total`). The cockpit shows an `OTHERWISE` row and draws the chain as a dashed bracket
  labelled with the rows passed on.
