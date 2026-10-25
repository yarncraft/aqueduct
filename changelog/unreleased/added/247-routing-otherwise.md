- **Routing: chain rules on one table with `otherwise`** (#247, ADR 0002). `otherwise = { route = "<name>" }` on a route
  passes the rows its `where` rejects (false or unknown) on to a later route that has no `match`, whose own `where`
  decides next; `otherwise = { topic = "<name>" }` writes them to a fixed topic with the route's key and field
  operations. A type error still dead-letters. Startup refuses a bare `otherwise = "<name>"`, an `otherwise` without a
  `where`, to an earlier, missing or matching route, to an invalid or reserved topic, or next to `outbox`, and a route
  without `match` that nothing names. A passed-on row counts as `filtered` and `passed_on` at its route and `matched` at
  the next (`/api/routes`, `/api/metrics`, `aqueduct_route_passed_on_total`). The cockpit shows an `OTHERWISE` row
  tagged `route` or `topic`, draws the chain as a funnel between adjacent cards (else a dashed bracket), labelled with
  the rows passed on per second, and an `otherwise` topic as a dashed line from the route. Hovering a table now lights
  up everything downstream of it, a route only its own lines, and a topic every path that leads to it.
