- **Routing: filter events per route and send failures to the route's own dead-letter topic** (#81, #177, ADR 0002). `[[sink.routes]]`
  takes `where`, a typed predicate with SQL-style three-valued logic (an unknown result is filtered). It filters
  operations too: `where = "op in ['insert', 'update']"` (`op` is `insert`, `update`, `delete`, `truncate` or `read`;
  any other value, and comparing `op`, `schema` or `table` with a number or boolean, fails startup). There is no
  separate `ops` key; a route that sets one fails startup and prints the equivalent `where`. The first matching route
  owns the event; nothing falls through. Rows a `where` cannot evaluate (a type error) go to the route's dead-letter
  topic with an `aqueduct-error` header and a table-only key. A `where` on `op`, `schema` and `table` alone cannot
  fail. Every route has a dead-letter topic, with no setting required: its own topic plus `.dlq` (`sim.assets` →
  `sim.assets.dlq`); a `{schema}`/`{table}` template plus `.dlq`, rendered per event and created on first use, `{op}`
  left out; `<topic_fallback>.dlq` for a content-routed or outbox topic. A route's `dead_letter_topic`, then
  `[sink] dead_letter_topic`, override it. Fixed dead-letter topics of routes that can dead-letter a row are opened at
  startup. Dead-letter names are checked at startup (valid, no leading `_`, not a fallback, not producible by any
  route's topic), and `dlq` is a reserved row value for content routing, so no row can render a dead-letter name.
  `/api/routes` gives each route's `dead_letter_topic`, and the inspector reads them all. Per-route counters
  (`matched`, `produced`, `filtered`, `fallback`, `errors`) and unseen-field warnings appear in `/api/metrics`, the new
  `GET /api/routes` (the compiled plan with live counts) and Prometheus (`aqueduct_route_records_total`,
  `aqueduct_route_field_unseen`). Additive: routes without the new keys behave exactly as before.
