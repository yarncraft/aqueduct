- **Cockpit: a Routing card draws the routing graph** (#87, ADR 0002). Tables on the left, routes in match order
  (numbered, first match wins) in the middle, topics and the routes' dead-letter topics on the right. Each route shows its
  `match`, `where`, per-route counts (produced, filtered, fallback, errors) with a bar, and an amber warning
  listing fields its `where` reads that no row has had. Edge width follows the counts and edges with recent traffic
  animate (static under reduced motion). Hover a table to light up its path, click a route for its details, click a
  topic to open it in the event inspector. Busy routes collapse to the 6 busiest tables and topics with "+N more".
  No new server route: it reads `/api/routes`, `/api/tables` and the stream.
