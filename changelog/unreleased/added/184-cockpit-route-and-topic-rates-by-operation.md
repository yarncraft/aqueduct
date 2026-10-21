- **Cockpit: route and topic rates by operation, with op buttons and op-share bars.** Routes count their produced events
  per operation (`counts.ops` in `/api/routes`, `/api/metrics` and the stream), topics their acknowledged records per
  operation (a tombstone counts as a delete); `/api/routes/history` and the stream's `rates` carry the per-op series
  (`ops`, `route_ops`, `topic_ops`) and per-topic totals (`topic_totals`). The route and topic detail panels (the
  dead-letter topic too) draw one line for the operations toggled on, with the same buttons as the table panel and one
  selection shared by all three; the route chart no longer overlays the filtered, fallback and error lines (the counts
  stay in the panel). Every Routing graph card shows a thin bar of its operation shares above its rate step line (on
  route cards instead of the produced/filtered bar). No config change; `/api/routes` and the history only gain keys.
