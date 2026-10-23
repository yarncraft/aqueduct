- **Cockpit: collapse rules, leaner table and topic panels** (#246). With more than 3 rules, **Collapse rules** in the
  Routing header folds every route card to its name, counts and line (remembered per browser). A table's panel no
  longer shows the global checkpoint, and a topic's panel no longer shows Kafka offsets (both stay in `/api/tables`).
  Table, route and topic cards show their operation shares as a stacked strip down the card's left edge instead of
  a bar above the step line, in higher-contrast colours, and the line is 1.5x taller. A route's panel gets the same
  op buttons as a table's and a topic's: its processed, matched, fallback and errors lines count only the operations
  toggled on (`/api/routes/history` adds `filtered_ops`, `fallback_ops` and `errors_ops`, stream frames
  `route_outcome_ops`).
