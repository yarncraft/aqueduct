- **Cockpit: each route in the Routing graph reads as SELECT / WHERE / FROM / INTO / PARTITIONED-BY** (#171). Every
  route card has the same five aligned rows: the fields (`*`, `include`, `* EXCEPT (...)`, `mask(col)`, `hash(col)`,
  `col AS new`, `ROW` and the shape tags, an outbox's `payload`), the `where`, the `match` glob with the tables it
  claimed, the topic with `ELSE` fallback and `ON ERROR` the route's dead-letter topic, and the route key, primary key
  or table. The graph draws one red node per distinct dead-letter topic or template, and a topic's panel names the
  dead-letter topic of the routes writing to it (#177).
  Unset clauses show their default, dimmed. The step number, name, counts, rate line and "never seen" warning
  follow below; the badge row is gone. No API change: it reads `/api/routes`.
