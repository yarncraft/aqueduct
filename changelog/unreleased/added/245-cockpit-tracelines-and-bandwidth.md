- **Cockpit: a trace line under each top tile, Kafka bandwidth, and 5-minute card windows** (#245). Throughput,
  Kafka bandwidth, end-to-end lag, sink latency and errors each get a line of the last 5 minutes, one sample per second,
  drawn like the routing cards' lines and moving every second (every stream frame carries them as `kpis`; new
  `GET /api/kpis/history`, needs the admin token). The Events delivered tile is replaced by **Kafka bandwidth**: key,
  value and header MB/s the broker acknowledged, also exported as `aqueduct_kafka_bytes_total`. Every card's line now
  covers 5 minutes instead of 2 (the Behind source backlog line 5 instead of 1; its rates and state still use the
  last minute). The routing graph's lines are sized by bandwidth (from new per-topic `topic_bytes` in
  `/api/routes/history` and the stream), and hovering any card shows each of its lines' bandwidth. The page uses the
  full width of the screen, card lines are 1.5x taller, and a legend names the operation bar colours (in
  place of the route, table and topic counts next to the Routing title). With more than 3 rules, **Collapse rules**
  folds the route cards to their name, counts and line. A table's panel no longer shows the global checkpoint, and a
  topic's panel no longer shows Kafka offsets (both stay in `/api/tables`).
