- **Cockpit: event rates per route and per topic as step lines in the Routing card.** Each route shows its produced
  rate over the last 120 s, with filtered, fallback and errors as thin lines; each topic its records acknowledged by
  Kafka per second; each "+N more" group the sum. A content-routed template now lists the topics it produced to.
  Clicking a topic opens a panel with a larger chart, the current,
  average and maximum rate and the routes that write to it; the inspector is opened from there with **Open in
  inspector** (it used to open on the click). Clicking a route adds the larger chart split by outcome. New
  `GET /api/routes/history` (admin token required) returns the window, the stream carries the newest second, and
  Prometheus gets `aqueduct_topic_records_total{topic}` (500 topics by name, the rest as `(other)`).
