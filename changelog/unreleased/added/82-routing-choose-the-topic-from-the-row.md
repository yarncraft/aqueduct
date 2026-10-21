- **Routing: choose the topic from the row's contents** (#82, ADR 0002). A route's `topic` can read row fields, such as
  `orders.{row.region}`, and also takes `{op}`. Such a template must start with fixed text and needs a fixed
  `topic_fallback`. An optional `topic_values` allow-list limits the values. No fixed topic in the config may match the
  template read as a glob. Each value must be one segment (`[A-Za-z0-9_-]{1,64}`). These events go to the fallback,
  counted under `fallback`, so one bad row never stalls the pipeline:
  - null or missing values;
  - values that are not one segment or not in `topic_values`;
  - names that differ from an existing topic only by `.` vs `_` (Kafka would refuse them);
  - creates the broker refuses for good (invalid name, create policy, missing ACL); the name is remembered until a
    restart, so later rows with that value cause no broker call;
  - new topics beyond the new `[sink] max_dynamic_topics` (default 100; `0` means existing topics only), which is
    counted from the broker at startup, so it holds across restarts.

  Passing create errors still fail the batch and are retried. A content-routed template may not overlap a
  `{schema}`/`{table}` template. Topic names hold the row value, so they show in Kafka and in log lines about that
  topic. Without create rights, pre-create the topics and set `topic_values` or `max_dynamic_topics = 0`.

  Fallbacks are opened at startup. The inspector reads the content-routed topics, and `GET /api/routes` shows
  `topic_fallback` and `topic_values`. Once any route reads a row field, no topic in the config (route topics,
  `sink.topic`, fallbacks) may start with `_`; configs without content routing are unaffected.
