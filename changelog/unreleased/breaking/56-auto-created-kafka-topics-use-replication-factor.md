- **Auto-created Kafka topics use replication factor 3** (#56). They used to be created with factor 1, so one broker
  loss meant data loss. New `replication_factor` under `[sink]` sets it explicitly. Unset, Aqueduct tries 3 and, if the
  cluster has fewer than 3 brokers, creates the topic with 1 and logs a warning. Topics that already exist are never
  changed; raise their factor with Kafka's partition-reassignment tooling.
