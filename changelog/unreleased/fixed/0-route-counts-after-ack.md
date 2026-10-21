- **Route counters no longer count a retried batch twice** (#0). `aqueduct_route_records_total`,
  `aqueduct_route_tombstones_skipped_total` and the cockpit's route counts were updated when a record was built, before
  Kafka acknowledged it, so a failed batch the pipeline retried was counted again. They now count a batch only once the
  broker acknowledged it, like the per-topic counts.
