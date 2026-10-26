- **Batches waiting on Kafka no longer hold their rows** (#258). With several batches in flight, the pipeline kept every
  event of each, row payloads included, until Kafka acknowledged it, only to update the metrics then. It now keeps a
  per-table summary and frees the events once the sink has encoded them, so memory no longer grows with
  `in_flight` × the batch's row size twice over. Metrics are unchanged. No action needed.
