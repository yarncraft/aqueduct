- **The Kafka sink produces several batches at once** (#250). `[sink] in_flight_batches` (default `4`, `1` to `16`)
  batches are in flight together, so the broker round trip no longer caps throughput (bench, six partitions, ~4 KB
  events: 26 to 116 MB/s). Each partition has one lane with one produce request outstanding, which writes the batches
  in order, and a batch is checkpointed only once it and every earlier batch are acknowledged, so per-partition order
  and at-least-once are unchanged; a crash can replay up to that many batches. A failed request stops its partition
  and the process exits as before. The cockpit's sink latency is now each batch's time from submit to
  acknowledgement. Set `in_flight_batches = 1` for the old one-batch-at-a-time behaviour. See
  `docs/adr/0004-pipelined-kafka-produce.md`.
