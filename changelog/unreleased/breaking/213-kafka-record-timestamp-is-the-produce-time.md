- **Kafka: the record timestamp is now the produce time, not the commit time (#213).**
  A replayed backlog was stamped with its old commit times, so on `CreateTime` topics retention could delete it
  at once. Like Debezium, the sink now stamps the time it produces the record. Any consumer that read the record
  timestamp as the commit time sees a different value.
  - The commit time is always in `source.ts_ms` of the event.
  - Set `[sink] record_timestamp = "commit"` to restore the old behaviour.
  - The cockpit's `/api/events` and its time column show the record timestamp, so they now show the produce time.
