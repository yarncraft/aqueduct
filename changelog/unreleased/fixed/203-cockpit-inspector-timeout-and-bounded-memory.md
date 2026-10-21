- **Cockpit: a stalled Kafka broker no longer queues every event inspector request** (#203). `/api/events` waited on Kafka
  without a timeout while holding a lock, so each later request waited behind it; it now answers `504` after 10 s and
  holds no lock while Kafka is read. The inspector also keeps only the newest `limit` events while it reads, instead of
  parsing `limit` records from every partition of every topic first, so its memory no longer grows with the partition
  count. No action needed.
