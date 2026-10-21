- **A batch too large for one Kafka request no longer blocks the pipeline for good** (#0). The sink sent all of a
  batch's records for one partition in a single produce request; up to 1000 events (2000 records with tombstones) of
  large rows could exceed the broker's `message.max.bytes`, and the retry rebuilt the same refused request forever. Each
  partition's records now go out in consecutive requests of at most about 900 KiB, in order. A single record above
  that still goes alone, so raise `message.max.bytes` if your rows are that large.
