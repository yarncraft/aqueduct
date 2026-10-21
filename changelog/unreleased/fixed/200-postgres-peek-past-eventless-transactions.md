- **Postgres (poll mode): the source no longer falls behind on transactions that produce no event** (#200). Empty transactions
  (Postgres 14 and older send one for every write to an unpublished table), other tools' logical messages and
  watermark-only transactions only let the slot move at the next ack or heartbeat. Until then each poll re-read the same
  `batch_size` rows, so a busy server moved one batch per heartbeat, and with `heartbeat_interval_secs = 0` the source
  stopped for good. The slot now moves past them after every poll. No action needed.
