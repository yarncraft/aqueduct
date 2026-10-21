- **MongoDB: a restart soon after the first start no longer skips changes** (#77). Without a checkpoint the change
  stream opens at "now", but nothing saved that position until the first event or 30 s heartbeat, so a restart before
  then opened at a later "now" and lost the changes in between. The opening position is now checkpointed before
  anything else is read. No config change.
