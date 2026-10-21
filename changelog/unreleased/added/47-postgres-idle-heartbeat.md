- **Postgres idle heartbeat** (#47). Writes to tables outside the publication no longer pin WAL: every `heartbeat_interval_secs`
  (default 10, `0` disables) an idle source advances the slot to the server's WAL position, never past an event that is
  not yet checkpointed. Optional `heartbeat_action_query` for servers that need a write to move the slot. No action needed
  on upgrade; see DEPLOYMENT.md for the heartbeat-table recipe.
