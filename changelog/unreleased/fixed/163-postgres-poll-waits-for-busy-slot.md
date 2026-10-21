- **Postgres (poll mode): a restart no longer fails while the old connection still holds the slot** (#163). The peek and
  the slot advance now wait up to 15 s when the server reports the slot as active (55006), as stream mode already did,
  instead of failing the pipeline and restarting the container until the old backend ended. No action needed.
