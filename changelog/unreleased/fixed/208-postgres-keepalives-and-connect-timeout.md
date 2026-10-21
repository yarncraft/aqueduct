- **Postgres: a vanished server fails the source in about a minute instead of hanging for hours** (#208). Connections
  now default to `connect_timeout=30` and TCP keepalives (30 s idle, 10 s interval, 3 retries) unless the URL sets its
  own, so after a failover or a dropped NAT flow the source errors and the container restarts. No action needed.
