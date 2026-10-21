- **Cockpit: a flood of idle connections no longer fails the health probe** (#203). All 64 connection slots could be held
  by connections that never sent a request, each for 5 s, so `/healthz` went unanswered and the orchestrator restarted
  the container. A request head must now arrive within 2 s, and once the slots are full a small reserve (part of it only
  for loopback peers, such as the image `HEALTHCHECK`) still answers `/healthz` and `/readyz`. No action needed.
