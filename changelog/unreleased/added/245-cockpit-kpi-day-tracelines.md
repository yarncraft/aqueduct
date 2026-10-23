- **Cockpit: a 24 h trace line under each top tile** (#245). Throughput, events delivered, end-to-end lag, sink
  latency and errors each get a line of the last day, one mean per minute (lag over the seconds it is not `idle`, sink
  latency per event delivered in the minute). The server keeps the minutes in memory, so the lines are full when the
  page opens and start over on restart; new `GET /api/kpis/history` (needs the admin token).
