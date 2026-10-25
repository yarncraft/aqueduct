- **Cockpit: CPU and memory tiles replace Sink latency and Errors** (#254). The top row's last two tiles show the
  Aqueduct process's CPU (percent of one core, averaged over 15 s, with the CPU time per event delivered) and resident
  memory, each with its 5-minute line. CPU and memory come from `/proc/self`, so they read `–` off Linux. The last
  fault keeps its panel. `/api/kpis/history` and the stream frames' `kpis` carry `cpu_pct` and `mem_mb` instead of
  `sink_ms` and `errors`. To upgrade, read sink latency and the error count from `/metrics`
  (`aqueduct_sink_latency_seconds`, `aqueduct_errors_total`) or `/api/metrics` (`sink_avg_ms`, `errors`).
