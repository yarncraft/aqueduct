- **Route plan inside the Kafka sink** (#79). Routing now compiles once into a `RoutePlan` with a pure `render` step;
  output (topic, key, partition, value, headers) is unchanged, which a golden test pins. `[[sink.routes]]` accepts an
  optional unique `name` that labels the route in the per-route counters (`matched`, `produced`; shown in a later
  change). `aqueduct-bench --sink render` measures the render cost without a broker.
