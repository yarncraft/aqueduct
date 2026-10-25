- **Metrics and logs can be pushed to an OpenTelemetry Collector over OTLP** (#249). Off by default. Set
  `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318` (or `[telemetry] otlp_endpoint`) and the `aqueduct_*` series
  of `/metrics` (same names, labels and values, read from the same counters) and the log events go out over OTLP/HTTP
  with protobuf; `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_EXPORTER_OTLP_HEADERS` and the other standard
  variables apply. `/metrics` and the JSON logs on stdout are unchanged. A collector that is down is logged once and its
  data dropped; the pipeline never waits for it. Only `http/protobuf` is supported (not gRPC), and traces are not
  exported yet. See `docs/DEPLOYMENT.md`, "OpenTelemetry (OTLP)".
