- **`aqueduct --healthcheck` probes the configured `health_addr`** (#203). It always probed `127.0.0.1:8080`, so with
  another port (for example `health_addr = "0.0.0.0:9000"`) the image `HEALTHCHECK` failed forever. It now reads
  `health_addr` from the config file (a wildcard bind is probed on loopback) and falls back to `127.0.0.1:8080`;
  `AQUEDUCT_HEALTH_PROBE` still overrides it. No action needed.
