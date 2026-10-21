- **Admin token is accepted only as a Bearer header** (#11, PR #25). `?token=` is rejected on `/metrics` and `/api/*`
  with `401`, because URLs leak into proxy logs, browser history and Referer. Open the cockpit with
  `http://host:8080/#token=<token>` (the fragment is never sent to the server). Prometheus or other scrapers that used
  `/metrics?token=...` must send the header instead:

  ```yaml
  scrape_configs:
    - job_name: aqueduct
      metrics_path: /metrics
      authorization:
        type: Bearer
        credentials_file: /etc/prometheus/aqueduct-token   # or: credentials: <token>
      static_configs:
        - targets: ["aqueduct:8080"]
  ```

  `curl`: `curl -H "Authorization: Bearer $AQUEDUCT_ADMIN_TOKEN" http://host:8080/metrics`.
