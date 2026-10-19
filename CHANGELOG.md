# Changelog

Notable changes to Aqueduct. Newest first.

## Unreleased

### Breaking changes and upgrade notes

- **Postgres TLS defaults to `verify-full`** (#12, PR #14). With no `ssl_mode` and no `sslmode=` in the URL, Aqueduct
  now requires TLS and verifies the server certificate and hostname. A Postgres server without TLS fails at startup,
  **including on localhost** (verify-full never falls back to plaintext). To upgrade:
  - Production: enable TLS on the server and set `ssl_root_cert` if the CA is private.
  - Non-TLS dev Postgres: set `ssl_mode = "disable"` (or `sslmode=disable` in the URL) explicitly.
  - Plaintext to a non-loopback host is refused unless `ssl_mode = "disable"` is explicit, and then logs a warning.
- **Admin server fails closed** (#10, PR #15). With a non-loopback `health_addr` (the default `0.0.0.0:8080`), startup
  is refused unless `admin_token` (or `AQUEDUCT_ADMIN_TOKEN`) is set to a non-empty value. To upgrade, set a token, bind
  `health_addr` to `127.0.0.1`, or set `admin_insecure_no_auth = true` (logged as a warning; not for production).
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
- **Compose files no longer default the admin token** (#19). `docker-compose.yml` and `docker-compose.sim.yml` used to
  fall back to the publicly known `dev-token-change-me`. `docker compose up` now fails with
  `set AQUEDUCT_ADMIN_TOKEN to a secret value` unless `AQUEDUCT_ADMIN_TOKEN` is set. To upgrade, export one (or put it in
  a `.env` file next to the compose file):

  ```sh
  export AQUEDUCT_ADMIN_TOKEN=$(openssl rand -hex 32)
  docker compose up --build
  ```

  Open the cockpit with `http://localhost:8080/#token=$AQUEDUCT_ADMIN_TOKEN`. Existing deployments that relied on the
  default token should rotate it, because it was public.

- **Weak admin tokens are refused at startup** (#39). A non-empty `admin_token` (or `AQUEDUCT_ADMIN_TOKEN`) must be at
  least 16 characters, must not be whitespace-only or have leading/trailing whitespace, must not contain `${` (an
  unexpanded compose or shell placeholder would otherwise become the token), and must not be the published
  placeholders `dev-token-change-me` or `change-me` (case-insensitive). This applies on loopback binds too. An empty value still means "unset". To
  upgrade, replace short tokens such as `change-me` with a random one: `openssl rand -hex 32`.

### Changed

- **Postgres source errors now include the cause** (#18). A failed connect previously showed only
  `error performing TLS handshake`; it now reads e.g. `...: invalid peer certificate: UnknownIssuer`. This text appears in
  logs and the cockpit's `last_error`. Server-side errors add only the SQLSTATE and message, never `DETAIL`/`HINT`.

Details: [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) (config reference and the "Securing connections" checklist).
