- **Postgres TLS defaults to `verify-full`** (#12, PR #14). With no `ssl_mode` and no `sslmode=` in the URL, Aqueduct
  now requires TLS and verifies the server certificate and hostname. A Postgres server without TLS fails at startup,
  **including on localhost** (verify-full never falls back to plaintext). To upgrade:
  - Production: enable TLS on the server and set `ssl_root_cert` if the CA is private.
  - Non-TLS dev Postgres: set `ssl_mode = "disable"` (or `sslmode=disable` in the URL) explicitly.
  - Plaintext to a non-loopback host is refused unless `ssl_mode = "disable"` is explicit, and then logs a warning.
