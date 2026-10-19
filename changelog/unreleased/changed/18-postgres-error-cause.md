- **Postgres source errors now include the cause** (#18). A failed connect previously showed only
  `error performing TLS handshake`; it now reads e.g. `...: invalid peer certificate: UnknownIssuer`. This text appears in
  logs and the cockpit's `last_error`. Server-side errors add only the SQLSTATE and message, never `DETAIL`/`HINT`.
