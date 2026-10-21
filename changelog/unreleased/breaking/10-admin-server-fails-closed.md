- **Admin server fails closed** (#10, PR #15). With a non-loopback `health_addr` (the default `0.0.0.0:8080`), startup
  is refused unless `admin_token` (or `AQUEDUCT_ADMIN_TOKEN`) is set to a non-empty value. To upgrade, set a token, bind
  `health_addr` to `127.0.0.1`, or set `admin_insecure_no_auth = true` (logged as a warning; not for production).
