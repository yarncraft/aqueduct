- **Cockpit TLS guidance and startup warning** (#217). The admin server speaks plain HTTP, so `admin_token` and inspector rows
  are readable on the wire. A non-loopback `health_addr` now logs a warning, and `docs/DEPLOYMENT.md` explains how to put a
  TLS proxy (Caddy and nginx examples) in front or bind to loopback. No configuration change.
