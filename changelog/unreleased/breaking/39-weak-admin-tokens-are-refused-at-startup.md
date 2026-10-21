- **Weak admin tokens are refused at startup** (#39). A non-empty `admin_token` (or `AQUEDUCT_ADMIN_TOKEN`) must be at
  least 16 characters, must not be whitespace-only or have leading/trailing whitespace, must not contain `${` (an
  unexpanded compose or shell placeholder would otherwise become the token), and must not be the published
  placeholders `dev-token-change-me` or `change-me` (case-insensitive). This applies on loopback binds too. An empty value still means "unset". To
  upgrade, replace short tokens such as `change-me` with a random one: `openssl rand -hex 32`.
