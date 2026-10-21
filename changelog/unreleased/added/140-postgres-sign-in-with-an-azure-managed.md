- **Postgres: sign in with an Azure managed identity** (#140). New `[postgres]` keys `auth` (`password` by default,
  `azure-workload-identity` for AKS Workload Identity, `azure-container-apps`) and `azure_client_id`. A Microsoft
  Entra token, cached until 5 minutes before it expires, is the password of every connection (SQL and replication),
  so no static database secret is needed; every start, including a restart after an error, uses a valid token.
  Startup refuses an `azure-*` mode with any `ssl_mode` but `verify-full` (the hostname must be checked), with a
  password in the URL, or without the platform's environment variables. The image is unchanged. See DEPLOYMENT.md,
  "Azure managed identity".
