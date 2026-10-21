# Azure managed identity

Passwordless sign-in to Azure Database for PostgreSQL (AKS Workload Identity, Container Apps). Short version: [DEPLOYMENT.md](../DEPLOYMENT.md).

On Azure Database for PostgreSQL (Flexible Server) Aqueduct can sign in with a managed identity instead of a stored
password: the server accepts a Microsoft Entra access token as the password, so no static database secret is stored
anywhere. Aqueduct fetches a token, caches it until 5 minutes before the
expiry the identity endpoint reports (at most 24 hours), and uses a valid one as the password of **every** connection
it opens: the SQL session and, with `transport = "stream"`, the replication connection. Each start of the source,
including a restart after an error, uses a cached or fresh token. A failed token fetch fails that connection like a
wrong password would (a source error; the restart applies).

```toml
[postgres]
# The user is the Postgres role of the managed identity (its name). No password.
url = "postgres://aqueduct-mi@myserver.postgres.database.azure.com:5432/app"
auth = "azure-workload-identity"   # or "azure-container-apps"
# azure_client_id = "00000000-0000-0000-0000-000000000000"
```

Fail-closed rules, checked at startup:

- The token only travels over TLS with the certificate chain **and hostname** verified: `ssl_mode = "verify-full"`
  (the default), with the bundled public roots or an explicit `ssl_root_cert`. Every other mode is refused, including
  `verify-ca` and `require` with `ssl_root_cert`: without the hostname check, anyone holding a certificate from a
  trusted CA could intercept the bearer token. The `*.postgres.database.azure.com` certificates chain to public roots
  (DigiCert Global Root G2, Microsoft RSA Root Certificate Authority 2017) that the bundled set should contain, so the
  default `verify-full` should work without `ssl_root_cert`; this was not verified against a real server. If the
  handshake reports an unknown issuer, download Azure's root certificates and point `ssl_root_cert` at them.
- A URL that also carries a password is refused, and so is a URL without a user.
- A missing platform variable is refused with its name. Tokens, the federated assertion and `IDENTITY_HEADER` are
  never logged and are redacted from debug output; a token endpoint error shows the HTTP status and the OAuth `error`
  and `error_description` only.
- The token endpoint is reached over HTTPS with certificate and hostname checks. Plain `http://` is accepted only for
  a loopback host (the Container Apps identity endpoint): a name must resolve to loopback addresses only, and
  Aqueduct connects to those checked addresses.

The server asks for the token as a cleartext password. That is the same rule as for any password: Aqueduct sends it
only inside TLS, and the replication connection keeps the [ADR 0003 §6](adr/0003-streaming-replication.md) rules
(cleartext is never sent over plaintext to a remote host).

Nothing in the image changes: the token exchange is a small built-in HTTPS client, so the distroless image needs no
shell, `az` CLI or extra files.

### AKS (Workload Identity)

`auth = "azure-workload-identity"`. The workload identity webhook injects `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
`AZURE_FEDERATED_TOKEN_FILE` and `AZURE_AUTHORITY_HOST`. Aqueduct re-reads the federated token file on every fetch
(the kubelet rotates it) and exchanges it at `{AZURE_AUTHORITY_HOST}{AZURE_TENANT_ID}/oauth2/v2.0/token` for a token
with scope `https://ossrdbms-aad.database.windows.net/.default`. `azure_client_id` overrides `AZURE_CLIENT_ID`.

1. Enable the OIDC issuer and workload identity on the cluster
   (`az aks update -g <rg> -n <cluster> --enable-oidc-issuer --enable-workload-identity`).
2. Create a user-assigned managed identity and a federated credential that trusts the service account:

   ```sh
   az identity create -g <rg> -n aqueduct-mi
   az identity federated-credential create -g <rg> --identity-name aqueduct-mi --name aqueduct \
     --issuer "$(az aks show -g <rg> -n <cluster> --query oidcIssuerProfile.issuerUrl -o tsv)" \
     --subject system:serviceaccount:<namespace>:aqueduct --audience api://AzureADTokenExchange
   ```

3. Annotate the service account with the identity's client ID and label the pod:

   ```yaml
   apiVersion: v1
   kind: ServiceAccount
   metadata:
     name: aqueduct
     namespace: <namespace>
     annotations:
       azure.workload.identity/client-id: "<client ID of aqueduct-mi>"
   ---
   # in the Deployment
   spec:
     template:
       metadata:
         labels:
           azure.workload.identity/use: "true"
       spec:
         serviceAccountName: aqueduct
   ```

### Azure Container Apps

`auth = "azure-container-apps"`. Give the app a system-assigned identity (`az containerapp identity assign -g <rg>
-n <app> --system-assigned`) or a user-assigned one (`--user-assigned <identity resource ID>`, then set
`azure_client_id` to its client ID). Container Apps sets `IDENTITY_ENDPOINT` and `IDENTITY_HEADER`; Aqueduct calls
`$IDENTITY_ENDPOINT?api-version=2019-08-01&resource=https://ossrdbms-aad.database.windows.net` (plus `&client_id=`)
with the `X-IDENTITY-HEADER` header.

### Postgres side

1. Turn on Microsoft Entra authentication on the server and set an Entra admin.
2. As the Entra admin (signed in with an Entra token, connected to the `postgres` database), create the role for the
   identity. The role name is the identity's name: for a user-assigned identity its resource name, for a
   system-assigned one the name of the app. It is also the user in Aqueduct's `url`.

   ```sql
   select * from pgaadauth_create_principal('aqueduct-mi', false, false);
   ```

3. Grant what Aqueduct needs. **`REPLICATION` must be granted to the Entra role itself**, as for a password role.
   (Granting `REPLICATION` to a role made by `pgaadauth_create_principal` with `ALTER ROLE` has not been checked
   against Microsoft's documentation or a real server; if it is refused, see Azure's logical replication docs for
   the supported way to grant it.)

   ```sql
   ALTER ROLE "aqueduct-mi" WITH REPLICATION;
   -- in the replicated database:
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO "aqueduct-mi";
   CREATE PUBLICATION aqueduct FOR TABLE ...;   -- or let its owner create it
   ```

   The role creates and advances its slot (`REPLICATION`) and reads the published tables (`SELECT`, also needed for
   snapshots); snapshots on Postgres 14+ also need `EXECUTE` on `pg_logical_emit_message`. Set the server parameter
   `wal_level = logical` as in [Prepare Postgres](../DEPLOYMENT.md#1-prepare-postgres).
