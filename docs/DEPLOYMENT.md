# Deployment guide

Aqueduct runs only as a container. This guide covers preparing Postgres, running the image and operating it.
Settings are in [CONFIGURATION.md](CONFIGURATION.md), the web UI in [COCKPIT.md](COCKPIT.md), failure handling in [RUNBOOK.md](RUNBOOK.md).

## How it works

```
Postgres (logical slot, pgoutput) --> Aqueduct --> Kafka
                                          |
                                   checkpoint file  +  admin cockpit (:8080)
```

1. Aqueduct opens a plain SQL connection and creates a logical replication slot (`pgoutput`) if it does not exist.
2. It polls the slot (every 500 ms when idle) for whole committed transactions and decodes them into row-level events.
   With `transport = "stream"` it instead receives them over a replication connection as they commit; see
   [Streaming transport](deep-dive/postgres-operations.md#streaming-transport).
3. Each event is produced to Kafka. Only after the sink succeeds does Aqueduct write the checkpoint and advance the slot to that transaction's commit LSN.
4. On restart it resumes from the slot and the checkpoint. A crash between "sink succeeded" and "checkpoint written" replays at most that event, so **delivery is at-least-once**: consumers must tolerate duplicates (use `lsn` plus the row key to deduplicate).

Limits to know about:

- Only changes made after the slot is created are streamed. Rows that already exist are copied only by a
  [snapshot](deep-dive/snapshots.md#snapshots) (off by default).
- One Aqueduct instance per slot. Run a second instance with a different `slot` name, never two on one slot.
- Postgres connections use TLS with certificate and hostname verification (`ssl_mode = "verify-full"`) by default, so a server without TLS fails at startup, **including on localhost**. Plaintext needs an explicit `ssl_mode = "disable"` (non-loopback plaintext then logs a warning).
- DDL is not streamed. Add a new table to the publication, and expect `before` images only if the table's `REPLICA IDENTITY` allows it.

## 1. Prepare Postgres

Postgres 12 or newer (tested on 12, 13, 14, 16 and 17). Postgres 10 has no `pg_replication_slot_advance`, and on 11
repeated slot peeks over one connection fail with `out of relcache_callback_list slots`, so neither works. Snapshots
use logical messages on 14 or newer, or a [watermark table](deep-dive/snapshots.md#watermark-table-postgres-12-and-13) on 12 and 13.

```conf
# postgresql.conf (restart required)
wal_level = logical
max_replication_slots = 4      # at least one per Aqueduct instance
max_wal_senders = 4
```

Create a role, a publication and set the replica identity:

```sql
CREATE ROLE aqueduct LOGIN PASSWORD '...' REPLICATION;
GRANT USAGE ON SCHEMA public TO aqueduct;

CREATE PUBLICATION aqueduct FOR TABLE public.users, public.orders;
-- or: CREATE PUBLICATION aqueduct FOR ALL TABLES;

-- Updates and deletes carry the old row only if the table has a replica identity:
ALTER TABLE public.orders REPLICA IDENTITY FULL;   -- full "before" image
-- default identity (primary key) gives only the key columns in "before"
```

Tables without a primary key and with the default identity reject `UPDATE`/`DELETE` once they are published; set `REPLICA IDENTITY FULL` or add a key. Records are keyed by the primary key (see [Kafka sink](deep-dive/configuration.md#kafka-sink)), so give tables one: a table without a
primary key has a null key and all its rows share one partition.

The publication must exist before Aqueduct starts. The slot is created automatically.

## 2. Run the container

### Docker Compose (reference)

`docker-compose.yml` in the repository starts Postgres 16 with logical replication and Aqueduct:

```sh
docker compose up --build              # postgres + kafka + aqueduct
```

Podman works too (`podman-compose up -d --build`); build with `--format docker` if you want the image `HEALTHCHECK` kept.

### Plain `docker run`

Prebuilt images are on GitHub Packages, published by `.github/workflows/image.yml` (linux/amd64):

| Tag | Built from |
| --- | --- |
| `ghcr.io/yarncraft/aqueduct:latest`, `:main` | the head of `main` |
| `ghcr.io/yarncraft/aqueduct:sha-<short>` | that commit |
| `ghcr.io/yarncraft/aqueduct:<version>`, `:<major>.<minor>` | a `v<version>` tag |

Pin a `sha-` or version tag in production. To build locally instead, `docker build -t aqueduct .` and use `aqueduct`
as the image name below.

```sh
docker run -d --name aqueduct --restart unless-stopped \
  -e AQUEDUCT_PG_URL='postgres://aqueduct:SECRET@db.internal:5432/app' \
  -e AQUEDUCT_ADMIN_TOKEN='long-random-string' \
  -v aqueduct-state:/var/lib/aqueduct \
  -v ./my-aqueduct.toml:/etc/aqueduct/aqueduct.toml:ro \
  -p 127.0.0.1:8080:8080 \
  ghcr.io/yarncraft/aqueduct:latest
```

### Kubernetes (sketch)

Run a single replica (`strategy: Recreate`, never HPA) with a `PersistentVolumeClaim` on `/var/lib/aqueduct`,
the TOML in a `ConfigMap` mounted at `/etc/aqueduct/aqueduct.toml`, `AQUEDUCT_PG_URL` and `AQUEDUCT_ADMIN_TOKEN`
from a `Secret`, and an HTTP probe on `/healthz`. Use `fsGroup: 65532` if your storage class ignores image ownership.

### Image facts

- Distroless `cc-debian12`, runs as non-root uid 65532, no shell.
- State directory `/var/lib/aqueduct` (checkpoint) must be on a volume.
- Port 8080: cockpit, API and health probe. `HEALTHCHECK` runs `aqueduct --healthcheck`, which calls `/healthz` on the
  port of `health_addr`, read from the same config file (`AQUEDUCT_CONFIG`): a wildcard bind (`0.0.0.0`, `[::]`) is
  probed on `127.0.0.1` / `[::1]`, any other address as is; without a readable config file it probes `127.0.0.1:8080`.
  `AQUEDUCT_HEALTH_PROBE=<host:port>` overrides the address. It prints nothing; only the exit code counts.
- Logs are JSON lines on stdout/stderr (`RUST_LOG=debug` for more detail).

## 3. Operating Aqueduct

- **WAL retention.** An unacknowledged slot makes Postgres keep WAL. If Aqueduct is down for long, or the sink is stuck,
  disk usage grows. Monitor `pg_replication_slots` (`confirmed_flush_lsn`, `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`)
  and alert on `max_slot_wal_keep_size` pressure. Set `max_slot_wal_keep_size` on Postgres 13+ as a safety valve.
- **Idle databases.** Writes to tables outside the publication also produce WAL. The slot is moved forward for them by the
  [idle heartbeat](deep-dive/postgres-operations.md#idle-heartbeat), on by default.
- **Decommissioning.** Drop the slot or it will keep WAL forever: `SELECT pg_drop_replication_slot('aqueduct');`
- **Restarts and upgrades.** Stop the container, deploy the new image with the same volume, start. It resumes from the slot.
  Before an upgrade, and to roll back, see [UPGRADING.md](UPGRADING.md).
  Do not delete the state volume; losing the checkpoint only causes replay (the slot still decides what is pending).
- **Sink outages.** The pipeline stops on a sink failure and the container exits non-zero; `restart: unless-stopped`
  (or your orchestrator) brings it back and delivery resumes from the last acknowledged transaction.
- **Changing the publication or sink.** Edit the config and restart. Changing `slot` starts a new stream from "now".
- **Health.** `GET /healthz` returns `200 ok` while the process is up. Use the cockpit's state and error count (`/api/metrics`) for deeper checks.

### Going deeper

| Topic | Summary |
|-------|---------|
| [Idle heartbeat](deep-dive/postgres-operations.md#idle-heartbeat) | The slot is advanced while idle so unpublished writes don't pin WAL. On by default |
| [Checkpoint origin check](deep-dive/postgres-operations.md#checkpoint-origin-check) | Startup refuses a checkpoint from another cluster or database |
| [Streaming transport](deep-dive/postgres-operations.md#streaming-transport) | `transport = "stream"`: changes arrive as they commit instead of being polled |
| [Azure managed identity](deep-dive/azure.md) | Passwordless sign-in on AKS and Container Apps |
| [Snapshots](deep-dive/snapshots.md) | Copy rows that already exist, at start or on request (Postgres and MongoDB) |

## 4. Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| `invalid connection string` | `replication=database` in `postgres.url`; remove it |
| `publication "x" does not exist` | Create the publication before starting |
| `logical decoding requires wal_level >= logical` | Set `wal_level = logical` and restart Postgres |
| `permission denied to use replication slots` | The role needs `REPLICATION` (or superuser). With a managed identity: `ALTER ROLE "<identity name>" WITH REPLICATION` |
| `getting a Microsoft Entra token (...): ... HTTP 400; error: invalid_client` | AKS: often `AADSTS70021`, the federated credential's issuer or subject does not match the cluster's OIDC issuer and `system:serviceaccount:<namespace>:<name>` |
| `postgres auth = "azure-...": ... is not set` | AKS: the pod lacks the `azure.workload.identity/use: "true"` label or the service account annotation. Container Apps: the app has no managed identity |
| `password authentication failed` with an `azure-*` `auth` | The URL user is not the identity's role name, or the role was not created with `pgaadauth_create_principal` |
| `checkpoint error: Permission denied` | State volume not writable by uid 65532; use the provided Dockerfile or `chown 65532` the volume |
| `cannot update table ... does not have a replica identity` | Set a primary key or `REPLICA IDENTITY FULL` |
| Kafka: `InvalidTopicException` | A topic name with illegal characters; check route `topic` values and templates |
| Same transaction delivered twice after a crash | Expected with at-least-once delivery; deduplicate downstream |

## Cockpit over TLS

The cockpit, `/api/*` and `/metrics` are served over **plain HTTP**; Aqueduct has no native TLS. The `Authorization: Bearer`
token and the rows shown by the event inspector therefore travel in clear text on any hop that is not protected. Either:

- bind to loopback (`health_addr = "127.0.0.1:8080"`) and reach it through `ssh -L`, `kubectl port-forward` or a sidecar; or
- keep the listener private and put a TLS-terminating reverse proxy in front. Never publish port 8080 directly.

A non-loopback `health_addr` logs a startup warning as a reminder. Inside a container the bind is normally `0.0.0.0`, so
publish the port only to the proxy (a private Docker network, or `127.0.0.1:8080:8080` on the host). Examples:

```caddy
# Caddyfile: automatic certificates
aqueduct.example.com {
    reverse_proxy aqueduct:8080
}
```

```nginx
server {
    listen 443 ssl;
    server_name aqueduct.example.com;
    ssl_certificate     /etc/ssl/aqueduct.crt;
    ssl_certificate_key /etc/ssl/aqueduct.key;

    location / {
        proxy_pass http://aqueduct:8080;
        proxy_http_version 1.1;
        proxy_buffering off;  # /api/stream is Server-Sent Events
        proxy_set_header Host $host;
        proxy_read_timeout 1h;
    }
}
```

Open the cockpit as `https://aqueduct.example.com/#token=...`. Restrict the proxy to trusted networks as well; the token is the only login.

## Securing connections (checklist)

1. Postgres: `ssl_mode = "verify-full"` and `ssl_root_cert` pointing at your CA; create the Aqueduct role with a strong password and only `REPLICATION` + `SELECT` on the published tables. On Azure, a managed identity (`auth`, see [Azure managed identity](deep-dive/azure.md#azure-managed-identity)) needs no stored password at all.
2. Kafka: `tls = true`, SASL `scram-sha-512`, and ACLs limiting the principal to `Write`/`Describe`/`Create` on the target topics.
3. Cockpit: set `admin_token` (startup refuses a non-loopback `health_addr` without one) and [terminate TLS in front of it](#cockpit-over-tls); never set `admin_insecure_no_auth` in production.
4. Pass credentials through the environment (`AQUEDUCT_PG_URL`, `AQUEDUCT_KAFKA_SASL_PASSWORD`, `AQUEDUCT_ADMIN_TOKEN`, `AQUEDUCT_HASH_KEY`) or orchestrator secrets instead of committing them to the TOML file. Secrets in the config are redacted from debug output.

## Always-on simulation

`docker-compose.sim.yml` runs Postgres, Kafka, Aqueduct and a workload generator (`sim/simulate.sh`) that
keeps inserting, updating and deleting rows in the industrial schema (`organizations`, `sites`, `assets`, `asset_readings`, `work_orders`, `maintenance_events`) forever (tables are capped
at roughly 2000 recent rows; Kafka retention is capped at 256 MiB / 24 h).

**Upgrading from the old users/orders/payments sim:** run `podman-compose -f docker-compose.sim.yml -p aqsim down -v` (or `docker compose -f docker-compose.sim.yml -p aqsim down -v`) once first; a bare `down -v` from the repo root targets the regular compose file and deletes your dev Postgres volume. Postgres only runs `sim/init.sql` on an empty volume, so an old volume keeps the old schema, and the simulator then fails every transaction silently (the cockpit just looks idle).

```sh
export AQUEDUCT_ADMIN_TOKEN=$(openssl rand -hex 32)   # required, no default
podman-compose -f docker-compose.sim.yml -p aqsim up -d --build   # or: docker compose -f ... -p aqsim up -d --build
# UI: http://localhost:8088/#token=$AQUEDUCT_ADMIN_TOKEN
SIM_RATE=40 podman-compose -f docker-compose.sim.yml -p aqsim up -d simulator   # change load (tx/s)
podman-compose -f docker-compose.sim.yml -p aqsim down      # add -v to wipe data
```

All services use `restart: unless-stopped`. It publishes only port 8088, so it does not clash with the main compose file (8080).
