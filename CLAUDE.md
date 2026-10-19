# CLAUDE.md

Guidance for Claude Code and other agents working in this repository.

## What this is

Aqueduct: CDC in Rust. Sources are Postgres (logical replication, `pgoutput`) and MongoDB, picked by `[source] type`; Kafka is the only sink. **Container-only**: it must always run in
a container (Docker or Podman). Keep every feature working inside the distroless, non-root image.

## Crates

| Crate | Role |
|-------|------|
| `aqueduct-core` | `ChangeEvent`/`Position` (`Lsn(u64)` or `Text`)/`Op`, `Source`/`Sink`/`CheckpointStore` traits, `run_pipeline`, `Metrics` |
| `aqueduct-source-postgres` | Pure pgoutput decoder (`decoder.rs`) and the polling `Source` (`source.rs`) |
| `aqueduct-source-mongodb` | MongoDB change-stream source; resume token as the `Position` |
| `aqueduct-sinks` | Kafka sink and the Kafka-backed event inspector reader (`rskafka`, pure Rust). Kafka is the only sink |
| `aqueduct` | Binary: TOML config, file checkpoint, admin HTTP server (`admin.rs`, `admin.html`, `admin.css`) |

## Commands

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace            # always run the whole workspace, not one crate
docker compose up --build         # or: podman-compose up -d --build
```

CI (`.github/workflows/ci.yml`) runs fmt, clippy with `-D warnings`, tests and a docker build.

On the Windows dev machine: `RUSTUP_HOME=S:\rust\rustup`, `CARGO_HOME=S:\rust\cargo`, Podman in `S:\podman\bin`.

## Invariants (do not break)

- **At-least-once**: the pipeline checkpoints and acks the source only after `sink.send` and `flush` succeed.
  Sinks must tolerate redelivery; never ack earlier for speed.
- **Positions**: sources own their `Position` encoding. The event JSON field is still `lsn`, and text positions are checkpointed as `~<text>`. Adding a source = new crate, a `SourceConfig` variant in `config.rs` and a match arm in `main.rs`. SQL Server, Redis and the Kafka source were dropped on purpose.
- **Slot advance**: the source advances the replication slot only to a transaction's commit-end LSN.
  Acking a mid-transaction LSN does not move `confirmed_flush_lsn` and replays the transaction.
- Postgres URLs are plain SQL connections. Do not add `replication=database`; tokio-postgres rejects it.
- Kafka: record key is `schema.table`, partition is a stable FNV-1a hash of the key (per-table ordering).
- Rows are sensitive. The event inspector exposes row data over HTTP; keep it behind `admin_token`
  and never log tokens.
- **Fail closed**: the admin server refuses a non-loopback `health_addr` without `admin_token` (`Config::check_admin_exposure`,
  opt-out `admin_insecure_no_auth`). Postgres TLS defaults to `verify-full`, and a plaintext session to a non-loopback
  host aborts startup unless `ssl_mode = "disable"` is explicit (`aqueduct-source-postgres/src/tls.rs`). The admin token is accepted only as `Authorization: Bearer`; never
  reintroduce a `?token=` fallback (URLs leak into logs). Do not weaken these defaults.
- The runtime image is distroless: no shell, no curl. The healthcheck is `aqueduct --healthcheck` (GET `/healthz`).
- The state dir `/var/lib/aqueduct` is pre-created for uid 65532 in the Dockerfile; keep that when editing it.

## Admin UI

`crates/aqueduct/src/admin.html` is a single page styled with Tailwind v4 utility classes. `admin.css` is
generated and committed so the image build needs no Node. After editing the HTML:

```sh
TAILWIND=/path/to/tailwindcss crates/aqueduct/ui/build.sh
```

Class names built in JS must appear as literals in `admin.html`, or Tailwind will not generate them.
The server is a hand-written tokio HTTP loop (no framework); add routes in `admin.rs::handle` and cover them in its tests.

## Conventions

- Keep README.md and `docs/DEPLOYMENT.md` (config reference, operations) current in the same change as any user-visible behaviour: config keys, routes, sinks, ops notes. Breaking changes also get an entry under `## Unreleased` in `CHANGELOG.md` with the upgrade step.
- Kafka is the only sink by design; do not add others. The inspector reads from Kafka, never from an in-process buffer.
- Match the surrounding style; no unrelated refactors. Commit messages end with the repo's `Co-Authored-By` line when an agent authored them.
- Source files may have mixed CRLF/LF line endings (Windows checkout). Normalise before scripted edits.
- Git push to the Buzz relay needs a working `git-credential-nostr`; if it prints no credential, the push cannot succeed from that session.
