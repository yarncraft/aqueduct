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
| `aqueduct-tls` | Shared rustls client config (ring, no OpenSSL) used by the Postgres source and the Kafka sink |
| `aqueduct` | Binary: TOML config, file checkpoint, admin HTTP server (`admin.rs`, `admin.html`, `admin.css`) |
| `aqueduct-bench` | Throughput benchmark of the pipeline (null or Kafka sink) |

The README `## Architecture` section has Mermaid diagrams of the components, the at-least-once loop and the crate graph.

## Commands

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace            # always run the whole workspace, not one crate
docker compose up --build         # or: podman-compose up -d --build
```

**There is no CI**, local or remote. Before you post a SHA for review, run these yourself (in podman on the Windows machine) and paste the commands and their `test result` lines on the PR:

- Every PR: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace --locked`.
- PRs that touch a source, the Kafka sink or the pipeline: the integration suite too, `cargo test --locked -p aqueduct-source-mongodb -p aqueduct-source-postgres -- --ignored` and `cargo test --locked -p aqueduct-sinks --test kafka_it`, against local Kafka, MongoDB and Postgres containers (`AQUEDUCT_TEST_KAFKA`, `AQUEDUCT_KAFKA_BROKERS`, `AQUEDUCT_TEST_PG_*`). The passed count must be non-zero; a test that skips itself when its variable is unset does not count.
- PRs that change `Cargo.lock`: `cargo audit`.
- PRs that change the `Dockerfile` or the image: `podman build .`.

On the Windows dev machine: `RUSTUP_HOME=S:\rust\rustup`, `CARGO_HOME=S:\rust\cargo`, Podman in `S:\podman\bin`.

## Invariants (do not break)

- **At-least-once**: the pipeline checkpoints and acks the source only after `sink.send` and `flush` succeed.
  Sinks must tolerate redelivery; never ack earlier for speed.
- **Positions**: sources own their `Position` encoding. The event JSON field is still `lsn`, and text positions are checkpointed as `~<text>`. Adding a source = new crate, a `SourceConfig` variant in `config.rs` and a match arm in `main.rs`. SQL Server, Redis and the Kafka source were dropped on purpose.
- **Slot advance**: the source advances the replication slot only to a transaction's commit-end LSN.
  Acking a mid-transaction LSN does not move `confirmed_flush_lsn` and replays the transaction.
- Postgres URLs are plain SQL connections. Do not add `replication=database`; tokio-postgres rejects it.
- Kafka: record key is `schema.table` (default `partition_by = "table"`, per-table ordering) or `schema.table:<identity>`
  (`partition_by = "key"`); partition is a stable FNV-1a hash of the key (`kafka.rs::record_key`). The identity part is the
  replica-identity columns joined with `0x1f`, binary datums hex (`decoder.rs::key`), or the MongoDB `documentKey` JSON; under `REPLICA IDENTITY FULL` `decoder.rs::key` returns `None` (the whole row would be the key), so those tables use `schema.table`; `key` mode otherwise only orders per row while identity columns are immutable. Truncates use `schema.table`.
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

- Keep README.md and `docs/DEPLOYMENT.md` (config reference, operations) current in the same change as any user-visible behaviour: config keys, routes, sinks, ops notes. Update the README architecture diagrams when you add or remove a crate, source, route or pipeline step. User-visible changes get a changelog fragment, `changelog/unreleased/<section>/<issue>-<slug>.md` (see `changelog/README.md`). Breaking changes always need one in `breaking/`, with the upgrade step. Never edit the Unreleased notes in `CHANGELOG.md` directly.
- Kafka is the only sink by design; do not add others. The inspector reads from Kafka, never from an in-process buffer.
- Match the surrounding style; no unrelated refactors. Commit messages end with the repo's `Co-Authored-By` line when an agent authored them.
- Source files may have mixed CRLF/LF line endings (Windows checkout). Normalise before scripted edits.
- Git push to the Buzz relay needs a working `git-credential-nostr`; if it prints no credential, the push cannot succeed from that session.

## Issue and PR titles

Titles must make sense to someone who has not read the plan. Reviewers send back a PR whose title breaks these rules.

- Format: `<Area>: <what changes, in plain words>`. Put the result first. A plan code, if any, goes last in parentheses.
- Areas: `Postgres source`, `MongoDB source`, `Kafka sink`, `Routing`, `Snapshots`, `Cockpit`, `Pipeline`, `CI`, `Docs`.
- Don't lead with plan codes (R1, S4, T3) or use internal symbols (K, `ord`, `RoutePlan`) as the title. No people's names.
- Bugs describe what the user sees ("rows lost when transactions overlap"), not the mechanism.
- PR titles end with `(#issue)`.
- Examples: `Routing: set the Kafka message key per route (R5)`, `Postgres source: decode logical replication messages, needed for snapshot watermarks (S3)`.

Branch names are not affected. Commit messages follow the next section.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/): `<type>(<scope>): <summary>`.

- Types: `feat` (new behaviour), `fix` (bug fix), `docs`, `test`, `refactor` (no behaviour change), `perf`, `build`, `ci`, `chore`.
- Scopes match the title areas: `postgres`, `mongodb`, `kafka`, `routing`, `snapshots`, `cockpit`, `pipeline`, `ci`, `docs`. Leave the scope out if a change spans several areas.
- Summary: imperative, lower case, no trailing period, plain words as in titles. For example, `fix(postgres): stop skipping rows on multi-table TRUNCATE`.
- Breaking changes: add `!` after the scope (`feat(routing)!: ...`) and a `BREAKING CHANGE:` footer with the upgrade step, matching the changelog fragment.
- The body explains why. Agent-authored commits end with the `Co-Authored-By` line.
- When squash-merging, rewrite the PR title into this form and keep the PR number: `feat(routing): set the Kafka message key per route (#81) (#105)`.
