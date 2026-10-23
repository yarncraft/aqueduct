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

`docs/ARCHITECTURE.md` has Mermaid diagrams of the components, the at-least-once loop and the crate graph.

## Commands

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace            # always run the whole workspace, not one crate
docker compose up --build         # or: podman-compose up -d --build
```

**CI is `.github/workflows/ci.yml`**: on Linux, on every PR and push to `main`: `cargo fmt --all --check`, clippy, `cargo test --workspace --locked`, `cargo audit`, `cargo deny check` (policy in `deny.toml`), `docker build`, and the integration suite for PRs that touch `aqueduct-core`, a source, the Kafka sink, `aqueduct-tls`, the binary, `Cargo.*` or the CI files. The Linux run is what shows a change works on the production platform; a Windows-only run does not count for review.

**While GitHub Actions is blocked on billing**, run the same checks on Linux yourself and paste the commands and their `test result` lines on the PR, for the exact head SHA you ask to have reviewed:

```sh
CT=podman scripts/ci-linux.sh --integration   # fmt, clippy, tests, audit, deny + Kafka/MongoDB/Postgres suite, in containers
```

- Every PR: `scripts/ci-linux.sh` (fmt, clippy, workspace tests, audit, deny; the script prints the SHA it ran and whether the worktree was dirty).
- PRs that touch a source, the Kafka sink or the pipeline: add `--integration`, which starts the Kafka, MongoDB and Postgres (TLS, plaintext, trust) containers from `scripts/ci-integration-services.sh` and runs `cargo test --locked -p aqueduct-source-mongodb -p aqueduct-source-postgres -- --ignored` and `cargo test --locked -p aqueduct-sinks --test kafka_it`. The passed count must be non-zero; a test that skips itself when its variable is unset does not count.
- PRs that change the `Dockerfile` or the image: add `--image` (`build .`).
- Base images in the `Dockerfile` are pinned by digest (tag kept in a comment). To bump, resolve the new multi-arch index digest of the tag (`podman manifest inspect` or the registry's `Docker-Content-Digest`), update both `FROM` lines and build.
- For quick iteration on the Windows machine, native `cargo fmt`/`clippy`/`test` is fine (below); it does not replace the Linux run before you post a SHA.

On the Windows dev machine, cargo runs natively with the toolchain on `S:` (`RUSTUP_HOME=S:\rust\rustup`,
`CARGO_HOME=S:\rust\cargo`, `S:\rust\cargo\bin` on `PATH`, `CARGO_TARGET_DIR=S:\rust\target`). The one exception is `scripts/ci-linux.sh`, which runs cargo in a `rust` container on purpose: that is the Linux run.
Only the services the integration suite needs (Kafka, MongoDB, Postgres) run in Podman (`S:\podman\bin`): publish their
ports and reach them on `localhost`. `podman build .` still builds the image; pulling `gcr.io` images needs the corporate
(Zscaler) root CA trusted in the Podman machine. After a machine restart, `podman machine stop` and `podman machine start`
restore the Docker API pipe that `docker compose` uses.

### Cleanup (always)

Podman runs pile up images, containers and volumes on the dev machine's disk, and the Podman VM disk does not shrink by
itself. Clean up after every check, test run or image build, and say so in the PR or report:

- Remove every pod and container you started (`podman pod rm -f <pod>`, `podman rm -f <name>`), including throwaway
  Kafka, Postgres and MongoDB containers, and the volumes they used (`podman volume rm`).
- Remove every image you built (`podman rmi aqueduct:check-<sha>`, test images).
- At the end of a task, `podman image prune -f` (dangling layers only) is safe. Do **not** run a blanket
  `podman container prune` or `podman volume prune`: they also delete stopped containers and their volumes that are not
  yours, such as the simulation's Postgres data after a machine restart. Remove containers and volumes by name instead.
  Never stop or remove containers or volumes you did not start.
- Named cargo cache volumes (`aq-*-target`, registry) are caches: delete yours when the task is done. `scripts/ci-linux.sh` uses `aq-cargo-registry`, `aq-cargo-bin` and one `aq-target-<checksum>` per checkout (`podman volume ls | grep aq-target`); remove yours with `podman volume rm` when you finish (it removes its service containers and the `aq-certs` volume itself).
- Check `podman system df` after a batch of work; tens of GB reclaimable means something was left behind.
- Agents you delegate to get the same rule in their prompt.

### Merge rule

Nothing merges without review, and branch protection is not available, so the rule is by convention (accepted by the owner on 2026-10-23, #230):

- There is one named merger, Optimus Prime.
- Before merging, the merger posts `merging #N at <SHA>`, and merges only the exact head SHA the reviewer (Megatron) approved. A push after the approval voids it.
- CI (or the pasted Linux run above) must be green on that same SHA.
- Nobody else merges, and no one merges their own PR without that approval.

## Invariants (do not break)

- **At-least-once**: the pipeline checkpoints and acks the source only after `sink.send` and `flush` succeed.
  Sinks must tolerate redelivery; never ack earlier for speed.
- **Positions**: sources own their `Position` encoding. The event JSON field is still `lsn`, and text positions are checkpointed as `~<text>`. Adding a source = new crate, a `SourceConfig` variant in `config.rs` and a match arm in `main.rs`. SQL Server, Redis and the Kafka source were dropped on purpose.
- **Slot advance**: the source advances the replication slot only to a transaction's commit-end LSN.
  Acking a mid-transaction LSN does not move `confirmed_flush_lsn` and replays the transaction.
- Postgres URLs are plain SQL connections. Do not add `replication=database`; tokio-postgres rejects it. With
  `transport = "stream"` the source opens its own replication connection from the same URL (`replication/`, hand-written
  on `postgres-protocol`, same TLS and auth rules); poll stays the default and is kept permanently. Keys are assigned
  in one place (`source.rs::apply`) for both transports.
- Kafka: events are Debezium-style envelopes (`lib.rs::encode`: `before`/`after`/`source`/`op` letters/`ts_ms`,
  `timestamptz` ISO-8601 UTC, `timestamp` epoch µs, `date` epoch days, from `DateStyle = ISO` text). The record key is
  JSON `{"db","schema","table","key":{<primary key>}}` (MongoDB `{"db","collection","key":<documentKey>}`), built by
  `lib.rs::record_key`; the primary key comes from the catalog (`decoder.rs::key`, identity columns only as a fallback),
  `null` without one and for truncates. Partition = stable FNV-1a hash of the whole key (default `partition_by = "key"`,
  per-row order) or of `schema.table` (`"table"`, per-table order).
- Rows are sensitive. The event inspector exposes row data over HTTP; keep it behind `admin_token`
  and never log tokens.
- **Fail closed**: the admin server refuses a non-loopback `health_addr` without `admin_token` (`Config::check_admin_exposure`,
  opt-out `admin_insecure_no_auth`). Postgres TLS defaults to `verify-full`, and a plaintext session to a non-loopback
  host aborts startup unless `ssl_mode = "disable"` is explicit (`aqueduct-source-postgres/src/tls.rs`). MongoDB likewise
  refuses a plaintext URI to a non-loopback host unless it says `tls=false` (`aqueduct-source-mongodb/src/config.rs`). The admin token is accepted only as `Authorization: Bearer`; never
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

- Keep README.md and `docs/` current: the short pages (`CONFIGURATION.md`, `COCKPIT.md`, `DEPLOYMENT.md`) stay scannable cheat sheets and the full semantics live in `docs/deep-dive/`. Update both in the same change as any user-visible behaviour: config keys, routes, sinks, ops notes. Update the `docs/ARCHITECTURE.md` diagrams when you add or remove a crate, source, route or pipeline step. User-visible changes get a changelog fragment, `changelog/unreleased/<section>/<issue>-<slug>.md` (see `changelog/README.md`). Breaking changes always need one in `breaking/`, with the upgrade step. Never edit the Unreleased notes in `CHANGELOG.md` directly.
- Releases follow `docs/RELEASING.md` (`scripts/release.sh`). Never create a tag or a GitHub release outside that order, and never overwrite a pushed image tag.
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
