#!/usr/bin/env bash
# Runs the jobs of .github/workflows/ci.yml in podman (or docker) against one exact commit.
#
#   scripts/ci-local.sh [--post] [--jobs rust,docker,audit,integration] [SHA]
#
# SHA defaults to HEAD. The commit is exported with `git archive`, so uncommitted changes are never
# tested and a green run is attributable to that SHA. Keep this script in step with ci.yml.
#
#   --post   publish a `local-ci/<job>` commit status for every job, using the token from
#            `git credential fill`. Requires all four jobs and HEAD == SHA (check the commit out
#            first). A skipped job is published as failure. Without --post nothing leaves the machine.
#   --jobs   comma-separated subset for a quick look; default is all four. `integration` is skipped
#            (and counts as red) if `rust` failed, like `needs: rust` in ci.yml.
#
# Merge-gate use: the reviewer runs main's copy of this script on the exact head and pastes the
# final report. The job commands are in this script, so they always come from main, never from the PR.
# It stops, and needs --allow-gate-changes after Megatron's written diff review, when
#   - the running copy of this script differs from origin/main's, or
#   - the PR changes scripts/ci-local.sh, .github/workflows/ or .githooks/.
# It refuses to run when the environment points at an external database or broker: the integration
# job only talks to the throwaway containers it starts itself (COPY FROM PROGRAM needs a superuser).
#
# Env: CONTAINER_CLI (podman or docker, auto-detected), LOCAL_CI_RUST_IMAGE (default rust:1-bookworm).
set -uo pipefail

POST=0
ALLOW_GATE=0
JOBS=rust,docker,audit,integration
SHA=
while [ $# -gt 0 ]; do
  case "$1" in
    --post) POST=1 ;;
    --allow-gate-changes) ALLOW_GATE=1 ;;
    --jobs) JOBS="${2:?--jobs needs a value}"; shift ;;
    -h|--help) sed -n '2,26p' "$0"; exit 0 ;;
    -*) echo "unknown option $1" >&2; exit 1 ;;
    *) SHA="$1" ;;
  esac
  shift
done

cd "$(git rev-parse --show-toplevel)"
SHA=$(git rev-parse --verify "${SHA:-HEAD}^{commit}") || { echo "not a commit: ${SHA:-HEAD}" >&2; exit 1; }
SHORT=${SHA:0:7}

# --- gate integrity ----------------------------------------------------------------------------
for v in DATABASE_URL PGHOST PGPASSWORD MONGO_URI AQUEDUCT_KAFKA_BROKERS          AQUEDUCT_TEST_KAFKA AQUEDUCT_TEST_PG_TLS AQUEDUCT_TEST_PG_PLAIN AQUEDUCT_TEST_PG_TRUST AQUEDUCT_TEST_PG_CA AQUEDUCT_TEST_PG_WRONG_CA; do
  if [ -n "${!v:-}" ]; then echo "refusing to run: $v is set; the integration job only uses its own throwaway containers" >&2; exit 1; fi
done
# A stray control byte (e.g. 0x01 where a sed back-reference `\1` belongs) renders as nothing in diffs.
if grep -nP '[\x00-\x08\x0e-\x1f]' "$0" >&2; then echo "refusing to run: $0 contains control characters" >&2; exit 1; fi
git fetch -q origin main 2>/dev/null || echo "warning: could not fetch origin/main; gate checks use the local ref" >&2
BASE=origin/main
if git rev-parse --verify -q "$BASE" >/dev/null; then
  gate_msg=
  if git cat-file -e "$BASE:scripts/ci-local.sh" 2>/dev/null      && ! git show "$BASE:scripts/ci-local.sh" | cmp -s - "$0"; then
    gate_msg="the running scripts/ci-local.sh differs from $BASE's (run main's copy)"
  fi
  changed=$(git diff --name-only "$(git merge-base "$BASE" "$SHA")" "$SHA" -- scripts/ci-local.sh .github/workflows .githooks 2>/dev/null)
  [ -n "$changed" ] && gate_msg="${gate_msg:+$gate_msg; }$SHORT changes the gate: $(echo $changed)"
  if [ -n "$gate_msg" ] && [ $ALLOW_GATE = 0 ]; then
    echo "STOP: $gate_msg" >&2
    echo "This needs Megatron's written diff review. After that, rerun with --allow-gate-changes." >&2
    exit 2
  fi
fi
if [ $POST = 1 ]; then
  [ "$(git rev-parse HEAD)" = "$SHA" ] || { echo "refusing to post: HEAD is $(git rev-parse HEAD), not $SHA" >&2; exit 1; }
  [ "$JOBS" = rust,docker,audit,integration ] || { echo "refusing to post: --post needs all four jobs" >&2; exit 1; }
fi
CLI=${CONTAINER_CLI:-$(command -v podman >/dev/null 2>&1 && echo podman || echo docker)}
RUST_IMAGE=${LOCAL_CI_RUST_IMAGE:-rust:1-bookworm}
export MSYS_NO_PATHCONV=1 # Git Bash on Windows must not rewrite the /src and /certs container paths

WORK=$(mktemp -d)
LOGS=$WORK/logs
mkdir -p "$LOGS" "$WORK/src"
git archive "$SHA" | tar -x -C "$WORK/src"
host_path() { if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else echo "$1"; fi; }
SRC=$(host_path "$WORK/src")

POD=aqueduct-ci-$SHORT-$$
cleanup() {
  "$CLI" pod rm -f "$POD" >/dev/null 2>&1 || true
  "$CLI" volume rm -f "$POD-certs" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

# Caches that are safe to share between runs. The target dir is deliberately not cached: cargo trusts
# mtimes, and a stale binary after a source change is worse than a slow run.
CARGO_VOLS=(-v aqueduct-ci-cargo-registry:/usr/local/cargo/registry -v aqueduct-ci-cargo-git:/usr/local/cargo/git)

cargo_run() { # cargo_run <extra podman args...> -- <shell script>
  local extra=()
  while [ "$1" != "--" ]; do extra+=("$1"); shift; done
  shift
  "$CLI" run --rm "${extra[@]}" "${CARGO_VOLS[@]}" -v "$SRC":/src -w /src \
    -e CARGO_TARGET_DIR=/tmp/target -e CARGO_TERM_COLOR=never "$RUST_IMAGE" bash -euo pipefail -c "$1"
}

job_rust() {
  cargo_run -- '
    rustup component add rustfmt clippy
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace --locked'
}

job_docker() {
  "$CLI" build -t "aqueduct:ci-$SHORT" "$SRC"
}

job_audit() {
  # rustsec/audit-check runs `cargo audit`; install it into the shared registry volume once.
  cargo_run -- 'command -v cargo-audit >/dev/null || cargo install cargo-audit --locked; cargo audit'
}

job_integration() {
  # One pod, so every service is on the pod'"'"'s localhost like the runner'"'"'s published ports. Postgres
  # instances take different ports instead of different host mappings.
  "$CLI" pod create --name "$POD" >/dev/null || return 1
  "$CLI" volume create "$POD-certs" >/dev/null || return 1

  echo "== certs"
  "$CLI" run --rm -u 0 -v "$POD-certs":/certs "$RUST_IMAGE" bash -euo pipefail -c '
    command -v openssl >/dev/null || { apt-get update -qq && apt-get install -y -qq openssl; }
    cd /certs
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=aq-test-ca -keyout ca.key -out ca.pem
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=aq-other-ca -keyout other.key -out other.pem
    openssl req -newkey rsa:2048 -nodes -subj /CN=localhost -keyout server.key -out server.csr
    printf "subjectAltName=DNS:localhost\n" > san.ext
    openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 -extfile san.ext -out server.crt
    chown 999:999 server.key server.crt; chmod 600 server.key; chmod 644 ca.pem other.pem' || return 1

  echo "== kafka, mongo, postgres"
  "$CLI" run -d --pod "$POD" --name kafka \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 apache/kafka:3.8.0 >/dev/null || return 1
  "$CLI" run -d --pod "$POD" --name mongo mongo:7 --replSet rs0 --bind_ip_all >/dev/null || return 1
  local pgcommon=(-e POSTGRES_PASSWORD=aq postgres:16 -c wal_level=logical -c max_replication_slots=20)
  "$CLI" run -d --pod "$POD" --name pg-tls -v "$POD-certs":/certs:ro "${pgcommon[@]}" -c port=5433 \
    -c ssl=on -c ssl_cert_file=/certs/server.crt -c ssl_key_file=/certs/server.key >/dev/null || return 1
  "$CLI" run -d --pod "$POD" --name pg-plain "${pgcommon[@]}" -c port=5434 >/dev/null || return 1
  # Trust authentication, for the replication client, which has no password auth yet.
  "$CLI" run -d --pod "$POD" --name pg-trust -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16 \
    -c wal_level=logical -c max_replication_slots=20 -c max_wal_senders=10 -c port=5435 >/dev/null || return 1

  local i ok state c
  ok=0
  for i in $(seq 1 30); do
    if "$CLI" exec mongo mongosh --quiet --eval 'db.runCommand({ping:1}).ok' >/dev/null 2>&1; then ok=1; break; fi
    sleep 2
  done
  [ "$ok" = 1 ] || { echo "mongod never answered ping"; return 1; }
  "$CLI" exec mongo mongosh --quiet --eval \
    "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})" || return 1
  ok=0
  for i in $(seq 1 30); do
    state=$("$CLI" exec mongo mongosh --quiet --eval 'rs.status().members[0].stateStr' 2>/dev/null || true)
    if [ "$state" = PRIMARY ]; then ok=1; break; fi
    sleep 2
  done
  [ "$ok" = 1 ] || { echo "replica set never became primary"; return 1; }

  for c in pg-tls:5433 pg-plain:5434 pg-trust:5435; do
    local name=${c%%:*} port=${c##*:}
    ok=0
    for i in $(seq 1 30); do
      if "$CLI" exec "$name" pg_isready -U postgres -p "$port" >/dev/null 2>&1; then ok=1; break; fi
      sleep 2
    done
    [ "$ok" = 1 ] || { echo "$name never became ready"; return 1; }
    # pg_isready can pass during init, before the final restart: retry the DDL.
    ok=0
    for i in $(seq 1 15); do
      if "$CLI" exec "$name" psql -U postgres -p "$port" -c 'CREATE PUBLICATION aq_pub FOR ALL TABLES'; then ok=1; break; fi
      sleep 2
    done
    [ "$ok" = 1 ] || { echo "$name: could not create publication"; return 1; }
  done

  ok=0
  for i in $(seq 1 30); do
    if "$CLI" exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then ok=1; break; fi
    sleep 2
  done
  [ "$ok" = 1 ] || { echo "kafka never became ready"; return 1; }

  echo "== integration tests"
  cargo_run --pod "$POD" -v "$POD-certs":/certs:ro \
    -e AQUEDUCT_TEST_KAFKA=localhost:9092 -e AQUEDUCT_KAFKA_BROKERS=localhost:9092 \
    -e 'MONGO_URI=mongodb://localhost:27017/?replicaSet=rs0&directConnection=true' \
    -e AQUEDUCT_TEST_PG_TLS=localhost:5433 -e AQUEDUCT_TEST_PG_PLAIN=localhost:5434 \
    -e AQUEDUCT_TEST_PG_TRUST=localhost:5435 \
    -e AQUEDUCT_TEST_PG_CA=/certs/ca.pem -e AQUEDUCT_TEST_PG_WRONG_CA=/certs/other.pem -- '
    cargo test --locked -p aqueduct-source-mongodb -p aqueduct-source-postgres -- --ignored
    cargo test --locked -p aqueduct-sinks --test kafka_it'
  local rc=$?
  if [ $rc -ne 0 ]; then
    for c in kafka mongo pg-tls pg-plain; do echo "== logs: $c"; "$CLI" logs "$c" 2>&1 | tail -50; done
  fi
  return $rc
}

# The `--ignored` tests silently not running would make a green result meaningless.
check_integration_ran() {
  local n
  n=$(sed -n 's/^test result: ok\. \([0-9][0-9]*\) passed.*/\1/p' "$LOGS/integration.log" | awk '{t+=$1} END{print t+0}')
  echo "integration tests passed: $n" >> "$LOGS/integration.log"
  [ "$n" -gt 0 ]
}

post_status() { # post_status <job> <success|failure>
  local job=$1 state=$2 slug cred token
  slug=$(git remote get-url origin | sed -E 's#^(git@github.com:|https://([^@/]+@)?github.com/)##; s#\.git$##')
  cred=$(printf 'protocol=https\nhost=github.com\n\n' | git credential fill 2>/dev/null) || cred=
  token=$(printf '%s\n' "$cred" | sed -n 's/^password=//p')
  if [ -z "$token" ]; then echo "  no GitHub credential from git: status for $job not posted"; return 1; fi
  curl -fsS -o /dev/null -X POST -H "Authorization: Bearer $token" -H 'Accept: application/vnd.github+json' \
    "https://api.github.com/repos/$slug/statuses/$SHA" \
    -d "{\"state\":\"$state\",\"context\":\"local-ci/$job\",\"description\":\"podman run on $SHORT\"}" \
    && echo "  posted local-ci/$job = $state" || { echo "  posting local-ci/$job failed"; return 1; }
}

RESULT=()
DIGEST=()
rust_failed=0
FAILED=0
for job in rust docker audit integration; do
  case ",$JOBS," in *",$job,"*) ;; *) continue ;; esac
  if [ "$job" = integration ] && [ $rust_failed = 1 ]; then
    RESULT+=("integration: SKIPPED (rust failed) = red")
    FAILED=1
    [ $POST = 1 ] && post_status integration failure
    continue
  fi
  echo "=== $job @ $SHORT"
  start=$(date +%s)
  "job_$job" >"$LOGS/$job.log" 2>&1
  rc=$?
  [ $rc -eq 0 ] && [ "$job" = integration ] && { check_integration_ran || rc=1; }
  secs=$(( $(date +%s) - start ))
  sum=$(sha256sum "$LOGS/$job.log" | cut -c1-12)
  DIGEST+=("[$job] log sha256 $sum, $(wc -l <"$LOGS/$job.log") lines")
  while IFS= read -r l; do DIGEST+=("  $l"); done < <(grep -E '^test result:|^integration tests passed|error(\[|:)' "$LOGS/$job.log" | head -20)
  if [ $rc -eq 0 ]; then
    RESULT+=("$job: ok (${secs}s)")
    state=success
  else
    RESULT+=("$job: FAILED (${secs}s)")
    state=failure
    FAILED=1
    [ "$job" = rust ] && rust_failed=1
    echo "--- last 60 lines of $job"; tail -60 "$LOGS/$job.log"
    mkdir -p .scratch && cp "$LOGS/$job.log" ".scratch/local-ci-$SHORT-$job.log" && echo "full log: .scratch/local-ci-$SHORT-$job.log"
  fi
  [ $POST = 1 ] && post_status "$job" "$state"
done

echo
echo "local-ci report"
echo "  sha:    $SHA"
echo "  run by: $(git config user.name 2>/dev/null || echo unknown) on $(hostname), $(date -u +%FT%TZ)"
echo "  runtime: $("$CLI" --version | head -1), image $RUST_IMAGE"
printf '  %s
' "${RESULT[@]}"
[ ${#DIGEST[@]} -gt 0 ] && { echo "  digest:"; printf '    %s
' "${DIGEST[@]}"; }
[ $FAILED = 0 ] && echo "  verdict: GREEN on $SHA" || echo "  verdict: RED"
exit $FAILED
