#!/usr/bin/env bash
# Runs the CI checks on Linux in containers, for when GitHub Actions is unavailable (billing) and
# for reproducing a CI failure. Same checks as .github/workflows/ci.yml. Paste the output on the PR.
#
#   scripts/ci-linux.sh            # fmt, clippy, tests, audit, deny
#   scripts/ci-linux.sh --integration   # ... plus the integration suite (source, sink or pipeline PRs)
#   scripts/ci-linux.sh --image    # ... plus `build .` of the Dockerfile (Dockerfile or image PRs)
#
# CT is the container CLI (docker by default; on the Windows dev machine: CT=podman with S:\podman\bin
# on PATH). Run it from the repository root of the checkout you want to verify.
set -euo pipefail
CT=${CT:-docker}
export MSYS_NO_PATHCONV=1  # Git Bash on Windows would rewrite /src-style arguments
integration=0; image=0
for a in "$@"; do
  case $a in
    --integration) integration=1 ;;
    --image) image=1 ;;
    *) echo "usage: $0 [--integration] [--image]" >&2; exit 1 ;;
  esac
done
here=$(cd "$(dirname "$0")/.." && pwd)
cd "$here"
echo "== $(git rev-parse HEAD) $([ -z "$(git status --porcelain)" ] && echo clean || echo 'DIRTY WORKTREE')"
src=$(cygpath -w "$here" 2>/dev/null || echo "$here")
# One target volume per checkout: cargo compares mtimes, so a target shared between checkouts could
# reuse another checkout's build outputs and report green for code it never compiled.
target_vol=aq-target-$(printf %s "$here" | cksum | cut -d' ' -f1)

env_args=()
if [ $integration = 1 ]; then
  certs=$(mktemp -d)
  trap 'CT=$CT bash scripts/ci-integration-services.sh down "$certs"; rm -rf "$certs"' EXIT
  CT=$CT bash scripts/ci-integration-services.sh up "$certs"
  # The test container shares the host network so the published service ports are on its localhost.
  # It reads the variables from services.env; the cert paths are the /certs mount, set last so they win.
  env_args=(--network host --env-file "$(cygpath -w "$certs/services.env" 2>/dev/null || echo "$certs/services.env")"
    -e AQUEDUCT_TEST_PG_CA=/certs/ca.pem -e AQUEDUCT_TEST_PG_WRONG_CA=/certs/other.pem
    -v "$(cygpath -w "$certs" 2>/dev/null || echo "$certs")":/certs:ro)
fi

# Cargo state and the target dir live in named volumes: a bind-mounted target is very slow on a
# Windows host, and the volumes make a re-run incremental.
$CT run --rm "${env_args[@]}" -v "$src":/src -v aq-cargo-registry:/usr/local/cargo/registry \
  -v aq-cargo-bin:/usr/local/cargo/bin-extra -v "$target_vol":/src/target -w /src \
  -e CARGO_TARGET_DIR=/src/target -e "RUN_INTEGRATION=$integration" -e CARGO_TERM_COLOR=never \
  docker.io/library/rust:1 bash -ec '
    export PATH=/usr/local/cargo/bin-extra/bin:$PATH
    rustup component add rustfmt clippy >/dev/null
    command -v cargo-audit >/dev/null || cargo install --locked --root /usr/local/cargo/bin-extra cargo-audit cargo-deny
    set -x
    cargo fmt --all --check
    cargo clippy --workspace --all-targets --locked -- -D warnings
    cargo test --workspace --locked
    cargo audit
    cargo deny check
    if [ "$RUN_INTEGRATION" = 1 ]; then
      export AQUEDUCT_TEST_PG_REMOTE_HOST=$(hostname -I | cut -d" " -f1)
      cargo test --locked -p aqueduct-source-mongodb -p aqueduct-source-postgres -- --ignored
      cargo test --locked -p aqueduct-sinks --test kafka_it
    fi'

if [ $image = 1 ]; then
  $CT build -t aqueduct:ci .
fi
echo "== ci-linux.sh passed"
