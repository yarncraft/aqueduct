#!/usr/bin/env bash
# Release mechanics. It never creates a git tag or a GitHub release; see docs/RELEASING.md for the order.
#
#   scripts/release.sh prepare <version>         fold the changelog fragments into CHANGELOG.md, set the crate
#                                                version and refresh Cargo.lock (run on a release branch)
#   scripts/release.sh check <version>           verify the tree is ready to be tagged
#   scripts/release.sh notes <version>           print the GitHub release notes for <version>
#   scripts/release.sh image <version> [--push]  build the container image (linux, host architecture) from the
#                                                committed HEAD; with --push run `check`, refuse if the tag
#                                                already exists in the registry, publish and record the digest in
#                                                dist/aqueduct-<version>.digest
#   scripts/release.sh smoke <version>           pull the published image by its recorded digest, run it with
#                                                Postgres and Kafka in a throwaway pod and wait for /healthz
#
# Needs Podman. Environment: AQUEDUCT_IMAGE  image repository, default ghcr.io/yarncraft/aqueduct
#              AQUEDUCT_FIRST_PUSH=1  only for the very first publish: accept a 'denied' answer from the existence probe
#              AQUEDUCT_REGISTRY_ARGS  extra flags for registry calls, e.g. --tls-verify=false for a local registry
set -euo pipefail
cd "$(dirname "$0")/.."

image_repo=${AQUEDUCT_IMAGE:-ghcr.io/yarncraft/aqueduct}
cli=podman
registry_args=${AQUEDUCT_REGISTRY_ARGS:-}
usage() { sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 1; }
die() { echo "release: $*" >&2; exit 1; }

cmd=${1:-}; version=${2:-}
[ -n "$cmd" ] && [ -n "$version" ] || usage
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "version must be X.Y.Z (no leading v, no suffix): $version"
digest_file=dist/aqueduct-$version.digest

crate_version() { awk '/^\[workspace\.package\]/ { w = 1; next } /^\[/ { w = 0 } w && $1 == "version" { gsub(/"/, "", $3); print $3; exit }' Cargo.toml; }

# The "## <version> (<date>)" section of CHANGELOG.md, heading included.
section() {
  awk -v h="## $version (" 'index($0, h) == 1 { on = 1; print; next } on && /^## / { exit } on { print }' CHANGELOG.md |
    awk '{ sub(/\r$/, ""); lines[NR] = $0; if ($0 ~ /[^ \t]/) last = NR } END { for (i = 1; i <= last; i++) print lines[i] }'
}

prepare() {
  [ "$(git rev-parse --abbrev-ref HEAD)" != main ] || die "prepare on a release branch, not on main"
  [ -z "$(git status --porcelain)" ] || die "working tree is not clean"
  ! section | grep -q . || die "CHANGELOG.md already has a $version section"
  scripts/changelog.sh --release "$version"
  sed -i -E '/^\[workspace\.package\]/,/^\[/ s/^version = ".*"/version = "'"$version"'"/' Cargo.toml
  [ "$(crate_version)" = "$version" ] || die "could not set the version in Cargo.toml"
  # Only the workspace crates change in Cargo.lock.
  if command -v cargo >/dev/null; then
    cargo update --workspace
  else
    rust_image=$(sed -n 's/^FROM \(rust:[^ ]*\) AS build.*/\1/p' Dockerfile)
    [ -n "$rust_image" ] || die "no rust build image found in the Dockerfile"
    "$cli" run --rm -v "$(pwd -W 2>/dev/null || pwd)":/src -w /src "$rust_image" cargo update --workspace
  fi
  git diff --stat
  echo "Review the diff, commit it as 'chore: release $version' and open the release PR." >&2
}

check() {
  local ok=1
  bad() { echo "not ready: $*" >&2; ok=0; }
  [ "$(crate_version)" = "$version" ] || bad "Cargo.toml version is $(crate_version), not $version"
  section | grep -q . || bad "CHANGELOG.md has no '## $version (' section (run prepare)"
  if [ -d changelog/unreleased ] && [ -n "$(find changelog/unreleased -type f -name '*.md' | head -1)" ]; then
    bad "changelog fragments are still in changelog/unreleased/"
  fi
  [ -z "$(git status --porcelain)" ] || bad "working tree is not clean"
  git fetch -q origin main --tags
  [ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] || bad "HEAD is not origin/main"
  ! git rev-parse -q --verify "refs/tags/v$version" >/dev/null || bad "tag v$version already exists"
  [ "$ok" = 1 ] || exit 1
  echo "ready to tag v$version at $(git rev-parse --short HEAD)"
}

notes() {
  local body
  body=$(section | tail -n +2)
  [ -n "$body" ] || die "CHANGELOG.md has no '## $version (' section"
  printf '%s\n' "$body" | sed '1{/^$/d}'
  if [ -f "$digest_file" ]; then
    printf '\n### Container image\n\n`%s@%s`\n' "$image_repo" "$(cat "$digest_file")"
  fi
}

image() {
  local push=${3:-} probe
  [ -z "$push" ] || [ "$push" = --push ] || usage
  if [ "$push" = --push ]; then
    check
    # `podman manifest inspect` cannot read a single-image manifest, so probe by pulling. Only a clear "absent" error lets
    # the push go ahead: a failed login, DNS error, rate limit or typo must stop the release, not be read as "absent".
    # shellcheck disable=SC2086 # the extra flags are meant to word-split
    if probe=$("$cli" pull -q $registry_args "$image_repo:$version" 2>&1); then
      "$cli" rmi -f "$image_repo:$version" >/dev/null 2>&1 || true
      die "$image_repo:$version already exists in the registry; a published tag is never overwritten (use a new patch version)"
    elif grep -Eiq 'manifest unknown|name unknown|not found' <<< "$probe"; then
      :
    elif [ "${AQUEDUCT_FIRST_PUSH:-}" = 1 ] && grep -Eiq 'denied' <<< "$probe"; then
      echo "release: AQUEDUCT_FIRST_PUSH=1: treating 'denied' as 'package not published yet'" >&2
    else
      die "could not check whether $image_repo:$version exists: $probe"
    fi
  fi
  local rev ctx=dist/context-$version
  rev=$(git rev-parse HEAD)
  # Build from the committed tree only: git-ignored files in the working directory cannot reach the image.
  rm -rf "$ctx"; mkdir -p "$ctx"
  git archive HEAD | tar -x -C "$ctx"
  "$cli" build --label "org.opencontainers.image.version=$version" --label "org.opencontainers.image.revision=$rev"     --label "org.opencontainers.image.source=https://github.com/yarncraft/aqueduct" -t "$image_repo:$version" "$ctx"
  rm -rf "$ctx"
  [ "$push" = --push ] || { echo "built $image_repo:$version from $rev (not pushed)" >&2; return; }
  rm -f "$digest_file"
  # shellcheck disable=SC2086
  "$cli" push $registry_args --digestfile "$digest_file" "$image_repo:$version"
  grep -Eq '^sha256:[0-9a-f]{64}$' "$digest_file" || die "no valid digest recorded in $digest_file"
  echo "published $image_repo:$version"
  echo "digest    $(cat "$digest_file")"
  echo "pin with  $image_repo@$(cat "$digest_file")"
}

# Run the published image, pulled by digest, next to Postgres and Kafka and wait for its own health probe.
smoke() {
  [ -f "$digest_file" ] || die "no $digest_file; run 'image $version --push' first"
  local ref pod="aq-smoke-$version-$$" ok=0
  ref="$image_repo@$(cat "$digest_file")"
  # Expanded now: $pod is local to this function and gone when the EXIT trap fires.
  # shellcheck disable=SC2064
  trap "$cli pod rm -f $pod >/dev/null 2>&1 || true" EXIT
  "$cli" rmi -f "$image_repo:$version" >/dev/null 2>&1 || true
  # shellcheck disable=SC2086
  "$cli" pull $registry_args "$ref" >/dev/null
  "$cli" pod create --name "$pod" --add-host kafka:127.0.0.1 >/dev/null
  "$cli" run -d --pod "$pod" --name "$pod-pg" -e POSTGRES_USER=aqueduct -e POSTGRES_PASSWORD=aqueduct \
    -e POSTGRES_DB=app -v "$(pwd -W 2>/dev/null || pwd)/config/init.sql":/docker-entrypoint-initdb.d/init.sql:ro,Z \
    docker.io/library/postgres:16 postgres -c wal_level=logical -c max_replication_slots=4 -c max_wal_senders=4 >/dev/null
  "$cli" run -d --pod "$pod" --name "$pod-kafka" -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    docker.io/apache/kafka:3.8.0 >/dev/null
  sleep 20
  "$cli" run -d --pod "$pod" --name "$pod-aq" --read-only --cap-drop ALL --security-opt no-new-privileges \
    -e AQUEDUCT_PG_URL='postgres://aqueduct:aqueduct@localhost:5432/app?sslmode=disable' \
    -e AQUEDUCT_ADMIN_TOKEN="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')" "$ref" >/dev/null
  for _ in $(seq 1 30); do
    if "$cli" exec "$pod-aq" /usr/local/bin/aqueduct --healthcheck; then ok=1; break; fi
    sleep 3
  done
  echo "image    $("$cli" inspect --format '{{.Image}}' "$pod-aq" | cut -c1-12) from $ref" >&2
  if [ "$ok" = 1 ]; then echo "smoke ok: /healthz returned 200 for $ref"; else
    "$cli" logs "$pod-aq" 2>&1 | tail -20 >&2; die "/healthz did not return 200 for $ref"
  fi
}

case "$cmd" in
  prepare) prepare ;;
  check) check ;;
  notes) notes ;;
  image) image "$@" ;;
  smoke) smoke ;;
  *) usage ;;
esac
