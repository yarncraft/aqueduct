#!/usr/bin/env bash
# Shared helpers for the chaos scenarios. Requires docker (or podman with a docker shim) + compose v2.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DC=(docker compose -p aqueduct-chaos -f "$ROOT/docker-compose.yml" -f "$ROOT/scripts/chaos/docker-compose.chaos.yml")
WORK="${CHAOS_WORK:-$(mktemp -d)}"
NEXT_ID=1          # next row id the load generator will insert
FAILS=0

log()  { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { printf '  FAIL: %s\n' "$*"; FAILS=$((FAILS + 1)); }
ok()   { printf '  ok:   %s\n' "$*"; }

psql_() { "${DC[@]}" exec -T postgres psql -U aqueduct -d app -v ON_ERROR_STOP=1 -qAt "$@"; }

wait_for() { # wait_for <seconds> <description> <command...>
  local secs=$1 desc=$2; shift 2
  for _ in $(seq "$secs"); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done
  fail "timeout (${secs}s) waiting for: $desc"; return 1
}

pg_ready()       { "${DC[@]}" exec -T postgres pg_isready -U aqueduct -d app; }
aq_running()     { [ "$("${DC[@]}" ps --format '{{.State}}' aqueduct)" = running ]; }
aq_healthy()     { curl -fsS http://127.0.0.1:8080/healthz; }
aq_container()   { "${DC[@]}" ps -q aqueduct; }

# Insert N rows (ids NEXT_ID..), one transaction each so every row is its own commit.
load() {
  local n=$1 i
  for ((i = 0; i < n; i++)); do
    psql_ -c "INSERT INTO users (id, name) VALUES ($NEXT_ID, 'u$NEXT_ID')" >/dev/null || return 1
    NEXT_ID=$((NEXT_ID + 1))
  done
}

# Background load that tolerates errors (used while a dependency is down).
load_bg() { ( while :; do load 1 2>/dev/null || sleep 0.2; done ) & echo $!; }

fetch_events() { # copy events.jsonl out of the state volume
  "${DC[@]}" cp aqueduct:/var/lib/aqueduct/events.jsonl "$WORK/events.jsonl" 2>/dev/null || : > "$WORK/events.jsonl"
}

# delivered_ids: ids of insert events on public.users, in delivery order
delivered_ids() {
  sed -n 's/.*"table":"users","op":"insert".*"after":{[^}]*"id":\([0-9][0-9]*\).*/\1/p' "$WORK/events.jsonl"
}

# assert_delivery <max_duplicates>: no gaps among 1..NEXT_ID-1, duplicates <= max
assert_delivery() {
  local max_dups=${1:-1} expected=$((NEXT_ID - 1))
  fetch_events
  delivered_ids > "$WORK/ids"
  sort -n "$WORK/ids" | uniq > "$WORK/uniq"
  local total uniq_n missing dups
  total=$(wc -l < "$WORK/ids"); uniq_n=$(wc -l < "$WORK/uniq")
  seq 1 "$expected" | comm -23 - "$WORK/uniq" > "$WORK/missing"
  missing=$(wc -l < "$WORK/missing"); dups=$((total - uniq_n))
  [ "$missing" -eq 0 ] && ok "no gaps ($expected rows delivered)" || fail "$missing rows missing, e.g. $(head -5 "$WORK/missing" | tr '\n' ' ')"
  [ "$dups" -le "$max_dups" ] && ok "duplicates: $dups (limit $max_dups)" || fail "duplicates: $dups exceeds limit $max_dups"
}

# settle: wait until all inserted rows reached the sink (or time out)
settle() {
  local want=$((NEXT_ID - 1))
  for _ in $(seq 60); do
    fetch_events; delivered_ids | sort -un > "$WORK/uniq"
    [ "$(wc -l < "$WORK/uniq")" -ge "$want" ] && return 0
    sleep 1
  done
  log "settle timed out (sink did not catch up)"
}

reset_stack() { # clean slate: fresh volumes, stack up, row counter reset
  "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  "${DC[@]}" up -d --build
  wait_for 90 "postgres ready" pg_ready
  wait_for 90 "aqueduct healthy" aq_healthy
  NEXT_ID=1
}
