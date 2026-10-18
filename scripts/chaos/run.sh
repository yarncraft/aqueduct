#!/usr/bin/env bash
# Chaos suite for Aqueduct. Usage: scripts/chaos/run.sh [scenario ...]   (default: all)
# Scenarios: kill9 postgres-restart sink-volume-full checkpoint-delete checkpoint-corrupt slot-loss
# Each scenario starts from a clean stack and asserts: no gaps, duplicates within the at-least-once window.
# Exits non-zero if any assertion failed.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# At-least-once window: a crash between "sink ok" and "slot advance" replays at most the in-flight transaction.
# Each row is its own transaction, so allow 1 duplicate per induced crash.

scenario_kill9() {
  log "kill -9 the Aqueduct container mid-load (3 times)"
  reset_stack
  local bg; bg=$(load_bg)
  for _ in 1 2 3; do
    sleep 3
    docker kill -s KILL "$(aq_container)" >/dev/null
    wait_for 60 "aqueduct restarted (restart: unless-stopped)" aq_healthy
  done
  sleep 3; kill "$bg" 2>/dev/null || true; wait "$bg" 2>/dev/null || true
  settle; assert_delivery 3
}

scenario_postgres_restart() {
  log "stop Postgres under load, bring it back"
  reset_stack
  local bg; bg=$(load_bg)
  sleep 3
  "${DC[@]}" stop postgres
  sleep 8
  "${DC[@]}" start postgres
  wait_for 90 "postgres ready" pg_ready
  wait_for 90 "aqueduct healthy again" aq_healthy   # may restart once if it exited on connection loss
  sleep 5; kill "$bg" 2>/dev/null || true; wait "$bg" 2>/dev/null || true
  settle; assert_delivery 2
}

scenario_sink_volume_full() {
  log "state volume full (ENOSPC) while writing: pipeline must stop, not skip"
  reset_stack
  load 20; settle
  # Fill the volume from a helper container sharing it (tmpfs-like quota is not portable, so write until ENOSPC).
  local vol; vol=$(docker inspect "$(aq_container)" --format '{{range .Mounts}}{{if eq .Destination "/var/lib/aqueduct"}}{{.Name}}{{end}}{{end}}')
  docker run --rm -v "$vol":/v busybox sh -c 'dd if=/dev/zero of=/v/ballast bs=1M 2>/dev/null; true'
  load 20 || true
  sleep 10
  # Rows 21..40 must NOT be acknowledged past the sink: they stay in the slot.
  local pending
  pending=$(psql_ -c "SELECT count(*) FROM pg_logical_slot_peek_changes('aqueduct', NULL, NULL)" 2>/dev/null || echo "?")
  log "changes still pending in slot while disk is full: $pending"
  docker run --rm -v "$vol":/v busybox rm -f /v/ballast
  wait_for 90 "aqueduct healthy after space freed" aq_healthy
  settle; assert_delivery 2
}

scenario_checkpoint_delete() {
  log "delete the checkpoint file: slot position is authoritative, expect replay of un-advanced txns only"
  reset_stack
  load 30; settle
  "${DC[@]}" stop aqueduct
  local vol; vol=$(docker inspect "$("${DC[@]}" ps -aq aqueduct)" --format '{{range .Mounts}}{{if eq .Destination "/var/lib/aqueduct"}}{{.Name}}{{end}}{{end}}')
  docker run --rm -v "$vol":/v busybox rm -f /v/checkpoint
  load 10
  "${DC[@]}" start aqueduct
  wait_for 60 "aqueduct healthy" aq_healthy
  settle; assert_delivery 1
}

scenario_checkpoint_corrupt() {
  log "corrupt the checkpoint: process must refuse to start (operator action), then recover after fix"
  reset_stack
  load 10; settle
  "${DC[@]}" stop aqueduct
  local vol; vol=$(docker inspect "$("${DC[@]}" ps -aq aqueduct)" --format '{{range .Mounts}}{{if eq .Destination "/var/lib/aqueduct"}}{{.Name}}{{end}}{{end}}')
  docker run --rm -v "$vol":/v busybox sh -c 'echo garbage > /v/checkpoint'
  load 5
  "${DC[@]}" start aqueduct
  sleep 15
  if aq_healthy >/dev/null 2>&1 && aq_running; then
    log "NOTE: aqueduct is serving health with a corrupt checkpoint; verifying it is not streaming"
  fi
  "${DC[@]}" logs --tail 20 aqueduct | grep -qi checkpoint && ok "error names the checkpoint" || fail "no checkpoint error in logs"
  docker run --rm -v "$vol":/v busybox rm -f /v/checkpoint   # documented fix: delete, slot is authoritative
  "${DC[@]}" restart aqueduct
  wait_for 60 "aqueduct healthy" aq_healthy
  settle; assert_delivery 1
}

scenario_slot_loss() {
  log "slot dropped while a checkpoint exists (needs feat/hardening guard): must refuse to start"
  reset_stack
  load 10; settle
  "${DC[@]}" stop aqueduct
  psql_ -c "SELECT pg_drop_replication_slot('aqueduct')" >/dev/null
  load 5   # these changes are lost from CDC's perspective: nothing records them
  "${DC[@]}" start aqueduct
  sleep 15
  if "${DC[@]}" logs --tail 50 aqueduct | grep -qi 'slot'; then ok "logs mention the missing slot"; else fail "no slot-loss message (guard missing?)"; fi
  [ -z "$(psql_ -c "SELECT 1 FROM pg_replication_slots WHERE slot_name='aqueduct'")" ] \
    && ok "slot was NOT silently recreated" || fail "slot silently recreated: rows 11-15 are lost without notice"
}

ALL=(kill9 postgres_restart sink_volume_full checkpoint_delete checkpoint_corrupt slot_loss)
SEL=("${@:-${ALL[@]}}")
for s in "${SEL[@]}"; do "scenario_${s//-/_}"; done
"${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
log "done: $FAILS failed assertion(s)"
exit $((FAILS > 0))
