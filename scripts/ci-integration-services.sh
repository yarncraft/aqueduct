#!/usr/bin/env bash
# Starts (or stops) the Kafka, MongoDB and Postgres containers the integration suite needs, with the
# env the tests expect. Shared by .github/workflows/ci.yml and scripts/ci-linux.sh.
#
#   scripts/ci-integration-services.sh up <dir>     # <dir> receives the TLS material and services.env
#   scripts/ci-integration-services.sh down <dir>   # stops what `up <dir>` started, and only that
#
# Every run gets its own id: container and volume names carry it and the published ports are free
# ones, so several runs (several agents on one podman machine) never collide, and `down` cannot
# remove another run's containers. `up` writes <dir>/services.env: source it for the test variables.
#
# CT is the container CLI (docker by default, podman works too).
set -euo pipefail
CT=${CT:-docker}
export MSYS_NO_PATHCONV=1  # Git Bash on Windows would rewrite /certs-style arguments

wait_for() { # <label> <command...>: retry for 60 s
  local label=$1; shift
  for _ in $(seq 1 30); do "$@" >/dev/null 2>&1 && return 0; sleep 2; done
  echo "$label never became ready" >&2; return 1
}

used_ports=" "
free_port() { # a port nothing listens on, and that this run has not handed out yet
  local p
  while :; do
    p=$((20000 + RANDOM % 40000))
    case $used_ports in *" $p "*) continue ;; esac
    if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then continue; fi
    used_ports="$used_ports$p "; echo "$p"; return
  done
}

up() {
  local d; d=$(mkdir -p "$1" && cd "$1" && pwd); cd "$d"
  local run=${AQ_RUN:-$(date +%s)-$RANDOM}
  local kafka=aq-$run-kafka mongo=aq-$run-mongo tls=aq-$run-pg-tls plain=aq-$run-pg-plain trust=aq-$run-pg-trust
  local vol=aq-$run-certs gen=aq-$run-certgen
  local kp mp tp pp rp
  kp=$(free_port); used_ports="$used_ports$kp "
  mp=$(free_port); used_ports="$used_ports$mp "
  tp=$(free_port); used_ports="$used_ports$tp "
  pp=$(free_port); used_ports="$used_ports$pp "
  rp=$(free_port)
  # Written first, so `down` can clean up even when `up` fails half-way.
  cat > "$d/services.env" <<ENV
AQ_RUN=$run
AQUEDUCT_TEST_KAFKA=localhost:$kp
AQUEDUCT_KAFKA_BROKERS=localhost:$kp
MONGO_URI=mongodb://localhost:$mp/?replicaSet=rs0&directConnection=true
AQUEDUCT_TEST_PG_TLS=localhost:$tp
AQUEDUCT_TEST_PG_PLAIN=localhost:$pp
AQUEDUCT_TEST_PG_TRUST=localhost:$rp
ENV

  # The broker listens on the published port itself, so the advertised address works from the host.
  $CT run -d --name $kafka -p $kp:$kp \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:$kp,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:$kp \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    docker.io/apache/kafka:3.8.0 >/dev/null
  # mongod cannot be a service container: it needs --replSet.
  $CT run -d --name $mongo -p $mp:$mp docker.io/library/mongo:7 --replSet rs0 --bind_ip_all --port $mp >/dev/null

  # Throwaway CA, a second unrelated CA, and a server cert whose SAN is localhost only, made inside a
  # container so the host needs no openssl. They live in a named volume (postgres refuses a key it
  # does not own, and the image runs as uid 999) and are copied out for the tests.
  $CT run --name $gen -v $vol:/c --entrypoint sh docker.io/library/postgres:16 -ec '
    cd /c
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=aq-test-ca" -keyout ca.key -out ca.pem 2>/dev/null
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=aq-other-ca" -keyout other.key -out other.pem 2>/dev/null
    openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout server.key -out server.csr 2>/dev/null
    printf "subjectAltName=DNS:localhost
" > san.ext
    openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 -extfile san.ext -out server.crt 2>/dev/null
    chown 999:999 server.key server.crt; chmod 600 server.key; chmod 644 ca.pem other.pem'
  $CT cp $gen:/c/. "$(cygpath -w "$d" 2>/dev/null || echo "$d")"
  $CT rm -f $gen >/dev/null
  # CA paths as the host sees them; scripts/ci-linux.sh overrides them for the test container.
  printf 'AQUEDUCT_TEST_PG_CA=%s/ca.pem\nAQUEDUCT_TEST_PG_WRONG_CA=%s/other.pem\n' "$d" "$d" >> "$d/services.env"

  common="-e POSTGRES_PASSWORD=aq docker.io/library/postgres:16 -c wal_level=logical -c max_replication_slots=20 -c max_wal_senders=10"
  $CT run -d --name $tls -p $tp:5432 -v $vol:/certs:ro $common -c ssl=on -c ssl_cert_file=/certs/server.crt -c ssl_key_file=/certs/server.key >/dev/null
  $CT run -d --name $plain -p $pp:5432 $common >/dev/null
  # Trust authentication, for the replication client, which has no password auth yet.
  $CT run -d --name $trust -p $rp:5432 -e POSTGRES_HOST_AUTH_METHOD=trust \
    docker.io/library/postgres:16 -c wal_level=logical -c max_replication_slots=20 -c max_wal_senders=10 >/dev/null

  for c in $tls $plain $trust; do
    wait_for "$c" $CT exec $c pg_isready -U postgres
    # pg_isready can pass during init, before the final restart: retry the DDL.
    wait_for "$c publication" $CT exec $c psql -U postgres -c 'CREATE PUBLICATION aq_pub FOR ALL TABLES'
  done
  # The roles of replication_auth_it and azure_identity_it, each with its own password method. The
  # pg_hba lines go first (the first matching line wins).
  for c in $tls $plain; do
    $CT exec $c psql -U postgres -v ON_ERROR_STOP=1 \
      -c "SET password_encryption = 'md5'" -c "CREATE ROLE aq_md5 LOGIN REPLICATION PASSWORD 'aq'" \
      -c "RESET password_encryption" \
      -c "CREATE ROLE aq_clear LOGIN REPLICATION PASSWORD 'aq'" \
      -c "CREATE ROLE aq_scram LOGIN REPLICATION PASSWORD 'aq'" >/dev/null
    $CT exec $c sh -ec 'f=$(psql -U postgres -Atc "SHOW hba_file")
      { printf "host all aq_md5 all md5\nhost all aq_clear all password\nhost all aq_scram all scram-sha-256\n"; cat "$f"; } > /tmp/hba
      cat /tmp/hba > "$f"
      psql -U postgres -c "SELECT pg_reload_conf()"' >/dev/null
  done
  wait_for kafka $CT exec $kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:$kp --list
  wait_for mongod $CT exec $mongo mongosh --quiet --port $mp --eval 'db.runCommand({ping:1}).ok'
  $CT exec $mongo mongosh --quiet --port $mp --eval "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:$mp'}]})" >/dev/null
  wait_for "mongo primary" bash -c "$CT exec $mongo mongosh --quiet --port $mp --eval 'rs.status().members[0].stateStr' | grep -q PRIMARY"
  echo "services up (run $run); env in $d/services.env"
}

down() {
  local d=$1 run
  [ -f "$d/services.env" ] || { echo "no $d/services.env: nothing to stop" >&2; return 0; }
  run=$(sed -n 's/^AQ_RUN=//p' "$d/services.env")
  local names="aq-$run-kafka aq-$run-mongo aq-$run-pg-tls aq-$run-pg-plain aq-$run-pg-trust aq-$run-certgen" c
  for c in $names; do echo "--- $c"; $CT logs --tail 50 $c 2>&1 || true; done > "${CI_SERVICE_LOGS:-/dev/null}" 2>&1 || true
  $CT rm -f $names >/dev/null 2>&1 || true
  $CT volume rm -f aq-$run-certs >/dev/null 2>&1 || true
}

case ${1:-} in
  up) up "${2:?usage: $0 up <dir>}" ;;
  down) down "${2:?usage: $0 down <dir>}" ;;
  *) echo "usage: $0 up <dir> | down <dir>" >&2; exit 1 ;;
esac
