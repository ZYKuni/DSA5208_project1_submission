#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
MAX_ATTEMPTS=60
WAIT_SECONDS=2

compose() {
  docker compose -f "$PROJECT_DIR/docker-compose.yml" "$@"
}

fail() {
  echo "ERROR: $*" >&2
  echo "Inspect the containers with: docker compose -f $PROJECT_DIR/docker-compose.yml logs" >&2
  exit 1
}

wait_for_mongod() {
  service=$1
  attempt=1

  while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
    if compose exec -T "$service" mongosh --quiet \
      --host 127.0.0.1 --port 27017 \
      --eval 'quit(db.adminCommand({ ping: 1 }).ok ? 0 : 2)' \
      >/dev/null 2>&1; then
      echo "  $service is accepting connections."
      return 0
    fi

    attempt=$((attempt + 1))
    sleep "$WAIT_SECONDS"
  done

  return 1
}

replica_set_is_healthy() {
  compose exec -T mongo1 mongosh --quiet \
    --host 127.0.0.1 --port 27017 \
    --eval '
      try {
        const members = rs.status().members;
        const healthy = members.filter((member) => member.health === 1).length;
        const primaries = members.filter((member) => member.stateStr === "PRIMARY").length;
        const secondaries = members.filter((member) => member.stateStr === "SECONDARY").length;
        quit(healthy === 3 && primaries === 1 && secondaries === 2 ? 0 : 1);
      } catch (error) {
        quit(1);
      }
    ' >/dev/null 2>&1
}

command -v docker >/dev/null 2>&1 || fail "Docker is not installed or is not on PATH."
docker info >/dev/null 2>&1 || fail "Docker is not running. Start Docker Desktop and retry."
docker compose version >/dev/null 2>&1 || fail "Docker Compose v2 is unavailable."

echo "Starting mongo1, mongo2, and mongo3..."
compose up -d mongo1 mongo2 mongo3

echo "Waiting for all MongoDB processes..."
for service in mongo1 mongo2 mongo3; do
  wait_for_mongod "$service" || fail "$service did not become ready in time."
done

echo "Initializing replica set when required..."
compose exec -T mongo1 mongosh --quiet \
  --host 127.0.0.1 --port 27017 \
  --file /opt/project/init-replica-set.js \
  || fail "Replica-set initialization failed."

echo "Waiting for one PRIMARY and two SECONDARY members..."
attempt=1
while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
  if replica_set_is_healthy; then
    echo "Replica set rs0 is healthy."
    compose exec -T mongo1 mongosh --quiet \
      --host 127.0.0.1 --port 27017 \
      --eval '
        const status = rs.status();
        print(`Replica set: ${status.set}`);
        status.members
          .sort((left, right) => left._id - right._id)
          .forEach((member) => print(`${member.name}: ${member.stateStr}, health=${member.health}`));
      '
    exit 0
  fi

  attempt=$((attempt + 1))
  sleep "$WAIT_SECONDS"
done

fail "Replica set did not reach 1 PRIMARY + 2 SECONDARY members in time."
