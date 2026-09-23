#!/usr/bin/env sh
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
. "$SCRIPT_DIR/cluster-lib.sh"
[ "$#" -eq 1 ] || { echo 'Usage: heal-partition.sh mongo1|mongo2|mongo3' >&2; exit 2; }
validate_node "$1"
id=$(node_id "$1")
[ -n "$id" ] || { echo 'Node container does not exist' >&2; exit 1; }
project=$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project"}}' "$id")
network=$(docker network ls --filter "label=com.docker.compose.project=$project" --filter 'label=com.docker.compose.network=mongo-rs-network' --format '{{.Name}}')
[ -n "$network" ] || { echo 'Project replica-set network not found' >&2; exit 1; }
attached=$(docker inspect --format '{{range $k, $v := .NetworkSettings.Networks}}{{println $k}}{{end}}' "$id")
is_attached=false
for existing in $attached; do [ "$existing" != "$network" ] || is_attached=true; done
if [ "$is_attached" = false ]; then
  docker network connect --alias "$1" "$network" "$id"
fi
"$SCRIPT_DIR/cluster-status.sh" --wait
