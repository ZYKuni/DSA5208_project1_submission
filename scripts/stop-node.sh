#!/usr/bin/env sh
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
. "$SCRIPT_DIR/cluster-lib.sh"
[ "$#" -eq 1 ] || { echo 'Usage: stop-node.sh mongo1|mongo2|mongo3' >&2; exit 2; }
validate_node "$1"
# Graceful stop: deliberately NOT a crash/rollback simulation.
compose stop "$1"
