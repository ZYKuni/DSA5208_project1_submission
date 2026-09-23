#!/usr/bin/env sh
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
. "$SCRIPT_DIR/cluster-lib.sh"
[ "$#" -eq 1 ] || { echo 'Usage: start-node.sh mongo1|mongo2|mongo3' >&2; exit 2; }
validate_node "$1"
compose start "$1"
"$SCRIPT_DIR/cluster-status.sh" --wait
