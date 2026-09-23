#!/usr/bin/env sh
# Sourced by control scripts; never delete volumes or guess a node name.
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
compose() { docker compose -f "$PROJECT_DIR/docker-compose.yml" "$@"; }
validate_node() {
  case "$1" in mongo1|mongo2|mongo3) ;; *) echo 'Node must be mongo1, mongo2, or mongo3' >&2; exit 2;; esac
}
node_id() {
  compose ps -a -q "$1"
}
