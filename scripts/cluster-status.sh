#!/usr/bin/env sh
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
. "$SCRIPT_DIR/cluster-lib.sh"
mode=${1:---status}
case "$mode" in --status|--wait|--primary) ;; *) echo 'Usage: cluster-status.sh [--wait|--primary]' >&2; exit 2;; esac
attempt=0
while :; do
  for node in mongo1 mongo2 mongo3; do
    if [ "$mode" = '--primary' ]; then
      if compose exec -T "$node" mongosh --quiet --host 127.0.0.1 --eval \
        'quit(db.hello().isWritablePrimary ? 0 : 1)' >/dev/null 2>&1; then
        echo "$node"
        exit 0
      fi
    else
      if output=$(compose exec -T "$node" mongosh --quiet --host 127.0.0.1 --eval '
        const s=rs.status();
        print(JSON.stringify({set:s.set,term:s.term,members:s.members.map(m=>({name:m.name,state:m.stateStr,health:m.health,optime:m.optime}))}));
        quit(s.members.length===3 && s.members.every(m=>m.health===1) && s.members.filter(m=>m.stateStr==="PRIMARY").length===1 && s.members.filter(m=>m.stateStr==="SECONDARY").length===2 ? 0 : 1);
      ' 2>/dev/null); then
        echo "$output"
        exit 0
      fi
    fi
  done
  attempt=$((attempt + 1))
  if [ "$mode" != '--wait' ] || [ "$attempt" -ge 60 ]; then
    if [ -n "${output:-}" ]; then echo "$output"; fi
    echo 'Cluster did not satisfy the requested readiness condition' >&2
    exit 1
  fi
  sleep 1
 done
