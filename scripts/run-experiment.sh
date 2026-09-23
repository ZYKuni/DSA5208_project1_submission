#!/usr/bin/env sh
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
cd "$PROJECT_DIR"
SOURCE_BASE_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo unavailable)
SOURCE_WORKTREE_STATUS=$(git status --porcelain 2>/dev/null || echo unavailable)
export SOURCE_BASE_COMMIT SOURCE_WORKTREE_STATUS
exec docker compose -f "$PROJECT_DIR/docker-compose.yml" run --rm --no-deps \
  -e SOURCE_BASE_COMMIT -e SOURCE_WORKTREE_STATUS runner \
  python -m experiments.run_matrix "$@"
