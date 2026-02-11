#!/usr/bin/env bash
set -euo pipefail

(
  uv run ruff format
  uv run ruff check --fix
  # uv run ty check
  uv run pyright
  uv run pytest
) &
python_pid=$!

(
  cd bubus-ts
  pnpm run lint
  pnpm run test
) &
ts_pid=$!

for _ in 1 2; do
  if ! wait -n; then
    kill "$python_pid" "$ts_pid" 2>/dev/null || true
    wait "$python_pid" "$ts_pid" 2>/dev/null || true
    exit 1
  fi
done
