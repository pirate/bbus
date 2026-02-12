#!/usr/bin/env bash
set -euo pipefail

(
  uv run ruff format
  uv run ruff check --fix
  uv run ty check
  uv run pyright
  uv run pytest
  shopt -s nullglob
  for example_file in examples/*.py; do
    uv run python "$example_file"
  done
) &
python_pid=$!

(
  cd bubus-ts
  pnpm run lint
  pnpm run test
  shopt -s nullglob
  for example_file in examples/*.ts; do
    node --import tsx "$example_file"
  done
) &
ts_pid=$!

wait "$python_pid"
wait "$ts_pid"

# Perf suites run at the end, outside the default parallel checks.
uv run tests/performance_runtime.py
(
  cd bubus-ts
  pnpm run perf
)
