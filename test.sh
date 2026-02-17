#!/usr/bin/env bash
set -euo pipefail

uv run ruff format bubus examples tests
export UV_NO_SYNC=1
uv run ruff check --fix bubus examples tests
uv run ty check bubus examples tests &
ty_pid=$!
uv run pyright &
pyright_pid=$!
wait "$ty_pid"
wait "$pyright_pid"

(
  cd bubus-ts
  pnpm run lint
)

# Run Python and TypeScript test phases sequentially to avoid cross-runtime
# resource contention that can cause performance-threshold flakes.
uv run pytest

(
  cd bubus-ts
  pnpm run test
)

shopt -s nullglob
python_example_pids=()
for example_file in examples/*.py; do
  timeout 120 uv run python "$example_file" &
  python_example_pids+=("$!")
done
for pid in "${python_example_pids[@]}"; do
  wait "$pid"
done

(
  cd bubus-ts
  shopt -s nullglob
  ts_example_pids=()
  for example_file in examples/*.ts; do
    timeout 120 node --import tsx "$example_file" &
    ts_example_pids+=("$!")
  done
  for pid in "${ts_example_pids[@]}"; do
    wait "$pid"
  done
)

# Perf suites are expensive and can push total runtime well past the main CI budget.
# Run them explicitly with RUN_PERF=1 (or use ./test_perf.sh).
if [[ "${RUN_PERF:-0}" == "1" ]]; then
  uv run tests/performance_runtime.py
  (
    cd bubus-ts
    pnpm run perf
  )
else
  echo "Skipping perf suites (set RUN_PERF=1 to include them)."
fi
