#!/usr/bin/env bash
set -euo pipefail

uv run ruff format
uv run ruff check --fix
uv run ty check bubus examples
uv run pyright

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
for example_file in examples/*.py; do
  timeout 120 uv run python "$example_file"
done

(
  cd bubus-ts
  shopt -s nullglob
  for example_file in examples/*.ts; do
    timeout 120 node --import tsx "$example_file"
  done
)

# Perf suites run at the end, outside the default parallel checks.
uv run tests/performance_runtime.py
(
  cd bubus-ts
  pnpm run perf
)
