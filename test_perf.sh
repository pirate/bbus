#!/usr/bin/env bash
set -euo pipefail

uv run tests/performance_runtime.py
(
  cd bubus-ts
  pnpm run perf
)
