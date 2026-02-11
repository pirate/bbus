#!/usr/bin/env bash
set -euo pipefail

PERF_LOCK_DIR="${TMPDIR:-/tmp}/bubus-test-perf.lock"
SYNC_DIR="${TMPDIR:-/tmp}/bubus-test-sync-$$"
PYTEST_DONE_FILE="$SYNC_DIR/python_pytest_done"

mkdir -p "$SYNC_DIR"
cleanup_sync() {
  rm -rf "$SYNC_DIR"
}
trap cleanup_sync EXIT

acquire_perf_lock() {
  while ! mkdir "$PERF_LOCK_DIR" 2>/dev/null; do
    sleep 0.1
  done
}

release_perf_lock() {
  rmdir "$PERF_LOCK_DIR" 2>/dev/null || true
}

(
  uv run ruff format
  uv run ruff check --fix
  # uv run ty check
  uv run pyright
  uv run pytest
  : > "$PYTEST_DONE_FILE"

  acquire_perf_lock
  trap release_perf_lock EXIT
  uv run python tests/performance_runtime.py --json
  shopt -s nullglob
  python_examples=(examples/*.py)
  for example_file in "${python_examples[@]}"; do
    uv run python "$example_file"
  done
  trap - EXIT
  release_perf_lock
) &
python_pid=$!

(
  cd bubus-ts
  pnpm run lint
  pnpm run test
  while [ ! -f "$PYTEST_DONE_FILE" ]; do
    sleep 0.1
  done

  acquire_perf_lock
  trap release_perf_lock EXIT
  pnpm run perf
  shopt -s nullglob
  ts_examples=(examples/*.ts)
  for example_file in "${ts_examples[@]}"; do
    node --import tsx "$example_file"
  done
  trap - EXIT
  release_perf_lock
) &
ts_pid=$!

for _ in 1 2; do
  if ! wait -n; then
    kill "$python_pid" "$ts_pid" 2>/dev/null || true
    wait "$python_pid" "$ts_pid" 2>/dev/null || true
    exit 1
  fi
done
