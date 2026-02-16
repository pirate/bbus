#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

LOG_FILE="$(mktemp /tmp/bubus-playwright-perf.XXXXXX).log"
trap 'rm -f "$LOG_FILE"' EXIT

run_playwright_perf() {
  npx --yes --package=playwright -c 'PW_BIN="$(command -v playwright)"; PW_NODE_MODULES="$(cd "$(dirname "$PW_BIN")/.." && pwd)"; NODE_PATH="$PW_NODE_MODULES" playwright test tests/performance.browser.spec.cjs --config=playwright.perf.config.cjs --project=browser-perf --workers=1 --reporter=line --output=/tmp/bubus-playwright-results'
}

resolve_fallback_executable() {
  local candidate
  for candidate in \
    /usr/bin/chromium \
    /usr/bin/chromium-browser \
    /usr/bin/google-chrome \
    '/Applications/Chromium.app/Contents/MacOS/Chromium' \
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
  do
    if [[ -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

echo "[perf:browser] trying Playwright default chromium resolution..."
if run_playwright_perf 2>&1 | tee "$LOG_FILE"; then
  exit 0
fi

if ! grep -Eq "Executable doesn't exist|Failed to launch|Failed to download chromium|does not support chromium" "$LOG_FILE"; then
  echo "[perf:browser] failed for a non-launch reason; not attempting executable fallback." >&2
  exit 1
fi

FALLBACK_EXECUTABLE="$(resolve_fallback_executable || true)"
if [[ -z "$FALLBACK_EXECUTABLE" ]]; then
  echo "[perf:browser] chromium fallback path not found. Tried /usr/bin/chromium, /usr/bin/chromium-browser, /usr/bin/google-chrome, /Applications/Chromium.app, /Applications/Google Chrome.app" >&2
  exit 1
fi

echo "[perf:browser] retrying with chromium executable: $FALLBACK_EXECUTABLE"
PW_CHROMIUM_EXECUTABLE_PATH="$FALLBACK_EXECUTABLE" run_playwright_perf
