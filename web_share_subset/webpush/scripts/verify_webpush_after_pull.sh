#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

NVM_NODE_DIR="$HOME/.nvm/versions/node/v24.14.1/bin"
if [[ -d "$NVM_NODE_DIR" ]]; then
  export PATH="$NVM_NODE_DIR:$PATH"
fi

echo "Repo: $REPO_ROOT"
echo "Node: $(node -v)"
echo "npm: $(npm -v)"

echo "==> npm ci"
npm ci

echo "==> npm run build"
npm run build

echo "==> dev server smoke test"
DEV_LOG="$(mktemp -t webpush-dev-log.XXXXXX)"
PID=""
cleanup() {
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

npm run dev -- --host 127.0.0.1 --port 4173 --strictPort >"$DEV_LOG" 2>&1 &
PID=$!
sleep 4

if kill -0 "$PID" 2>/dev/null; then
  echo "dev server started successfully on http://127.0.0.1:4173"
else
  echo "dev server failed to start. Last log lines:"
  tail -n 80 "$DEV_LOG" || true
  exit 1
fi

echo "All checks passed."
