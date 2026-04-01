#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NVM_NODE_DIR="$HOME/.nvm/versions/node/v24.14.1/bin"
if [[ -d "$NVM_NODE_DIR" ]]; then
  export PATH="$NVM_NODE_DIR:$PATH"
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"
DB_PATH="${WEBSITE_DB_PATH:-$ROOT/website.db}"
WRANGLER_CONFIG="$ROOT/wrangler.jsonc"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || "${1:-}" == "help" ]]; then
  echo "Usage: ./scripts/update_d1_from_sqlite_yandex.sh [db_path]"
  echo "Pushes stg_yandex_stats from local SQLite to D1 and optionally triggers rebuild."
  exit 0
fi

if [[ $# -ge 1 ]]; then
  DB_PATH="$1"
fi

echo "==> Pushing stg_yandex_stats from local SQLite to D1"
echo "==> DB: $DB_PATH"
"$PYTHON_BIN" "$ROOT/db/d1/push_from_sqlite.py" \
  --remote \
  --db "$DB_PATH" \
  --tables stg_yandex_stats \
  --no-json \
  --wrangler-config "$WRANGLER_CONFIG" \
  --database-name cybered

if [[ -n "${D1_ANALYTICS_REBUILD_URL:-}" && -n "${ANALYTICS_REBUILD_SECRET:-}" ]]; then
  echo "==> Triggering analytics rebuild"
  curl -fsS -X POST "$D1_ANALYTICS_REBUILD_URL" \
    -H "Authorization: Bearer $ANALYTICS_REBUILD_SECRET" \
    -H "Content-Type: application/json" \
    -d '{}'
  echo
else
  echo "==> Skipping rebuild; set D1_ANALYTICS_REBUILD_URL and ANALYTICS_REBUILD_SECRET to trigger it automatically"
fi
