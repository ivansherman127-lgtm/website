#!/usr/bin/env bash
set -euo pipefail

WEBPUSH_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MAIN_ROOT="$(cd "$WEBPUSH_ROOT/../.." && pwd)"

# Prefer modern Node/npm for wrangler commands.
NVM_NODE_DIR="$HOME/.nvm/versions/node/v24.14.1/bin"
if [[ -d "$NVM_NODE_DIR" ]]; then
  export PATH="$NVM_NODE_DIR:$PATH"
fi

PYTHON_BIN="$MAIN_ROOT/.venv/bin/python3"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

WEBPUSH_WRANGLER_CONFIG="$WEBPUSH_ROOT/wrangler.toml"
MAIN_WEB_WRANGLER_CONFIG="$MAIN_ROOT/web/wrangler.toml"
DB_NAME="${D1_DATABASE_NAME:-cybered}"

usage() {
  cat <<'USAGE'
Usage:
  ./scripts/sync_d1_from_sqlite.sh check
  ./scripts/sync_d1_from_sqlite.sh migrate
  ./scripts/sync_d1_from_sqlite.sh push
  ./scripts/sync_d1_from_sqlite.sh push-local
  ./scripts/sync_d1_from_sqlite.sh rebuild
  ./scripts/sync_d1_from_sqlite.sh full

What each mode does:
  check      Validate required files/tools and print versions.
  migrate    Apply D1 migrations remotely (uses ../../web/wrangler.toml).
  push       Push local SQLite tables + JSON to remote D1 with --strict.
  push-local Push local SQLite tables + JSON to local D1 (no --remote).
  push-raw   Upsert raw Bitrix into SQLite, rebuild marts, push marts to D1.
  rebuild    POST cloud rebuild endpoint (env required).
  full       migrate + upsert raw Bitrix + run slices + push + rebuild.

Required for rebuild/full:
  D1_ANALYTICS_REBUILD_URL
  ANALYTICS_REBUILD_SECRET

Optional env:
  D1_DATABASE_NAME (default: cybered)
USAGE
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

run_check() {
  need_cmd "$PYTHON_BIN"
  need_cmd npx
  [[ -f "$WEBPUSH_WRANGLER_CONFIG" ]] || { echo "Missing $WEBPUSH_WRANGLER_CONFIG" >&2; exit 1; }
  [[ -f "$MAIN_WEB_WRANGLER_CONFIG" ]] || { echo "Missing $MAIN_WEB_WRANGLER_CONFIG" >&2; exit 1; }
  [[ -f "$MAIN_ROOT/db/d1/push_from_sqlite.py" ]] || { echo "Missing $MAIN_ROOT/db/d1/push_from_sqlite.py" >&2; exit 1; }
  [[ -f "$MAIN_ROOT/db/run_all_slices.py" ]] || { echo "Missing $MAIN_ROOT/db/run_all_slices.py" >&2; exit 1; }
  [[ -f "$MAIN_ROOT/db/upsert_raw_bitrix_from_union.py" ]] || { echo "Missing $MAIN_ROOT/db/upsert_raw_bitrix_from_union.py" >&2; exit 1; }

  echo "WEBPUSH_ROOT=$WEBPUSH_ROOT"
  echo "MAIN_ROOT=$MAIN_ROOT"
  echo "DB_NAME=$DB_NAME"
  echo "node=$(node -v 2>/dev/null || echo missing)"
  echo "npm=$(npm -v 2>/dev/null || echo missing)"
  echo "python=$($PYTHON_BIN -V 2>&1 || true)"
}

run_migrate() {
  echo "==> Applying D1 migrations to remote ($DB_NAME)"
  (cd "$WEBPUSH_ROOT" && npx wrangler d1 migrations apply "$DB_NAME" --remote --config "$MAIN_WEB_WRANGLER_CONFIG")
}

run_upsert_raw() {
  echo "==> Upserting raw Bitrix rows into local SQLite"
  "$PYTHON_BIN" "$MAIN_ROOT/db/upsert_raw_bitrix_from_union.py"
}

run_slices() {
  echo "==> Rebuilding local marts/slices from SQLite"
  "$PYTHON_BIN" "$MAIN_ROOT/db/run_all_slices.py"
}

run_push_remote() {
  echo "==> Pushing local SQLite + JSON to remote D1"
  "$PYTHON_BIN" "$MAIN_ROOT/db/d1/push_from_sqlite.py" \
    --remote \
    --strict \
    --wrangler-config "$WEBPUSH_WRANGLER_CONFIG" \
    --database-name "$DB_NAME"
}

run_push_local() {
  echo "==> Pushing local SQLite + JSON to local D1"
  "$PYTHON_BIN" "$MAIN_ROOT/db/d1/push_from_sqlite.py" \
    --strict \
    --wrangler-config "$WEBPUSH_WRANGLER_CONFIG" \
    --database-name "$DB_NAME"
}



run_rebuild() {
  local required="${1:-required}"
  local url="${D1_ANALYTICS_REBUILD_URL:-}"
  local secret="${ANALYTICS_REBUILD_SECRET:-}"
  if [[ -z "$url" || -z "$secret" ]]; then
    if [[ "$required" == "required" ]]; then
      echo "Missing D1_ANALYTICS_REBUILD_URL or ANALYTICS_REBUILD_SECRET for rebuild." >&2
      exit 1
    else
      echo "==> Skipping cloud rebuild (D1_ANALYTICS_REBUILD_URL / ANALYTICS_REBUILD_SECRET not set)"
      return 0
    fi
  fi
  echo "==> Triggering cloud rebuild: $url"
  curl -fsS -X POST "$url" \
    -H "Authorization: Bearer $secret" \
    -H "Content-Type: application/json" \
    -d '{}'
  echo
}

MODE="${1:-full}"
case "$MODE" in
  check)
    run_check
    ;;
  migrate)
    run_migrate
    ;;
  push)
    run_push_remote
    ;;
  push-local)
    run_push_local
    ;;
  push-raw)
    run_upsert_raw
    run_slices
    run_push_remote
    ;;
  rebuild)
    run_rebuild required
    ;;
  full)
    run_migrate
    run_upsert_raw
    run_slices
    run_push_remote
    run_rebuild optional
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    usage
    exit 1
    ;;
esac
