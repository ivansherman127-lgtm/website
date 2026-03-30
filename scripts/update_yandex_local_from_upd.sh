#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DB_PATH="${DEVED_DB_PATH:-$ROOT/deved.db}"

if [[ $# -lt 1 ]]; then
  echo "Usage: ./scripts/update_yandex_local_from_upd.sh <path/to/yandex_upd.csv> [db_path]" >&2
  exit 1
fi

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || "${1:-}" == "help" ]]; then
  echo "Usage: ./scripts/update_yandex_local_from_upd.sh <path/to/yandex_upd.csv> [db_path]"
  echo "Updates local SQLite only; does not push to D1."
  exit 0
fi

CSV_PATH="$1"
if [[ $# -ge 2 ]]; then
  DB_PATH="$2"
fi

if [[ ! -f "$CSV_PATH" ]]; then
  echo "CSV not found: $CSV_PATH" >&2
  exit 1
fi

echo "==> Updating local SQLite from $CSV_PATH"
echo "==> DB: $DB_PATH"
"$PYTHON_BIN" "$ROOT/db/upsert_yandex_from_csv.py" "$CSV_PATH" --db-path "$DB_PATH" --skip-push --skip-rebuild
