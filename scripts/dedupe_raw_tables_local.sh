#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DB_PATH="${WEBSITE_DB_PATH:-$ROOT/website.db}"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || "${1:-}" == "help" ]]; then
  echo "Usage: ./scripts/dedupe_raw_tables_local.sh [db_path]"
  echo "Removes exact duplicate rows from raw/staging import tables in local SQLite."
  exit 0
fi

if [[ $# -ge 1 ]]; then
  DB_PATH="$1"
fi

echo "==> Deduping exact raw/staging rows in $DB_PATH"
"$PYTHON_BIN" "$ROOT/db/dedupe_exact_rows.py" --db-path "$DB_PATH"
