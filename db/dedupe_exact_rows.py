#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_TABLES = [
    "raw_bitrix_deals",
    "raw_source_batches",
    "stg_yandex_stats",
    "stg_email_sends",
    "stg_deals_analytics",
    "stg_bitrix_deals_wide",
    "stg_matched_yandex",
    "deals",
    "email_sends",
    "yandex_stats",
    "sheets_sync_log",
]


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def existing_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {str(row[0]) for row in rows}


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({quote_ident(table)})").fetchall()
    return [str(row[1]) for row in rows]


def row_count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}").fetchone()[0])


def dedupe_table(conn: sqlite3.Connection, table: str) -> tuple[int, int]:
    cols = table_columns(conn, table)
    if not cols:
        return 0, 0
    before = row_count(conn, table)
    partition_by = ", ".join(quote_ident(col) for col in cols)
    sql = f'''
DELETE FROM {quote_ident(table)}
WHERE rowid IN (
  SELECT rowid FROM (
    SELECT rowid,
           ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY rowid) AS rn
    FROM {quote_ident(table)}
  ) t
  WHERE rn > 1
)
'''
    conn.execute(sql)
    after = row_count(conn, table)
    return before, before - after


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Remove exact duplicate rows from raw/staging SQLite tables.")
        parser.add_argument("--db-path", default=str(ROOT / "website.db"), help="Path to SQLite DB")
    parser.add_argument(
        "--tables",
        default=",".join(DEFAULT_TABLES),
        help="Comma-separated table list. Missing tables are skipped.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    if not db_path.is_absolute():
        db_path = ROOT / db_path
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    tables = [item.strip() for item in args.tables.split(",") if item.strip()]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        existing = existing_tables(conn)
        total_removed = 0
        print(f"DB: {db_path}", flush=True)
        with conn:
            for table in tables:
                if table not in existing:
                    print(f"SKIP {table}: missing", flush=True)
                    continue
                before, removed = dedupe_table(conn, table)
                total_removed += removed
                print(f"{table}: before={before} removed={removed} after={before - removed}", flush=True)
        print(f"Total exact duplicates removed: {total_removed}", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
