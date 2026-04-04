"""
Bitrix24 REST webhook → SQLite full CRM ingest.

Downloads ALL deals and contacts from Bitrix24 into local SQLite tables:
  raw_b24_deals     — one row per deal, all fields as TEXT
  raw_b24_contacts  — one row per contact, all fields as TEXT

Array/object values (phones, emails, addresses) are serialized as JSON strings.
The table schema is derived dynamically from the API response, so all custom
UF_CRM_* fields are captured without any hardcoding.

Usage:
    B24_WEBHOOK_URL=https://your.bitrix24.ru/rest/1/TOKEN/ python -m db.b24_fetch_crm
    B24_WEBHOOK_URL=... python -m db.b24_fetch_crm --entity deals
    B24_WEBHOOK_URL=... python -m db.b24_fetch_crm --entity contacts

Environment variables:
    B24_WEBHOOK_URL   (required) — webhook base URL ending in /
    WEBSITE_DB_PATH   (optional) — path to SQLite file; default: ./website.db
    B24_PAGE_SIZE     (optional) — records per page; default: 50 (B24 maximum)
    B24_THROTTLE      (optional) — seconds between pages; default: 0.3
"""

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_DB_PATH = os.environ.get(
    "WEBSITE_DB_PATH",
    str(Path(__file__).resolve().parent.parent / "website.db"),
)
PAGE_SIZE = int(os.environ.get("B24_PAGE_SIZE", "50"))
THROTTLE = float(os.environ.get("B24_THROTTLE", "0.3"))
MAX_RETRIES = 3
RETRY_DELAY = 5.0  # seconds before retrying after a rate-limit or 5xx error


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _call(webhook_url: str, method: str, params: Dict[str, Any] = {}) -> Dict[str, Any]:
    """POST to a Bitrix24 REST method and return the parsed response."""
    url = webhook_url.rstrip("/") + "/" + method
    data = json.dumps(params).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = json.load(resp)
            if "error" in body:
                code = body.get("error", "")
                desc = body.get("error_description", body.get("error", ""))
                # QUERY_LIMIT_EXCEEDED — back off and retry
                if "LIMIT" in str(code).upper() and attempt < MAX_RETRIES:
                    print(f"  [rate limit] sleeping {RETRY_DELAY}s before retry {attempt}/{MAX_RETRIES}…")
                    time.sleep(RETRY_DELAY)
                    continue
                raise RuntimeError(f"B24 API error [{code}]: {desc}")
            return body
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                print(f"  [HTTP {exc.code}] sleeping {RETRY_DELAY}s before retry {attempt}/{MAX_RETRIES}…")
                time.sleep(RETRY_DELAY)
                continue
            raise
    raise RuntimeError(f"B24 call to {method} failed after {MAX_RETRIES} retries")


def _serialize(value: Any) -> Optional[str]:
    """Flatten a value to TEXT for SQLite storage."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


# ---------------------------------------------------------------------------
# Fetch helpers — paginated list
# ---------------------------------------------------------------------------

def fetch_all(
    webhook_url: str,
    method: str,
    select: List[str],
    label: str,
) -> List[Dict[str, Any]]:
    """Fetch all records for a crm.*.list method with pagination."""
    rows: List[Dict[str, Any]] = []
    start = 0
    t0 = time.time()

    while True:
        resp = _call(webhook_url, method, {
            "start": start,
            "ORDER": {"ID": "ASC"},
            "SELECT": select,
        })

        page: List[Dict[str, Any]] = resp.get("result", [])
        rows.extend(page)
        total = resp.get("total", "?")
        elapsed = time.time() - t0
        rate = len(rows) / elapsed if elapsed > 0 else 0
        print(
            f"  {label}: {len(rows)}/{total} fetched  "
            f"({elapsed:.0f}s elapsed, {rate:.1f} rec/s)",
            end="\r",
            flush=True,
        )

        if len(page) < PAGE_SIZE:
            break
        start += PAGE_SIZE
        time.sleep(THROTTLE)

    print()  # newline after \r
    return rows


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def _col_name(key: str) -> str:
    """Quote a column name for SQLite."""
    return '"' + key.replace('"', '""') + '"'


def ensure_table(conn: sqlite3.Connection, table: str, columns: List[str]) -> None:
    """Create table if not exists; ALTER TABLE to add any new columns."""
    cur = conn.cursor()
    # Build CREATE TABLE with all known columns
    col_defs = ", ".join(
        f"{_col_name(c)} TEXT" for c in columns
    )
    cur.execute(
        f'CREATE TABLE IF NOT EXISTS "{table}" ("ID" TEXT PRIMARY KEY, {col_defs})'
    )

    # Check for columns that exist in data but not yet in the table
    cur.execute(f'PRAGMA table_info("{table}")')
    existing = {row[1] for row in cur.fetchall()}
    for col in columns:
        if col not in existing:
            print(f"  [schema] ALTER TABLE {table} ADD COLUMN {col}")
            cur.execute(f'ALTER TABLE "{table}" ADD COLUMN {_col_name(col)} TEXT')

    conn.commit()


def upsert_rows(
    conn: sqlite3.Connection,
    table: str,
    rows: List[Dict[str, Any]],
) -> int:
    """INSERT OR REPLACE rows into table; returns count written."""
    if not rows:
        return 0

    # Collect all keys that appear across all rows (some pages may have extra fields)
    all_keys: List[str] = []
    seen: set = set()
    for row in rows:
        for k in row:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    ensure_table(conn, table, [k for k in all_keys if k != "ID"])

    col_list = ", ".join(_col_name(k) for k in all_keys)
    placeholders = ", ".join("?" for _ in all_keys)
    sql = f'INSERT OR REPLACE INTO "{table}" ({col_list}) VALUES ({placeholders})'

    cur = conn.cursor()
    values = [
        tuple(_serialize(row.get(k)) for k in all_keys)
        for row in rows
    ]
    cur.executemany(sql, values)
    conn.commit()
    return cur.rowcount


# ---------------------------------------------------------------------------
# Entity runners
# ---------------------------------------------------------------------------

def run_deals(webhook_url: str, conn: sqlite3.Connection) -> None:
    print("── Deals ──────────────────────────────────────────────")
    print("  Fetching records…")
    rows = fetch_all(webhook_url, "crm.deal.list", select=["*", "UF_*"], label="deals")
    print(f"  Upserting {len(rows)} deals into raw_b24_deals…")
    n = upsert_rows(conn, "raw_b24_deals", rows)
    print(f"  Done — {len(rows)} rows fetched, {n} written.")


def run_contacts(webhook_url: str, conn: sqlite3.Connection) -> None:
    print("── Contacts ───────────────────────────────────────────")
    print("  Fetching records…")
    rows = fetch_all(webhook_url, "crm.contact.list", select=["*", "UF_*"], label="contacts")
    print(f"  Upserting {len(rows)} contacts into raw_b24_contacts…")
    n = upsert_rows(conn, "raw_b24_contacts", rows)
    print(f"  Done — {len(rows)} rows fetched, {n} written.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Bitrix24 CRM → SQLite full ingest")
    parser.add_argument(
        "--entity",
        choices=["deals", "contacts", "all"],
        default="all",
        help="Which CRM entity to fetch (default: all)",
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite database file (default: {DEFAULT_DB_PATH})",
    )
    args = parser.parse_args()

    webhook_url = os.environ.get("B24_WEBHOOK_URL", "").strip()
    if not webhook_url:
        print("ERROR: B24_WEBHOOK_URL environment variable is not set.", file=sys.stderr)
        print("  Example: B24_WEBHOOK_URL=https://your.bitrix24.ru/rest/1/TOKEN/ python -m db.b24_fetch_crm", file=sys.stderr)
        sys.exit(1)

    db_path = args.db
    print(f"Database : {db_path}")
    print(f"Webhook  : {webhook_url.rstrip('/')}/...")
    print(f"Entity   : {args.entity}")
    print()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    t0 = time.time()
    try:
        if args.entity in ("deals", "all"):
            run_deals(webhook_url, conn)
        if args.entity in ("contacts", "all"):
            run_contacts(webhook_url, conn)
    finally:
        conn.close()

    print(f"\nAll done in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
