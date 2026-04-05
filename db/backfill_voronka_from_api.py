#!/usr/bin/env python3
"""
One-time backfill: fetch CATEGORY_ID for ALL deals from Bitrix24 API,
resolve category ID → pipeline name, then UPDATE raw_bitrix_deals.Воронка
for every row that is currently empty.

Usage:
    B24_WEBHOOK_URL=https://... python db/backfill_voronka_from_api.py [--db PATH] [--dry-run]
    # or set WEBSITE_DB_PATH env var for db path
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = os.environ.get("WEBSITE_DB_PATH", str(ROOT / "website.db"))
WEBHOOK_URL = os.environ.get("B24_WEBHOOK_URL", "").strip()
PAGE_SIZE = 50
THROTTLE = 0.3
MAX_RETRIES = 3
RETRY_DELAY = 5.0


def api_call(webhook: str, method: str, params: Dict[str, Any] = {}) -> Dict[str, Any]:
    url = webhook.rstrip("/") + "/" + method
    data = json.dumps(params).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = json.load(resp)
            if "error" in body:
                code = body.get("error", "")
                desc = body.get("error_description", code)
                if "LIMIT" in str(code).upper() and attempt < MAX_RETRIES:
                    print(f"  [rate limit] retry {attempt}/{MAX_RETRIES}…")
                    time.sleep(RETRY_DELAY)
                    continue
                raise RuntimeError(f"B24 error [{code}]: {desc}")
            return body
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                print(f"  [HTTP {exc.code}] retry {attempt}/{MAX_RETRIES}…")
                time.sleep(RETRY_DELAY)
                continue
            raise
    raise RuntimeError(f"B24 {method} failed after {MAX_RETRIES} retries")


def fetch_category_map(webhook: str) -> Dict[str, str]:
    """Returns {str(category_id): pipeline_name, ...}.  CATEGORY_ID 0 → '' (default pipeline)."""
    resp = api_call(webhook, "crm.dealcategory.list")
    result = resp.get("result", [])
    mapping: Dict[str, str] = {"0": ""}  # default pipeline has no name in Bitrix
    for cat in result:
        mapping[str(cat["ID"])] = cat["NAME"]
    return mapping


def fetch_all_deal_categories(webhook: str) -> Dict[str, str]:
    """Returns {deal_id: pipeline_name} for ALL deals in Bitrix24."""
    category_map = fetch_category_map(webhook)
    print(f"  Category map: {category_map}")

    deal_categories: Dict[str, str] = {}
    start = 0
    total_fetched = 0
    t0 = time.time()

    while True:
        resp = api_call(webhook, "crm.deal.list", {
            "start": start,
            "ORDER": {"ID": "ASC"},
            "SELECT": ["ID", "CATEGORY_ID"],
        })
        page: List[Dict[str, Any]] = resp.get("result", [])
        total = resp.get("total", "?")

        for row in page:
            deal_id = str(row.get("ID", "")).strip()
            cat_id = str(row.get("CATEGORY_ID", "0"))
            voronka = category_map.get(cat_id, "")
            if deal_id:
                deal_categories[deal_id] = voronka

        total_fetched += len(page)
        elapsed = time.time() - t0
        rate = total_fetched / elapsed if elapsed > 0 else 0
        print(f"  deals: {total_fetched}/{total} ({elapsed:.0f}s, {rate:.1f}/s) …", end="\r", flush=True)

        if len(page) < PAGE_SIZE:
            break
        start += PAGE_SIZE
        time.sleep(THROTTLE)

    print()
    return deal_categories


def backfill_voronka(db_path: str, deal_categories: Dict[str, str], dry_run: bool = False) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    # Check current state
    cur = conn.execute("SELECT COUNT(*) FROM raw_bitrix_deals WHERE Воронка IS NULL OR Воронка = ''")
    empty_count = cur.fetchone()[0]
    print(f"\nraw_bitrix_deals: {empty_count} rows with empty Воронка")

    updates = []
    for deal_id, voronka in deal_categories.items():
        if voronka:  # Only update if we have a non-empty name
            updates.append((voronka, deal_id))

    print(f"API returned {len(deal_categories)} deals, {len(updates)} with non-empty pipeline name")

    if dry_run:
        print("DRY RUN — no changes written")
        conn.close()
        return

    if updates:
        # Use a temp table + single UPDATE JOIN for O(n) instead of O(n²) per-row scans
        conn.execute("CREATE TEMP TABLE _voronka_patch (id TEXT PRIMARY KEY, voronka TEXT)")
        conn.executemany("INSERT OR REPLACE INTO _voronka_patch VALUES (?, ?)", updates)
        conn.execute("""
            UPDATE raw_bitrix_deals
            SET Воронка = (SELECT voronka FROM _voronka_patch WHERE id = raw_bitrix_deals.ID)
            WHERE (Воронка IS NULL OR Воронка = '')
              AND EXISTS (SELECT 1 FROM _voronka_patch WHERE id = raw_bitrix_deals.ID)
        """)
        conn.execute("DROP TABLE _voronka_patch")
        conn.commit()

    # Check result
    cur = conn.execute("SELECT Воронка, COUNT(*) FROM raw_bitrix_deals GROUP BY Воронка ORDER BY COUNT(*) DESC LIMIT 15")
    print("\nAfter backfill — Воронка distribution:")
    for row in cur.fetchall():
        label = row[0] if row[0] else "(empty)"
        print(f"  {row[1]:6d}  {label}")

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Воронка in raw_bitrix_deals from Bitrix24 API")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path")
    parser.add_argument("--dry-run", action="store_true", help="Fetch from API but don't update DB")
    args = parser.parse_args()

    webhook = WEBHOOK_URL or os.environ.get("B24_WEBHOOK_URL", "").strip()
    if not webhook:
        print("ERROR: set B24_WEBHOOK_URL env var", file=sys.stderr)
        sys.exit(1)

    print(f"DB       : {args.db}")
    print(f"Webhook  : {webhook.split('/rest/')[0]}/rest/…")
    print()

    print("Step 1: Fetching all deal CATEGORY_IDs from Bitrix24 API…")
    deal_categories = fetch_all_deal_categories(webhook)
    print(f"Fetched {len(deal_categories)} deal→category mappings")

    print("\nStep 2: Backfilling raw_bitrix_deals.Воронка…")
    backfill_voronka(args.db, deal_categories, dry_run=args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
