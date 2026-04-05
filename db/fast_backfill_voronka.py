#!/usr/bin/env python3
"""
Fast backfill using temp table + single UPDATE (already have category map from API).
Runs in seconds vs hours.
"""
import json
import re
import sqlite3
import urllib.request

DB = "/Users/ivan/Documents/website/website.db"
WEBHOOK = "https://cybered.bitrix24.ru/rest/9113/lraewnlygjatsn5h/"

def api_get(method, params={}):
    import json as _json
    url = WEBHOOK.rstrip("/") + "/" + method
    data = _json.dumps(params).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as r:
        return _json.load(r)

# Step 1: get category map
cats = api_get("crm.dealcategory.list")["result"]
cat_map = {str(c["ID"]): c["NAME"] for c in cats}
cat_map["0"] = ""
print("Category map:", cat_map)

# Step 2: load cached log file to get all deal→category from previous fetch
# Parse lines like: "  deals: 68861/68861 ..." and extract progress
# The log captured with \r has per-page data but we need to re-fetch only IDs
# Since we already know CATEGORY_ID per deal, just re-fetch lightly from API
# -- but wait, we have the full fetch already done (968s). 
# Re-fetch just ID+CATEGORY_ID from API using stored pages is not possible from log.
# We need to re-fetch. With temp table UPDATE it will be fast.

import time
import sys

PAGE_SIZE = 50
THROTTLE = 0.3
MAX_RETRIES = 3
RETRY_DELAY = 5.0

def api_post(method, params):
    import urllib.error
    url = WEBHOOK.rstrip("/") + "/" + method
    data = json.dumps(params).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                body = json.load(r)
            if "error" in body:
                raise RuntimeError(body)
            return body
        except urllib.error.HTTPError as e:
            if e.code in (429, 503, 500) and attempt < MAX_RETRIES:
                print(f"  retry {attempt}...")
                time.sleep(RETRY_DELAY)
                continue
            raise
    raise RuntimeError("failed")

print("Fetching all deal ID+CATEGORY_ID from API...")
rows = []
start = 0
t0 = time.time()
while True:
    resp = api_post("crm.deal.list", {"start": start, "ORDER": {"ID": "ASC"}, "SELECT": ["ID", "CATEGORY_ID"]})
    page = resp.get("result", [])
    rows.extend(page)
    total = resp.get("total", "?")
    print(f"  {len(rows)}/{total} ({time.time()-t0:.0f}s)", end="\r", flush=True)
    if len(page) < PAGE_SIZE:
        break
    start += PAGE_SIZE
    time.sleep(THROTTLE)
print(f"\nFetched {len(rows)} deals")

# Build (id, voronka) pairs
pairs = []
for row in rows:
    vid = str(row.get("ID", "")).strip()
    cat = str(row.get("CATEGORY_ID", "0"))
    name = cat_map.get(cat, "")
    if vid and name:
        pairs.append((vid, name))
print(f"{len(pairs)} deals with non-empty pipeline")

# Step 3: fast UPDATE via temp table
conn = sqlite3.connect(DB)
conn.execute("PRAGMA journal_mode=WAL")

# Create index if not exists to speed up UPDATE
try:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rbd_id ON raw_bitrix_deals(ID)")
    conn.commit()
    print("Index created")
except Exception as e:
    print(f"Index: {e}")

conn.execute("CREATE TEMP TABLE _vp (id TEXT PRIMARY KEY, v TEXT)")
conn.executemany("INSERT OR REPLACE INTO _vp VALUES (?,?)", pairs)
print("Temp table populated, running UPDATE...")

cur = conn.execute("""
    UPDATE raw_bitrix_deals
    SET Воронка = (SELECT v FROM _vp WHERE id = raw_bitrix_deals.ID)
    WHERE (Воронка IS NULL OR Воронка = '')
      AND EXISTS (SELECT 1 FROM _vp WHERE id = raw_bitrix_deals.ID)
""")
conn.commit()
print(f"Updated {cur.rowcount} rows")

conn.execute("DROP TABLE _vp")
conn.commit()

# Show result
print("\nVoronka distribution:")
for row in conn.execute("SELECT CASE WHEN length(trim(ifnull(\"Воронка\",'')))=0 THEN '(empty)' ELSE \"Воронка\" END as V, COUNT(*) c FROM raw_bitrix_deals GROUP BY 1 ORDER BY 2 DESC"):
    print(f"  {row[1]:6d}  {row[0]}")

conn.close()
