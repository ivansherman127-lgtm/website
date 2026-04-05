#!/usr/bin/env python3
"""Push stg_deals_analytics from local SQLite to server, then trigger TS rebuild."""
import sqlite3
import subprocess
import sys
import tempfile
import os
import json

LOCAL_DB = "/Users/ivan/Documents/website/website.db"
SERVER = "deploy@130.49.149.212"
SERVER_DB = "/home/website/website.db"
SERVER_PASS = "cybered-lending9463!"

conn = sqlite3.connect(LOCAL_DB)

# Get schema
schema = conn.execute(
    "SELECT sql FROM sqlite_master WHERE type='table' AND name='stg_deals_analytics'"
).fetchone()[0]

rows = conn.execute("SELECT * FROM stg_deals_analytics").fetchall()
cols_info = conn.execute("PRAGMA table_info(stg_deals_analytics)").fetchall()
col_names = [c[1] for c in cols_info]
conn.close()

print(f"Exporting stg_deals_analytics: {len(rows)} rows, {len(col_names)} cols")

lines = ["BEGIN;"]
lines.append("DROP TABLE IF EXISTS stg_deals_analytics;")
lines.append(schema + ";")

# Insert in batches
BATCH = 500
for i in range(0, len(rows), BATCH):
    batch = rows[i:i+BATCH]
    col_list = ", ".join(f'"{c}"' for c in col_names)
    value_list = []
    for row in batch:
        vals = []
        for v in row:
            if v is None:
                vals.append("NULL")
            else:
                safe = str(v).replace("'", "''")
                vals.append(f"'{safe}'")
        value_list.append(f"({', '.join(vals)})")
    lines.append(f"INSERT INTO stg_deals_analytics ({col_list}) VALUES\n  " + ",\n  ".join(value_list) + ";")

lines.append("COMMIT;")
sql = "\n".join(lines)

with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
    f.write(sql)
    tmp_path = f.name

size_kb = os.path.getsize(tmp_path) // 1024
print(f"SQL written: {size_kb}KB, uploading...")

subprocess.run([
    "sshpass", "-p", SERVER_PASS,
    "scp", "-o", "StrictHostKeyChecking=no",
    tmp_path, f"{SERVER}:/tmp/stg_deals_patch.sql"
], check=True)
os.unlink(tmp_path)

print("Applying on server...")
result = subprocess.run([
    "sshpass", "-p", SERVER_PASS,
    "ssh", "-o", "StrictHostKeyChecking=no", SERVER,
    f'sqlite3 {SERVER_DB} < /tmp/stg_deals_patch.sql && '
    f'echo "stg_deals_analytics rows:" && sqlite3 {SERVER_DB} "SELECT COUNT(*) FROM stg_deals_analytics;" && '
    f'sqlite3 {SERVER_DB} "SELECT funnel_raw, COUNT(*) c FROM stg_deals_analytics GROUP BY 1 ORDER BY 2 DESC LIMIT 10;" && '
    f'rm /tmp/stg_deals_patch.sql'
], capture_output=True, text=True)

print(result.stdout)
if result.returncode != 0:
    print("STDERR:", result.stderr, file=sys.stderr)
    sys.exit(result.returncode)

print("Done pushing stg_deals_analytics.")
