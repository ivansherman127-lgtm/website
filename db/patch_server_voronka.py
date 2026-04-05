#!/usr/bin/env python3
"""Generate a SQL patch to update Воронка from local DB and apply it to server."""
import sqlite3
import subprocess
import sys
import tempfile
import os

LOCAL_DB = "/Users/ivan/Documents/website/website.db"

conn = sqlite3.connect(LOCAL_DB)
rows = conn.execute(
    "SELECT ID, \"Воронка\" FROM raw_bitrix_deals WHERE \"Воронка\" IS NOT NULL AND trim(\"Воронка\") != ''"
).fetchall()
conn.close()

print(f"Generating patch for {len(rows)} rows with non-empty Воронка...")

# Build SQL using temp table + single UPDATE (efficient)
lines = ["BEGIN;"]
lines.append("CREATE TEMP TABLE IF NOT EXISTS _vp (id TEXT PRIMARY KEY, v TEXT);")
for deal_id, voronka in rows:
    safe_v = voronka.replace("'", "''")
    safe_id = str(deal_id).replace("'", "''")
    lines.append(f"INSERT OR REPLACE INTO _vp VALUES ('{safe_id}', '{safe_v}');")
lines.append("""UPDATE raw_bitrix_deals
SET "Воронка" = (SELECT v FROM _vp WHERE id = raw_bitrix_deals.ID)
WHERE ("Воронка" IS NULL OR "Воронка" = '')
  AND EXISTS (SELECT 1 FROM _vp WHERE id = raw_bitrix_deals.ID);""")
lines.append("DROP TABLE _vp;")
lines.append("COMMIT;")

sql = "\n".join(lines)

with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
    f.write(sql)
    tmp_path = f.name

print(f"SQL patch written to {tmp_path} ({len(sql)//1024}KB)")
print("Uploading to server...")

subprocess.run([
    "sshpass", "-p", "cybered-lending9463!",
    "scp", "-o", "StrictHostKeyChecking=no",
    tmp_path, "deploy@130.49.149.212:/tmp/voronka_patch.sql"
], check=True)
os.unlink(tmp_path)
print("Uploaded. Applying on server...")

result = subprocess.run([
    "sshpass", "-p", "cybered-lending9463!",
    "ssh", "-o", "StrictHostKeyChecking=no",
    "deploy@130.49.149.212",
    "sqlite3 /home/website/website.db < /tmp/voronka_patch.sql && "
    "sqlite3 /home/website/website.db \"SELECT \\\"Воронка\\\", COUNT(*) c FROM raw_bitrix_deals GROUP BY 1 ORDER BY 2 DESC LIMIT 12;\" && "
    "rm /tmp/voronka_patch.sql"
], capture_output=True, text=True)

print(result.stdout)
if result.returncode != 0:
    print("STDERR:", result.stderr, file=sys.stderr)
    sys.exit(result.returncode)
print("Done.")
