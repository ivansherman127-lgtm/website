import json
import sqlite3
import time
import urllib.request

WEBHOOK = "https://cybered.bitrix24.ru/rest/9113/lraewnlygjatsn5h/"
DB_PATH = "/home/website/website.db"
PAGE_SIZE = 50
THROTTLE = 1.2
MAX_RETRIES = 8
RETRY_DELAY = 12.0


def call(method: str, params: dict) -> dict:
    url = WEBHOOK.rstrip("/") + "/" + method
    data = json.dumps(params).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                out = json.load(resp)
            if "error" in out:
                raise RuntimeError(out.get("error_description") or out.get("error"))
            return out
        except Exception:
            if attempt >= MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY * attempt)
    raise RuntimeError("unreachable")


def main() -> None:
    rows = []
    start = 0
    while True:
        params = {
            "start": start,
            "ORDER": {"ID": "ASC"},
            "SELECT": ["*", "UF_*"],
            "FILTER": {
                ">=DATE_CREATE": "2026-01-01T00:00:00+00:00",
                "<DATE_CREATE": "2027-01-01T00:00:00+00:00",
            },
        }
        out = call("crm.deal.list", params)
        page = out.get("result", [])
        rows.extend(page)
        print(f"fetched {len(rows)}/{out.get('total', '?')}", flush=True)
        if len(page) < PAGE_SIZE:
            break
        start += len(page)
        time.sleep(THROTTLE)

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS raw_b24_deals")

    keys: list[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                keys.append(key)

    cur.execute("CREATE TABLE raw_b24_deals (_rowid_ INTEGER PRIMARY KEY AUTOINCREMENT, ID TEXT, ingested_at TEXT)")
    for key in keys:
        if key in ("ID", "ingested_at"):
            continue
        quoted = '"' + key.replace('"', '""') + '"'
        cur.execute(f"ALTER TABLE raw_b24_deals ADD COLUMN {quoted} TEXT")

    insert_cols = ["ingested_at"] + keys
    cols_sql = ",".join('"' + c.replace('"', '""') + '"' for c in insert_cols)
    placeholders = ",".join("?" for _ in insert_cols)
    now = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())

    payload = []
    for row in rows:
        vals = [now]
        for key in keys:
            val = row.get(key)
            if isinstance(val, (list, dict)):
                val = json.dumps(val, ensure_ascii=False)
            elif val is None:
                val = ""
            else:
                val = str(val)
            vals.append(val)
        payload.append(tuple(vals))

    cur.executemany(f"INSERT INTO raw_b24_deals ({cols_sql}) VALUES ({placeholders})", payload)
    con.commit()
    print(f"inserted {len(rows)}")


if __name__ == "__main__":
    main()
