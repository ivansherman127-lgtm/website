#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import pathlib
import time
import urllib.request


def main() -> int:
    cfg = json.loads(pathlib.Path("/home/website/web_share_subset/webpush/.env.server.json").read_text())
    pwd = str(cfg.get("ANALYTICS_PASSWORD", ""))
    token = hashlib.sha256(("analytics:" + pwd).encode()).hexdigest()
    base = "http://127.0.0.1:3000"
    paths = [
        "/api/assoc-revenue?dims=event",
        "/api/assoc-revenue?dims=yandex_campaign",
        "/api/assoc-revenue?dims=email_campaign",
        "/api/leads-breakdown?dim=course",
        "/api/leads-breakdown?dim=project",
        "/api/leads-breakdown?dim=medium",
        "/api/data?path=data/bitrix_month_total_full.json",
        "/api/data?path=data/email_hierarchy_by_send.json",
        "/api/data?path=data/global/yandex_projects_revenue_by_month.json",
        "/api/data?path=data/global/yandex_projects_revenue_no_month.json",
    ]
    for p in paths:
        req = urllib.request.Request(base + p, headers={"Cookie": f"analytics_auth={token}"})
        t0 = time.time()
        with urllib.request.urlopen(req, timeout=90) as r:
            _ = r.read()
        print(f"warm {p} {time.time()-t0:.2f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

