#!/usr/bin/env python3
"""
Export Bitrix24 pipeline + stage semantic maps for the API mart pipeline.

crm.dealcategory.list → category id → pipeline NAME (as in CRM UI).
crm.status.list per ENTITY_ID DEAL_STAGE / DEAL_STAGE_{id} → STATUS_ID → NAME.

Writes JSON consumed by rawBitrixSource.ts (and optionally Python).

  B24_WEBHOOK_URL=https://xxx.bitrix24.ru/rest/1/TOKEN/ python db/export_b24_semantic_maps.py

Output (default):
  web_share_subset/webpush/functions/lib/analytics/b24CrmSemanticMaps.json
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "web_share_subset/webpush/functions/lib/analytics/b24CrmSemanticMaps.json"
THROTTLE = 0.35


def call(webhook: str, method: str, params: dict) -> dict:
    url = webhook.rstrip("/") + "/" + method
    data = json.dumps(params).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=90) as resp:
        body = json.load(resp)
    if "error" in body:
        raise RuntimeError(body.get("error_description") or body.get("error"))
    return body


def main() -> None:
    webhook = os.environ.get("B24_WEBHOOK_URL", "").strip()
    if not webhook:
        print("Set B24_WEBHOOK_URL", file=sys.stderr)
        sys.exit(1)

    cats = call(webhook, "crm.dealcategory.list", {})["result"] or []
    time.sleep(THROTTLE)

    categories: dict[str, str] = {}
    for c in cats:
        cid = str(c.get("ID", "")).strip()
        name = str(c.get("NAME", "")).strip()
        if cid:
            categories[cid] = name

    stages: dict[str, str] = {}

    def fetch_stages(entity_id: str) -> None:
        start = 0
        while True:
            out = call(
                webhook,
                "crm.status.list",
                {"filter": {"ENTITY_ID": entity_id}, "start": start},
            )
            rows = out.get("result") or []
            for r in rows:
                sid = str(r.get("STATUS_ID", "")).strip()
                name = str(r.get("NAME", "")).strip()
                if sid and name:
                    stages[sid] = name
            time.sleep(THROTTLE)
            if "next" not in out:
                break
            start = int(out["next"])

    # Default pipeline (CATEGORY_ID 0) — Bitrix uses ENTITY_ID "DEAL_STAGE"
    fetch_stages("DEAL_STAGE")

    for cid in sorted(categories.keys(), key=lambda x: int(x) if x.isdigit() else 0):
        ent = f"DEAL_STAGE_{cid}"
        try:
            fetch_stages(ent)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue
            raise
        except RuntimeError as e:
            if "not found" in str(e).lower() or "404" in str(e):
                continue
            raise

    payload = {
        "schema_version": "2026-04-10",
        "source": "crm.dealcategory.list + crm.status.list",
        "categories": categories,
        "stages": stages,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {OUT} ({len(categories)} categories, {len(stages)} stages)")


if __name__ == "__main__":
    main()
