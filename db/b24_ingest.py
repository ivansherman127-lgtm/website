"""
Bitrix24 REST API → deals table ingest.
Set B24_WEBHOOK_URL (e.g. https://your.bitrix24.ru/rest/1/xxx/) to enable.
Optional: B24_FIELD_MAPPING as JSON dict mapping B24 field names to our column names.
"""
import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd

from .conn import get_engine, ensure_schema
from .ingest import DEALS_COLS

# Default B24 → deals column mapping (Bitrix24 often returns ID, DATE_CREATE, STAGE_ID, etc.)
# Override with B24_FIELD_MAPPING env (JSON) if your portal uses different keys.
DEFAULT_B24_MAPPING = {
    "ID": "ID",
    "DATE_CREATE": "Дата создания",
    "STAGE_ID": "Стадия сделки",
    "CATEGORY_ID": "Воронка",
    "UF_CRM_UTM_SOURCE": "UTM Source",
    "UF_CRM_UTM_MEDIUM": "UTM Medium",
    "UF_CRM_UTM_CAMPAIGN": "UTM Campaign",
    "UF_CRM_UTM_CONTENT": "UTM Content",
    "UF_CRM_UTM_TERM": "UTM Term",
}


def _get_mapping() -> Dict[str, str]:
    import json
    raw = os.environ.get("B24_FIELD_MAPPING")
    if raw:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
    return dict(DEFAULT_B24_MAPPING)


def _fetch_b24_deals_page(
    webhook_url: str,
    start: int,
    select: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    import urllib.request
    import json

    url = webhook_url.rstrip("/") + "/crm.deal.list"
    body = {"start": start, "ORDER": {"ID": "ASC"}}
    if select:
        body["SELECT"] = select
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as resp:
        out = json.load(resp)
    if "result" not in out:
        raise RuntimeError("B24 API error: %s" % out.get("error_description", out))
    return out["result"]


def fetch_b24_deals(
    webhook_url: str,
    page_size: int = 50,
    throttle_seconds: float = 0.2,
    field_mapping: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """Fetch all deals from Bitrix24 with pagination; map to our column names."""
    mapping = field_mapping or _get_mapping()
    # Request all B24 keys we need for mapping (and ID for pagination)
    select = list(mapping.keys())
    rows: List[Dict[str, Any]] = []
    start = 0
    while True:
        page = _fetch_b24_deals_page(webhook_url, start, select=select)
        rows.extend(page)
        if len(page) < page_size:
            break
        start += page_size
        time.sleep(throttle_seconds)
    if not rows:
        return pd.DataFrame(columns=DEALS_COLS)
    # Map B24 keys → our columns
    df = pd.DataFrame(rows)
    out = pd.DataFrame()
    for b24_key, our_col in mapping.items():
        if b24_key in df.columns and our_col in DEALS_COLS:
            out[our_col] = df[b24_key].astype(str).replace("nan", "-")
    # Ensure all DEALS_COLS exist
    for c in DEALS_COLS:
        if c not in out.columns:
            out[c] = "-"
    out = out[DEALS_COLS]
    out["ID"] = out["ID"].astype(str).str.strip()
    return out


def upsert_deals(engine, df: pd.DataFrame) -> int:
    """Replace deals table with the given DataFrame (same columns as schema)."""
    if df.empty:
        return 0
    df = df[DEALS_COLS].copy()
    df["ID"] = df["ID"].astype(str).str.strip()
    df.to_sql("deals", engine, if_exists="replace", index=False)
    return len(df)


def ingest_b24_deals(
    webhook_url: Optional[str] = None,
    engine=None,
    db_path: Optional[str] = None,
) -> int:
    """
    Fetch deals from Bitrix24 REST API and upsert into deals table.
    Returns number of deals written. Returns 0 if B24_WEBHOOK_URL is not set.
    """
    url = webhook_url or os.environ.get("B24_WEBHOOK_URL")
    if not url:
        return 0
    engine = engine or get_engine(db_path)
    ensure_schema(engine)
    df = fetch_b24_deals(url)
    return upsert_deals(engine, df)
