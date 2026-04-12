"""Resolve Bitrix API CATEGORY_ID / STAGE_ID to CRM labels (same JSON as rawBitrixSource.ts)."""

from __future__ import annotations

import functools
import json
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_MAPS_PATH = _ROOT / "web_share_subset/webpush/functions/lib/analytics/b24CrmSemanticMaps.json"


@functools.lru_cache(maxsize=1)
def load_b24_semantic_maps() -> dict:
    with open(_MAPS_PATH, encoding="utf-8") as f:
        return json.load(f)


def resolve_b24_category_label(category_id: str) -> str:
    cid = str(category_id or "").strip()
    if not cid:
        return ""
    cats = (load_b24_semantic_maps().get("categories") or {})
    return cats.get(cid) or (cats.get(str(int(cid))) if cid.isdigit() else "") or cid


def resolve_b24_stage_label(stage_id: str) -> str:
    sid = str(stage_id or "").strip()
    if not sid:
        return ""
    st = load_b24_semantic_maps().get("stages") or {}
    return st.get(sid) or st.get(sid.upper()) or sid
