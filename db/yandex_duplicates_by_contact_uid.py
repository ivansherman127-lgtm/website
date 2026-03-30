from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SRC_YD = ROOT / "web" / "public" / "data" / "yd_hierarchy.json"
SRC_UID = ROOT / "web" / "public" / "data" / "bitrix_contacts_uid.json"
OUT_CSV = ROOT / "reports" / "sheets" / "yandex_duplicates_by_contact_uid.csv"
OUT_SUMMARY = ROOT / "reports" / "sheets" / "yandex_duplicates_by_contact_uid_summary.json"

SPLIT_RE = re.compile(r"[,\s;|]+")


def _norm_id(v: object) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if re.fullmatch(r"\d+\.0+", s):
        return s.split(".", 1)[0]
    return s


def _parse_ids(v: object) -> list[str]:
    s = str(v or "").strip()
    if not s:
        return []
    out: list[str] = []
    for tok in SPLIT_RE.split(s):
        t = _norm_id(tok)
        if t:
            out.append(t)
    return out


def _is_month_row(row: dict) -> bool:
    return str(row.get("Level", "")).strip() == "Month"


def run() -> dict:
    yd_rows = json.loads(SRC_YD.read_text(encoding="utf-8"))
    uid_rows = json.loads(SRC_UID.read_text(encoding="utf-8"))

    # map deal id -> contact_uid
    deal_to_uid: dict[str, str] = {}
    for r in uid_rows:
        uid = str(r.get("contact_uid", "")).strip()
        if not uid:
            continue
        for did in _parse_ids(r.get("all_contact_ids", "")):
            deal_to_uid[did] = uid

    lead_rows = [r for r in yd_rows if _is_month_row(r)]

    expanded: list[dict] = []
    lead_seq = 0
    for row in lead_rows:
        month = str(row.get("Месяц", "")).strip()
        ids = _parse_ids(row.get("fl_IDs", ""))
        for did in ids:
            lead_seq += 1
            expanded.append(
                {
                    "lead_seq": lead_seq,
                    "month": month,
                    "deal_id": did,
                    "contact_uid": deal_to_uid.get(did, ""),
                }
            )

    seen_uid: set[str] = set()
    payments_with_duplicates = 0
    payments_unique_leads_only = 0
    duplicate_leads = 0
    mapped_leads = 0

    for r in expanded:
        uid = r["contact_uid"]
        is_mapped = bool(uid)
        is_duplicate = False
        if is_mapped:
            mapped_leads += 1
            if uid in seen_uid:
                is_duplicate = True
                duplicate_leads += 1
            else:
                seen_uid.add(uid)

        # payment proxy from Yandex lead rows = one lead entry (deal id) in flow.
        payments_with_duplicates += 1
        if not is_duplicate:
            payments_unique_leads_only += 1

        r["is_mapped"] = is_mapped
        r["is_duplicate"] = is_duplicate

    det = pd.DataFrame(expanded)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    det.to_csv(OUT_CSV, index=False, encoding="utf-8")

    summary = {
        "source_yandex": str(SRC_YD),
        "source_uid": str(SRC_UID),
        "total_leads": int(len(expanded)),
        "total_duplicate_leads": int(duplicate_leads),
        "payments_with_duplicates": int(payments_with_duplicates),
        "payments_unique_leads_only": int(payments_unique_leads_only),
        "mapped_leads": int(mapped_leads),
        "unmapped_leads": int(len(expanded) - mapped_leads),
        "mapped_share": float((mapped_leads / len(expanded)) if expanded else 0.0),
        "checks": {
            "payments_unique_leads_only_le_payments_with_duplicates": bool(
                payments_unique_leads_only <= payments_with_duplicates
            )
        },
        "outputs": {
            "detail_csv": str(OUT_CSV),
            "summary_json": str(OUT_SUMMARY),
        },
    }
    OUT_SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
