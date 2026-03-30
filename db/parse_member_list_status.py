from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "sheets" / "member_list_03.26.csv"
OUT_CSV = ROOT / "reports" / "sheets" / "member_list_03.26_statuses.csv"
OUT_JSON = ROOT / "reports" / "sheets" / "member_list_03.26_statuses_summary.json"
WEB_DATA_DIR = ROOT / "web" / "public" / "data"
WEB_CATEGORIES_JSON = WEB_DATA_DIR / "member_list_03.26_statuses_categories.json"
WEB_TOP_ERRORS_JSON = WEB_DATA_DIR / "member_list_03.26_statuses_top_errors.json"


def _parse_meta(raw: str) -> dict:
    s = str(raw or "").strip()
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        return {}


def _category_from_text(msg: str, issue: str, err: str) -> str:
    t = (msg or "").lower()
    if "spam message rejected" in t or "notspam-support" in t:
        return "spam_rejected"
    if "user unknown" in t or "5.1.1" in t:
        return "user_unknown"
    if "out of storage space" in t or "overquotatemp" in t or "mailbox full" in t:
        return "mailbox_full"
    if "unstandard delivery error report" in t:
        return "delivery_error"
    if "invalid recipient" in t or "recipient address rejected" in t:
        return "recipient_rejected"
    if "no such domain" in t or "domain not found" in t or "host not found" in t:
        return "invalid_domain"
    if "blocked" in t or "blacklist" in t:
        return "blocked"
    if issue == "231":
        return "spam_rejected"
    if err == "6":
        return "mailbox_full"
    if err == "1":
        return "rejected"
    if err == "5":
        return "delivery_error"
    return "ok_or_unknown"


def run() -> dict:
    # File has no headers; schema observed from source rows.
    cols = [
        "email",
        "issue_code_raw",
        "unused_1",
        "meta_json",
        "error_code_raw",
        "lock_flag_raw",
        "lock_issue_raw",
        "row_id",
    ]
    df = pd.read_csv(SRC, sep=",", names=cols, dtype=str, low_memory=False, keep_default_na=False)

    meta = df["meta_json"].map(_parse_meta)
    meta_df = pd.DataFrame(meta.tolist())
    for c in ["str", "date", "lock", "letter", "issue", "lock_issue", "error", "lock_letter"]:
        if c not in meta_df.columns:
            meta_df[c] = ""

    out = pd.DataFrame(
        {
            "email": df["email"].astype(str).str.strip().str.lower(),
            "status_text": meta_df["str"].fillna("").astype(str),
            "status_date": meta_df["date"].fillna("").astype(str),
            "issue_code": meta_df["issue"].fillna("").astype(str),
            "error_code": meta_df["error"].fillna("").astype(str),
            "locked": meta_df["lock"].fillna("").astype(str),
            "lock_issue": meta_df["lock_issue"].fillna("").astype(str),
            "lock_letter": meta_df["lock_letter"].fillna("").astype(str),
        }
    )

    out["category"] = [
        _category_from_text(m, i, e)
        for m, i, e in zip(out["status_text"], out["issue_code"], out["error_code"])
    ]
    out["is_problem"] = out["category"] != "ok_or_unknown"

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False, encoding="utf-8")
    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    category_counts = out["category"].value_counts(dropna=False).to_dict()
    cat_rows = [{"category": k, "count": int(v)} for k, v in category_counts.items()]
    WEB_CATEGORIES_JSON.write_text(json.dumps(cat_rows, ensure_ascii=False), encoding="utf-8")

    top_errors = (
        out[out["is_problem"]]
        .loc[:, ["email", "category", "issue_code", "error_code", "status_text"]]
        .head(200)
        .to_dict(orient="records")
    )
    WEB_TOP_ERRORS_JSON.write_text(json.dumps(top_errors, ensure_ascii=False), encoding="utf-8")

    summary = {
        "source": str(SRC),
        "rows_total": int(len(out)),
        "rows_with_problem": int(out["is_problem"].sum()),
        "rows_ok_or_unknown": int((~out["is_problem"]).sum()),
        "category_counts": {k: int(v) for k, v in category_counts.items()},
        "top_problem_examples": top_errors[:20],
        "outputs": {
            "statuses_csv": str(OUT_CSV),
            "summary_json": str(OUT_JSON),
            "web_categories_json": str(WEB_CATEGORIES_JSON),
            "web_top_errors_json": str(WEB_TOP_ERRORS_JSON),
        },
    }
    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
