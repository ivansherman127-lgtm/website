from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd

from bitrix_union_io import load_bitrix_deals_union
from event_classifier import classify_event_from_row

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONTACTS = PROJECT_ROOT / "sheets" / "bitrix_contact_export.csv"
DEFAULT_OUT = PROJECT_ROOT / "bitrix_contacts_attacking_january.csv"
DEFAULT_REPORT = PROJECT_ROOT / "bitrix_contacts_attacking_january_report.json"
DEFAULT_UNMATCHED_OUT = PROJECT_ROOT / "bitrix_contacts_attacking_january_unmatched.csv"


def _norm_str(v: object) -> str:
    if v is None or pd.isna(v):
        return ""
    s = str(v).strip()
    if s.lower() in {"", "nan", "none", "null"}:
        return ""
    return s


def _norm_id(v: object) -> str:
    s = _norm_str(v)
    if re.fullmatch(r"\d+\.0+", s):
        return s.split(".", 1)[0]
    return s


def run(
    contacts_path: Path,
    deals_path: Path | None,
    out_path: Path,
    report_path: Path,
    unmatched_out_path: Path,
) -> None:
    contacts = pd.read_csv(contacts_path, sep=";", encoding="utf-8", low_memory=False, dtype=str)
    if deals_path is None:
        deals = load_bitrix_deals_union()
    else:
        deals = pd.read_csv(deals_path, sep=";", encoding="utf-8", low_memory=False, dtype=str)

    contacts["ID"] = contacts["ID"].map(_norm_id)
    deals["ID"] = deals["ID"].map(_norm_id)
    deals["Контакт: ID"] = deals["Контакт: ID"].map(_norm_id)

    deals["event_class"] = deals.apply(lambda r: classify_event_from_row(r.to_dict()).event, axis=1)
    jan_deals = deals[deals["event_class"] == "Attacking January"].copy()

    jan_deals = jan_deals[jan_deals["Контакт: ID"] != ""]
    grouped = (
        jan_deals.groupby("Контакт: ID", dropna=False)["ID"]
        .agg(lambda s: " | ".join(sorted({_norm_id(x) for x in s if _norm_id(x)})))
        .reset_index()
        .rename(columns={"Контакт: ID": "contact_id", "ID": "deal_ids"})
    )
    grouped["deals_count"] = grouped["deal_ids"].map(lambda x: len([p for p in x.split(" | ") if p]))

    out = contacts.merge(grouped, left_on="ID", right_on="contact_id", how="inner")
    out = out.sort_values(["deals_count", "ID"], ascending=[False, True]).reset_index(drop=True)
    out.to_csv(out_path, index=False, encoding="utf-8")

    matched_ids = set(out["ID"].astype(str))
    unmatched = grouped[~grouped["contact_id"].astype(str).isin(matched_ids)].copy()
    unmatched = unmatched.sort_values(["deals_count", "contact_id"], ascending=[False, True]).reset_index(drop=True)
    unmatched.to_csv(unmatched_out_path, index=False, encoding="utf-8")

    report = {
        "contacts_rows": int(len(contacts)),
        "deals_rows": int(len(deals)),
        "attacking_january_deals_rows": int(len(jan_deals)),
        "unique_contact_ids_in_attacking_january_deals": int(grouped["contact_id"].nunique()),
        "matched_contacts_in_export": int(len(out)),
        "unmatched_contacts_not_in_export": int(len(unmatched)),
        "output_file": str(out_path),
        "unmatched_output_file": str(unmatched_out_path),
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(report, ensure_ascii=False, indent=2))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cross-check contacts export with attacking january deals.")
    p.add_argument("--contacts", type=Path, default=DEFAULT_CONTACTS)
    p.add_argument(
        "--deals",
        type=Path,
        default=None,
        help="По умолчанию: fl_raw_09-03 + bitrix_upd_27.03",
    )
    p.add_argument("--output", type=Path, default=DEFAULT_OUT)
    p.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    p.add_argument("--unmatched-output", type=Path, default=DEFAULT_UNMATCHED_OUT)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.contacts, args.deals, args.output, args.report, args.unmatched_output)
