from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from bitrix_lead_quality import drop_rows_excluded_funnels
from bitrix_union_io import load_bitrix_deals_union
from event_classifier import classify_event_from_row, normalize_course_code
from revenue_variant3 import variant3_revenue_mask
from utils import _n, _id, _amt


ROOT = Path(__file__).resolve().parent.parent
CONTACTS_PATH = ROOT / "sheets" / "bitrix_contact_export.csv"


def _apply_revenue_variant3(df: pd.DataFrame) -> pd.DataFrame:
    rev_mask = variant3_revenue_mask(df)
    out = df.copy()
    out["Сумма_число"] = out.get("Сумма", "").map(_amt)
    out["Выручка_учитывается"] = rev_mask
    out["Выручка"] = out["Сумма_число"].where(rev_mask, 0.0)
    return out


def _event_columns(df: pd.DataFrame) -> pd.DataFrame:
    cls = df.apply(
        lambda r: classify_event_from_row(r.to_dict()),
        axis=1,
    )
    out = df.copy()
    out["Мероприятие_класс"] = cls.map(lambda x: x.event)
    out["Источник_классификации"] = cls.map(lambda x: x.source_field)
    out["Матч_паттерн"] = cls.map(lambda x: x.matched_pattern)
    out["Уверенность"] = cls.map(lambda x: x.confidence)
    course_raw = out.get("Код_курса_сайт", "").fillna("")
    if "Код курса" in out.columns:
        course_raw = course_raw.mask(course_raw.astype(str).str.strip().eq(""), out["Код курса"].fillna(""))
    out["Нормализованный_код_курса"] = course_raw.map(normalize_course_code)
    return out


def _contact_type_map(contacts: pd.DataFrame) -> dict[str, str]:
    contacts = contacts.copy()
    contacts["ID"] = contacts["ID"].map(_id)
    contacts["Тип контакта"] = contacts.get("Тип контакта", "").fillna("").astype(str).str.strip()
    return dict(zip(contacts["ID"], contacts["Тип контакта"]))


def _contact_type(contact_id: str, company_val: str, type_map: dict[str, str]) -> str:
    s = _n(type_map.get(contact_id, "")).lower()
    if "компан" in s or _n(company_val):
        return "Компания"
    return "Физ лицо"


def _event_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    rev = df[df["Выручка_учитывается"]].copy()
    agg = (
        rev.groupby("Мероприятие_класс", dropna=False)
        .agg(
            Контактов_с_выручкой=("Контакт: ID", lambda s: pd.Series([_id(x) for x in s if _id(x)]).nunique()),
            Сделок_с_выручкой=("ID", "nunique"),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
        .rename(columns={"Мероприятие_класс": "Мероприятие"})
    )
    agg["Средний_чек"] = agg["Выручка"] / agg["Сделок_с_выручкой"].replace(0, pd.NA)
    agg["Средний_чек"] = agg["Средний_чек"].fillna(0.0)
    return agg.sort_values("Выручка", ascending=False).reset_index(drop=True)


def main() -> None:
    deals = load_bitrix_deals_union()
    contacts = pd.read_csv(CONTACTS_PATH, sep=";", encoding="utf-8", low_memory=False, dtype=str)
    deals["Контакт: ID"] = deals["Контакт: ID"].map(_id)
    deals = drop_rows_excluded_funnels(deals)
    deals = _apply_revenue_variant3(deals)
    deals = _event_columns(deals)

    # Global outputs
    global_breakdown = _event_breakdown(deals)
    global_breakdown.to_csv(ROOT / "bitrix_global_revenue_events_smart.csv", index=False, encoding="utf-8")

    type_map = _contact_type_map(contacts)
    rev = deals[deals["Выручка_учитывается"] & deals["Контакт: ID"].ne("")].copy()
    rev["Тип_контакта"] = rev.apply(lambda r: _contact_type(_id(r.get("Контакт: ID")), _n(r.get("Компания", "")), type_map), axis=1)
    global_types = (
        rev.groupby("Тип_контакта")
        .agg(Контактов_с_выручкой=("Контакт: ID", "nunique"), Выручка=("Выручка", "sum"), Сделок_с_выручкой=("ID", "nunique"))
        .reset_index()
    )
    global_types["Средний_чек"] = global_types["Выручка"] / global_types["Сделок_с_выручкой"].replace(0, pd.NA)
    global_types.to_csv(ROOT / "bitrix_global_revenue_contact_types_smart.csv", index=False, encoding="utf-8")

    # Cohort outputs
    cohort_ids = set(deals.loc[deals["Мероприятие_класс"].eq("Attacking January") & deals["Контакт: ID"].ne(""), "Контакт: ID"])
    cohort = deals[deals["Контакт: ID"].isin(cohort_ids)].copy()
    cohort_breakdown = _event_breakdown(cohort)
    cohort_breakdown.to_csv(ROOT / "bitrix_attacking_january_revenue_events_smart.csv", index=False, encoding="utf-8")

    cohort_codes = (
        cohort.loc[cohort["Выручка_учитывается"]]
        .groupby("Нормализованный_код_курса", dropna=False)
        .agg(
            Контактов_с_выручкой=("Контакт: ID", lambda s: pd.Series([_id(x) for x in s if _id(x)]).nunique()),
            Сделок_с_выручкой=("ID", "nunique"),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
    )
    cohort_codes = cohort_codes.rename(columns={"Нормализованный_код_курса": "Код_курса_норм"}).sort_values("Выручка", ascending=False)
    cohort_codes["Средний_чек"] = cohort_codes["Выручка"] / cohort_codes["Сделок_с_выручкой"].replace(0, pd.NA)
    cohort_codes.to_csv(ROOT / "bitrix_attacking_january_revenue_by_normalized_course_code_smart.csv", index=False, encoding="utf-8")

    # QA + diff vs old cohort breakdown
    old_path = ROOT / "bitrix_attacking_january_revenue_by_events_variant3.csv"
    diff_df = None
    if old_path.exists():
        old = pd.read_csv(old_path, encoding="utf-8")
        new = cohort_breakdown.rename(columns={"Мероприятие": "event", "Выручка": "new_revenue"})[["event", "new_revenue"]]
        old2 = old.rename(columns={"Мероприятие": "event", "Выручка": "old_revenue"})[["event", "old_revenue"]]
        diff_df = old2.merge(new, on="event", how="outer").fillna(0.0)
        diff_df["delta"] = diff_df["new_revenue"] - diff_df["old_revenue"]
        diff_df.to_csv(ROOT / "bitrix_attacking_january_events_diff_old_vs_smart.csv", index=False, encoding="utf-8")

    other_rows = cohort[cohort["Мероприятие_класс"] == "Другое"].copy()
    review_cols = [c for c in ["ID", "Контакт: ID", "Название сделки", "Код_курса_сайт", "Код курса", "UTM Campaign", "Источник (подробно)", "Источник обращения", "Выручка", "Источник_классификации"] if c in other_rows.columns]
    other_rows[review_cols].head(50).to_csv(ROOT / "bitrix_attacking_january_other_review_top50_smart.csv", index=False, encoding="utf-8")

    qa = {
        "global": {
            "deals_total": int(deals["ID"].nunique()),
            "deals_with_revenue": int(deals.loc[deals["Выручка_учитывается"], "ID"].nunique()),
            "other_share_revenue_deals": round(float((deals.loc[deals["Выручка_учитывается"], "Мероприятие_класс"] == "Другое").mean()), 4),
        },
        "attacking_january": {
            "cohort_contacts": int(len(cohort_ids)),
            "deals_total": int(cohort["ID"].nunique()),
            "deals_with_revenue": int(cohort.loc[cohort["Выручка_учитывается"], "ID"].nunique()),
            "other_share_revenue_deals": round(float((cohort.loc[cohort["Выручка_учитывается"], "Мероприятие_класс"] == "Другое").mean()), 4),
            "revenue_total": round(float(cohort.loc[cohort["Выручка_учитывается"], "Выручка"].sum()), 2),
        },
        "files": {
            "global_events": str(ROOT / "bitrix_global_revenue_events_smart.csv"),
            "global_types": str(ROOT / "bitrix_global_revenue_contact_types_smart.csv"),
            "cohort_events": str(ROOT / "bitrix_attacking_january_revenue_events_smart.csv"),
            "cohort_other_review_top50": str(ROOT / "bitrix_attacking_january_other_review_top50_smart.csv"),
            "cohort_diff_old_vs_smart": str(ROOT / "bitrix_attacking_january_events_diff_old_vs_smart.csv") if diff_df is not None else "",
            "cohort_by_normalized_course_code": str(ROOT / "bitrix_attacking_january_revenue_by_normalized_course_code_smart.csv"),
        },
    }
    (ROOT / "bitrix_smart_classification_qa_report.json").write_text(json.dumps(qa, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(qa, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
