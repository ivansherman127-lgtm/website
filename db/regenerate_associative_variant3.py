from __future__ import annotations

import json
import math
import re
from pathlib import Path

import pandas as pd

from bitrix_lead_quality import (
    apply_notebook_lead_flags,
    coalesce_columns,
    deal_funnel_raw_series,
    drop_rows_excluded_funnels,
    funnel_report_bucket_series,
    funnel_report_sort_key,
    in_work_series,
)
from revenue_variant3 import variant3_revenue_mask

ROOT = Path(__file__).resolve().parent.parent
BASE = ROOT / "bitrix_attacking_january_associative_base_smart.csv"
OUT_EVENTS = ROOT / "bitrix_attacking_january_associative_revenue_by_events.csv"
OUT_CODES = ROOT / "bitrix_attacking_january_associative_revenue_by_course_codes.csv"
OUT_EVENT_CODE = ROOT / "bitrix_attacking_january_associative_revenue_by_event_and_code.csv"
OUT_MONTH = ROOT / "bitrix_attacking_january_associative_revenue_by_month.csv"
OUT_MANAGERS = ROOT / "bitrix_attacking_january_associative_revenue_by_managers.csv"
OUT_FUNNELS = ROOT / "bitrix_attacking_january_associative_revenue_by_funnels.csv"
OUT_MONTH_CODE = ROOT / "bitrix_attacking_january_associative_revenue_by_month_and_code.csv"
OUT_CODE_MONTH = ROOT / "bitrix_attacking_january_associative_revenue_by_code_and_month.csv"
OUT_FUNNEL_MONTH_CODE = ROOT / "bitrix_attacking_january_associative_funnel_month_code_full.csv"
OUT_MONTH_TOTAL = ROOT / "bitrix_attacking_january_associative_month_total_full.csv"
OUT_REPORT = ROOT / "bitrix_attacking_january_associative_revenue_report.json"
OUT_MONTH_REPORT = ROOT / "bitrix_attacking_january_associative_revenue_by_month_report.json"
INVALID_MONTH = "Невалидная дата оплаты"
WEB_DATA_DIR = ROOT / "web" / "public" / "data"
WEB_MONTH_JSON = WEB_DATA_DIR / "attacking_january_associative_revenue_by_month.json"
WEB_EVENTS_JSON = WEB_DATA_DIR / "attacking_january_associative_revenue_by_events.json"
WEB_CODES_JSON = WEB_DATA_DIR / "attacking_january_associative_revenue_by_course_codes.json"
WEB_MANAGERS_JSON = WEB_DATA_DIR / "attacking_january_associative_revenue_by_managers.json"
WEB_FUNNELS_JSON = WEB_DATA_DIR / "attacking_january_associative_revenue_by_funnels.json"
WEB_MONTH_CODE_JSON = WEB_DATA_DIR / "attacking_january_associative_revenue_by_month_and_code.json"
WEB_CODE_MONTH_JSON = WEB_DATA_DIR / "attacking_january_associative_revenue_by_code_and_month.json"
WEB_FUNNEL_MONTH_CODE_JSON = WEB_DATA_DIR / "attacking_january_associative_funnel_month_code_full.json"
WEB_MONTH_TOTAL_JSON = WEB_DATA_DIR / "attacking_january_associative_month_total_full.json"
WEB_DEALS_JSON = WEB_DATA_DIR / "attacking_january_associative_deals_base.json"


def _n(v: object) -> str:
    if v is None or pd.isna(v):
        return ""
    s = str(v).strip()
    return "" if s.lower() in {"", "nan", "none", "null"} else s


def _id(v: object) -> str:
    s = _n(v)
    return s.split(".", 1)[0] if re.fullmatch(r"\d+\.0+", s) else s


def _amt(v: object) -> float:
    s = _n(v).replace(" ", "").replace("\xa0", "").replace(",", ".")
    try:
        return float(s) if s else 0.0
    except ValueError:
        return 0.0


def _safe_group(df: pd.DataFrame, by: list[str]) -> pd.core.groupby.DataFrameGroupBy:
    for c in by:
        if c not in df.columns:
            df[c] = ""
    return df.groupby(by, dropna=False)


def _json_cell(v: object) -> object | None:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        if pd.isna(v):
            return None
    except TypeError:
        pass
    return v


def _json_records(df: pd.DataFrame) -> str:
    rows = [{str(k): _json_cell(v) for k, v in row.items()} for row in df.to_dict(orient="records")]
    return json.dumps(rows, ensure_ascii=False, allow_nan=False)


def _nunique_nonempty(s: pd.Series) -> int:
    return int(pd.Series([x for x in s if _n(x)], dtype="object").nunique())

def _add_quality_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    leads = out["Лиды"].replace(0, pd.NA)
    out["Конверсия в Квал"] = (out["Квал"] / leads).fillna(0.0)
    out["Конверсия в Неквал"] = (out["Неквал"] / leads).fillna(0.0)
    out["Конверсия в Отказ"] = (out["Отказы"] / leads).fillna(0.0)
    out["Конверсия в работе"] = (out["В работе"] / leads).fillna(0.0)
    out["Средний_чек"] = (out["Выручка"] / out["Сделок_с_выручкой"].replace(0, pd.NA)).fillna(0.0)
    return out


def main() -> None:
    if not BASE.exists():
        raise SystemExit(f"Base file not found: {BASE}")

    d = pd.read_csv(BASE, encoding="utf-8", low_memory=False)
    d = drop_rows_excluded_funnels(d)
    d["Воронка_группа"] = funnel_report_bucket_series(deal_funnel_raw_series(d))
    d["ID"] = d.get("ID", "").map(_id)
    d["Контакт: ID"] = d.get("Контакт: ID", "").map(_id)
    d["Сумма_число"] = d.get("Сумма", 0).map(_amt)

    # Track diagnostics before applying canonical stage-based revenue gating.
    stage = coalesce_columns(d, "Стадия сделки").fillna("").astype(str).str.lower()
    closed_stage = stage.str.contains("сделка заключена", na=False)
    post_stage = stage.str.contains("постоплат", na=False)
    inst_stage = stage.str.contains("рассроч", na=False)
    variant3_core = closed_stage | post_stage | inst_stage
    pay_date_raw = d.get("Дата оплаты", "").fillna("").astype(str).str.strip()
    pay_date_valid = pd.to_datetime(pay_date_raw, errors="coerce", dayfirst=True).notna()

    mask = variant3_revenue_mask(d)
    d["Выручка_учитывается"] = mask
    d["Выручка"] = d["Сумма_число"].where(mask, 0.0)

    # keep backward compatibility with legacy helper columns
    d["Выручка_для_расчета"] = d["Выручка"]

    # overwrite base with corrected flag/revenue
    d.to_csv(BASE, index=False, encoding="utf-8")

    rev = d[d["Выручка_учитывается"]].copy()

    ev_col = "Мероприятие_класс" if "Мероприятие_класс" in rev.columns else "Мероприятие"
    code_col = (
        "Нормализованный_код_курса"
        if "Нормализованный_код_курса" in rev.columns
        else ("Код_курса_норм" if "Код_курса_норм" in rev.columns else "Код_курса_сайт")
    )

    # by events
    by_events = (
        _safe_group(rev, [ev_col])
        .agg(
            Контактов_с_выручкой=("Контакт: ID", lambda s: pd.Series([x for x in s if _n(x)]).nunique()),
            Сделок_с_выручкой=("ID", "nunique"),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
        .rename(columns={ev_col: "Мероприятие"})
        .sort_values("Выручка", ascending=False)
        .reset_index(drop=True)
    )
    by_events["Средний_чек"] = by_events["Выручка"] / by_events["Сделок_с_выручкой"].replace(0, pd.NA)
    by_events["Средний_чек"] = by_events["Средний_чек"].fillna(0.0)
    by_events.to_csv(OUT_EVENTS, index=False, encoding="utf-8")

    # by course codes
    by_codes = (
        _safe_group(rev, [code_col])
        .agg(
            Контактов_с_выручкой=("Контакт: ID", lambda s: pd.Series([x for x in s if _n(x)]).nunique()),
            Сделок_с_выручкой=("ID", "nunique"),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
        .rename(columns={code_col: "Код_курса_норм"})
        .sort_values("Выручка", ascending=False)
        .reset_index(drop=True)
    )
    by_codes["Средний_чек"] = by_codes["Выручка"] / by_codes["Сделок_с_выручкой"].replace(0, pd.NA)
    by_codes["Средний_чек"] = by_codes["Средний_чек"].fillna(0.0)
    by_codes.to_csv(OUT_CODES, index=False, encoding="utf-8")

    # by event and code
    by_event_code = (
        _safe_group(rev, [ev_col, code_col])
        .agg(
            Контактов_с_выручкой=("Контакт: ID", lambda s: pd.Series([x for x in s if _n(x)]).nunique()),
            Сделок_с_выручкой=("ID", "nunique"),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
        .rename(columns={ev_col: "Мероприятие", code_col: "Код_курса_норм"})
        .sort_values("Выручка", ascending=False)
        .reset_index(drop=True)
    )
    by_event_code["Средний_чек"] = (
        by_event_code["Выручка"] / by_event_code["Сделок_с_выручкой"].replace(0, pd.NA)
    )
    by_event_code["Средний_чек"] = by_event_code["Средний_чек"].fillna(0.0)
    by_event_code.to_csv(OUT_EVENT_CODE, index=False, encoding="utf-8")

    # by month from payment date
    pay_date_raw_rev = rev.get("Дата оплаты", "").fillna("").astype(str).str.strip()
    dt = pd.to_datetime(pay_date_raw_rev, errors="coerce", dayfirst=True)
    rev_m = rev.copy()
    rev_m["Месяц"] = dt.dt.strftime("%Y-%m")
    rev_m.loc[dt.isna(), "Месяц"] = INVALID_MONTH
    by_month = (
        _safe_group(rev_m, ["Месяц"])
        .agg(
            Контактов_с_выручкой=("Контакт: ID", lambda s: pd.Series([x for x in s if _n(x)]).nunique()),
            Сделок_с_выручкой=("ID", "nunique"),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
        .sort_values("Месяц")
        .reset_index(drop=True)
    )
    by_month["Средний_чек"] = by_month["Выручка"] / by_month["Сделок_с_выручкой"].replace(0, pd.NA)
    by_month["Средний_чек"] = by_month["Средний_чек"].fillna(0.0)
    by_month.to_csv(OUT_MONTH, index=False, encoding="utf-8")

    # full KPI base for month<->course slices (not revenue-only)
    base = apply_notebook_lead_flags(d.copy())
    base["is_in_work"] = in_work_series(base)
    base["is_revenue"] = base.get("Выручка_учитывается", False).fillna(False).astype(bool)
    base["ID_выручка"] = base.get("ID", "").where(base["is_revenue"], "")
    dt_all = pd.to_datetime(base.get("Дата оплаты", "").fillna("").astype(str).str.strip(), errors="coerce", dayfirst=True)
    base["Месяц"] = dt_all.dt.strftime("%Y-%m")
    base.loc[dt_all.isna(), "Месяц"] = INVALID_MONTH
    base["Код_курса_норм"] = base.get(code_col, "").fillna("").astype(str).str.strip()

    # total by month (all leads with variant3 revenue metrics)
    by_month_total = (
        _safe_group(base, ["Месяц"])
        .agg(
            Лиды=("ID", "count"),
            Квал=("is_qual", "sum"),
            Неквал=("is_unqual", "sum"),
            Неизвестно=("is_unknown", "sum"),
            Отказы=("is_refusal", "sum"),
            В_работе=("is_in_work", "sum"),
            Невалидные_лиды=("is_invalid", "sum"),
            Сделок_с_выручкой=("ID_выручка", _nunique_nonempty),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
        .sort_values(["Месяц"], ascending=[True])
        .reset_index(drop=True)
        .rename(columns={"В_работе": "В работе"})
    )
    by_month_total = _add_quality_columns(by_month_total)
    by_month_total.to_csv(OUT_MONTH_TOTAL, index=False, encoding="utf-8")

    # by month -> course code with full indicators
    by_month_code = (
        _safe_group(base, ["Месяц", "Код_курса_норм"])
        .agg(
            Лиды=("ID", "count"),
            Квал=("is_qual", "sum"),
            Неквал=("is_unqual", "sum"),
            Неизвестно=("is_unknown", "sum"),
            Отказы=("is_refusal", "sum"),
            В_работе=("is_in_work", "sum"),
            Невалидные_лиды=("is_invalid", "sum"),
            Сделок_с_выручкой=("ID_выручка", _nunique_nonempty),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
        .sort_values(["Месяц", "Выручка"], ascending=[True, False])
        .reset_index(drop=True)
        .rename(columns={"В_работе": "В работе"})
    )
    by_month_code = _add_quality_columns(by_month_code)
    by_month_code.to_csv(OUT_MONTH_CODE, index=False, encoding="utf-8")

    # by course code -> month with full indicators
    by_code_month = (
        _safe_group(base, ["Код_курса_норм", "Месяц"])
        .agg(
            Лиды=("ID", "count"),
            Квал=("is_qual", "sum"),
            Неквал=("is_unqual", "sum"),
            Неизвестно=("is_unknown", "sum"),
            Отказы=("is_refusal", "sum"),
            В_работе=("is_in_work", "sum"),
            Невалидные_лиды=("is_invalid", "sum"),
            Сделок_с_выручкой=("ID_выручка", _nunique_nonempty),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
        .sort_values(["Код_курса_норм", "Выручка"], ascending=[True, False])
        .reset_index(drop=True)
        .rename(columns={"В_работе": "В работе"})
    )
    by_code_month = _add_quality_columns(by_code_month)
    by_code_month.to_csv(OUT_CODE_MONTH, index=False, encoding="utf-8")

    # funnel -> month -> code (all leads, not revenue-only)
    by_funnel_month_code = (
        _safe_group(base, ["Воронка_группа", "Месяц", "Код_курса_норм"])
        .agg(
            Лиды=("ID", "count"),
            Квал=("is_qual", "sum"),
            Неквал=("is_unqual_reported", "sum"),
            Отказы=("is_refusal", "sum"),
            В_работе=("is_in_work", "sum"),
            Невалидные_лиды=("is_invalid", "sum"),
            Сделок_с_выручкой=("ID_выручка", _nunique_nonempty),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
        .rename(columns={"Воронка_группа": "Воронка", "В_работе": "В работе"})
    )
    by_funnel_month_code["_f_ord"] = by_funnel_month_code["Воронка"].map(funnel_report_sort_key)
    by_funnel_month_code = (
        by_funnel_month_code.sort_values(["_f_ord", "Месяц", "Выручка"], ascending=[True, True, False])
        .drop(columns=["_f_ord"])
        .reset_index(drop=True)
    )
    by_funnel_month_code = _add_quality_columns(by_funnel_month_code)
    by_funnel_month_code.to_csv(OUT_FUNNEL_MONTH_CODE, index=False, encoding="utf-8")

    # by managers
    by_managers = (
        _safe_group(rev, ["Ответственный"])
        .agg(
            Контактов_с_выручкой=("Контакт: ID", lambda s: pd.Series([x for x in s if _n(x)]).nunique()),
            Сделок_с_выручкой=("ID", "nunique"),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
        .rename(columns={"Ответственный": "Менеджер"})
        .sort_values("Выручка", ascending=False)
        .reset_index(drop=True)
    )
    by_managers["Средний_чек"] = by_managers["Выручка"] / by_managers["Сделок_с_выручкой"].replace(0, pd.NA)
    by_managers["Средний_чек"] = by_managers["Средний_чек"].fillna(0.0)
    by_managers.to_csv(OUT_MANAGERS, index=False, encoding="utf-8")

    # by funnels
    by_funnels = (
        _safe_group(rev, ["Воронка_группа"])
        .agg(
            Контактов_с_выручкой=("Контакт: ID", lambda s: pd.Series([x for x in s if _n(x)]).nunique()),
            Сделок_с_выручкой=("ID", "nunique"),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
        .rename(columns={"Воронка_группа": "Воронка"})
    )
    by_funnels["_f_ord"] = by_funnels["Воронка"].map(funnel_report_sort_key)
    by_funnels = (
        by_funnels.sort_values(["_f_ord", "Выручка"], ascending=[True, False])
        .drop(columns=["_f_ord"])
        .reset_index(drop=True)
    )
    by_funnels["Средний_чек"] = by_funnels["Выручка"] / by_funnels["Сделок_с_выручкой"].replace(0, pd.NA)
    by_funnels["Средний_чек"] = by_funnels["Средний_чек"].fillna(0.0)
    by_funnels.to_csv(OUT_FUNNELS, index=False, encoding="utf-8")

    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)
    WEB_EVENTS_JSON.write_text(_json_records(by_events), encoding="utf-8")
    WEB_CODES_JSON.write_text(_json_records(by_codes), encoding="utf-8")
    WEB_MONTH_JSON.write_text(_json_records(by_month), encoding="utf-8")
    WEB_MANAGERS_JSON.write_text(_json_records(by_managers), encoding="utf-8")
    WEB_FUNNELS_JSON.write_text(_json_records(by_funnels), encoding="utf-8")
    WEB_MONTH_CODE_JSON.write_text(_json_records(by_month_code), encoding="utf-8")
    WEB_CODE_MONTH_JSON.write_text(_json_records(by_code_month), encoding="utf-8")
    WEB_FUNNEL_MONTH_CODE_JSON.write_text(_json_records(by_funnel_month_code), encoding="utf-8")
    WEB_MONTH_TOTAL_JSON.write_text(_json_records(by_month_total), encoding="utf-8")
    WEB_DEALS_JSON.write_text(_json_records(d), encoding="utf-8")

    report = {
        "кохорта_контактов": int(d["Контакт: ID"].replace("", pd.NA).dropna().nunique()),
        "сделок_кохорты_после_фильтров": int(d["ID"].nunique()),
        "сделок_с_выручкой": int(rev["ID"].nunique()),
        "контактов_с_выручкой": int(rev["Контакт: ID"].replace("", pd.NA).dropna().nunique()),
        "выручка_ассоциативная_итого": float(rev["Выручка"].sum()),
        "variant3_core_rows": int(variant3_core.sum()),
        "with_invalid_payment_date_format": int((mask & ~pay_date_valid).sum()),
        "logic": "variant3_stage_only_with_installment_dates; no_preferred_payment_method",
        "files": {
            "base_revenue_deals": str(BASE),
            "by_events": str(OUT_EVENTS),
            "by_course_codes": str(OUT_CODES),
            "by_event_and_code": str(OUT_EVENT_CODE),
            "by_month": str(OUT_MONTH),
            "by_month_and_code": str(OUT_MONTH_CODE),
            "by_code_and_month": str(OUT_CODE_MONTH),
            "by_funnel_month_code_full": str(OUT_FUNNEL_MONTH_CODE),
            "by_month_total_full": str(OUT_MONTH_TOTAL),
            "by_managers": str(OUT_MANAGERS),
            "by_funnels": str(OUT_FUNNELS),
        },
    }
    OUT_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_MONTH_REPORT.write_text(
        json.dumps(
            {
                "rows": int(len(by_month)),
                "revenue_total": float(by_month["Выручка"].sum() if len(by_month) else 0.0),
                "deals_total": int(by_month["Сделок_с_выручкой"].sum() if len(by_month) else 0),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

