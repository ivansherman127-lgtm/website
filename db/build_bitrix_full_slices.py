from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bitrix_lead_quality import (
    apply_notebook_lead_flags,
    coalesce_columns,
    drop_rows_excluded_funnels,
    funnel_report_sort_key,
    in_work_series,
)
from bitrix_union_io import load_bitrix_deals_union
from revenue_variant3 import variant3_revenue_mask

ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = ROOT / "web" / "public" / "data"
OUT_MONTH = WEB_DIR / "bitrix_month_total_full.json"
OUT_FUNNEL = WEB_DIR / "bitrix_funnel_month_code_full.json"

INVALID_MONTH = "Невалидная дата оплаты"


def _n(v: object) -> str:
    if v is None or pd.isna(v):
        return ""
    s = str(v).strip()
    return "" if s.lower() in {"", "nan", "none", "null"} else s


def _amt(v: object) -> float:
    s = _n(v).replace(" ", "").replace("\xa0", "").replace(",", ".")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _add_kpi(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    leads = out["Лиды"].replace(0, pd.NA)
    out["Конверсия в Квал"] = (out["Квал"] / leads).fillna(0.0)
    out["Конверсия в Неквал"] = (out["Неквал"] / leads).fillna(0.0)
    out["Конверсия в Отказ"] = (out["Отказы"] / leads).fillna(0.0)
    out["Конверсия в работе"] = (out["В работе"] / leads).fillna(0.0)
    out["Средний_чек"] = (out["Выручка"] / out["Сделок_с_выручкой"].replace(0, pd.NA)).fillna(0.0)
    return out


def _json_records(df: pd.DataFrame) -> str:
    safe = df.where(pd.notna(df), None)
    return json.dumps(safe.to_dict(orient="records"), ensure_ascii=False, allow_nan=False)


def run() -> None:
    d = load_bitrix_deals_union()
    d = drop_rows_excluded_funnels(d)
    d["ID"] = d.get("ID", "").fillna("").astype(str).str.replace(r"\.0+$", "", regex=True).str.strip()
    d["Сумма_число"] = d.get("Сумма", "").map(_amt)
    d["is_revenue"] = variant3_revenue_mask(d)
    d["Выручка"] = d["Сумма_число"].where(d["is_revenue"], 0.0)
    d["ID_выручка"] = d["ID"].where(d["is_revenue"], "")

    d = apply_notebook_lead_flags(d)
    d["is_in_work"] = in_work_series(d)

    # For month slices we need ALL leads, so month is based on lead creation date.
    dt = pd.to_datetime(d.get("Дата создания", "").fillna("").astype(str).str.strip(), errors="coerce", dayfirst=True)
    d["Месяц"] = dt.dt.strftime("%Y-%m")
    d.loc[dt.isna(), "Месяц"] = INVALID_MONTH
    d["Код_курса_норм"] = d.get("Нормализованный_код_курса", d.get("Код_курса_сайт", "")).fillna("").astype(str).str.strip()

    by_month = (
        d.groupby(["Месяц"], dropna=False)
        .agg(
            Лиды=("ID", "count"),
            Квал=("is_qual", "sum"),
            Неквал=("is_unqual_reported", "sum"),
            Отказы=("is_refusal", "sum"),
            В_работе=("is_in_work", "sum"),
            Невалидные_лиды=("is_invalid", "sum"),
            Сделок_с_выручкой=("ID_выручка", lambda s: int(pd.Series([x for x in s if _n(x)], dtype="object").nunique())),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
        .rename(columns={"В_работе": "В работе"})
        .sort_values(["Месяц"], ascending=[True])
        .reset_index(drop=True)
    )
    by_month = _add_kpi(by_month)

    by_funnel = (
        d.groupby(["Воронка_группа", "Месяц", "Код_курса_норм"], dropna=False)
        .agg(
            Лиды=("ID", "count"),
            Квал=("is_qual", "sum"),
            Неквал=("is_unqual_reported", "sum"),
            Отказы=("is_refusal", "sum"),
            В_работе=("is_in_work", "sum"),
            Невалидные_лиды=("is_invalid", "sum"),
            Сделок_с_выручкой=("ID_выручка", lambda s: int(pd.Series([x for x in s if _n(x)], dtype="object").nunique())),
            Выручка=("Выручка", "sum"),
        )
        .reset_index()
        .rename(columns={"Воронка_группа": "Воронка", "В_работе": "В работе"})
    )
    by_funnel["_f_ord"] = by_funnel["Воронка"].map(funnel_report_sort_key)
    by_funnel = (
        by_funnel.sort_values(["_f_ord", "Месяц", "Выручка"], ascending=[True, True, False])
        .drop(columns=["_f_ord"])
        .reset_index(drop=True)
    )
    by_funnel = _add_kpi(by_funnel)

    WEB_DIR.mkdir(parents=True, exist_ok=True)
    OUT_MONTH.write_text(_json_records(by_month), encoding="utf-8")
    OUT_FUNNEL.write_text(_json_records(by_funnel), encoding="utf-8")


if __name__ == "__main__":
    run()
