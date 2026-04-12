"""
Revenue recognition helpers.

- ``variant3_api_revenue_mask`` — mart pipeline for ``raw_b24_deals`` / ``STAGE_ID``:
  same intent as CSV ``variant3_revenue_mask`` (сделка заключена / постоплата / рассрочка),
  expressed via Bitrix codes: ``…:WON``, ``WON``, optional extra IDs (рассрочка в вашей воронке),
  plus Russian substrings when стадия уже в текстовом виде.

- ``payment_verified_revenue_mask`` — сумма > 0 и непустая дата оплаты / рассрочка (узже/шире
  в зависимости от заполнения полей; для марта 2026 в CSV все variant3 ⊂ payment_verified).

- ``variant3_revenue_mask`` — только русские подстроки в «Стадия сделки» (CSV).

See REVENUE_CALCULATION.md and ``web_share_subset/webpush/functions/lib/analytics/revenue.ts``.
"""
from __future__ import annotations

import os

import pandas as pd

from bitrix_lead_quality import coalesce_columns
from utils import _amt

_INSTALLMENT_COL = "Даты платежей по рассрочке "


def _nonempty_signal(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    sl = s.str.lower()
    junk = {"", "-", "nan", "none", "null", "undefined"}
    return s.ne("") & ~sl.isin(junk)


def _extra_variant3_stage_ids() -> set[str]:
    raw = os.environ.get("B24_VARIANT3_EXTRA_STAGE_IDS", "C7:UC_P7HXNZ").strip()
    return {x.strip().upper() for x in raw.split(",") if x.strip()}


def variant3_api_revenue_mask(df: pd.DataFrame) -> pd.Series:
    """
    API/``STAGE_ID`` equivalent of CSV variant3 + positive ``Сумма``.
    Cross-checked: March 2026 CSV variant3 deals → C7:WON, WON, C7:UC_P7HXNZ (Рассрочка).
    """
    idx = df.index
    sum_src = df["Сумма"] if "Сумма" in df.columns else pd.Series([""] * len(df), index=idx, dtype=object)
    sum_amt = sum_src.map(_amt)
    stage = df["Стадия сделки"] if "Стадия сделки" in df.columns else pd.Series([""] * len(df), index=idx, dtype=object)
    st = stage.fillna("").astype(str).str.strip()
    up = st.str.upper()
    won = up.str.endswith("WON") | up.eq("WON")
    in_extra = up.isin(_extra_variant3_stage_ids())
    sl = st.str.lower()
    ru = (
        sl.str.contains("сделка заключена", na=False)
        | sl.str.contains("постоплат", na=False)
        | sl.str.contains("рассроч", na=False)
    )
    return (sum_amt > 0) & (won | in_extra | ru)


def payment_verified_revenue_mask(df: pd.DataFrame) -> pd.Series:
    """
    True where the deal should count toward dashboard revenue: amount > 0 and we have
    either a payment date or an installment schedule string (actual money timeline).
    """
    idx = df.index
    sum_src = df["Сумма"] if "Сумма" in df.columns else pd.Series([""] * len(df), index=idx, dtype=object)
    pay = df["Дата оплаты"] if "Дата оплаты" in df.columns else pd.Series([""] * len(df), index=idx, dtype=object)
    inst = df[_INSTALLMENT_COL] if _INSTALLMENT_COL in df.columns else pd.Series([""] * len(df), index=idx, dtype=object)
    sum_amt = sum_src.map(_amt)
    return (sum_amt > 0) & (_nonempty_signal(pay) | _nonempty_signal(inst))


def variant3_revenue_mask(df: pd.DataFrame) -> pd.Series:
    """
    Boolean Series: True where deal revenue counts toward variant3 totals.
    """
    stage = coalesce_columns(df, "Стадия сделки").fillna("").astype(str).str.lower()
    closed_stage = stage.str.contains("сделка заключена", na=False)
    post_stage = stage.str.contains("постоплат", na=False)
    inst_stage = stage.str.contains("рассроч", na=False)
    return closed_stage | post_stage | inst_stage
