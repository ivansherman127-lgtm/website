"""
Lead qual/unqual and funnel exclusion — aligned with bitrix_notebook.ipynb (check_lead_type).

- Воронка для отчётов: только склеенные колонки «Воронка», «Воронка.1», … (без fl_*, без «Откуда лид»).
  Пусто или не из канонических корзин → «Другое» (funnel_report_bucket). Сырые строки — union
  fl_raw_09-03 + bitrix_upd_27.03 (bitrix_union_io).
- Спец/УЦ: drop rows only for exact names (as in notebook), never include in metrics.
- Квал / Неквал / Отказ: from funnel + «Стадия сделки», not from «Неквалифицированные заявки».
  Unqual bucket in notebook = lead_type in {unqual, unknown}; Refusal separate.
- CRM «типы некачественного лида» (INVALID_TOKENS): forced to unqual (except left as refusal
  if already refusal), so Неквал includes them; separate column is_invalid keeps token hits.
"""
from __future__ import annotations

import re
from typing import FrozenSet

import numpy as np
import pandas as pd

# Синхронно с bitrix_funnel_reporting.json (правила отчётных корзин).
EXCLUDED_FUNNEL_NAMES: FrozenSet[str] = frozenset(
    {
        "Спец. проекты",
        "Учебный центр",
        "Спецпроекты",
    }
)

# Точные подписи воронки из CRM → такая же строка в отчёте; всё остальное (и пусто) → FUNNEL_REPORT_OTHER.
CANONICAL_REPORT_FUNNELS: tuple[str, ...] = (
    "B2B",
    "B2C",
    "Горячая воронка",
    "Карьерная консультация",
    "Реактивация",
    "Холодная воронка",
)
CANONICAL_REPORT_FUNNEL_SET: FrozenSet[str] = frozenset(CANONICAL_REPORT_FUNNELS)
FUNNEL_REPORT_OTHER = "Другое"

INVALID_TOKENS = ("дубль", "тест", "спам", "чс", "неправильные данные", "партнер", "сотрудник")

# Квал/неквал по стадиям: как в ноутбуке + «Карьерная консультация» с теми же правилами, что горячая/холодная.
_INBOUND_HOT_COLD = frozenset(
    {"Входящие лиды", "Горячая воронка", "Холодная воронка", "Карьерная консультация"}
)
_B2 = frozenset({"B2B", "B2C"})


def coalesce_columns(df: pd.DataFrame, base: str) -> pd.Series:
    """First non-empty among base, base.1, … (Bitrix duplicate headers)."""
    if base in df.columns:
        v = df[base]
        if isinstance(v, pd.DataFrame):
            acc = v.iloc[:, 0].fillna("").astype(str).str.strip()
            for i in range(1, v.shape[1]):
                s = v.iloc[:, i].fillna("").astype(str).str.strip()
                acc = acc.where(acc.ne(""), s)
            return acc
    pat = re.compile(rf"^{re.escape(base)}(\.\d+)?$")
    names = [str(c) for c in df.columns if pat.match(str(c).strip())]
    if not names:
        return pd.Series([""] * len(df), index=df.index, dtype=str)
    acc = df[names[0]].fillna("").astype(str).str.strip()
    for c in names[1:]:
        s = df[c].fillna("").astype(str).str.strip()
        acc = acc.where(acc.ne(""), s)
    return acc


def deal_funnel_raw_series(df: pd.DataFrame) -> pd.Series:
    """
    Только колонка CRM «Воронка» (первое непустое среди Воронка, Воронка.1, …).
    Без fl_Воронка и без «Откуда лид»: пусто или не из canonical → «Другое» на этапе funnel_report_bucket.
    """
    return coalesce_columns(df, "Воронка").fillna("").astype(str).str.strip()


def drop_rows_excluded_funnels(df: pd.DataFrame) -> pd.DataFrame:
    f = deal_funnel_raw_series(df)
    t = f.fillna("").astype(str).str.strip()
    return df.loc[~t.isin(EXCLUDED_FUNNEL_NAMES)].copy()


def funnel_report_bucket(raw: object) -> str:
    """
    Корзина для группировки в отчётах. Исключённые воронки в датафрейм не должны попадать (см. drop_rows_excluded_funnels).
    Пустое / неизвестное → FUNNEL_REPORT_OTHER.
    """
    if raw is None:
        t = ""
    else:
        try:
            if pd.isna(raw):
                t = ""
            else:
                t = str(raw).strip()
        except (ValueError, TypeError):
            t = str(raw).strip()
    if not t or t.lower() == "nan":
        return FUNNEL_REPORT_OTHER
    if t in EXCLUDED_FUNNEL_NAMES:
        return FUNNEL_REPORT_OTHER
    if t in CANONICAL_REPORT_FUNNEL_SET:
        return t
    return FUNNEL_REPORT_OTHER


def funnel_report_bucket_series(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip().map(funnel_report_bucket)


def funnel_report_sort_key(bucket_label: object) -> int:
    """Порядок строк в сводках: канонические воронки, затем Другое (уже сгруппированные подписи)."""
    name = str(bucket_label).strip() if bucket_label is not None else ""
    if not name:
        name = FUNNEL_REPORT_OTHER
    order = list(CANONICAL_REPORT_FUNNELS) + [FUNNEL_REPORT_OTHER]
    try:
        return order.index(name)
    except ValueError:
        return len(order)


def invalid_token_mask(df: pd.DataFrame) -> pd.Series:
    a = df.get("Типы некачественного лида", "")
    b = df.get("Типы некачественных лидов", "")
    if not isinstance(a, pd.Series):
        a = pd.Series([""] * len(df), index=df.index)
    if not isinstance(b, pd.Series):
        b = pd.Series([""] * len(df), index=df.index)
    blob = (a.fillna("").astype(str) + " " + b.fillna("").astype(str)).str.lower()
    return blob.apply(lambda s: any(tok in s for tok in INVALID_TOKENS))


def lead_type_series(funnel: pd.Series, stage: pd.Series) -> pd.Series:
    """Same branches as check_lead_type in bitrix_notebook.ipynb (vectorized)."""
    ft = funnel.fillna("").astype(str).str.strip()
    st = stage.fillna("").astype(str).str.strip()

    m_ihc = ft.isin(_INBOUND_HOT_COLD)
    m_b2 = ft.isin(_B2)
    m_re = ft == "Реактивация"
    st_nq = st == "Некачественный лид"
    st_demo = st == "Получившие демо-доступ"
    st_ref = st == "Сделка не заключена"

    n = len(ft)
    out = np.full(n, "ignore", dtype=object)
    out[m_ihc & st_nq] = "unqual"
    out[m_ihc & st_demo] = "qual"
    out[m_ihc & ~st_nq & ~st_demo] = "unknown"
    out[m_b2 & st_ref] = "refusal"
    out[m_b2 & ~st_ref] = "qual"
    out[m_re & st_nq] = "unqual"
    out[m_re & ~st_nq] = "unknown"
    return pd.Series(out, index=funnel.index, dtype=object)


def in_work_series(df: pd.DataFrame) -> pd.Series:
    """Heuristic «в работе»: не закрыт/не отказ, не помечен невалидом по токенам."""
    stage = coalesce_columns(df, "Стадия сделки").fillna("").astype(str).str.lower()
    bad = ("сделка заключена", "сделка закрыта", "отказ", "неквал", "спам", "дубл", "чс")
    inv = df["is_invalid"] if "is_invalid" in df.columns else invalid_token_mask(df)
    return stage.ne("") & (~stage.apply(lambda s: any(t in s for t in bad))) & (~inv)


def apply_notebook_lead_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Adds Воронка (как в ноутбуке: fl_* или CRM), lead_type, is_qual, …
    
    Classification (per bitrix_lead_logic.json):
    - is_qual: qual + refusal + qual_from_date (all we consider qualified)
    - is_unqual: not_qual + not_yet (we know they are not qualified)
    - is_unknown: unassigned (we don't know their qual state)
    - Total: is_qual + is_unqual + is_unknown = all leads
    """
    d = df.copy()
    funnel_c = deal_funnel_raw_series(d)
    stage_c = coalesce_columns(d, "Стадия сделки")
    d["Воронка"] = funnel_c
    d["Воронка_группа"] = funnel_report_bucket_series(funnel_c)

    lt = lead_type_series(funnel_c, stage_c)
    inv = invalid_token_mask(d)
    lt_arr = lt.astype(str).to_numpy()
    inv_arr = inv.to_numpy()
    ref_mask = lt_arr == "refusal"
    lt_arr = np.where(inv_arr & ~ref_mask, "unqual", lt_arr)
    d["lead_type"] = lt_arr
    d["is_invalid"] = inv
    # Qual: includes refusals (must be qualified to refuse), qual_from_date, and qual
    d["is_qual"] = (lt_arr == "qual") | (lt_arr == "refusal")
    # Unqual: not qualified or qual tokens (invalid leads without refusal marker)
    d["is_unqual"] = (lt_arr == "unqual")
    # Unknown: unassigned qual state (we don't know if they're qual or not)
    d["is_unknown"] = (lt_arr == "unknown")
    # Legacy column: unqual_reported = unqual + unknown (kept for backward compat)
    d["is_unqual_reported"] = d["is_unqual"] | d["is_unknown"]
    # Refusal: separate flag for conversion analysis (but counted in is_qual)
    d["is_refusal"] = lt_arr == "refusal"
    return d
