"""
Email hierarchy: Bitrix revenue (variant3) + KPI/конверсии + детализация по сделкам.

Используется из email_notebook после сборки email_hierarchy_by_send.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Tuple, Union

import numpy as np
import pandas as pd

from bitrix_lead_quality import drop_rows_excluded_funnels
from revenue_variant3 import variant3_revenue_mask


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


def _exclude_spec_funnels(df: pd.DataFrame) -> pd.DataFrame:
    return drop_rows_excluded_funnels(df)


def _load_deals(source: Union[Path, str, pd.DataFrame]) -> pd.DataFrame:
    if isinstance(source, pd.DataFrame):
        return source.copy()
    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f"Deals file not found: {path}")
    sep = ";" if path.suffix.lower() in (".csv", ".26", "") else ","
    return pd.read_csv(path, sep=sep, encoding="utf-8", low_memory=False, dtype=str)


def deal_id_to_revenue(deals: pd.DataFrame) -> Dict[str, float]:
    d = deals.copy()
    if "ID" not in d.columns:
        return {}
    d["ID"] = d["ID"].map(_id)
    d = d[d["ID"] != ""].drop_duplicates("ID", keep="last")
    d = _exclude_spec_funnels(d)
    mask = variant3_revenue_mask(d)
    amt = pd.to_numeric(d.get("Сумма", 0).map(_amt), errors="coerce").fillna(0.0)
    rev = amt.where(mask, 0.0)
    return {str(i): float(r) for i, r in zip(d["ID"], rev)}


def _deal_residual_row(row: pd.Series) -> float:
    total = _amt(row.get("Сумма", 0))
    paid_col = row.get("Оплачено", None)
    paid = (
        _amt(paid_col)
        if paid_col is not None and str(paid_col).strip() not in {"", "-", "nan"}
        else 0.0
    )
    return max(0.0, total - paid)


def _parse_fl_ids(raw: object) -> List[str]:
    s = str(raw).strip() if raw is not None and not pd.isna(raw) else ""
    if not s or s.lower() in {"", "nan", "none", "-"}:
        return []
    return [
        x.strip()
        for x in s.split(",")
        if x.strip() and x.strip().lower() not in {"", "nan", "none"}
    ]


def _pct(num: float, den: float, digits: int = 2) -> float:
    if den <= 0:
        return 0.0
    return round(100.0 * float(num) / float(den), digits)


def _deal_lookup(deals: pd.DataFrame) -> pd.DataFrame:
    d = deals.copy()
    d["ID"] = d["ID"].map(_id)
    d = d[d["ID"] != ""].drop_duplicates("ID", keep="last")
    return d.set_index("ID", drop=False)


def _fl_ids_revenue_stats(
    ids_raw: object,
    id_rev: Dict[str, float],
    by_id: pd.DataFrame,
) -> Tuple[float, int, float, float]:
    ids = _parse_fl_ids(ids_raw)
    rev_ids = [i for i in ids if id_rev.get(i, 0.0) > 0]
    total = sum(id_rev.get(i, 0.0) for i in ids)
    n = len(rev_ids)
    avg_rev = round(total / n, 2) if n else 0.0
    residuals: List[float] = []
    for i in rev_ids:
        if i in by_id.index:
            residuals.append(_deal_residual_row(by_id.loc[i]))
    avg_res = round(sum(residuals) / len(residuals), 2) if residuals else 0.0
    return (total, n, avg_rev, avg_res)


def _month_spacer_sync(out: pd.DataFrame, month_col: str, cols: List[str]) -> None:
    month_mask = out.get("Level", pd.Series(dtype=str)).astype(str).eq("Month")
    spacer_mask = out.get("Level", pd.Series(dtype=str)).astype(str).eq("Spacer")

    def month_from_spacer(label: str) -> str:
        s = str(label).strip()
        if "итого:" in s.lower():
            return s.split(":", 1)[1].strip()
        return s

    for idx in out.loc[spacer_mask].index:
        m = month_from_spacer(str(out.at[idx, month_col]))
        mrows = out.loc[month_mask & (out[month_col].astype(str).str.strip() == m)]
        if len(mrows):
            for c in cols:
                if c in mrows.columns:
                    out.at[idx, c] = float(pd.to_numeric(mrows[c], errors="coerce").iloc[0])


def enrich_hierarchy_with_revenue(
    hierarchy: pd.DataFrame,
    deals_source: Union[Path, str, pd.DataFrame],
    *,
    ids_col: str = "fl_IDs",
    revenue_col: str = "Выручка",
    month_col: str = "Месяц",
    add_deal_stats: bool = True,
) -> pd.DataFrame:
    deals = _load_deals(deals_source)
    id_rev = deal_id_to_revenue(deals)
    by_id = _deal_lookup(_exclude_spec_funnels(deals.copy()))

    out = hierarchy.copy()

    def row_rev(cell: object) -> float:
        return sum(id_rev.get(i, 0.0) for i in _parse_fl_ids(cell))

    if ids_col in out.columns:
        out[revenue_col] = out[ids_col].map(row_rev)
        if add_deal_stats:
            stats = out[ids_col].map(lambda c: _fl_ids_revenue_stats(c, id_rev, by_id))
            out["Сделок с выручкой"] = stats.map(lambda t: int(t[1]))
            out["Средняя выручка на сделку"] = stats.map(lambda t: float(t[2]))
            out["Средний остаток по сделке"] = stats.map(lambda t: float(t[3]))
    else:
        out[revenue_col] = 0.0
        if add_deal_stats:
            out["Сделок с выручкой"] = 0
            out["Средняя выручка на сделку"] = 0.0
            out["Средний остаток по сделке"] = 0.0

    month_mask = out.get("Level", pd.Series(dtype=str)).astype(str).eq("Month")
    send_mask = out.get("Level", pd.Series(dtype=str)).astype(str).eq("Send")

    if not (month_mask.any() and send_mask.any() and month_col in out.columns):
        return out

    sync_cols = [revenue_col]
    if add_deal_stats:
        sync_cols.extend(["Сделок с выручкой", "Средняя выручка на сделку", "Средний остаток по сделке"])

    sp = out.loc[send_mask].copy()
    for c in sync_cols:
        sp[c] = pd.to_numeric(sp[c], errors="coerce").fillna(0.0)

    grp = sp.groupby(month_col, dropna=False)
    rev_sum = grp[revenue_col].sum()
    if add_deal_stats:
        n_sum = grp["Сделок с выручкой"].sum()
        avg_rev_month = (rev_sum / n_sum.replace(0, np.nan)).fillna(0).round(2)
        w_num = (
            pd.to_numeric(out.loc[send_mask, "Средний остаток по сделке"], errors="coerce").fillna(0)
            * pd.to_numeric(out.loc[send_mask, "Сделок с выручкой"], errors="coerce").fillna(0)
        )
        tmp = out.loc[send_mask, [month_col]].copy()
        tmp["_w"] = w_num.values
        avg_res_month = (tmp.groupby(month_col, dropna=False)["_w"].sum() / n_sum.replace(0, np.nan)).fillna(0).round(2)

    months = out.loc[month_mask, month_col].astype(str).str.strip()
    out.loc[month_mask, revenue_col] = months.map(lambda m: float(rev_sum.get(m, 0.0)))
    if add_deal_stats:
        out.loc[month_mask, "Сделок с выручкой"] = months.map(lambda m: float(n_sum.get(m, 0.0)))
        out.loc[month_mask, "Средняя выручка на сделку"] = months.map(
            lambda m: float(avg_rev_month.get(m, 0.0))
        )
        out.loc[month_mask, "Средний остаток по сделке"] = months.map(
            lambda m: float(avg_res_month.get(m, 0.0))
        )

    _month_spacer_sync(out, month_col, sync_cols)

    return out


def add_email_kpi_columns(df: pd.DataFrame, month_col: str = "Месяц") -> pd.DataFrame:
    """
    Конверсии от базы «Доставлено».
    QualCheck: сумма квал+неквал+отказы vs Лиды (OK / ≠) на строках Send.
    """
    out = df.copy()
    lvl = out.get("Level", pd.Series("", index=out.index)).astype(str)

    dlv = pd.to_numeric(out.get("Доставлено", 0), errors="coerce").fillna(0)
    u_open = pd.to_numeric(out.get("Уник. открытий", 0), errors="coerce").fillna(0)
    u_click = pd.to_numeric(out.get("Уник. кликов", 0), errors="coerce").fillna(0)
    unsub = pd.to_numeric(out.get("Отписок", 0), errors="coerce").fillna(0)

    leads = pd.to_numeric(out.get("Leads", 0), errors="coerce").fillna(0)
    qual = pd.to_numeric(out.get("Qual", 0), errors="coerce").fillna(0)
    unqual = pd.to_numeric(out.get("Unqual", 0), errors="coerce").fillna(0)
    refusal = pd.to_numeric(out.get("Refusal", 0), errors="coerce").fillna(0)

    out["Лиды"] = leads
    out["Qual"] = qual
    out["Квал Лиды"] = qual
    out["Неквал"] = unqual
    out["Отказы"] = refusal

    check = qual + unqual + refusal - leads
    out["QualCheck"] = ""
    send_lvl = ~(lvl.isin(["Month", "Spacer"]))
    out.loc[send_lvl, "QualCheck"] = np.where(
        check[send_lvl].abs() < 0.5,
        "OK",
        "≠",
    )

    ctor_source = out.get("CTOR, %", pd.Series(index=out.index, dtype=float))
    ctor_num = pd.to_numeric(
        ctor_source.astype(str).str.replace(",", "."), errors="coerce"
    )
    ctor_calc = pd.Series(
        [_pct(float(uc), float(uo)) for uc, uo in zip(u_click, u_open)],
        index=out.index,
    )
    out["CTOR%"] = ctor_num.where(ctor_num.notna() & (ctor_num > 0), ctor_calc).fillna(0)

    out["%Уник открытий"] = [_pct(float(u), float(dd)) for u, dd in zip(u_open, dlv)]
    out["Конверсия в Отписки"] = [_pct(float(u), float(dd)) for u, dd in zip(unsub, dlv)]
    out["Конверсия в Лиды"] = [_pct(float(l), float(dd)) for l, dd in zip(leads, dlv)]
    out["Конверсия в Квал"] = [_pct(float(q), float(dd)) for q, dd in zip(qual, dlv)]
    out["Конверсия в Неквал"] = [_pct(float(u), float(dd)) for u, dd in zip(unqual, dlv)]
    out["Конверсия в Отказ"] = [_pct(float(r), float(dd)) for r, dd in zip(refusal, dlv)]

    send_mask = lvl.eq("Send")
    month_mask = lvl.eq("Month")
    spacer_mask = lvl.eq("Spacer")

    def month_from_spacer_label(label: str) -> str:
        s = str(label).strip()
        if "итого:" in s.lower():
            return s.split(":", 1)[1].strip()
        return s

    pct_cols = [
        "CTOR%",
        "%Уник открытий",
        "Конверсия в Отписки",
        "Конверсия в Лиды",
        "Конверсия в Квал",
        "Конверсия в Неквал",
        "Конверсия в Отказ",
    ]

    if send_mask.any() and month_mask.any():
        g = out.loc[send_mask]
        agg = (
            g.groupby(month_col, dropna=False)
            .agg(
                Доставлено=("Доставлено", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                Уник_открытий=("Уник. открытий", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                Уник_кликов=("Уник. кликов", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                Отписок=("Отписок", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                Лиды=("Лиды", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                Квал=("Квал Лиды", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                Неквал=("Неквал", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                Отказы=("Отказы", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
            )
            .reset_index()
        )
        for _, row in agg.iterrows():
            m = str(row[month_col]).strip()
            dd = float(row["Доставлено"])
            uo = float(row["Уник_открытий"])
            uc = float(row["Уник_кликов"])
            us = float(row["Отписок"])
            lm = float(row["Лиды"])
            qm = float(row["Квал"])
            um = float(row["Неквал"])
            rm = float(row["Отказы"])
            idx = out.index[month_mask & (out[month_col].astype(str).str.strip() == m)]
            for i in idx:
                out.at[i, "CTOR%"] = _pct(uc, uo)
                out.at[i, "%Уник открытий"] = _pct(uo, dd)
                out.at[i, "Конверсия в Отписки"] = _pct(us, dd)
                out.at[i, "Конверсия в Лиды"] = _pct(lm, dd)
                out.at[i, "Конверсия в Квал"] = _pct(qm, dd)
                out.at[i, "Конверсия в Неквал"] = _pct(um, dd)
                out.at[i, "Конверсия в Отказ"] = _pct(rm, dd)
                ch = qm + um + rm - lm
                out.at[i, "QualCheck"] = "" if abs(ch) < 0.5 else "≠"

        for idx in out.loc[spacer_mask].index:
            m = month_from_spacer_label(str(out.at[idx, month_col]))
            midx = out.index[month_mask & (out[month_col].astype(str).str.strip() == m)]
            if len(midx):
                mi = midx[0]
                for c in pct_cols:
                    if c in out.columns:
                        out.at[idx, c] = out.at[mi, c]
                out.at[idx, "QualCheck"] = out.at[mi, "QualCheck"]

    return out


def finalize_email_sheet(
    hierarchy: pd.DataFrame,
    deals_source: Union[Path, str, pd.DataFrame],
    *,
    month_col: str = "Месяц",
) -> pd.DataFrame:
    """Полный набор: KPI, затем выручка и метрики по сделкам с пересчётом Month/Spacer."""
    h = add_email_kpi_columns(hierarchy, month_col=month_col)
    return enrich_hierarchy_with_revenue(h, deals_source, month_col=month_col, add_deal_stats=True)
