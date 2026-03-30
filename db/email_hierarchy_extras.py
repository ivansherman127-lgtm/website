"""
Operational columns for email hierarchy (Задачи: «Email неправильный», рассылки/месяц).

Expects `work_me` = matched_email narrowed to rows with real month and send (same as hierarchy build).
"""
from __future__ import annotations

import re
import pandas as pd

_EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@"
    r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"
    r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$"
)


def is_plausible_email(val: object) -> bool:
    if val is None or pd.isna(val):
        return False
    s = str(val).strip()
    if s in {"", "-", "nan", "none"}:
        return False
    return bool(_EMAIL_RE.match(s))


def _month_from_spacer(label: str) -> str:
    s = str(label).strip()
    if "итого:" in s.lower():
        return s.split(":", 1)[1].strip()
    return s


def _sync_spacer_from_month(
    out: pd.DataFrame,
    month_col: str,
    cols: list[str],
) -> None:
    month_mask = out.get("Level", pd.Series(dtype=str)).astype(str).eq("Month")
    spacer_mask = out.get("Level", pd.Series(dtype=str)).astype(str).eq("Spacer")
    for idx in out.loc[spacer_mask].index:
        m = _month_from_spacer(str(out.at[idx, month_col]))
        mrows = out.loc[month_mask & (out[month_col].astype(str).str.strip() == m)]
        if len(mrows):
            for c in cols:
                if c in mrows.columns:
                    out.at[idx, c] = mrows[c].iloc[0]


def add_email_operational_metrics(
    hierarchy: pd.DataFrame,
    work_me: pd.DataFrame,
    *,
    month_col: str = "Месяц",
    send_col: str = "Название выпуска",
    fl_email_col: str = "fl_Личная почта",
    level_col: str = "Level",
) -> pd.DataFrame:
    """
    Add:
      - Рассылок за месяц — distinct sends in month (Month/Spacer only)
      - Лидов с некорр. email — FL «Личная почта» не похожа на email
      - Доля некорр. email (лиды) — доля от числа лидов в строке (Send) или в месяце (Month)
    """
    out = hierarchy.copy()
    c_r = "Рассылок за месяц"
    c_b = "Лидов с некорр. email"
    c_p = "Доля некорр. email (лиды)"
    # Use object dtype so both numbers and blanks are accepted across pandas versions.
    out[c_r] = pd.Series([None] * len(out), index=out.index, dtype="object")
    out[c_b] = pd.Series([None] * len(out), index=out.index, dtype="object")
    out[c_p] = pd.Series([None] * len(out), index=out.index, dtype="object")

    wm = work_me.copy()
    if month_col not in wm.columns or send_col not in wm.columns:
        return out

    wm[month_col] = wm[month_col].astype(str).str.strip()
    wm[send_col] = wm[send_col].astype(str).str.strip()
    wm = wm[
        (~wm[month_col].isin(["", "-", "nan"]))
        & (~wm[send_col].isin(["", "nan", "Unmatched"]))
    ]

    if wm.empty:
        return out

    has_fl = fl_email_col in wm.columns
    bad_mask = wm[fl_email_col].map(lambda x: not is_plausible_email(x)) if has_fl else pd.Series(
        False, index=wm.index
    )

    sends_per_m = wm.groupby(month_col, dropna=False)[send_col].nunique()

    ms_tot = (
        wm.groupby([month_col, send_col], dropna=False)
        .size()
        .reset_index(name="_tot")
    )
    ms_bad = (
        wm.loc[bad_mask]
        .groupby([month_col, send_col], dropna=False)
        .size()
        .reset_index(name="_bad")
    )
    ms = ms_tot.merge(ms_bad, on=[month_col, send_col], how="left")
    if "_bad" not in ms.columns:
        ms["_bad"] = 0
    ms["_bad"] = pd.to_numeric(ms["_bad"], errors="coerce").fillna(0).astype(int)

    month_bad = ms.groupby(month_col, dropna=False)["_bad"].sum()
    month_tot = ms.groupby(month_col, dropna=False)["_tot"].sum()

    lvl = out.get(level_col, pd.Series("", index=out.index)).astype(str)
    m_idx = out[month_col].astype(str).str.strip()

    for i in out.index:
        lv = lvl.at[i]
        m = m_idx.at[i]
        if lv == "Send":
            sn = str(out.at[i, send_col]).strip()
            hit = ms[(ms[month_col] == m) & (ms[send_col] == sn)]
            if len(hit):
                tot = int(hit["_tot"].iloc[0])
                nb = int(hit["_bad"].iloc[0])
                out.at[i, c_b] = nb
                out.at[i, c_p] = round(100.0 * nb / tot, 2) if tot else 0.0
        elif lv == "Month":
            tot = int(month_tot.get(m, 0))
            nb = int(month_bad.get(m, 0))
            out.at[i, c_r] = int(sends_per_m.get(m, 0))
            out.at[i, c_b] = nb
            out.at[i, c_p] = round(100.0 * nb / tot, 2) if tot else 0.0

    _sync_spacer_from_month(out, month_col, [c_r, c_b, c_p])
    return out
