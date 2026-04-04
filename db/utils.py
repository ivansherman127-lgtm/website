"""Shared data-normalisation helpers used across db/ scripts."""
from __future__ import annotations

import re

import pandas as pd


def _n(v: object) -> str:
    """Return a clean string, collapsing None / NaN / 'nan' / 'null' to ''."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    return "" if s.lower() in {"", "nan", "none", "null"} else s


def _id(v: object) -> str:
    """Normalise a Bitrix ID – strip trailing '.0' produced by float coercion."""
    s = _n(v)
    return s.split(".", 1)[0] if re.fullmatch(r"\d+\.0+", s) else s


def _amt(v: object) -> float:
    """Parse a Russian-formatted monetary amount to float."""
    s = _n(v).replace(" ", "").replace("\xa0", "").replace(",", ".")
    try:
        return float(s) if s else 0.0
    except ValueError:
        return 0.0


def _month(v: object) -> str:
    """Return YYYY-MM from a date-like value; empty string on failure."""
    dt = pd.to_datetime(v, dayfirst=True, errors="coerce")
    return dt.strftime("%Y-%m") if pd.notna(dt) else ""
