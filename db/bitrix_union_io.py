"""
Объединённый вход по сделкам Bitrix: исторический срез + актуальная дозагрузка.

  - sheets/fl_raw_09-03.csv — покрывает прошлый период (воронка «Воронка» заполнена).
  - sheets/bitrix_upd_27.03.csv — доп. выгрузка до сегодня.

Склейка: concat → нормализация ID → drop_duplicates(..., keep='last'), чтобы пересекающиеся
сделки брались из более позднего файла (upd).
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd
from pandas.io.common import dedup_names

ROOT = Path(__file__).resolve().parent.parent


def _sql_bind_key(name: str) -> str:
    """
    Approximate SQLAlchemy identifier normalization for SQLite INSERT binds.
    Distinct headers like «Тип Клиента.1» vs «Тип Клиента 1» can map to the same bind name
    and trigger SQLAlchemy AssertionError in to_sql.
    """
    s = str(name).strip()
    s = re.sub(r"[\s.]+", "_", s)
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_").lower()
    return s or "col"


def dedup_columns_sqlalchemy_safe(columns: list[str]) -> list[str]:
    """Rename columns whose _sql_bind_key collides so pandas/SQLAlchemy to_sql succeeds."""
    seen: dict[str, int] = {}
    out: list[str] = []
    used = set()
    for c in columns:
        c = str(c)
        k = _sql_bind_key(c)
        if k not in seen:
            seen[k] = 1
            name = c
        else:
            seen[k] += 1
            n = seen[k]
            base = f"{c}__sqldup{n}"
            name = base
            m = n
            while name in used:
                m += 1
                name = f"{c}__sqldup{m}"
        used.add(name)
        out.append(name)
    return out
DEFAULT_FL_RAW = ROOT / "sheets" / "fl_raw_09-03.csv"
DEFAULT_BITRIX_UPD = ROOT / "sheets" / "bitrix_upd_27.03.csv"


def _norm_id(v: object) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return ""
    if re.fullmatch(r"\d+\.0+", s):
        return s.split(".", 1)[0]
    return s


def read_bitrix_export(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(path)
    return pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str, low_memory=False)


def load_bitrix_deals_union(
    fl_raw_path: Optional[Path] = None,
    bitrix_upd_path: Optional[Path] = None,
) -> pd.DataFrame:
    """Две выгрузки подряд; при совпадении ID побеждает последняя таблица (upd)."""
    fl_raw_path = fl_raw_path or DEFAULT_FL_RAW
    bitrix_upd_path = bitrix_upd_path or DEFAULT_BITRIX_UPD
    parts: list[pd.DataFrame] = []
    if fl_raw_path.is_file():
        parts.append(read_bitrix_export(fl_raw_path))
    if bitrix_upd_path.is_file():
        parts.append(read_bitrix_export(bitrix_upd_path))
    if not parts:
        raise FileNotFoundError(
            f"No Bitrix inputs: missing both {fl_raw_path} and {bitrix_upd_path}"
        )
    out = pd.concat(parts, ignore_index=True)
    # Bitrix export может дублировать заголовки («Воронка» дважды) — to_sql/SQLAlchemy требуют уникальные имена.
    out.columns = dedup_names(list(out.columns), is_potential_multiindex=False)
    out.columns = dedup_columns_sqlalchemy_safe(list(out.columns))
    if "ID" not in out.columns:
        raise ValueError("Union frames must contain column ID")
    out = out.copy()
    out["ID"] = out["ID"].map(_norm_id)
    out = out[out["ID"].astype(str).str.strip().ne("")].drop_duplicates(subset=["ID"], keep="last")
    return out.reset_index(drop=True)
