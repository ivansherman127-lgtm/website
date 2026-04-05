"""
Объединённый вход по сделкам Bitrix: исторический срез + актуальная дозагрузка.

    - bitrix_19.03.26.csv — полный исторический экспорт.
    - bitrix_60_days_03.04.2026.csv / bitrix_upd_27.03.csv — актуальная дозагрузка.

Склейка: concat → нормализация ID → дедуп по ID с выбором строки с максимальной «Сумма».
Если суммы равны, допустимо оставить любую из строк.
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
DEFAULT_FL_RAW = ROOT / "bitrix_19.03.26.csv"
DEFAULT_BITRIX_UPD = ROOT / "bitrix_60_days_03.04.2026.csv"


def _norm_id(v: object) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return ""
    if re.fullmatch(r"\d+\.0+", s):
        return s.split(".", 1)[0]
    return s


def _amount_num(v: object) -> float:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 0.0
    s = str(v).replace(" ", "").replace("\xa0", "").replace(",", ".").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def dedup_bitrix_deals_by_highest_amount(df: pd.DataFrame) -> pd.DataFrame:
    if "ID" not in df.columns:
        raise ValueError("Bitrix frame must contain column ID")
    out = df.copy()
    out["ID"] = out["ID"].map(_norm_id)
    out = out[out["ID"].astype(str).str.strip().ne("")]
    if out.empty:
        return out.reset_index(drop=True)

    sum_col = out["Сумма"] if "Сумма" in out.columns else pd.Series([0.0] * len(out), index=out.index)
    out["__dedup_amount"] = sum_col.map(_amount_num)
    out["__dedup_pos"] = range(len(out))
    keep_idx = out.groupby("ID", sort=False)["__dedup_amount"].idxmax()
    deduped = out.loc[keep_idx].sort_values("__dedup_pos").drop(columns=["__dedup_amount", "__dedup_pos"])
    return deduped.reset_index(drop=True)


def read_bitrix_export(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(path)
    return pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str, low_memory=False)


def _build_voronka_patch(paths: list[Path]) -> pd.DataFrame:
    """
    Build a patch table {ID → Воронка} from files that have funnel data.
    Earlier files are overridden by later files (last non-empty wins).
    """
    frames: list[pd.DataFrame] = []
    for p in paths:
        if not p.is_file():
            continue
        df = read_bitrix_export(p)
        if "ID" not in df.columns or "Воронка" not in df.columns:
            continue
        sub = df[["ID", "Воронка"]].copy()
        sub["ID"] = sub["ID"].map(_norm_id)
        sub = sub[sub["ID"].str.strip().ne("")]
        sub = sub[sub["Воронка"].notna() & sub["Воронка"].str.strip().ne("")]
        if not sub.empty:
            frames.append(sub)
    if not frames:
        return pd.DataFrame(columns=["ID", "Воронка"])
    combined = pd.concat(frames, ignore_index=True)
    # Last non-empty value for each ID wins (later file = fresher data)
    return combined.drop_duplicates(subset=["ID"], keep="last").reset_index(drop=True)


def load_bitrix_deals_union(
    fl_raw_path: Optional[Path] = None,
    bitrix_upd_path: Optional[Path] = None,
) -> pd.DataFrame:
    """Две выгрузки подряд; при совпадении ID берётся строка с максимальной «Сумма»."""
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
    deduped = dedup_bitrix_deals_by_highest_amount(out)

    # Patch «Воронка» from files that have funnel data, for rows missing it.
    # bitrix_2025.csv covers 2025 deals; bitrix_60_days covers recent deals.
    patch_paths = [ROOT / "bitrix_2025.csv", bitrix_upd_path]
    patch = _build_voronka_patch(patch_paths)
    if not patch.empty and "Воронка" in deduped.columns:
        missing_mask = deduped["Воронка"].isna() | deduped["Воронка"].str.strip().eq("")
        if missing_mask.any():
            deduped = deduped.merge(
                patch.rename(columns={"Воронка": "__voronka_patch"}),
                on="ID",
                how="left",
            )
            deduped.loc[missing_mask, "Воронка"] = deduped.loc[missing_mask, "__voronka_patch"]
            deduped.drop(columns=["__voronka_patch"], inplace=True)

    return deduped
