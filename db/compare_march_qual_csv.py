#!/usr/bin/env python3
"""
March 2026 deals in Bitrix CSV: «старая» классификация (lead_type_series / ноутбук)
против текущей из bitrix_lead_logic.json (apply_notebook_lead_flags).

Запуск из корня репозитория:
  PYTHONPATH=db /opt/anaconda3/bin/python db/compare_march_qual_csv.py bitrix-march-26.csv
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from bitrix_lead_quality import (
    apply_notebook_lead_flags,
    coalesce_columns,
    deal_funnel_raw_series,
    invalid_token_mask,
    lead_type_series,
)


def _march_2026_mask(created: pd.Series) -> pd.Series:
    def ok(s: str) -> bool:
        m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", str(s).strip())
        if not m:
            return False
        _d, mo, y = m.groups()
        return y == "2026" and mo == "03"

    return created.fillna("").astype(str).map(ok)


def main() -> None:
    csv_path = Path(sys.argv[1] if len(sys.argv) > 1 else "bitrix-march-26.csv").resolve()
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8", low_memory=False)
    m = df[_march_2026_mask(df["Дата создания"])].copy()
    if "Дата изменения" in m.columns:
        m = m.sort_values("Дата изменения").drop_duplicates("ID", keep="last")
    else:
        m = m.drop_duplicates("ID", keep="last")

    funnel_c = deal_funnel_raw_series(m)
    stage_c = coalesce_columns(m, "Стадия сделки")
    lt_old = lead_type_series(funnel_c, stage_c)
    inv = invalid_token_mask(m)
    lt_arr = lt_old.astype(str).to_numpy()
    inv_arr = inv.to_numpy()
    ref_mask = lt_arr == "refusal"
    lt_arr = np.where(inv_arr & ~ref_mask, "unqual", lt_arr)
    old_qual = int(((lt_arr == "qual") | (lt_arr == "refusal")).sum())
    old_unqual = int((lt_arr == "unqual").sum())
    old_unk = int((lt_arr == "unknown").sum())

    d2 = apply_notebook_lead_flags(m.copy())
    new_qual = int(d2["is_qual"].sum())
    new_unqual = int(d2["is_unqual"].sum())
    new_unk = int(d2["is_unknown"].sum())

    print(f"File: {csv_path}")
    print(f"March 2026 create, unique deals: {len(m)}")
    print(f"  Old (notebook lead_type_series):  qual+refusal={old_qual}  unqual={old_unqual}  unknown={old_unk}")
    print(f"  New (bitrix_lead_logic.json):     qual+refusal={new_qual}  unqual={new_unqual}  unknown={new_unk}")

    d2["_lt_old"] = lt_arr
    d2["lead_type_new"] = d2["lead_type"].astype(str)
    mismatch = d2["_lt_old"] != d2["lead_type_new"]
    # Map new lead_type to comparable bucket
    def bucket(lt: str) -> str:
        if lt in ("qual", "refusal"):
            return "qualish"
        if lt == "unqual":
            return "unqual"
        return "unknown"

    d2["_b_old"] = [bucket(x) for x in d2["_lt_old"]]
    d2["_b_new"] = [bucket(x) for x in d2["lead_type_new"]]
    bucket_mis = d2["_b_old"] != d2["_b_new"]
    print(f"  Row-level lead_type string mismatch: {int(mismatch.sum())}")
    print(f"  Bucket mismatch (qualish/unqual/unknown): {int(bucket_mis.sum())}")
    if bucket_mis.any():
        sm = d2.loc[bucket_mis, ["ID", "Воронка", "Стадия сделки", "_lt_old", "lead_type_new"]].head(25)
        print(sm.to_string(index=False))


if __name__ == "__main__":
    main()
