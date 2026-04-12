#!/usr/bin/env python3
"""
Cross-check March deals: CSV variant3 vs what variant3_api_revenue_mask predicts on CSV rows
(STAGE_ID column absent — uses Russian «Стадия сделки» only in this script's second pass).

Usage (from repo root):
  python db/compare_march_csv_api.py path/to/bitrix-march-26.csv

Requires running from ``db/`` on PYTHONPATH or: ``cd db && python compare_march_csv_api.py ../bitrix-march-26.csv``
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

from revenue_variant3 import variant3_api_revenue_mask, variant3_revenue_mask
from utils import _amt


def _march_2026_mask(created: pd.Series) -> pd.Series:
    def ok(s: str) -> bool:
        m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", str(s).strip())
        if not m:
            return False
        _d, mo, y = m.groups()
        return y == "2026" and mo == "03"

    return created.fillna("").astype(str).map(ok)


def main() -> None:
    csv_path = Path(sys.argv[1] if len(sys.argv) > 1 else "../bitrix-march-26.csv").resolve()
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8", low_memory=False)
    m = df[_march_2026_mask(df["Дата создания"])].copy()
    if "Дата изменения" in m.columns:
        m = m.sort_values("Дата изменения").drop_duplicates("ID", keep="last")
    else:
        m = m.drop_duplicates("ID", keep="last")
    print(f"File: {csv_path}")
    print(f"March 2026 (create), unique IDs: {len(m)}")

    v3_csv = variant3_revenue_mask(m)
    v3_api_on_csv = variant3_api_revenue_mask(m)

    print(f"variant3 (Russian stage only, CSV):     {int(v3_csv.sum())} deals")
    print(f"variant3_api on same «Стадия» column:   {int(v3_api_on_csv.sum())} deals")
    mismatch = v3_csv != v3_api_on_csv
    if mismatch.any():
        print("MISMATCH rows:", int(mismatch.sum()))
        print(m.loc[mismatch, ["ID", "Стадия сделки", "Сумма"]].head(20).to_string())
    else:
        print("variant3_csv == variant3_api on CSV stage text (OK).")

    sum_amt = m["Сумма"].map(_amt)
    print(f"Sum Сумма (v3_csv & sum>0):   {sum_amt.where(v3_csv & (sum_amt > 0)).sum():,.2f}")
    print(f"Sum Сумма (v3_api & sum>0):  {sum_amt.where(v3_api_on_csv & (sum_amt > 0)).sum():,.2f}")


if __name__ == "__main__":
    main()
