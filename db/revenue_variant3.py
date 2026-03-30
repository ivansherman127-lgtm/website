"""
Canonical variant3 revenue recognition (single source of truth for Python + TS port).

Rule (aligned with regenerate_associative_variant3 / build_bitrix_full_slices):
- Do not use «Предпочитаемый способ оплаты».
- Рассрочка/постоплата inferred only from «Стадия сделки».
- For installment/postpay, «Даты платежей по рассрочке » must be filled.
- **Дата оплаты** is mandatory for any row to count revenue (strict gating).

See also: web/functions/lib/analytics/revenue.ts (mirror for Workers).
"""
from __future__ import annotations

import pandas as pd

from bitrix_lead_quality import coalesce_columns


def variant3_revenue_mask(df: pd.DataFrame) -> pd.Series:
    """
    Boolean Series: True where deal revenue counts toward variant3 totals.
    """
    stage = coalesce_columns(df, "Стадия сделки").fillna("").astype(str).str.lower()
    closed_flag = (
        df.get("Сделка закрыта", "").fillna("").astype(str).str.strip().str.lower().eq("да")
    )
    closed_stage = stage.str.contains("сделка заключена", na=False)
    post_stage = stage.str.contains("постоплат", na=False)
    inst_stage = stage.str.contains("рассроч", na=False)
    pay_dates = (
        df.get("Даты платежей по рассрочке ", "")
        .fillna("")
        .astype(str)
        .str.strip()
        .ne("")
    )
    pay_date_raw = df.get("Дата оплаты", "").fillna("").astype(str).str.strip()
    pay_date_present = pay_date_raw.ne("")
    variant3_core = (closed_flag | closed_stage) | ((post_stage | inst_stage) & pay_dates)
    return variant3_core & pay_date_present
