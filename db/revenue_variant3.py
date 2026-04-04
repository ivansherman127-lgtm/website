"""
Canonical variant3 revenue recognition (single source of truth for Python + TS port).

Rule (aligned with regenerate_associative_variant3 / build_bitrix_full_slices):
- Revenue counts only when «Стадия сделки» is one of:
    «Сделка заключена», «Постоплата», «Рассрочка».

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
    closed_stage = stage.str.contains("сделка заключена", na=False)
    post_stage = stage.str.contains("постоплат", na=False)
    inst_stage = stage.str.contains("рассроч", na=False)
    return closed_stage | post_stage | inst_stage
