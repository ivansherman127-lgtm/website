"""variant3_api_revenue_mask — API STAGE_ID aligned with CSV variant3."""
from __future__ import annotations

import pandas as pd

from revenue_variant3 import variant3_api_revenue_mask


def test_c7_won_positive() -> None:
    df = pd.DataFrame({"Сумма": ["1000"], "Стадия сделки": ["C7:WON"]})
    assert variant3_api_revenue_mask(df).iloc[0]


def test_won_alone() -> None:
    df = pd.DataFrame({"Сумма": ["500"], "Стадия сделки": ["WON"]})
    assert variant3_api_revenue_mask(df).iloc[0]


def test_installment_custom_stage() -> None:
    df = pd.DataFrame({"Сумма": ["100"], "Стадия сделки": ["C7:UC_P7HXNZ"]})
    assert variant3_api_revenue_mask(df).iloc[0]


def test_russian_closed_stage() -> None:
    df = pd.DataFrame({"Сумма": ["200"], "Стадия сделки": ["Сделка заключена"]})
    assert variant3_api_revenue_mask(df).iloc[0]


def test_potential_not_revenue() -> None:
    df = pd.DataFrame({"Сумма": ["99999"], "Стадия сделки": ["Потенциал 01"]})
    assert not variant3_api_revenue_mask(df).iloc[0]


def test_zero_sum_not_revenue_even_won() -> None:
    df = pd.DataFrame({"Сумма": ["0"], "Стадия сделки": ["C7:WON"]})
    assert not variant3_api_revenue_mask(df).iloc[0]
