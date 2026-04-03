"""Parity checks for canonical variant3 revenue mask."""
from __future__ import annotations

import pandas as pd

from revenue_variant3 import variant3_revenue_mask


def test_closed_stage_is_revenue_without_other_gates() -> None:
    df = pd.DataFrame(
        {
            "Стадия сделки": ["Сделка заключена"],
            "Дата оплаты": [""],
            "Даты платежей по рассрочке ": [""],
        }
    )
    assert variant3_revenue_mask(df).iloc[0]


def test_postpay_stage_is_revenue() -> None:
    df = pd.DataFrame(
        {
            "Стадия сделки": ["Постоплата"],
            "Дата оплаты": [""],
            "Даты платежей по рассрочке ": [""],
        }
    )
    assert variant3_revenue_mask(df).iloc[0]


def test_installment_stage_is_revenue() -> None:
    df = pd.DataFrame(
        {
            "Стадия сделки": ["Рассрочка"],
            "Дата оплаты": [""],
            "Даты платежей по рассрочке ": [""],
        }
    )
    assert variant3_revenue_mask(df).iloc[0]


def test_other_stage_not_revenue() -> None:
    df = pd.DataFrame(
        {
            "Стадия сделки": ["Некачественный лид"],
            "Дата оплаты": ["2024-01-15"],
            "Даты платежей по рассрочке ": ["1/2/3"],
        }
    )
    assert not variant3_revenue_mask(df).iloc[0]
