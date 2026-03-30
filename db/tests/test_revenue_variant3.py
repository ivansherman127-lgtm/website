"""Parity checks for canonical variant3 revenue mask."""
from __future__ import annotations

import pandas as pd

from revenue_variant3 import variant3_revenue_mask


def test_closed_without_pay_date_not_revenue() -> None:
    df = pd.DataFrame(
        {
            "Стадия сделки": ["Сделка заключена"],
            "Сделка закрыта": ["да"],
            "Дата оплаты": [""],
            "Даты платежей по рассрочке ": [""],
        }
    )
    assert not variant3_revenue_mask(df).iloc[0]


def test_closed_with_pay_date_is_revenue() -> None:
    df = pd.DataFrame(
        {
            "Стадия сделки": ["Сделка заключена"],
            "Сделка закрыта": ["да"],
            "Дата оплаты": ["2024-01-15"],
            "Даты платежей по рассрочке ": [""],
        }
    )
    assert variant3_revenue_mask(df).iloc[0]


def test_installment_requires_schedule_and_pay_date() -> None:
    base = {
        "Стадия сделки": ["рассрочка"],
        "Сделка закрыта": [""],
        "Дата оплаты": ["2024-02-01"],
    }
    no_sched = pd.DataFrame({**base, "Даты платежей по рассрочке ": [""]})
    assert not variant3_revenue_mask(no_sched).iloc[0]
    with_sched = pd.DataFrame({**base, "Даты платежей по рассрочке ": ["1/2/3"]})
    assert variant3_revenue_mask(with_sched).iloc[0]
