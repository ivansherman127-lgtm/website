"""Tests for payment-verified mart revenue (API pipeline)."""
from __future__ import annotations

import pandas as pd

from revenue_variant3 import payment_verified_revenue_mask


def test_pay_date_and_positive_sum() -> None:
    df = pd.DataFrame(
        {
            "Сумма": ["100000"],
            "Дата оплаты": ["2026-04-01"],
            "Даты платежей по рассрочке ": [""],
        }
    )
    assert payment_verified_revenue_mask(df).iloc[0]


def test_installment_only() -> None:
    df = pd.DataFrame(
        {
            "Сумма": ["50000"],
            "Дата оплаты": [""],
            "Даты платежей по рассрочке ": ["2026-03-01;2026-04-01"],
        }
    )
    assert payment_verified_revenue_mask(df).iloc[0]


def test_stage_won_id_without_pay_not_revenue() -> None:
    df = pd.DataFrame(
        {
            "Сумма": ["200000"],
            "Дата оплаты": [""],
            "Даты платежей по рассрочке ": [""],
            "Стадия сделки": ["C7:WON"],
        }
    )
    assert not payment_verified_revenue_mask(df).iloc[0]


def test_zero_sum_not_revenue_even_with_pay_date() -> None:
    df = pd.DataFrame(
        {
            "Сумма": ["0"],
            "Дата оплаты": ["2026-04-01"],
            "Даты платежей по рассрочке ": [""],
        }
    )
    assert not payment_verified_revenue_mask(df).iloc[0]
