"""
Yandex Direct API → yandex_stats table ingest.
Set YANDEX_DIRECT_TOKEN (and optionally YANDEX_DIRECT_CLIENT_LOGIN) to enable.
Fetches report data and merges into yandex_stats. Otherwise use CSV import (db.ingest).
"""
import os
from typing import Optional

import pandas as pd

from .conn import get_engine, ensure_schema

# Schema columns we fill from API (must match db/schema.sql)
YANDEX_STATS_COLS = [
    "Месяц",
    "№ Кампании",
    "Название кампании",
    "№ Группы",
    "Название группы",
    "№ Объявления",
    "Статус объявления",
    "Тип объявления",
    "Заголовок",
    "Текст",
    "Ссылка",
    "Расход, ₽",
    "Клики",
    "Конверсии",
    "CR, %",
    "CPA, ₽",
]


def fetch_yandex_direct_report(
    token: str,
    client_login: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch campaign/ad statistics from Yandex Direct Reporting API.
    Returns a DataFrame with columns matching YANDEX_STATS_COLS where possible.
    Requires token (OAuth or API key). Optional client_login for agency accounts.
    """
    try:
        import requests
    except ImportError:
        return pd.DataFrame(columns=YANDEX_STATS_COLS)
    # Yandex Direct API v5 Reporting: report type "ACCOUNT_PERFORMANCE_REPORT" or custom
    # See https://yandex.ru/dev/direct/doc/ref-v5/reports/reports.html
    url = "https://api.direct.yandex.com/json/v5/reports"
    headers = {
        "Authorization": "Bearer %s" % token,
        "Accept-Language": "ru",
        "Content-Type": "application/json; charset=utf-8",
    }
    # Request a report that includes CampaignId, AdId, Impressions, Clicks, Cost, etc.
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from or "2020-01-01",
                "DateTo": date_to or "2030-12-31",
            },
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",
                "AdGroupId",
                "AdGroupName",
                "AdId",
                "AdState",
                "Cost",
                "Clicks",
                "Conversions",
            ],
            "ReportName": "Deved Yandex Stats",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
        }
    }
    if client_login:
        body["params"]["ClientLogin"] = client_login
    try:
        resp = requests.post(url, json=body, headers=headers, timeout=120)
        resp.raise_for_status()
        # Response is TSV
        from io import StringIO
        tsv = resp.text
        df = pd.read_csv(StringIO(tsv), sep="\t", encoding="utf-8")
        # Map to our schema (approximate)
        out = pd.DataFrame()
        col_map = {
            "Date": "Месяц",
            "CampaignId": "№ Кампании",
            "CampaignName": "Название кампании",
            "AdGroupId": "№ Группы",
            "AdGroupName": "Название группы",
            "AdId": "№ Объявления",
            "AdState": "Статус объявления",
            "Cost": "Расход, ₽",
            "Clicks": "Клики",
            "Conversions": "Конверсии",
        }
        for api_col, our_col in col_map.items():
            if api_col in df.columns and our_col in YANDEX_STATS_COLS:
                out[our_col] = df[api_col]
        for c in YANDEX_STATS_COLS:
            if c not in out.columns:
                out[c] = None
        out = out[[c for c in YANDEX_STATS_COLS if c in out.columns]]
        return out
    except Exception as e:
        # If API format or auth fails, return empty; user can use CSV import
        return pd.DataFrame(columns=YANDEX_STATS_COLS)


def merge_yandex_stats(engine, df: pd.DataFrame, if_exists: str = "append") -> int:
    """Append or replace yandex_stats with the given DataFrame."""
    if df.empty:
        return 0
    # Ensure only schema columns
    use = [c for c in YANDEX_STATS_COLS if c in df.columns]
    out = df[use].copy()
    out.to_sql("yandex_stats", engine, if_exists=if_exists, index=False)
    return len(out)


def ingest_yandex_direct(
    token: Optional[str] = None,
    client_login: Optional[str] = None,
    engine=None,
    db_path: Optional[str] = None,
) -> int:
    """
    Fetch stats from Yandex Direct API and append to yandex_stats.
    Returns number of rows added. Returns 0 if YANDEX_DIRECT_TOKEN is not set.
    """
    token = token or os.environ.get("YANDEX_DIRECT_TOKEN")
    if not token:
        return 0
    client_login = client_login or os.environ.get("YANDEX_DIRECT_CLIENT_LOGIN")
    engine = engine or get_engine(db_path)
    ensure_schema(engine)
    df = fetch_yandex_direct_report(token, client_login=client_login)
    if df.empty:
        return 0
    return merge_yandex_stats(engine, df, if_exists="replace")
