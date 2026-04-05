#!/usr/bin/env python3
"""
fetch_sendsay_emails.py
~~~~~~~~~~~~~~~~~~~~~~~
Downloads email campaign statistics from the Sendsay API and writes them to
``stg_email_sends`` in the local SQLite database.

Replaces the manual CSV-export workflow that previously used
``sheets/mass_email_good.csv``.

Prerequisites
=============
  pip install sendsay-api-python

Credentials (via env vars or CLI flags)
========================================
  SENDSAY_LOGIN     – account login
  SENDSAY_SUBLOGIN  – sub-user login (optional; leave empty for main account)
  SENDSAY_PASSWORD  – account password

Usage
=====
  python fetch_sendsay_emails.py
  python fetch_sendsay_emails.py --from 2024-01-01
  python fetch_sendsay_emails.py --dry-run
  python fetch_sendsay_emails.py --save-csv            # also write sheets/mass_email_good.csv
  python fetch_sendsay_emails.py --if-exists append    # incremental update
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from conn import get_engine  # noqa: E402

DEFAULT_DB_PATH = os.environ.get("WEBSITE_DB_PATH", str(ROOT / "website.db"))
CSV_PATH = ROOT / "sheets" / "mass_email_good.csv"

# stat.uni fields to select – order must match COLUMNS_ORDERED below
_SELECT = [
    "issue.id",
    "issue.name",
    "issue.subject",
    "issue.dt",
    "issue.members",
    "issue.deliv_ok",
    "issue.deliv_bad",
    "issue.readed",
    "issue.u_readed",
    "issue.clicked",
    "issue.u_clicked",
    "issue.unsubed",
    "issue.track.id",
    "issue.utm.campaign",
    "issue.utm.content",
    "issue.utm.medium",
    "issue.utm.source",
    "issue.utm.term",
]


def _pct(num: float, denom: float) -> float:
    """Return (num / denom * 100) rounded to 2 dp, or 0.0 if denom is zero."""
    try:
        if denom and float(denom) > 0:
            return round(float(num) / float(denom) * 100, 2)
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    return 0.0


def _to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(int)


def _to_str(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str)


def fetch_issues(
    api,
    date_from: Optional[str] = None,
) -> pd.DataFrame:
    """
    Call stat.uni to retrieve per-issue aggregate stats for all email campaigns.

    Returns a raw DataFrame with columns named after the _SELECT fields.
    """
    filt = [{"a": "issue.format", "op": "==", "v": "e"}]
    if date_from:
        filt.append({"a": "issue.dt:YD", "op": ">=", "v": date_from})

    params: dict = {
        "select": _SELECT,
        "filter": filt,
        "order": ["issue.dt"],
    }

    resp = api.request("stat.uni", params)
    rows = resp.data.get("list", [])
    if not rows:
        return pd.DataFrame(columns=_SELECT)

    return pd.DataFrame(rows, columns=_SELECT)


def transform_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map raw API DataFrame to the legacy column names used by stg_email_sends
    (identical to the Sendsay CSV export columns).
    """
    if df.empty:
        return df

    # Numeric base columns
    members = _to_int(df["issue.members"])
    deliv_ok = _to_int(df["issue.deliv_ok"])
    deliv_bad = _to_int(df["issue.deliv_bad"])
    readed = _to_int(df["issue.readed"])
    u_readed = _to_int(df["issue.u_readed"])
    clicked = _to_int(df["issue.clicked"])
    u_clicked = _to_int(df["issue.u_clicked"])
    unsubed = _to_int(df["issue.unsubed"])

    ctor = [_pct(c, o) for c, o in zip(u_clicked, u_readed)]
    utor = [_pct(u, o) for u, o in zip(unsubed, u_readed)]

    out = pd.DataFrame({
        "Дата отправки":  df["issue.dt"].fillna(""),
        "Название выпуска": _to_str(df["issue.name"]),
        "Получатели":     members,
        "Тема":           _to_str(df["issue.subject"]),
        "Отправлено":     members,
        "Доставлено":     deliv_ok,
        "Ошибок":         deliv_bad,
        "Открытий":       readed,
        "Уник. открытий": u_readed,
        "Кликов":         clicked,
        "Уник. кликов":   u_clicked,
        "CTOR, %":        ctor,
        "Отписок":        unsubed,
        "UTOR, %":        utor,
        "ID":             df["issue.id"].fillna(""),
        "Номер задания":  _to_str(df["issue.track.id"]),
        "utm_campaign":   _to_str(df["issue.utm.campaign"]),
        "utm_content":    _to_str(df["issue.utm.content"]),
        "utm_medium":     _to_str(df["issue.utm.medium"]),
        "utm_source":     _to_str(df["issue.utm.source"]),
        "utm_term":       _to_str(df["issue.utm.term"]),
    })

    # Add month column expected by the run_all_slices pipeline
    dt = pd.to_datetime(out["Дата отправки"], dayfirst=True, errors="coerce")
    out["month"] = dt.dt.strftime("%Y-%m")

    return out


def fetch_and_write(
    login: str,
    password: str,
    sublogin: str = "",
    date_from: Optional[str] = None,
    db_path: Optional[str] = None,
    dry_run: bool = False,
    if_exists: Literal["replace", "append"] = "replace",
    save_csv: bool = False,
) -> int:
    """
    Main pipeline:
      1. Authenticate and pull stat.uni from Sendsay.
      2. Transform to legacy schema.
      3. Write to SQLite (and optionally to CSV).

    Returns the number of rows written.
    """
    try:
        from sendsay.api import SendsayAPI  # type: ignore
    except ImportError:
        print(
            "Error: sendsay-api-python is not installed.\n"
            "Run: pip install sendsay-api-python",
            file=sys.stderr,
        )
        sys.exit(1)

    api = SendsayAPI(login=login, sublogin=sublogin, password=password)

    print(f"Fetching Sendsay issues (from={date_from or 'all time'}) …")
    raw_df = fetch_issues(api, date_from=date_from)

    if raw_df.empty:
        print("No email issues returned from Sendsay API.")
        return 0

    df = transform_to_schema(raw_df)
    print(f"  {len(df)} email campaigns received.")

    if dry_run:
        print("Dry run – skipping database write.")
        with pd.option_context("display.max_columns", None, "display.width", 200):
            print(df.head(10).to_string(index=False))
        return len(df)

    engine = get_engine(db_path or DEFAULT_DB_PATH)
    df.to_sql("stg_email_sends", engine, if_exists=if_exists, index=False)
    print(f"  Written {len(df)} rows → stg_email_sends (if_exists={if_exists!r})")

    if save_csv:
        CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(CSV_PATH, index=False, encoding="utf-8")
        print(f"  Saved CSV → {CSV_PATH.relative_to(ROOT)}")

    return len(df)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Sendsay email campaign stats and write to stg_email_sends"
    )
    parser.add_argument(
        "--login",
        default=os.environ.get("SENDSAY_LOGIN", "").strip(),
        help="Sendsay account login (or set SENDSAY_LOGIN env var)",
    )
    parser.add_argument(
        "--sublogin",
        default=os.environ.get("SENDSAY_SUBLOGIN", "").strip(),
        help="Sendsay sub-user login (optional; or set SENDSAY_SUBLOGIN env var)",
    )
    parser.add_argument(
        "--password",
        default=os.environ.get("SENDSAY_PASSWORD", "").strip(),
        help="Sendsay password (or set SENDSAY_PASSWORD env var)",
    )
    parser.add_argument(
        "--from",
        dest="date_from",
        default=None,
        metavar="YYYY-MM-DD",
        help="Only fetch issues on or after this date. Omit to fetch all history.",
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help="Path to local SQLite database (default: project root / website.db)",
    )
    parser.add_argument(
        "--if-exists",
        choices=["replace", "append"],
        default="replace",
        help="pandas to_sql strategy: 'replace' (default) truncates first; 'append' adds rows.",
    )
    parser.add_argument(
        "--save-csv",
        action="store_true",
        help=f"Also write output to {CSV_PATH.relative_to(ROOT)} for backward compatibility",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and preview data without writing to the database",
    )
    args = parser.parse_args()

    if not args.login or not args.password:
        parser.error(
            "Credentials required. Set SENDSAY_LOGIN and SENDSAY_PASSWORD environment "
            "variables, or pass --login / --password."
        )

    n = fetch_and_write(
        login=args.login,
        password=args.password,
        sublogin=args.sublogin,
        date_from=args.date_from,
        db_path=args.db_path,
        dry_run=args.dry_run,
        if_exists=args.if_exists,
        save_csv=args.save_csv,
    )
    print(f"Done. {n} rows.")


if __name__ == "__main__":
    main()
