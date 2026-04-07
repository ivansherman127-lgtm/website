"""db/yandex_api_sync.py
Yandex Direct → SQLite incremental sync (stdlib-only, no pandas).

Tables written:
  raw_yandex_stats  — daily ad-level rows from the Reports API
  raw_yandex_ads    — ad metadata (title/text/href) from the Ads.get API
  raw_yandex_meta   — last_sync_date per entity
  yandex_stats      — monthly aggregate matching the existing D1 schema

Usage:
  python -m db.yandex_api_sync                 # incremental, all entities
  python -m db.yandex_api_sync --full          # re-download last N days
  python -m db.yandex_api_sync --watch         # daemon mode
  python -m db.yandex_api_sync --get-token CODE  # exchange OAuth code → token

Required env var:
  YANDEX_TOKEN      — OAuth Bearer token for Yandex Direct API v5

OAuth first-time setup:
  1. Open in browser:
     https://oauth.yandex.ru/authorize?response_type=code&client_id=c0519acddcac4f1797a5414762522492
  2. Authorise and copy the code.
  3. python -m db.yandex_api_sync --get-token <CODE>
     (this writes YANDEX_TOKEN into web_share_subset/webpush/.env.server.json)
"""

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_DB_PATH = os.environ.get(
    "WEBSITE_DB_PATH",
    str(Path(__file__).resolve().parent.parent / "website.db"),
)

DIRECT_REPORTS_URL = "https://api.direct.yandex.com/json/v5/reports"
DIRECT_ADS_URL     = "https://api.direct.yandex.com/json/v5/ads"
TOKEN_URL          = "https://oauth.yandex.ru/token"

OAUTH_CLIENT_ID     = "c0519acddcac4f1797a5414762522492"
OAUTH_CLIENT_SECRET = "57d8bde7268d49518e635dd087413204"

# Optional: agency/representative account login.
# Set YANDEX_CLIENT_LOGIN to the advertiser's Yandex login when the token
# belongs to a representative (agency) account.
DEFAULT_CLIENT_LOGIN = os.environ.get("YANDEX_CLIENT_LOGIN", "").strip()

# How far back to fetch on the first full sync
DEFAULT_LOOKBACK_DAYS = 90

# Reports API polling
POLL_INTERVAL = 15   # seconds between poll retries
MAX_POLL      = 120  # max poll attempts before giving up

# Retry config for transient HTTP errors in the Ads API
MAX_RETRIES  = 5
RETRY_DELAY  = 10.0  # seconds

# Fields to request from the Reports API
REPORT_FIELDS = [
    "Date", "CampaignId", "CampaignName",
    "AdGroupId", "AdGroupName", "AdId",
    "Impressions", "Clicks", "Cost", "Conversions",
]

ADS_PAGE_SIZE = 10_000

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _today() -> str:
    return date.today().isoformat()


def _days_ago(n: int) -> str:
    return (date.today() - timedelta(days=n)).isoformat()


# ---------------------------------------------------------------------------
# OAuth helpers
# ---------------------------------------------------------------------------

def exchange_code_for_token(code: str) -> str:
    """Exchange an OAuth authorisation code for an access token."""
    body = urllib.parse.urlencode({
        "grant_type":    "authorization_code",
        "code":          code,
        "client_id":     OAUTH_CLIENT_ID,
        "client_secret": OAUTH_CLIENT_SECRET,
    }).encode()
    req = urllib.request.Request(TOKEN_URL, data=body, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode(errors="replace")
        raise RuntimeError(f"Token exchange failed HTTP {exc.code}: {body_text}") from exc
    if "access_token" not in data:
        raise RuntimeError(f"Token exchange returned no access_token: {data}")
    return data["access_token"]


def save_token_to_env(token: str, env_path: str) -> None:
    """Merge YANDEX_TOKEN into the .env.server.json file."""
    p = Path(env_path)
    existing: Dict[str, Any] = {}
    if p.exists():
        try:
            existing = json.loads(p.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    existing["YANDEX_TOKEN"] = token
    p.write_text(json.dumps(existing, ensure_ascii=False, indent=2))
    print(f"  Token saved to {env_path}")


# ---------------------------------------------------------------------------
# Reports API — offline polling
# ---------------------------------------------------------------------------

def _build_report_body(date_from: str, date_to: str) -> bytes:
    payload = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo":   date_to,
            },
            "FieldNames":      REPORT_FIELDS,
            "ReportName":      f"DailyAdStats_{date_from}_{date_to}",
            "ReportType":      "AD_PERFORMANCE_REPORT",
            "DateRangeType":   "CUSTOM_DATE",
            "Format":          "TSV",
            "IncludeVAT":      "NO",
            "IncludeDiscount": "NO",
        }
    }
    return json.dumps(payload, ensure_ascii=False).encode()


def fetch_report(
    token: str, date_from: str, date_to: str, client_login: str = ""
) -> List[Dict[str, str]]:
    """
    Fetch daily stats from the Reports API using offline (queued) mode.
    Polls every POLL_INTERVAL seconds on HTTP 201/202 until 200.
    Returns a list of dicts keyed by TSV column name.
    """
    body = _build_report_body(date_from, date_to)
    headers = {
        "Authorization":     f"Bearer {token}",
        "Accept-Language":   "ru",
        "Content-Type":      "application/json",
        "returnMoneyInMicros": "false",   # Cost in roubles, not micros
        "processingMode":    "offline",
        "skipReportHeader":  "true",      # omit report-name line
        "skipReportSummary": "true",      # omit totals line
    }
    if client_login:
        headers["Client-Login"] = client_login

    for attempt in range(MAX_POLL):
        req = urllib.request.Request(
            DIRECT_REPORTS_URL, data=body, headers=headers, method="POST"
        )
        status: int
        raw: str
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                status = resp.status
                raw = resp.read().decode("utf-8-sig")
        except urllib.error.HTTPError as exc:
            status = exc.code
            raw = exc.read().decode(errors="replace")
            if status not in (200, 201, 202):
                _explain_api_error(status, raw)
                raise RuntimeError(
                    f"Reports API error HTTP {status}: {raw[:400]}"
                ) from exc

        if status == 200:
            return _parse_tsv(raw)

        if status == 201:
            wait = int(
                (getattr(exc, "headers", {}) or {}).get("retryIn", str(POLL_INTERVAL))
                if "exc" in dir() else str(POLL_INTERVAL)
            )
            print(f"  [report] queued (201) — waiting {wait}s… (attempt {attempt+1})", flush=True)
            time.sleep(wait)
        elif status == 202:
            wait = int(
                (getattr(exc, "headers", {}) or {}).get("retryIn", str(POLL_INTERVAL))
                if "exc" in dir() else str(POLL_INTERVAL)
            )
            print(f"  [report] building (202) — waiting {wait}s… (attempt {attempt+1})", flush=True)
            time.sleep(wait)
        else:
            raise RuntimeError(f"Reports API unexpected HTTP {status}: {raw[:400]}")

    raise RuntimeError(f"Reports API: timed out after {MAX_POLL} polling attempts")


def _explain_api_error(status: int, body: str) -> None:
    """Print a human-readable hint for common Yandex Direct API errors."""
    try:
        err = json.loads(body).get("error", {})
        code = int(err.get("error_code", 0))
    except Exception:
        return
    if code == 513:
        print(
            "\n[hint] Error 513: The authorized Yandex account is not connected to "
            "Yandex.Direct.\n"
            "  If this is an agency/representative account, set YANDEX_CLIENT_LOGIN "
            "to the advertiser's Yandex login in .env.server.json, e.g.:\n"
            '    { "YANDEX_CLIENT_LOGIN": "advertiser-login" }\n',
            file=sys.stderr,
        )
    elif code == 58:
        print(
            "\n[hint] Error 58: The OAuth application has not been approved for "
            "Yandex.Direct API access yet.\n"
            "  Go to https://direct.yandex.ru → API → submit access application\n"
            "  and wait for confirmation. This is a one-time step.\n",
            file=sys.stderr,
        )


def _parse_tsv(raw: str) -> List[Dict[str, str]]:
    """Parse TSV text where the first non-empty line is the header."""
    lines = [ln for ln in raw.splitlines() if ln.strip()]
    if not lines:
        return []
    headers = lines[0].split("\t")
    rows: List[Dict[str, str]] = []
    for line in lines[1:]:
        parts = line.split("\t")
        rows.append({headers[i]: (parts[i] if i < len(parts) else "") for i in range(len(headers))})
    return rows


# ---------------------------------------------------------------------------
# Ads API
# ---------------------------------------------------------------------------

def _call_ads(
    token: str, payload: Dict[str, Any], client_login: str = ""
) -> Dict[str, Any]:
    """POST to Ads API with exponential back-off on transient errors."""
    body = json.dumps(payload, ensure_ascii=False).encode()
    headers = {
        "Authorization":   f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type":    "application/json",
    }
    if client_login:
        headers["Client-Login"] = client_login
    for attempt in range(MAX_RETRIES):
        req = urllib.request.Request(
            DIRECT_ADS_URL, data=body, headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAY * (attempt + 1)
                print(f"  [ads] HTTP {exc.code}, retry {attempt+1}/{MAX_RETRIES} in {wait:.0f}s…", flush=True)
                time.sleep(wait)
                continue
            body_text = exc.read().decode(errors="replace")
            _explain_api_error(exc.code, body_text)
            raise RuntimeError(f"Ads API error HTTP {exc.code}: {body_text[:400]}") from exc
    raise RuntimeError("Ads API: max retries exceeded")


def fetch_ads(
    token: str,
    ad_ids: Optional[List[int]] = None,
    client_login: str = "",
) -> List[Dict[str, str]]:
    """
    Fetch ad metadata via Ads.get.
    If ad_ids is given, fetches only those; otherwise fetches all ads.
    Returns flat dicts with: Id, CampaignId, AdGroupId, State, Type,
                              Title, Title2, Text, Href.
    """
    offset = 0
    all_ads: List[Dict[str, str]] = []
    selection: Dict[str, Any] = {}
    if ad_ids:
        selection["Ids"] = ad_ids

    while True:
        payload = {
            "method": "get",
            "params": {
                "SelectionCriteria": selection,
                "FieldNames":        ["Id", "CampaignId", "AdGroupId", "State", "Type"],
                "TextAdFieldNames":  ["Title", "Title2", "Text", "Href"],
                "Page":              {"Limit": ADS_PAGE_SIZE, "Offset": offset},
            },
        }
        resp = _call_ads(token, payload, client_login=client_login)
        result = resp.get("result", {})
        ads_page: List[Dict[str, Any]] = result.get("Ads", [])

        for ad in ads_page:
            text_ad = ad.pop("TextAd", None) or {}
            all_ads.append({
                "Id":         str(ad.get("Id", "")),
                "CampaignId": str(ad.get("CampaignId", "")),
                "AdGroupId":  str(ad.get("AdGroupId", "")),
                "State":      str(ad.get("State", "")),
                "Type":       str(ad.get("Type", "")),
                "Title":      str(text_ad.get("Title", "") or ""),
                "Title2":     str(text_ad.get("Title2", "") or ""),
                "Text":       str(text_ad.get("Text", "") or ""),
                "Href":       str(text_ad.get("Href", "") or ""),
            })

        print(f"  [ads] fetched {len(all_ads)} ads…", end="\r", flush=True)

        if result.get("LimitedBy") is None:
            break
        offset += ADS_PAGE_SIZE

    print()
    return all_ads


# ---------------------------------------------------------------------------
# SQLite — meta table
# ---------------------------------------------------------------------------

def _ensure_meta_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_yandex_meta (
            entity         TEXT PRIMARY KEY,
            last_sync_date TEXT NOT NULL
        )
    """)
    conn.commit()


def get_last_sync_date(conn: sqlite3.Connection, entity: str) -> Optional[str]:
    _ensure_meta_table(conn)
    row = conn.execute(
        "SELECT last_sync_date FROM raw_yandex_meta WHERE entity = ?", (entity,)
    ).fetchone()
    return row[0] if row else None


def set_last_sync_date(conn: sqlite3.Connection, entity: str, d: str) -> None:
    _ensure_meta_table(conn)
    conn.execute(
        "INSERT OR REPLACE INTO raw_yandex_meta (entity, last_sync_date) VALUES (?, ?)",
        (entity, d),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# SQLite — raw_yandex_stats
# ---------------------------------------------------------------------------

# Ordered columns — index 0 is ingested_at (not a report field)
_STATS_COLS = [
    "ingested_at",
    "Date", "CampaignId", "CampaignName",
    "AdGroupId", "AdGroupName", "AdId",
    "Impressions", "Clicks", "Cost", "Conversions",
]


def _ensure_stats_table(conn: sqlite3.Connection) -> None:
    extra_cols = ", ".join(
        f'"{c}" TEXT' for c in _STATS_COLS if c not in ("ingested_at", "Date")
    )
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS raw_yandex_stats (
            _rowid_     INTEGER PRIMARY KEY AUTOINCREMENT,
            ingested_at TEXT NOT NULL,
            "Date"      TEXT NOT NULL,
            {extra_cols}
        )
    """)
    conn.execute(
        'CREATE INDEX IF NOT EXISTS ix_rys_date ON raw_yandex_stats ("Date")'
    )
    conn.execute(
        'CREATE INDEX IF NOT EXISTS ix_rys_adid ON raw_yandex_stats ("AdId")'
    )
    conn.commit()


def insert_stats_rows(
    conn: sqlite3.Connection,
    rows: List[Dict[str, str]],
    ingested_at: str,
) -> int:
    if not rows:
        return 0
    _ensure_stats_table(conn)

    # Replace any existing rows for the fetched dates (idempotent re-runs)
    dates = list({r["Date"] for r in rows if r.get("Date")})
    if dates:
        conn.execute(
            f'DELETE FROM raw_yandex_stats WHERE "Date" IN ({",".join("?" * len(dates))})',
            dates,
        )

    col_list    = ",".join(f'"{c}"' for c in _STATS_COLS)
    placeholders = ",".join("?" * len(_STATS_COLS))
    sql = f'INSERT INTO raw_yandex_stats ({col_list}) VALUES ({placeholders})'
    values = [
        (ingested_at,) + tuple(r.get(c, "") for c in _STATS_COLS[1:])
        for r in rows
    ]
    conn.executemany(sql, values)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# SQLite — raw_yandex_ads
# ---------------------------------------------------------------------------

_ADS_COLS = [
    "ingested_at",
    "Id", "CampaignId", "AdGroupId", "State", "Type",
    "Title", "Title2", "Text", "Href",
]


def _ensure_ads_table(conn: sqlite3.Connection) -> None:
    extra_cols = ", ".join(
        f'"{c}" TEXT' for c in _ADS_COLS if c not in ("ingested_at", "Id")
    )
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS raw_yandex_ads (
            "Id"        TEXT PRIMARY KEY,
            ingested_at TEXT NOT NULL,
            {extra_cols}
        )
    """)
    conn.commit()


def upsert_ads_rows(
    conn: sqlite3.Connection,
    rows: List[Dict[str, str]],
    ingested_at: str,
) -> int:
    if not rows:
        return 0
    _ensure_ads_table(conn)
    col_list    = ",".join(f'"{c}"' for c in _ADS_COLS)
    placeholders = ",".join("?" * len(_ADS_COLS))
    sql = f'INSERT OR REPLACE INTO raw_yandex_ads ({col_list}) VALUES ({placeholders})'
    values = [
        (ingested_at,) + tuple(r.get(c, "") for c in _ADS_COLS[1:])
        for r in rows
    ]
    conn.executemany(sql, values)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Aggregate → yandex_stats  (monthly, matches existing D1 schema)
# ---------------------------------------------------------------------------

def rebuild_yandex_stats(conn: sqlite3.Connection) -> int:
    """
    Truncate and repopulate yandex_stats from raw_yandex_stats JOIN raw_yandex_ads.
    The schema matches the existing D1/SQLite table so D1 push works unchanged.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS yandex_stats (
            row_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            "Месяц"                 TEXT,
            "№ Кампании"            TEXT,
            "Название кампании"     TEXT,
            "№ Группы"              TEXT,
            "Название группы"       TEXT,
            "№ Объявления"          TEXT,
            "Статус объявления"     TEXT,
            "Тип объявления"        TEXT,
            "Заголовок"             TEXT,
            "Текст"                 TEXT,
            "Ссылка"                TEXT,
            "Расход, ₽"             REAL,
            "Клики"                 INTEGER,
            "Конверсии"             INTEGER,
            "CR, %"                 REAL,
            "CPA, ₽"                REAL
        )
    """)

    # Truncate and repopulate inside one transaction
    with conn:
        conn.execute("DELETE FROM yandex_stats")
        conn.execute("""
            INSERT INTO yandex_stats (
                "Месяц", "№ Кампании", "Название кампании",
                "№ Группы", "Название группы", "№ Объявления",
                "Статус объявления", "Тип объявления",
                "Заголовок", "Текст", "Ссылка",
                "Расход, ₽", "Клики", "Конверсии", "CR, %", "CPA, ₽"
            )
            SELECT
                substr(s."Date", 1, 7)                   AS "Месяц",
                s."CampaignId"                           AS "№ Кампании",
                s."CampaignName"                         AS "Название кампании",
                s."AdGroupId"                            AS "№ Группы",
                s."AdGroupName"                          AS "Название группы",
                s."AdId"                                 AS "№ Объявления",
                COALESCE(a."State", '')                  AS "Статус объявления",
                COALESCE(a."Type",  '')                  AS "Тип объявления",
                COALESCE(a."Title", '')                  AS "Заголовок",
                COALESCE(a."Text",  '')                  AS "Текст",
                COALESCE(a."Href",  '')                  AS "Ссылка",
                ROUND(SUM(CAST(NULLIF(s."Cost",        '') AS REAL)),    2) AS "Расход, ₽",
                SUM(CAST(NULLIF(s."Clicks",      '') AS INTEGER))           AS "Клики",
                SUM(CAST(NULLIF(s."Conversions", '') AS INTEGER))           AS "Конверсии",
                CASE WHEN SUM(CAST(NULLIF(s."Clicks", '') AS INTEGER)) = 0 THEN 0.0
                     ELSE ROUND(
                        100.0 * SUM(CAST(NULLIF(s."Conversions", '') AS INTEGER))
                              / SUM(CAST(NULLIF(s."Clicks",      '') AS INTEGER)), 2)
                END AS "CR, %",
                CASE WHEN SUM(CAST(NULLIF(s."Conversions", '') AS INTEGER)) = 0 THEN 0.0
                     ELSE ROUND(
                        SUM(CAST(NULLIF(s."Cost",        '') AS REAL))
                      / SUM(CAST(NULLIF(s."Conversions", '') AS INTEGER)), 2)
                END AS "CPA, ₽"
            FROM raw_yandex_stats s
            LEFT JOIN raw_yandex_ads a ON a."Id" = s."AdId"
            WHERE s."Date" IS NOT NULL AND s."Date" != ''
            GROUP BY
                substr(s."Date", 1, 7),
                s."CampaignId", s."CampaignName",
                s."AdGroupId",  s."AdGroupName",
                s."AdId",
                a."State", a."Type", a."Title", a."Text", a."Href"
            ORDER BY "Месяц", "№ Кампании", "№ Группы", "№ Объявления"
        """)

    n: int = conn.execute("SELECT COUNT(*) FROM yandex_stats").fetchone()[0]
    return n


# ---------------------------------------------------------------------------
# SQLite — stg_yandex_stats  (daily, matches the analytics API schema)
# ---------------------------------------------------------------------------

def _ensure_stg_stats_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stg_yandex_stats (
            "Месяц"                        TEXT,
            "День"                         TEXT,
            "№ Кампании"                   REAL,
            "Название кампании"            TEXT,
            "№ Группы"                     REAL,
            "Название группы"              TEXT,
            "№ Объявления"                 REAL,
            "Статус объявления"            TEXT,
            "Тип объявления"               TEXT,
            "Заголовок"                    TEXT,
            "Текст"                        TEXT,
            "Ссылка"                       TEXT,
            "Путь до изображения"          TEXT,
            "Название файла изображения"   TEXT,
            "Идентификатор видео"          TEXT,
            "Путь до превью видео"         TEXT,
            "Место клика"                  TEXT,
            "Формат"                       TEXT,
            "Источник текста"              TEXT,
            "Расход, ₽"                    REAL,
            "Клики"                        INTEGER,
            "Конверсии"                    INTEGER,
            "CR, %"                        TEXT,
            "CPA, ₽"                       TEXT,
            month                          TEXT
        )
    """)
    conn.commit()


def rebuild_stg_yandex_stats(conn: sqlite3.Connection) -> int:
    """
    Replace stg_yandex_stats with daily rows from raw_yandex_stats JOIN raw_yandex_ads.
    This is the table read by the analytics server API.
    Image/video columns (not available from the Reports API) are set to ''.
    """
    _ensure_stg_stats_table(conn)
    with conn:
        conn.execute("DELETE FROM stg_yandex_stats")
        conn.execute("""
            INSERT INTO stg_yandex_stats (
                "Месяц", "День",
                "№ Кампании", "Название кампании",
                "№ Группы",   "Название группы",
                "№ Объявления",
                "Статус объявления", "Тип объявления",
                "Заголовок", "Текст", "Ссылка",
                "Путь до изображения", "Название файла изображения",
                "Идентификатор видео", "Путь до превью видео",
                "Место клика", "Формат", "Источник текста",
                "Расход, ₽", "Клики", "Конверсии", "CR, %", "CPA, ₽",
                month
            )
            SELECT
                substr(s."Date", 1, 7)                                       AS "Месяц",
                s."Date"                                                      AS "День",
                CAST(NULLIF(s."CampaignId", '') AS REAL)                      AS "№ Кампании",
                s."CampaignName"                                              AS "Название кампании",
                CAST(NULLIF(s."AdGroupId",   '') AS REAL)                     AS "№ Группы",
                s."AdGroupName"                                               AS "Название группы",
                CAST(NULLIF(s."AdId",        '') AS REAL)                     AS "№ Объявления",
                COALESCE(a."State",  '')                                      AS "Статус объявления",
                COALESCE(a."Type",   '')                                      AS "Тип объявления",
                COALESCE(a."Title",  '')                                      AS "Заголовок",
                COALESCE(a."Text",   '')                                      AS "Текст",
                COALESCE(a."Href",   '')                                      AS "Ссылка",
                '' AS "Путь до изображения",
                '' AS "Название файла изображения",
                '' AS "Идентификатор видео",
                '' AS "Путь до превью видео",
                '' AS "Место клика",
                '' AS "Формат",
                '' AS "Источник текста",
                ROUND(CAST(NULLIF(s."Cost",        '') AS REAL), 2)           AS "Расход, ₽",
                CAST(NULLIF(s."Clicks",      '') AS INTEGER)                  AS "Клики",
                CAST(NULLIF(s."Conversions", '') AS INTEGER)                  AS "Конверсии",
                CASE
                    WHEN CAST(NULLIF(s."Clicks", '') AS INTEGER) IS NULL
                      OR CAST(NULLIF(s."Clicks", '') AS INTEGER) = 0
                    THEN '0'
                    ELSE CAST(ROUND(
                        100.0 * CAST(NULLIF(s."Conversions", '') AS REAL)
                              / CAST(NULLIF(s."Clicks",      '') AS REAL), 2
                    ) AS TEXT)
                END AS "CR, %",
                CASE
                    WHEN CAST(NULLIF(s."Conversions", '') AS INTEGER) IS NULL
                      OR CAST(NULLIF(s."Conversions", '') AS INTEGER) = 0
                    THEN '0'
                    ELSE CAST(ROUND(
                        CAST(NULLIF(s."Cost",        '') AS REAL)
                      / CAST(NULLIF(s."Conversions", '') AS REAL), 2
                    ) AS TEXT)
                END AS "CPA, ₽",
                substr(s."Date", 1, 7)                                       AS month
            FROM raw_yandex_stats s
            LEFT JOIN raw_yandex_ads a ON a."Id" = s."AdId"
            WHERE s."Date" IS NOT NULL AND s."Date" != ''
            ORDER BY s."Date", s."CampaignId", s."AdGroupId", s."AdId"
        """)
    n: int = conn.execute("SELECT COUNT(*) FROM stg_yandex_stats").fetchone()[0]
    return n


# ---------------------------------------------------------------------------
# Analytics rebuild trigger
# ---------------------------------------------------------------------------

def trigger_analytics_rebuild(port: int = 3000) -> None:
    """POST to the analytics rebuild endpoint to invalidate the query cache."""
    url = os.environ.get(
        "ANALYTICS_REBUILD_URL", f"http://127.0.0.1:{port}/api/analytics/rebuild"
    )
    secret = os.environ.get("ANALYTICS_REBUILD_SECRET", "")
    try:
        payload = json.dumps({"force": False}).encode("utf-8")
        req = urllib.request.Request(url, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        if secret:
            req.add_header("Authorization", f"Bearer {secret}")
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read().decode())
        skipped = result.get("skipped") or (result.get("result") or {}).get("skipped")
        if skipped:
            print("  [rebuild] Analytics up to date — skipped.")
        else:
            paths = (result.get("result") or {}).get("dataset_paths", "?")
            print(f"  [rebuild] Analytics rebuild complete — {paths} paths materialized.")
    except Exception as exc:
        print(f"  [rebuild] Warning: analytics rebuild call failed: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Entity runners
# ---------------------------------------------------------------------------

def run_stats(
    token: str,
    conn: sqlite3.Connection,
    full: bool = False,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    client_login: str = "",
) -> List[int]:
    """Fetch daily stats, store in raw_yandex_stats. Returns list of seen AdIds."""
    print("── Stats ───────────────────────────────────────────────")
    if date_to is None:
        date_to = _today()
    if date_from is None:
        if full:
            date_from = _days_ago(DEFAULT_LOOKBACK_DAYS)
        else:
            last = get_last_sync_date(conn, "stats")
            if last:
                # Overlap by 2 days to catch delayed conversions
                date_from = (date.fromisoformat(last) - timedelta(days=2)).isoformat()
            else:
                print("  No prior sync found — performing full lookback.")
                date_from = _days_ago(DEFAULT_LOOKBACK_DAYS)

    print(f"  Date range : {date_from} → {date_to}")
    if client_login:
        print(f"  Client     : {client_login}")
    rows = fetch_report(token, date_from, date_to, client_login=client_login)
    print(f"  Received   : {len(rows)} daily rows from Reports API")

    n = insert_stats_rows(conn, rows, ingested_at=_now_iso())
    set_last_sync_date(conn, "stats", date_to)
    print(f"  Written    : {n} rows into raw_yandex_stats")

    ad_ids = [int(r["AdId"]) for r in rows if r.get("AdId", "").isdigit()]
    return list(set(ad_ids))


def run_ads(
    token: str,
    conn: sqlite3.Connection,
    ad_ids: Optional[List[int]] = None,
    client_login: str = "",
) -> None:
    """Fetch ad metadata, upsert into raw_yandex_ads."""
    print("── Ads metadata ────────────────────────────────────────")
    ads = fetch_ads(token, ad_ids=ad_ids, client_login=client_login)
    n = upsert_ads_rows(conn, ads, ingested_at=_now_iso())
    print(f"  Written    : {n} rows into raw_yandex_ads")


def run_once(
    token: str,
    conn: sqlite3.Connection,
    entity: str,
    full: bool,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    client_login: str = "",
) -> None:
    t0 = time.time()
    ad_ids: Optional[List[int]] = None

    if entity in ("stats", "all"):
        ad_ids = run_stats(
            token, conn, full=full,
            date_from=date_from, date_to=date_to,
            client_login=client_login,
        )

    if entity in ("ads", "all"):
        run_ads(token, conn, ad_ids=ad_ids, client_login=client_login)

    if entity in ("stats", "all"):
        print("── Aggregating yandex_stats ────────────────────────────")
        n = rebuild_yandex_stats(conn)
        print(f"  yandex_stats: {n} monthly rows")
        print("── Rebuilding stg_yandex_stats ─────────────────────────")
        n2 = rebuild_stg_yandex_stats(conn)
        print(f"  stg_yandex_stats: {n2} daily rows")
        print("── Triggering analytics cache rebuild ──────────────────")
        trigger_analytics_rebuild()

    print(f"\nSync complete in {time.time() - t0:.1f}s")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Yandex Direct → SQLite incremental sync"
    )
    parser.add_argument(
        "--entity",
        choices=["stats", "ads", "all"],
        default="all",
        help="Which entity to sync (default: all)",
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=f"Force full re-download (last {DEFAULT_LOOKBACK_DAYS} days)",
    )
    parser.add_argument(
        "--date-from",
        metavar="YYYY-MM-DD",
        help="Override start date for stats (inclusive)",
    )
    parser.add_argument(
        "--date-to",
        metavar="YYYY-MM-DD",
        help="Override end date for stats (inclusive, default: today)",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Run continuously on --interval schedule (daemon mode)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=180,
        help="Minutes between syncs in --watch mode (default: 180)",
    )
    parser.add_argument(
        "--client-login",
        default=DEFAULT_CLIENT_LOGIN,
        metavar="LOGIN",
        help="Yandex advertiser login for agency/representative accounts (env: YANDEX_CLIENT_LOGIN)",
    )
    parser.add_argument(
        "--get-token",
        metavar="CODE",
        help="Exchange an OAuth authorisation code for a token and save it",
    )
    parser.add_argument(
        "--env-file",
        default=str(
            Path(__file__).resolve().parent.parent
            / "web_share_subset" / "webpush" / ".env.server.json"
        ),
        help="Path to .env.server.json for --get-token storage",
    )
    args = parser.parse_args()

    # ── OAuth token exchange ─────────────────────────────────────────────────
    if args.get_token:
        print("Exchanging OAuth code for access token…")
        try:
            token = exchange_code_for_token(args.get_token)
        except RuntimeError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            sys.exit(1)
        print(f"  Access token obtained (length {len(token)})")
        save_token_to_env(token, args.env_file)
        print("\nDone. Restart PM2 (pm2 restart yandex-sync) to pick up the new token.")
        return

    # ── Normal sync ──────────────────────────────────────────────────────────
    token = os.environ.get("YANDEX_TOKEN", "").strip()
    if not token:
        print(
            "ERROR: YANDEX_TOKEN environment variable is not set.\n\n"
            "First-time setup:\n"
            "  1. Open in browser:\n"
            "     https://oauth.yandex.ru/authorize"
            "?response_type=code&client_id=c0519acddcac4f1797a5414762522492\n"
            "  2. Authorise and copy the verification code.\n"
            "  3. python -m db.yandex_api_sync --get-token <CODE>",
            file=sys.stderr,
        )
        sys.exit(1)

    db_path = args.db
    print(f"Database : {db_path}")
    print(f"Entity   : {args.entity}")
    if args.watch:
        print(f"Mode     : watch (every {args.interval} min)")
    elif args.full:
        print(f"Mode     : full re-download (last {DEFAULT_LOOKBACK_DAYS} days)")
    else:
        print(f"Mode     : incremental")
    if args.client_login:
        print(f"Client   : {args.client_login}")
    print()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    if not args.watch:
        try:
            run_once(
                token, conn, args.entity, args.full,
                date_from=args.date_from,
                date_to=args.date_to,
                client_login=args.client_login,
            )
        finally:
            conn.close()
        return

    # Daemon / watch mode
    print(f"[watch] Daemon started — syncing every {args.interval} min. Ctrl+C to stop.\n")
    try:
        while True:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"[watch] {ts}")
            try:
                run_once(
                    token, conn, args.entity, args.full,
                    client_login=args.client_login,
                )
                # After the first successful run, always do incremental
                args.full = False
            except Exception as exc:
                print(f"[watch] ERROR during sync: {exc}", file=sys.stderr)
            print(f"[watch] Next sync in {args.interval} min. (Ctrl+C to stop)\n")
            time.sleep(args.interval * 60)
    except KeyboardInterrupt:
        print("\n[watch] Stopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
