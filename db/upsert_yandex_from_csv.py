#!/usr/bin/env python3
"""
Upsert a new Yandex Ads CSV (daily granularity) into stg_yandex_stats (SQLite),
push the updated table to D1, and trigger the cloud rebuild.

Usage:
    python db/upsert_yandex_from_csv.py sheets/yandex_upd-03.26.csv

The script handles the new daily-export format (column «День» = DD.MM.YYYY) and
aggregates rows to monthly level to match the existing stg_yandex_stats schema.
Existing rows for the months present in the new file are replaced.

After updating SQLite it runs push_from_sqlite.py (--remote --tables stg_yandex_stats)
and then POSTs to the rebuild endpoint.
"""
from __future__ import annotations

import os
import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "deved.db"
PUSH_SCRIPT = ROOT / "db" / "d1" / "push_from_sqlite.py"
WRANGLER_CONFIG = ROOT / "wrangler.jsonc"

# Exact D1 schema for stg_yandex_stats (db/d1/migrations/0001_initial.sql)
STG_YANDEX_D1_SCHEMA: list[tuple[str, str]] = [
    ("Месяц", "TEXT"),
    ("№ Кампании", "REAL"),
    ("Название кампании", "TEXT"),
    ("№ Группы", "REAL"),
    ("Название группы", "TEXT"),
    ("№ Объявления", "REAL"),
    ("Статус объявления", "TEXT"),
    ("Тип объявления", "TEXT"),
    ("Заголовок", "TEXT"),
    ("Текст", "TEXT"),
    ("Ссылка", "TEXT"),
    ("Путь до изображения", "TEXT"),
    ("Название файла изображения", "TEXT"),
    ("Идентификатор видео", "TEXT"),
    ("Путь до превью видео", "TEXT"),
    ("Место клика", "TEXT"),
    ("Формат", "TEXT"),
    ("Источник текста", "TEXT"),
    ("Расход, ₽", "REAL"),
    ("Клики", "INTEGER"),
    ("Конверсии", "INTEGER"),
    ("CR, %", "TEXT"),
    ("CPA, ₽", "TEXT"),
    ("month", "TEXT"),
]
STG_YANDEX_D1_COLUMNS = [c for c, _t in STG_YANDEX_D1_SCHEMA]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _id(v: object) -> str:
    s = str(v).strip()
    if s.lower() in ("", "nan", "-"):
        return ""
    try:
        return str(int(float(s)))
    except (ValueError, OverflowError):
        return s


def _safe_div(num: float, den: float) -> float:
    return num / den if den else 0.0


def _normalize_yandex_month(v: object) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if len(s) >= 7 and s[:7].count("-") == 1 and s[:4].isdigit():
        return s[:7]

    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    if pd.notna(dt):
        return dt.strftime("%Y-%m")

    lower = s.lower()
    year_match = pd.Series([lower]).str.extract(r"(20\d{2})")[0].iloc[0]
    if not isinstance(year_match, str) or not year_match:
        return ""
    month_map = (
        ("январ", "01"),
        ("феврал", "02"),
        ("март", "03"),
        ("апрел", "04"),
        ("мая", "05"),
        ("май", "05"),
        ("июн", "06"),
        ("июл", "07"),
        ("август", "08"),
        ("сентябр", "09"),
        ("октябр", "10"),
        ("ноябр", "11"),
        ("декабр", "12"),
    )
    for token, mm in month_map:
        if token in lower:
            return f"{year_match}-{mm}"
    return ""


# ---------------------------------------------------------------------------
# Load + normalise new CSV
# ---------------------------------------------------------------------------

def load_and_normalize(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, encoding="utf-8", low_memory=False)
    print(f"Loaded {len(df)} rows from {csv_path}", flush=True)

    # Detect format: old export has «Месяц», new daily export has «День»
    if "День" in df.columns and "Месяц" not in df.columns:
        dt = pd.to_datetime(df["День"], dayfirst=True, errors="coerce")
        df["Месяц"] = dt.dt.strftime("%Y-%m")
        df = df.drop(columns=["День"])
        print("Converted «День» → «Месяц» (monthly bucket)", flush=True)
    elif "Месяц" not in df.columns:
        raise ValueError(f"Cannot find «Месяц» or «День» column in {csv_path}")

    df["Месяц"] = df["Месяц"].map(_normalize_yandex_month)
    df = df[df["Месяц"].astype(str).str.fullmatch(r"\d{4}-\d{2}", na=False)]

    # Clean ID columns (float → integer string)
    for col in ("№ Объявления", "№ Кампании", "№ Группы"):
        if col in df.columns:
            df[col] = df[col].map(_id)

    # Filter out summary/totals rows (Кампания = empty → Итого rows)
    if "№ Кампании" in df.columns:
        df = df[df["№ Кампании"].ne("")]

    # Numeric spend/clicks columns
    spend_col = "Расход, ₽"
    impressions_col = "Показы"
    clicks_col = "Клики"
    conv_col = "Конверсии"

    for col in (spend_col, impressions_col, clicks_col, conv_col):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Aggregate daily → monthly.
    # Group by all ID/string columns; sum numeric; recompute CR% and CPA.
    id_cols = [c for c in (
        "Месяц", "№ Кампании", "Название кампании",
        "№ Объявления", "Статус объявления", "Тип объявления",
        "Заголовок", "Текст", "Ссылка",
        "Путь до изображения", "Название файла изображения",
        "Идентификатор видео", "Путь до превью видео",
        "Формат", "Источник текста",
        "№ Группы", "Название группы",
        # old-format optional columns
        "Место клика", "Статус кампании",
    ) if c in df.columns]

    sum_cols = [c for c in (spend_col, impressions_col, clicks_col, conv_col) if c in df.columns]

    if id_cols:
        agg: dict[str, str] = {c: "sum" for c in sum_cols}
        df = df.groupby(id_cols, dropna=False, as_index=False).agg(agg)
        print(f"Aggregated to {len(df)} monthly rows", flush=True)

    # Recompute CR% and CPA₽ after aggregation
    if clicks_col in df.columns and conv_col in df.columns:
        df["CR, %"] = df.apply(
            lambda r: round(_safe_div(r[conv_col], r[clicks_col]) * 100, 2), axis=1
        )
    if spend_col in df.columns and conv_col in df.columns:
        df["CPA, ₽"] = df.apply(
            lambda r: round(_safe_div(r[spend_col], r[conv_col]), 2), axis=1
        )

    # Add month helper used by run_all_slices.py
    if "Месяц" in df.columns:
        df["month"] = df["Месяц"].map(_normalize_yandex_month)

    return df


# ---------------------------------------------------------------------------
# SQLite upsert
# ---------------------------------------------------------------------------

def upsert_to_sqlite(df: pd.DataFrame, engine) -> int:
    # Keep only columns that exist in D1 and fill any missing with NULL.
    for col in STG_YANDEX_D1_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[STG_YANDEX_D1_COLUMNS].copy()

    # Keep IDs numeric-compatible for D1 REAL columns while preserving empty values.
    for id_col in ("№ Кампании", "№ Группы", "№ Объявления"):
        df[id_col] = pd.to_numeric(df[id_col], errors="coerce")

    # D1 schema stores CR/CPA as text.
    for t_col in ("CR, %", "CPA, ₽"):
        df[t_col] = df[t_col].map(lambda v: "" if pd.isna(v) else str(v))

    # month helper expected by downstream queries.
    df["month"] = df["Месяц"].map(_normalize_yandex_month)

    months = df["Месяц"].dropna().unique().tolist()
    print(f"Will replace months in SQLite: {months}", flush=True)

    with engine.begin() as conn:
        exists = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='stg_yandex_stats'")).fetchone()

        # Recreate local table to match exact D1 schema (drops any accidental extra columns).
        conn.execute(text("DROP TABLE IF EXISTS stg_yandex_stats_new"))
        create_cols = ",\n  ".join([f'"{c}" {t}' for c, t in STG_YANDEX_D1_SCHEMA])
        conn.execute(text(f"CREATE TABLE stg_yandex_stats_new (\n  {create_cols}\n)"))

        if exists:
            old_cols = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(stg_yandex_stats)")).fetchall()
            }
            copy_cols = [c for c in STG_YANDEX_D1_COLUMNS if c in old_cols]
            if copy_cols:
                cols_sql = ", ".join([f'"{c}"' for c in copy_cols])
                conn.execute(
                    text(
                        f"INSERT INTO stg_yandex_stats_new ({cols_sql}) "
                        f"SELECT {cols_sql} FROM stg_yandex_stats"
                    )
                )
            conn.execute(text("DROP TABLE stg_yandex_stats"))
            conn.execute(text("ALTER TABLE stg_yandex_stats_new RENAME TO stg_yandex_stats"))
        else:
            conn.execute(text("ALTER TABLE stg_yandex_stats_new RENAME TO stg_yandex_stats"))

        for m in months:
            # Delete by both the normalized month col (YYYY-MM) AND the old Russian-format
            # "Месяц" col so that rows imported before normalization (e.g. "Март, 2026")
            # are also removed.  Using month satisfies both cases.
            conn.execute(text('DELETE FROM stg_yandex_stats WHERE month = :m OR "Месяц" = :m'), {"m": m})
        print(f"Deleted existing rows for {len(months)} month(s)", flush=True)

    chunksize = max(1, 998 // max(1, df.shape[1]))
    df.to_sql("stg_yandex_stats", engine, if_exists="append", index=False, chunksize=chunksize)
    print(f"Inserted {len(df)} rows into stg_yandex_stats", flush=True)
    return len(df)


# ---------------------------------------------------------------------------
# Push to D1
# ---------------------------------------------------------------------------

def push_to_d1(wrangler_config: Path) -> None:
    python = sys.executable
    cmd = [
        python, str(PUSH_SCRIPT),
        "--remote",
        "--tables", "stg_yandex_stats",
        "--no-json",
        "--wrangler-config", str(wrangler_config),
    ]
    print(f"\nPushing stg_yandex_stats to D1...", flush=True)
    print(f"  {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd, cwd=str(ROOT))
    if result.returncode != 0:
        print("ERROR: push_from_sqlite.py failed — D1 not updated", flush=True)
        sys.exit(result.returncode)
    print("D1 push complete", flush=True)


# ---------------------------------------------------------------------------
# Cloud rebuild
# ---------------------------------------------------------------------------

def trigger_rebuild() -> None:
    import urllib.request, urllib.error, json as _json

    url = os.environ.get("D1_ANALYTICS_REBUILD_URL", "").strip()
    secret = os.environ.get("ANALYTICS_REBUILD_SECRET", "").strip()

    if not url:
        print(
            "\nSkipping cloud rebuild — D1_ANALYTICS_REBUILD_URL not set.\n"
            "Set it and ANALYTICS_REBUILD_SECRET, then re-run or curl manually.",
            flush=True,
        )
        return

    print(f"\nTriggering rebuild at {url} ...", flush=True)
    req = urllib.request.Request(
        url,
        method="POST",
        headers={"X-Rebuild-Secret": secret, "Content-Type": "application/json"},
        data=b"{}",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode()
            print(f"Rebuild response ({resp.status}): {body}", flush=True)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"Rebuild HTTP {e.code}: {body}", flush=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upsert Yandex CSV into local SQLite, optionally push to D1 and trigger rebuild.")
    parser.add_argument("csv_path", help="Path to Yandex update CSV")
    parser.add_argument("--db-path", default=os.environ.get("DEVED_DB_PATH", str(DEFAULT_DB_PATH)), help="Path to local SQLite DB")
    parser.add_argument("--wrangler-config", default=str(WRANGLER_CONFIG), help="Wrangler config used for D1 push")
    parser.add_argument("--skip-push", action="store_true", help="Only update local SQLite; do not push stg_yandex_stats to D1")
    parser.add_argument("--skip-rebuild", action="store_true", help="Do not call cloud analytics rebuild after push")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    csv_path = Path(args.csv_path)
    if not csv_path.is_absolute():
        csv_path = ROOT / csv_path
    if not csv_path.exists():
        print(f"ERROR: file not found: {csv_path}", flush=True)
        sys.exit(1)

    db_path = Path(args.db_path)
    if not db_path.is_absolute():
        db_path = ROOT / db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)

    engine = create_engine(f"sqlite:///{db_path}")
    print(f"Using SQLite DB: {db_path}", flush=True)

    df = load_and_normalize(csv_path)
    upsert_to_sqlite(df, engine)
    if not args.skip_push:
        push_to_d1(Path(args.wrangler_config))
        if not args.skip_rebuild:
            trigger_rebuild()
    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
