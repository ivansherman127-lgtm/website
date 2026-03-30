#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

from bitrix_union_io import load_bitrix_deals_union
from conn import ensure_schema, get_engine

RAW_TABLE = "raw_bitrix_deals"
BATCH_TABLE = "raw_source_batches"


def _n(v: object) -> str:
    if v is None or pd.isna(v):
        return ""
    s = str(v).strip()
    return "" if s.lower() in {"", "nan", "none", "null"} else s


def _id(v: object) -> str:
    s = _n(v)
    if s.endswith(".0") and s.replace(".0", "").isdigit():
        return s.split(".", 1)[0]
    return s


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill raw_bitrix_deals from CSV union exports")
    parser.add_argument("--db", default=None, help="SQLite DB path (default: deved.db)")
    parser.add_argument("--source-batch", default=None, help="Optional batch key override")
    args = parser.parse_args()

    engine = get_engine(args.db)
    ensure_schema(engine)

    bitrix = load_bitrix_deals_union()
    if "ID" not in bitrix.columns:
        raise SystemExit("Union dataframe does not contain ID")

    bitrix = bitrix.copy()
    bitrix["ID"] = bitrix["ID"].map(_id)
    bitrix = bitrix[bitrix["ID"].astype(str).str.strip().ne("")].drop_duplicates(subset=["ID"], keep="last")

    ts = datetime.now(timezone.utc).isoformat()
    batch = args.source_batch or f"csv_union_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    raw = bitrix.copy()
    raw["source_batch"] = batch
    raw["ingested_at"] = ts
    raw.to_sql(RAW_TABLE, engine, if_exists="replace", index=False, chunksize=100)

    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT OR REPLACE INTO {BATCH_TABLE}
                (source_batch, source_type, source_ref, row_count, created_at)
                VALUES (:source_batch, :source_type, :source_ref, :row_count, :created_at)
                """
            ),
            {
                "source_batch": batch,
                "source_type": "csv_union",
                "source_ref": "sheets/fl_raw_09-03.csv + sheets/bitrix_upd_27.03.csv",
                "row_count": int(len(raw)),
                "created_at": ts,
            },
        )

    print(f"Backfilled {RAW_TABLE}: {len(raw)} rows (batch={batch})")


if __name__ == "__main__":
    main()
