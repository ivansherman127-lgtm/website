#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

from bitrix_lead_quality import drop_rows_excluded_funnels
from bitrix_union_io import dedup_bitrix_deals_by_highest_amount, load_bitrix_deals_union
from conn import ensure_schema, get_engine
from utils import _n, _id

RAW_TABLE = "raw_bitrix_deals"
BATCH_TABLE = "raw_source_batches"
SOURCE_REF = "bitrix_19.03.26.csv + bitrix_60_days_03.04.2026.csv"


def _load_union() -> pd.DataFrame:
    bitrix = load_bitrix_deals_union()
    if "ID" not in bitrix.columns:
        raise SystemExit("Union dataframe does not contain ID")
    bitrix = bitrix.copy()
    bitrix["ID"] = bitrix["ID"].map(_id)
    bitrix = dedup_bitrix_deals_by_highest_amount(bitrix)
    bitrix = drop_rows_excluded_funnels(bitrix)
    if "UTM Content" in bitrix.columns:
        bitrix["UTM Content"] = bitrix["UTM Content"].map(_n)
    return bitrix


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh relational raw_bitrix_deals from CSV union")
    parser.add_argument("--db", default=None, help="SQLite DB path (default: website.db)")
    parser.add_argument("--source-batch", default=None, help="Optional batch key override")
    parser.add_argument("--dry-run", action="store_true", help="Only print insert/update/unchanged counts")
    args = parser.parse_args()

    engine = get_engine(args.db)
    ensure_schema(engine)

    bitrix = _load_union()
    incoming = bitrix.copy()

    ts = datetime.now(timezone.utc).isoformat()
    batch = args.source_batch or f"csv_union_upsert_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    incoming["source_batch"] = batch
    incoming["ingested_at"] = ts

    with engine.begin() as conn:
        existing_count = 0
        try:
            existing_count = int(conn.execute(text(f"SELECT COUNT(*) FROM {RAW_TABLE}")).scalar() or 0)
        except Exception:
            existing_count = 0

        if not args.dry_run:
            incoming.to_sql(RAW_TABLE, conn, if_exists="replace", index=False, chunksize=100)
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
                    "source_type": "csv_union_incremental",
                    "source_ref": SOURCE_REF,
                    "row_count": int(len(incoming)),
                    "created_at": ts,
                },
            )

    print(
        "upsert_raw_bitrix_from_union: "
        f"incoming={len(incoming)} previous_rows={existing_count} replaced_rows={len(incoming)} "
        f"dry_run={args.dry_run} batch={batch}"
    )


if __name__ == "__main__":
    main()
