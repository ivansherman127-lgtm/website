#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from conn import get_conn


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = ROOT / "bitrix_contacts_uid.csv"


def norm_str(v: object) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in {"", "nan", "none", "null"} else s


def norm_contact_id(v: object) -> str:
    s = norm_str(v)
    if s.endswith(".0") and s.replace(".0", "").isdigit():
        return s.split(".", 1)[0]
    return s


def split_contact_ids(raw: str) -> list[str]:
    if not raw:
        return []
    parts = [norm_contact_id(p) for p in raw.split("|")]
    return [p for p in parts if p]


def to_int(v: object) -> int | None:
    s = norm_str(v)
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Load bitrix_contacts_uid.csv into SQLite stg_contacts_uid")
    parser.add_argument("--db", default=None, help="SQLite DB path (default: deved.db)")
    parser.add_argument("--csv", default=str(DEFAULT_CSV), help="Path to bitrix_contacts_uid.csv")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"Missing CSV: {csv_path}")

    rows_to_insert: list[tuple[object, ...]] = []
    uid_count = 0
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            uid = norm_str(row.get("contact_uid", ""))
            if not uid:
                continue
            uid_count += 1
            contact_ids = split_contact_ids(norm_str(row.get("all_contact_ids", "")))
            if not contact_ids:
                continue

            all_names = norm_str(row.get("all_names", "")) or None
            all_phones = norm_str(row.get("all_phones", "")) or None
            all_emails = norm_str(row.get("all_emails", "")) or None
            contact_ids_count = to_int(row.get("contact_ids_count", ""))
            names_count = to_int(row.get("names_count", ""))
            phones_count = to_int(row.get("phones_count", ""))
            emails_count = to_int(row.get("emails_count", ""))

            for contact_id in contact_ids:
                rows_to_insert.append(
                    (
                        uid,
                        contact_id,
                        all_names,
                        all_phones,
                        all_emails,
                        contact_ids_count,
                        names_count,
                        phones_count,
                        emails_count,
                    )
                )

    conn = get_conn(args.db)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stg_contacts_uid (
              contact_uid TEXT NOT NULL,
              contact_id TEXT NOT NULL,
              all_names TEXT,
              all_phones TEXT,
              all_emails TEXT,
              contact_ids_count INTEGER,
              names_count INTEGER,
              phones_count INTEGER,
              emails_count INTEGER,
              PRIMARY KEY (contact_uid, contact_id)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stg_contacts_uid_contact_id ON stg_contacts_uid(contact_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stg_contacts_uid_contact_uid ON stg_contacts_uid(contact_uid)")
        cur.execute("DELETE FROM stg_contacts_uid")

        cur.executemany(
            """
            INSERT INTO stg_contacts_uid (
              contact_uid,
              contact_id,
              all_names,
              all_phones,
              all_emails,
              contact_ids_count,
              names_count,
              phones_count,
              emails_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )
        conn.commit()
    finally:
        conn.close()

    print(f"Loaded stg_contacts_uid: {len(rows_to_insert)} rows from {uid_count} contact_uids")


if __name__ == "__main__":
    main()