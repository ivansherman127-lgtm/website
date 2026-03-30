#!/usr/bin/env python3
"""Merge a full Bitrix export with a smaller update export (same schema, ';' CSV).

Rows are keyed by ID. For IDs present in both: non-empty cells in the update
overwrite the base row (so you can patch fields without re-exporting history).

Usage:
  python db/merge_bitrix_csv.py \\
    --base sheets/bitrix_19.03.26.bak_before_upd_27.03 \\
    --update sheets/bitrix_upd_27.03.csv \\
    --out sheets/bitrix_19.03.26
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def _norm_id(v: object) -> str:
    s = "" if v is None else str(v).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def _nonempty(v: object) -> bool:
    if v is None:
        return False
    return str(v).strip() != ""


def read_deals(path: Path) -> tuple[list[str], dict[str, dict[str, str]]]:
    # utf-8-sig: Bitrix exports often start with BOM, which breaks the "ID" column name.
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";", quotechar='"')
        fieldnames = list(reader.fieldnames or [])
        by_id: dict[str, dict[str, str]] = {}
        for row in reader:
            rid = _norm_id(row.get("ID", "") or row.get("\ufeffID", ""))
            if not rid:
                continue
            by_id[rid] = {k: (row.get(k) if row.get(k) is not None else "") for k in fieldnames}
        return fieldnames, by_id


def merge_fieldnames(base_cols: list[str], upd_cols: list[str]) -> list[str]:
    seen = set(base_cols)
    out = list(base_cols)
    for c in upd_cols:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out


def merge_rows(
    cols: list[str],
    base: dict[str, dict[str, str]],
    upd: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    out = {}
    for rid, brow in base.items():
        out[rid] = {c: str(brow.get(c, "") or "") for c in cols}
    for rid, urow in upd.items():
        if rid in out:
            merged = dict(out[rid])
            for c in cols:
                if c in urow and _nonempty(urow[c]):
                    merged[c] = str(urow[c])
            out[rid] = merged
        else:
            out[rid] = {c: str(urow.get(c, "") or "") for c in cols}
    return out


def write_deals(path: Path, cols: list[str], by_id: dict[str, dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for rid in sorted(by_id.keys(), key=lambda x: int(x) if x.isdigit() else x):
            w.writerow({c: by_id[rid].get(c, "") for c in cols})


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge Bitrix ';' CSV exports by deal ID.")
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--update", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    base_cols, base_rows = read_deals(args.base)
    upd_cols, upd_rows = read_deals(args.update)
    cols = merge_fieldnames(base_cols, upd_cols)
    merged = merge_rows(cols, base_rows, upd_rows)
    write_deals(args.out, cols, merged)
    print(
        f"Wrote {args.out}: {len(merged)} deals "
        f"(base {len(base_rows)}, update {len(upd_rows)}, columns {len(cols)})"
    )


if __name__ == "__main__":
    main()
