#!/usr/bin/env python3
"""
Push selected tables from local deved.db and/or JSON files under public/data into Cloudflare D1.

  ./.venv/bin/python3 db/d1/push_from_sqlite.py --remote --wrangler-config web/wrangler.toml

**Prerequisite:** `deved.db` must already contain marts and staging (e.g. run
`python db/run_all_slices.py` from repo root). If those tables are missing locally, this
script skips them silently unless you use `--strict` — then you may end up with only
`dataset_json` rows (from public/data/*.json) and empty metric tables in D1.

Requires: wrangler CLI, and for --remote either `wrangler login` or CLOUDFLARE_API_TOKEN.

Syncs stg_deals_analytics and stg_yandex_stats (for POST /api/analytics/rebuild).
Does NOT sync stg_bitrix_deals_wide (too wide/large). Large JSON files are skipped unless
--include-large-json is set. Oversized rows are split (INSERT + chained UPDATEs; dataset_json
uses multiple rows with chunk index). See D1_MAX_INSERT_STATEMENT_BYTES / D1_MAX_DATASET_INSERT_BYTES.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterable, Optional

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "db"))

DEFAULT_TABLES = [
    "raw_bitrix_deals",
    "raw_source_batches",
    "stg_contacts_uid",
    "mart_deals_enriched",
    "mart_attacking_january_contacts",
    "mart_attacking_january_cohort_deals",
    "stg_yandex_stats",
    "stg_email_sends",
    "stg_deals_analytics",
    "deals",
    "yandex_stats",
    "email_sends",
    "sheets_sync_log",
]

SKIP_TABLES = {"stg_bitrix_deals_wide"}
AUTO_RECREATE_TABLES: set[str] = {
    "mart_deals_enriched",
    "mart_attacking_january_cohort_deals",
    "stg_deals_analytics",
}

# Tables too wide for D1's column limit are split into partitions: table_p01, table_p02, …
# Each partition contains the primary-key column + up to N data columns.
D1_MAX_COLS_PER_PARTITION = 90
VERTICAL_PARTITION_TABLES: dict[str, int] = {
    "raw_bitrix_deals": D1_MAX_COLS_PER_PARTITION,
}

# Must exist in local SQLite with at least one row for Cloudflare metrics / rebuild to work.
CRITICAL_TABLES = ("raw_bitrix_deals", "mart_deals_enriched", "stg_deals_analytics")

ROWS_PER_INSERT_OVERRIDES: dict[str, int] = {}

# Max UTF-8 bytes per wrangler SQL file (multiple statements). Kept large so INSERT+chunked UPDATE
# sequences for one oversized row are never split across files (last_insert_rowid must stay valid).
MAX_SQL_FILE_BYTES = 2_000_000
# D1/SQLite rejects any single INSERT that is too large (SQLITE_TOOBIG). Cap each statement.
MAX_INSERT_STATEMENT_BYTES = 48_000
# Per-chunk cap for dataset_json SQL literals (files are split across chunk rows). Override: D1_MAX_DATASET_INSERT_BYTES.
MAX_DATASET_JSON_STATEMENT_BYTES = MAX_INSERT_STATEMENT_BYTES


def table_row_count(conn, table: str) -> Optional[int]:
    try:
        cur = conn.execute(f'SELECT COUNT(*) FROM "{table}"')
        return int(cur.fetchone()[0])
    except Exception:
        return None


def preflight_report(conn, tables: list[str]) -> tuple[list[str], list[tuple[str, int]]]:
    """Returns (missing_tables, empty_critical_with_counts)."""
    missing: list[str] = []
    empty_critical: list[tuple[str, int]] = []
    for t in tables:
        n = table_row_count(conn, t)
        if n is None:
            missing.append(t)
        elif t in CRITICAL_TABLES and n == 0:
            empty_critical.append((t, 0))
    return missing, empty_critical


def sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v != v:  # NaN
            return "NULL"
        return repr(v)
    s = str(v)
    return "'" + s.replace("'", "''") + "'"


def table_columns(conn, table: str) -> list[str]:
    cur = conn.execute(f'PRAGMA table_info("{table}")')
    return [row[1] for row in cur.fetchall()]


def _sqlite_col_def(row: tuple[Any, ...]) -> str:
    # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
    name = str(row[1]).replace('"', '""')
    typ = (row[2] or "TEXT").strip() or "TEXT"
    notnull = " NOT NULL" if bool(row[3]) else ""
    dflt = ""
    if row[4] is not None:
        dflt = f" DEFAULT {row[4]}"
    return f'"{name}" {typ}{notnull}{dflt}'


def recreate_table_statements(conn, table: str) -> list[str]:
    info = _table_info_rows(conn, table)
    if not info:
        return []
    cols = [_sqlite_col_def(r) for r in info]
    pk_cols = [str(r[1]).replace('"', '""') for r in info if int(r[5] or 0) > 0]
    pk_sql = ""
    if pk_cols and len(pk_cols) > 1:
        pk_sql = ", PRIMARY KEY (" + ", ".join(f'"{c}"' for c in pk_cols) + ")"
    elif pk_cols and len(pk_cols) == 1:
        # Respect existing single-column PRIMARY KEY as table constraint if not in column type.
        one = pk_cols[0]
        col_defs = []
        for r in info:
            if str(r[1]) == one and "PRIMARY KEY" not in str(r[2]).upper():
                col_defs.append(_sqlite_col_def(r) + " PRIMARY KEY")
            else:
                col_defs.append(_sqlite_col_def(r))
        cols = col_defs
    create_stmt = f'CREATE TABLE "{table}" (' + ", ".join(cols) + pk_sql + ");"
    return [f'DROP TABLE IF EXISTS "{table}";', create_stmt]


def _table_info_rows(conn, table: str) -> list[tuple[Any, ...]]:
    cur = conn.execute(f'PRAGMA table_info("{table}")')
    return list(cur.fetchall())


def _pk_indices_from_info(info: list[tuple[Any, ...]]) -> set[int]:
    """Column indices (0..n-1) that are part of PRIMARY KEY."""
    return {i for i, row in enumerate(info) if row[5]}  # pk


def _stmt_utf8_len(s: str) -> int:
    return len(s.encode("utf-8"))


def _row_values_sql(row: tuple[Any, ...]) -> str:
    return "(" + ", ".join(sql_literal(v) for v in row) + ")"


def _update_text_column_statements(
    table: str,
    col_esc: str,
    text: str,
    max_stmt_bytes: int,
) -> list[str]:
    """Rebuild a long TEXT value with multiple UPDATEs bound by max_stmt_bytes per statement."""
    stmts: list[str] = []
    n = len(text)
    i = 0
    first = True
    while i < n:
        lo, hi = i, n
        best = i
        while lo <= hi:
            mid = (lo + hi) // 2
            piece = text[i:mid]
            lit = sql_literal(piece)
            if first:
                stmt = f'UPDATE "{table}" SET "{col_esc}" = {lit} WHERE rowid = last_insert_rowid();'
            else:
                stmt = (
                    f'UPDATE "{table}" SET "{col_esc}" = COALESCE("{col_esc}", \'\') || {lit} '
                    f"WHERE rowid = last_insert_rowid();"
                )
            if _stmt_utf8_len(stmt) <= max_stmt_bytes:
                best = mid
                lo = mid + 1
            else:
                hi = mid - 1
        if best == i:
            piece = text[i : i + 1]
            lit = sql_literal(piece)
            if first:
                stmt = f'UPDATE "{table}" SET "{col_esc}" = {lit} WHERE rowid = last_insert_rowid();'
            else:
                stmt = (
                    f'UPDATE "{table}" SET "{col_esc}" = COALESCE("{col_esc}", \'\') || {lit} '
                    f"WHERE rowid = last_insert_rowid();"
                )
            if _stmt_utf8_len(stmt) > max_stmt_bytes:
                raise ValueError("single character exceeds D1 statement budget after escaping")
            best = i + 1
        piece = text[i:best]
        lit = sql_literal(piece)
        if first:
            stmt = f'UPDATE "{table}" SET "{col_esc}" = {lit} WHERE rowid = last_insert_rowid();'
        else:
            stmt = (
                f'UPDATE "{table}" SET "{col_esc}" = COALESCE("{col_esc}", \'\') || {lit} '
                f"WHERE rowid = last_insert_rowid();"
            )
        stmts.append(stmt)
        first = False
        i = best
    return stmts


def _statements_for_oversized_row(
    table: str,
    columns: list[str],
    row: tuple[Any, ...],
    info: list[tuple[Any, ...]],
    pk_idx: set[int],
    max_stmt_bytes: int,
) -> list[str]:
    """
    One INSERT (NULL/0/'' placeholders) plus UPDATE ... WHERE rowid = last_insert_rowid()
    so no single statement exceeds max_stmt_bytes.
    """
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    header = f'INSERT INTO "{table}" ({cols_sql}) VALUES '
    trial: list[Any] = list(row)
    cleared: dict[int, Any] = {}

    def insert_sql() -> str:
        return header + _row_values_sql(tuple(trial)) + ";"

    def insert_fits() -> bool:
        return _stmt_utf8_len(insert_sql()) <= max_stmt_bytes

    order = [i for i in range(len(row)) if i not in pk_idx]
    order.sort(
        key=lambda i: _stmt_utf8_len(sql_literal(row[i])) if i < len(row) else 0,
        reverse=True,
    )

    for i in order:
        if insert_fits():
            break
        orig = row[i]
        if orig is None:
            continue
        cleared[i] = orig
        notnull = bool(info[i][3])
        typ = (info[i][2] or "").upper()
        if not notnull:
            trial[i] = None
        elif any(x in typ for x in ("INT", "REAL", "FLO", "NUM", "BOOL")):
            trial[i] = 0
        else:
            trial[i] = ""

    if not insert_fits():
        for i in order:
            if insert_fits():
                break
            orig = row[i]
            if orig is None or i in pk_idx:
                continue
            cleared[i] = orig
            notnull = bool(info[i][3])
            typ = (info[i][2] or "").upper()
            if not notnull:
                trial[i] = None
            elif any(x in typ for x in ("INT", "REAL", "FLO", "NUM", "BOOL")):
                trial[i] = 0
            else:
                trial[i] = ""

    if not insert_fits():
        print(
            f"  WARNING: {table}: skipping one row (cannot shrink INSERT below {max_stmt_bytes} bytes)",
            flush=True,
        )
        return []

    stmts: list[str] = [insert_sql()]

    for col_idx in sorted(cleared.keys()):
        orig = cleared[col_idx]
        col_esc = columns[col_idx].replace('"', '""')
        if isinstance(orig, bool):
            stmts.append(
                f'UPDATE "{table}" SET "{col_esc}" = {sql_literal(orig)} '
                f"WHERE rowid = last_insert_rowid();"
            )
            continue
        if isinstance(orig, (int, float)) and not isinstance(orig, bool):
            stmts.append(
                f'UPDATE "{table}" SET "{col_esc}" = {sql_literal(orig)} '
                f"WHERE rowid = last_insert_rowid();"
            )
            continue
        st = (
            str(orig)
            if not isinstance(orig, (bytes, memoryview))
            else bytes(orig).decode("utf-8", errors="replace")
        )
        stmts.extend(_update_text_column_statements(table, col_esc, st, max_stmt_bytes))

    return stmts


def rows_iter(conn, table: str, columns: list[str]) -> Iterable[tuple[Any, ...]]:
    cols_sql = ", ".join('"' + c.replace('"', '""') + '"' for c in columns)
    cur = conn.execute(f'SELECT {cols_sql} FROM "{table}"')
    yield from cur


def _nonempty_columns(conn, table: str, all_cols: list[str]) -> list[str]:
    """Return only columns that have at least one non-NULL, non-empty-string value."""
    kept = [all_cols[0]]  # always keep the PK (first column)
    for c in all_cols[1:]:
        ce = c.replace('"', '""')
        row = conn.execute(
            f'SELECT 1 FROM "{table}" WHERE "{ce}" IS NOT NULL AND "{ce}" != "" LIMIT 1'
        ).fetchone()
        if row is not None:
            kept.append(c)
    return kept


def _partition_column_groups(columns: list[str], cols_per_part: int) -> list[list[str]]:
    """Split columns into groups keeping column[0] (PK) in every partition."""
    pk = columns[0]
    rest = columns[1:]
    groups: list[list[str]] = []
    for i in range(0, len(rest), cols_per_part):
        groups.append([pk] + rest[i : i + cols_per_part])
    return groups


def export_table_partitioned(
    conn,
    table: str,
    cols_per_part: int,
    *,
    remote: bool,
    wrangler_config: Path,
    database_name: str,
    max_stmt_bytes: int,
) -> None:
    """Push a wide table to D1 by splitting it vertically into partition sub-tables."""
    all_cols = table_columns(conn, table)
    if not all_cols:
        print(f"  skip {table}: no columns", flush=True)
        return
    print(f"  {table}: scanning for non-empty columns…", flush=True)
    nonempty_cols = _nonempty_columns(conn, table, all_cols)
    dropped = len(all_cols) - len(nonempty_cols)
    groups = _partition_column_groups(nonempty_cols, cols_per_part)
    n_parts = len(groups)
    pk_col = all_cols[0]
    all_rows_raw = list(conn.execute(f'SELECT * FROM "{table}"'))
    print(f"  {table}: {len(all_rows_raw)} rows, {len(nonempty_cols)} cols (+{dropped} empty dropped) → {n_parts} partitions", flush=True)
    col_index = {c: i for i, c in enumerate(all_cols)}
    try:
        cap = int(os.environ.get("D1_MAX_INSERT_STATEMENT_BYTES", str(MAX_INSERT_STATEMENT_BYTES)))
    except ValueError:
        cap = MAX_INSERT_STATEMENT_BYTES
    per_stmt_cap = min(max_stmt_bytes, cap)
    for part_num, part_cols in enumerate(groups, start=1):
        part_name = f"{table}_p{part_num:02d}"
        part_indices = [col_index[c] for c in part_cols]
        part_rows = [tuple(r[i] for i in part_indices) for r in all_rows_raw]
        # Build DDL: all TEXT, PK on first column.
        pk_esc = pk_col.replace('"', '""')
        col_defs = [f'"{pk_esc}" TEXT PRIMARY KEY NOT NULL']
        for c in part_cols[1:]:
            ce = c.replace('"', '""')
            col_defs.append(f'"{ce}" TEXT')
        ddl = [
            f'DROP TABLE IF EXISTS "{part_name}";',
            f'CREATE TABLE "{part_name}" ({chr(44).join(col_defs)});',
        ]
        _, segments = build_insert_statements(
            conn,
            table,  # only used for ROWS_PER_INSERT_OVERRIDES lookup — falls back to 200
            part_cols,
            part_rows,
            max_stmt_bytes=per_stmt_cap,
            max_rows_per_statement=ROWS_PER_INSERT_OVERRIDES.get(table, 50),
        )
        # Override target table name in INSERT statements.
        fixed_segments: list[list[str]] = []
        for seg in segments:
            fixed_segments.append(
                [s.replace(f'INSERT INTO "{table}"', f'INSERT INTO "{part_name}"') for s in seg]
            )
        insert_batches = _pack_segments_for_wrangler(fixed_segments, MAX_SQL_FILE_BYTES)
        for bi, batch in enumerate(insert_batches):
            if bi == 0:
                chunk_lines = ddl + batch
            else:
                chunk_lines = batch
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".sql", delete=False, encoding="utf-8"
            ) as tmp:
                tmp_path = Path(tmp.name)
                tmp.write("\n".join(chunk_lines) + "\n")
            try:
                run_wrangler_file(
                    tmp_path,
                    remote=remote,
                    wrangler_config=wrangler_config,
                    database_name=database_name,
                )
            finally:
                tmp_path.unlink(missing_ok=True)
            if len(insert_batches) > 1:
                print(f"    {part_name}: batch {bi + 1}/{len(insert_batches)}", flush=True)
        print(f"    {part_name}: {len(part_rows)} rows ({len(part_cols)} cols)", flush=True)


def build_insert_statements(
    conn,
    table: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    *,
    max_stmt_bytes: int,
    max_rows_per_statement: int,
) -> tuple[str, list[list[str]]]:
    """
    Build DELETE statement plus **segments** (each segment is atomic for wrangler batching).
    Normal multi-row INSERTs are one segment each; oversized rows become one segment of
    INSERT + chained UPDATEs (must run in one wrangler file without other INSERTs between).
    """
    delete_stmt = f'DELETE FROM "{table}";'
    if not rows:
        return delete_stmt, []
    info = _table_info_rows(conn, table)
    pk_idx = _pk_indices_from_info(info)
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    header = f'INSERT INTO "{table}" ({cols_sql}) VALUES '
    segments: list[list[str]] = []
    chunk: list[tuple[Any, ...]] = []

    def flush_chunk() -> None:
        if not chunk:
            return
        body = ", ".join(_row_values_sql(r) for r in chunk)
        segments.append([header + body + ";"])
        chunk.clear()

    for row in rows:
        one = header + _row_values_sql(row) + ";"
        if _stmt_utf8_len(one) > max_stmt_bytes:
            flush_chunk()
            segments.append(
                _statements_for_oversized_row(
                    table,
                    columns,
                    row,
                    info,
                    pk_idx,
                    max_stmt_bytes,
                )
            )
            continue
        if not chunk:
            chunk.append(row)
            continue
        trial = header + ", ".join(_row_values_sql(r) for r in chunk + [row]) + ";"
        if len(chunk) >= max_rows_per_statement or _stmt_utf8_len(trial) > max_stmt_bytes:
            flush_chunk()
        chunk.append(row)
    flush_chunk()
    return delete_stmt, segments


def _dataset_json_insert_lines(rel: str, text: str, max_stmt_bytes: int) -> list[str]:
    """One INSERT per chunk; each statement stays under max_stmt_bytes (UTF-8)."""
    lines: list[str] = []
    n = len(text)
    ci = 0
    i = 0
    if n == 0:
        return [
            "INSERT INTO dataset_json (path, chunk, body, updated_at) VALUES ("
            f"{sql_literal(rel)}, 0, '', datetime('now'));"
        ]
    while i < n:
        lo, hi = i, n
        best = i
        while lo <= hi:
            mid = (lo + hi) // 2
            piece = text[i:mid]
            line = (
                "INSERT INTO dataset_json (path, chunk, body, updated_at) VALUES ("
                f"{sql_literal(rel)}, {ci}, {sql_literal(piece)}, datetime('now'));"
            )
            if _stmt_utf8_len(line) <= max_stmt_bytes:
                best = mid
                lo = mid + 1
            else:
                hi = mid - 1
        if best == i:
            best = i + 1
        piece = text[i:best]
        line = (
            "INSERT INTO dataset_json (path, chunk, body, updated_at) VALUES ("
            f"{sql_literal(rel)}, {ci}, {sql_literal(piece)}, datetime('now'));"
        )
        if _stmt_utf8_len(line) > max_stmt_bytes:
            raise ValueError(f"dataset_json: chunk exceeds cap for path {rel!r}")
        lines.append(line)
        i = best
        ci += 1
    return lines


def _pack_segments_for_wrangler(
    segments: list[list[str]],
    max_file_bytes: int,
) -> list[list[str]]:
    """Pack atomic statement segments into wrangler file batches without splitting a segment."""
    batches: list[list[str]] = []
    cur: list[str] = []
    cur_b = 0
    for seg in segments:
        seg_b = sum(_stmt_utf8_len(s) for s in seg)
        if seg_b > max_file_bytes:
            if cur:
                batches.append(cur)
                cur = []
                cur_b = 0
            batches.append(seg[:])
            continue
        if cur and cur_b + seg_b > max_file_bytes:
            batches.append(cur)
            cur = []
            cur_b = 0
        cur.extend(seg)
        cur_b += seg_b
    if cur:
        batches.append(cur)
    return batches


def write_sql_file(path: Path, statements: list[str]) -> None:
    path.write_text("\n".join(statements) + "\n", encoding="utf-8")


def run_wrangler_file(
    sql_path: Path,
    *,
    remote: bool,
    wrangler_config: Path,
    database_name: str,
) -> None:
    cmd = [
        "npx",
        "wrangler",
        "d1",
        "execute",
        database_name,
        "--config",
        str(wrangler_config),
        "--file",
        str(sql_path),
    ]
    if remote:
        cmd.append("--remote")
    # Non-interactive: avoid "Ok to proceed?" on every batch (wrangler -y).
    cmd.append("--yes")
    env = {**os.environ}
    if os.environ.get("CI", "").lower() in ("1", "true"):
        env.setdefault("WRANGLER_SEND_METRICS", "false")
    print(" +", " ".join(cmd), flush=True)
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
    )
    out = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        print(out, flush=True)
        if "TOOBIG" in out or "too long" in out.lower():
            raise SystemExit(
                "D1 rejected a SQL statement as too large (SQLITE_TOOBIG).\n"
                "  • Pull latest push_from_sqlite.py (byte-capped INSERTs + dataset_json cap).\n"
                "  • Lower D1_MAX_INSERT_STATEMENT_BYTES / D1_MAX_DATASET_INSERT_BYTES if needed.\n"
                "  • Or omit heavy tables: --tables '...' without yandex_stats; or --no-json for dataset_json.\n"
                f"  (wrangler exit code {proc.returncode})"
            )
        if "10000" in out or "Authentication error" in out or "Authentication" in out:
            raise SystemExit(
                "Cloudflare rejected this request (often OAuth expired or bad API token).\n"
                "  • Run: npx wrangler login\n"
                "  • Or set CLOUDFLARE_API_TOKEN to an API token with Account → D1 → Edit (and matching account).\n"
                "  • If CLOUDFLARE_API_TOKEN is set but old, unset it and use wrangler login again.\n"
                f"  (wrangler exit code {proc.returncode})"
            )
        raise SystemExit(f"wrangler d1 execute failed (exit {proc.returncode}). Output above.")


def export_table(
    conn,
    table: str,
    *,
    remote: bool,
    wrangler_config: Path,
    database_name: str,
    max_stmt_bytes: int,
) -> None:
    columns = table_columns(conn, table)
    if not columns:
        print(f"  skip {table}: no columns", flush=True)
        return
    rows = list(rows_iter(conn, table, columns))
    pre_stmts: list[str] = []
    if table in AUTO_RECREATE_TABLES:
        pre_stmts = recreate_table_statements(conn, table)
    max_rows = ROWS_PER_INSERT_OVERRIDES.get(table, 200)
    try:
        cap = int(os.environ.get("D1_MAX_INSERT_STATEMENT_BYTES", str(MAX_INSERT_STATEMENT_BYTES)))
    except ValueError:
        cap = MAX_INSERT_STATEMENT_BYTES
    per_stmt_cap = min(max_stmt_bytes, cap)
    delete_stmt, segments = build_insert_statements(
        conn,
        table,
        columns,
        rows,
        max_stmt_bytes=per_stmt_cap,
        max_rows_per_statement=max_rows,
    )
    if not segments:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".sql",
            delete=False,
            encoding="utf-8",
        ) as tmp:
            tmp_path = Path(tmp.name)
            lines = [*pre_stmts]
            if table not in AUTO_RECREATE_TABLES:
                lines.append(delete_stmt)
            tmp.write("\n".join(lines) + "\n")
        try:
            run_wrangler_file(
                tmp_path,
                remote=remote,
                wrangler_config=wrangler_config,
                database_name=database_name,
            )
        finally:
            tmp_path.unlink(missing_ok=True)
        print(f"  {table}: {len(rows)} rows", flush=True)
        return

    insert_batches = _pack_segments_for_wrangler(segments, MAX_SQL_FILE_BYTES)
    for bi, batch in enumerate(insert_batches):
        if bi == 0:
            chunk_lines = ([*pre_stmts] + batch) if table in AUTO_RECREATE_TABLES else ([delete_stmt] + batch)
        else:
            chunk_lines = batch
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".sql",
            delete=False,
            encoding="utf-8",
        ) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write("\n".join(chunk_lines) + "\n")
        try:
            run_wrangler_file(
                tmp_path,
                remote=remote,
                wrangler_config=wrangler_config,
                database_name=database_name,
            )
        finally:
            tmp_path.unlink(missing_ok=True)
        if len(insert_batches) > 1:
            print(f"  {table}: batch {bi + 1}/{len(insert_batches)}", flush=True)
    print(f"  {table}: {len(rows)} rows", flush=True)


def iter_json_files(base: Path) -> Iterable[Path]:
    if not base.is_dir():
        return
    for p in sorted(base.rglob("*.json")):
        if p.is_file():
            yield p


def push_json_dir(
    json_dir: Path,
    *,
    remote: bool,
    wrangler_config: Path,
    database_name: str,
    max_json_bytes: int,
    include_large: bool,
) -> None:
    files = list(iter_json_files(json_dir))
    count = 0
    chunks_total = 0
    skipped = 0
    pending: list[str] = []
    pending_bytes = 0
    first_batch = True
    max_batch = 75_000

    def flush() -> None:
        nonlocal pending, pending_bytes, first_batch
        if not pending:
            return
        lines = []
        if first_batch:
            lines.append("DELETE FROM dataset_json;")
            first_batch = False
        lines.extend(pending)
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".sql",
            delete=False,
            encoding="utf-8",
        ) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write("\n".join(lines) + "\n")
        try:
            run_wrangler_file(
                tmp_path,
                remote=remote,
                wrangler_config=wrangler_config,
                database_name=database_name,
            )
        finally:
            tmp_path.unlink(missing_ok=True)
        pending = []
        pending_bytes = 0

    for path in files:
        rel = path.relative_to(json_dir).as_posix()
        raw = path.read_bytes()
        if len(raw) > max_json_bytes and not include_large:
            print(f"  skip JSON (size {len(raw)}): {rel}", flush=True)
            skipped += 1
            continue
        try:
            text = raw.decode("utf-8")
            json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            print(f"  skip JSON (invalid): {rel} ({e})", flush=True)
            skipped += 1
            continue
        try:
            ds_cap = int(os.environ.get("D1_MAX_DATASET_INSERT_BYTES", str(MAX_DATASET_JSON_STATEMENT_BYTES)))
        except ValueError:
            ds_cap = MAX_DATASET_JSON_STATEMENT_BYTES
        try:
            ins_lines = _dataset_json_insert_lines(rel, text, ds_cap)
        except ValueError as e:
            print(f"  skip JSON: {rel} ({e})", flush=True)
            skipped += 1
            continue
        chunks_total += len(ins_lines)
        for line in ins_lines:
            est = _stmt_utf8_len(line)
            if est > max_batch:
                flush()
                one: list[str] = []
                if first_batch:
                    one.append("DELETE FROM dataset_json;")
                    first_batch = False
                one.append(line)
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    suffix=".sql",
                    delete=False,
                    encoding="utf-8",
                ) as tmp:
                    tmp_path = Path(tmp.name)
                    tmp.write("\n".join(one) + "\n")
                try:
                    run_wrangler_file(
                        tmp_path,
                        remote=remote,
                        wrangler_config=wrangler_config,
                        database_name=database_name,
                    )
                finally:
                    tmp_path.unlink(missing_ok=True)
                continue
            if pending and pending_bytes + est > max_batch:
                flush()
            pending.append(line)
            pending_bytes += est
        count += 1

    flush()
    if not count and skipped:
        print("  dataset_json: no files loaded (all skipped)", flush=True)
        return
    print(f"  dataset_json: {count} files, {chunks_total} chunks ({skipped} skipped)", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description="Push SQLite + JSON snapshots to Cloudflare D1")
    p.add_argument(
        "--db",
        default=None,
        help="Path to deved.db (default: project root deved.db)",
    )
    p.add_argument(
        "--wrangler-config",
        type=Path,
        default=ROOT / "web" / "wrangler.toml",
        help="wrangler.toml with [[d1_databases]]",
    )
    p.add_argument(
        "--database-name",
        default="cybered",
        help="D1 database name (must match wrangler.toml [[d1_databases]].database_name)",
    )
    p.add_argument(
        "--remote",
        action="store_true",
        help="Apply to remote D1 (omit for --file-only local testing)",
    )
    p.add_argument(
        "--tables",
        default=",".join(DEFAULT_TABLES),
        help="Comma-separated table list",
    )
    p.add_argument(
        "--no-json",
        action="store_true",
        help="Do not load public/data JSON into dataset_json",
    )
    p.add_argument(
        "--json-dir",
        type=Path,
        default=ROOT / "web" / "public" / "data",
        help="Root for JSON files (paths stored relative to this dir)",
    )
    p.add_argument(
        "--max-json-mb",
        type=float,
        default=48.0,
        help="Skip JSON files larger than this (default 48 MiB)",
    )
    p.add_argument(
        "--include-large-json",
        action="store_true",
        help="Attempt to load large JSON too (may fail on D1 limits)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions only",
    )
    p.add_argument(
        "--preflight",
        action="store_true",
        help="Print row counts for each --tables entry in local deved.db and exit (no push).",
    )
    p.add_argument(
        "--strict",
        action="store_true",
        help="Exit with error if any CRITICAL table is missing or empty (raw_bitrix_deals, mart_deals_enriched, stg_deals_analytics).",
    )
    args = p.parse_args()

    from conn import get_conn  # noqa: WPS433

    db_path = args.db or str(ROOT / "deved.db")
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    for t in tables:
        if t in SKIP_TABLES:
            raise SystemExit(f"Refusing to sync disallowed table: {t}")

    max_json_bytes = int(args.max_json_mb * 1024 * 1024)

    if args.dry_run:
        print("dry-run: tables:", tables)
        print("dry-run: json-dir:", args.json_dir, "no-json:", args.no_json)
        return

    conn = get_conn(db_path)
    try:
        print(f"Local DB: {db_path}", flush=True)
        missing, empty_crit = preflight_report(conn, tables)
        for t in tables:
            n = table_row_count(conn, t)
            label = "MISSING" if n is None else str(n)
            mark = "  **" if t in CRITICAL_TABLES and (n is None or n == 0) else ""
            print(f"  {t}: {label} rows{mark}", flush=True)
        if missing:
            print(
                "\nWARNING: Tables above are MISSING in local SQLite — they will NOT be pushed. "
                "Run: python db/run_all_slices.py",
                flush=True,
            )
        if empty_crit:
            print(
                "\nWARNING: Critical tables are EMPTY — Cloudflare will have no deal-level data for metrics. "
                "Run: python db/run_all_slices.py",
                flush=True,
            )
        if args.strict:
            for t in CRITICAL_TABLES:
                n = table_row_count(conn, t)
                if n is None or n == 0:
                    raise SystemExit(
                        f"--strict: table {t!r} is missing or empty in {db_path}. "
                        "Run: python db/run_all_slices.py (and ensure yandex.csv / matched_yd.csv exist if you need Yandex marts)."
                    )
        if args.preflight:
            return

        for table in tables:
            try:
                conn.execute(f'SELECT 1 FROM "{table}" LIMIT 1')
            except Exception as e:
                print(f"  skip {table}: {e}", flush=True)
                continue
            if table in VERTICAL_PARTITION_TABLES:
                export_table_partitioned(
                    conn,
                    table,
                    VERTICAL_PARTITION_TABLES[table],
                    remote=args.remote,
                    wrangler_config=args.wrangler_config,
                    database_name=args.database_name,
                    max_stmt_bytes=90_000,
                )
            else:
                export_table(
                    conn,
                    table,
                    remote=args.remote,
                    wrangler_config=args.wrangler_config,
                    database_name=args.database_name,
                    max_stmt_bytes=90_000,
                )
    finally:
        conn.close()

    if not args.no_json:
        push_json_dir(
            args.json_dir,
            remote=args.remote,
            wrangler_config=args.wrangler_config,
            database_name=args.database_name,
            max_json_bytes=max_json_bytes,
            include_large=args.include_large_json,
        )


if __name__ == "__main__":
    main()
