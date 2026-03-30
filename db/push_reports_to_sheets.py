"""
Push reports/slices/*.csv to Google Sheets with expandable row groups.

Reuses patterns from bitrix_notebook.ipynb (Month→Medium→Source) and
yandex_reload.ipynb (spreadsheet.batch_update for row groups).

Usage:
  python db/push_reports_to_sheets.py --credentials keys/....json
  python db/push_reports_to_sheets.py --credentials ... --sheet-id 'https://...'
  python db/push_reports_to_sheets.py --dry-run
  python db/push_reports_to_sheets.py --only bitrix_month,yandex_raw_vs_dedup
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
REPORTS_SLICES = ROOT / "reports" / "slices"
BITRIX_NOTEBOOK = ROOT / "bitrix_notebook.ipynb"


def _try_log_sync() -> Callable[..., None]:
    try:
        from sheets_log import log_sync as _ls

        return _ls
    except ImportError:
        pass
    try:
        from db.sheets_log import log_sync as _ls

        return _ls
    except ImportError:
        return lambda *a, **k: None


log_sync = _try_log_sync()


def resolve_credentials_path(path: Optional[str]) -> Optional[str]:
    import os

    creds_path = path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        return None
    p = Path(creds_path).expanduser()
    if p.is_dir():
        jsons = list(p.glob("*.json"))
        if not jsons:
            print(f"No .json in directory {creds_path}")
            return None
        return str(jsons[0])
    if "*" in str(creds_path):
        import glob

        jsons = glob.glob(str(p))
        if not jsons:
            print(f"No file matching {creds_path}")
            return None
        return jsons[0]
    if not p.exists():
        print(f"Credentials file not found: {creds_path}")
        return None
    return str(p)


def sheet_id_from_url_or_key(sheet_id: str) -> str:
    s = sheet_id.strip()
    if "/" in s and ("docs.google.com" in s or s.startswith("http")):
        return s.split("/d/")[-1].split("/")[0].split("?")[0]
    return s


def read_sheet_id_from_bitrix_notebook() -> Optional[str]:
    """Parse .ipynb JSON; source lines are real Python with quotes (not JSON-escaped in parsed form)."""
    if not BITRIX_NOTEBOOK.exists():
        return None
    try:
        nb = json.loads(BITRIX_NOTEBOOK.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    for cell in nb.get("cells", []):
        lines = cell.get("source", [])
        if isinstance(lines, str):
            lines = [lines]
        for line in lines:
            if "GOOGLE_SHEET_ID" not in line or "=" not in line:
                continue
            if line.strip().startswith("#"):
                continue
            m = re.search(
                r'GOOGLE_SHEET_ID\s*=\s*["\']([^"\']+)["\']',
                line,
            )
            if m:
                return m.group(1).strip()
            m2 = re.search(
                r"GOOGLE_SHEET_ID\s*=\s*\"([^\"]+)\"",
                line,
            )
            if m2:
                return m2.group(1).strip()
    return None


def resolve_spreadsheet_id(cli_sheet: Optional[str]) -> str:
    import os

    if cli_sheet:
        return cli_sheet.strip()
    env_id = os.environ.get("GOOGLE_SHEET_ID")
    if env_id:
        return env_id.strip()
    nb_id = read_sheet_id_from_bitrix_notebook()
    if nb_id:
        return nb_id
    raise SystemExit(
        "Spreadsheet not specified: pass --sheet-id, set GOOGLE_SHEET_ID, "
        f"or define GOOGLE_SHEET_ID in {BITRIX_NOTEBOOK.name}."
    )


def open_spreadsheet(creds_path: str, spreadsheet_id_or_url: str):
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    gc = gspread.authorize(creds)
    sid = spreadsheet_id_or_url
    is_url = "/" in sid and ("docs.google.com" in sid or sid.startswith("http"))
    key = sheet_id_from_url_or_key(sid)
    try:
        if is_url and hasattr(gc, "open_by_url"):
            return gc.open_by_url(sid), key
        return gc.open_by_key(key), key
    except Exception as e:
        raise SystemExit(
            f"Could not open spreadsheet (check ID and edit access for service account): {e}"
        ) from e


def ensure_worksheet(sh, worksheet_name: str, num_rows: int, num_cols: int):
    import gspread

    try:
        return sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(
            title=worksheet_name[:100],
            rows=max(1000, num_rows),
            cols=max(26, num_cols),
        )


def write_dataframe(ws, df: pd.DataFrame) -> Tuple[int, int]:
    df2 = df.fillna("").reset_index(drop=True)
    data = [df2.columns.tolist()] + df2.astype(str).values.tolist()
    ws.clear()
    ws.update(data, range_name="A1", value_input_option="USER_ENTERED")
    return len(data), len(data[0]) if data else 0


def build_row_groups_month_medium_source(
    data: List[List[str]], sheet_id_val: int, level_col: int = 0
) -> List[dict]:
    """Match bitrix_notebook push_fl_hierarchy_to_sheets grouping (header row 0)."""
    requests: List[dict] = []
    n_rows = len(data)
    if n_rows <= 1:
        return requests
    i = 1
    while i < n_rows:
        level = str(data[i][level_col]).strip() if level_col < len(data[i]) else ""
        if level != "Month":
            i += 1
            continue
        k = i + 1
        while k < n_rows:
            lv = str(data[k][level_col]).strip() if level_col < len(data[k]) else ""
            if lv == "Month":
                break
            k += 1
        end_block = k
        if end_block > i + 1:
            requests.append(
                {
                    "addDimensionGroup": {
                        "range": {
                            "sheetId": sheet_id_val,
                            "dimension": "ROWS",
                            "startIndex": i + 1,
                            "endIndex": end_block,
                        }
                    }
                }
            )
        j = i + 1
        while j < end_block:
            lev_j = str(data[j][level_col]).strip() if level_col < len(data[j]) else ""
            if lev_j == "Medium":
                end_med = j + 1
                while end_med < end_block:
                    lv = (
                        str(data[end_med][level_col]).strip()
                        if level_col < len(data[end_med])
                        else ""
                    )
                    if lv in ("Medium", "Month"):
                        break
                    end_med += 1
                if end_med > j + 1:
                    requests.append(
                        {
                            "addDimensionGroup": {
                                "range": {
                                    "sheetId": sheet_id_val,
                                    "dimension": "ROWS",
                                    "startIndex": j + 1,
                                    "endIndex": end_med,
                                }
                            }
                        }
                    )
                q = j + 1
                while q < end_med:
                    lv_q = (
                        str(data[q][level_col]).strip()
                        if level_col < len(data[q])
                        else ""
                    )
                    if lv_q == "Source":
                        end_src = q + 1
                        while end_src < end_med:
                            lv_s = (
                                str(data[end_src][level_col]).strip()
                                if level_col < len(data[end_src])
                                else ""
                            )
                            if lv_s in ("Source", "Medium", "Month"):
                                break
                            end_src += 1
                        if end_src > q + 1:
                            requests.append(
                                {
                                    "addDimensionGroup": {
                                        "range": {
                                            "sheetId": sheet_id_val,
                                            "dimension": "ROWS",
                                            "startIndex": q + 1,
                                            "endIndex": end_src,
                                        }
                                    }
                                }
                            )
                        q = end_src
                    else:
                        q += 1
                j = end_med
            else:
                j += 1
        i = k
    return requests


def build_row_groups_parent_child(
    df: pd.DataFrame,
    sheet_id_val: int,
    parent_level: str,
    child_level: str,
) -> List[dict]:
    """One outer group: parent row at Level=parent_level, children contiguous Level=child_level."""
    requests: List[dict] = []
    if "Level" not in df.columns:
        return requests
    n = len(df)
    i = 0
    while i < n:
        if str(df.iloc[i]["Level"]).strip() != parent_level:
            i += 1
            continue
        k = i + 1
        while k < n and str(df.iloc[k]["Level"]).strip() == child_level:
            k += 1
        if k > i + 1:
            start_index = i + 2
            end_index = k + 1
            requests.append(
                {
                    "addDimensionGroup": {
                        "range": {
                            "sheetId": sheet_id_val,
                            "dimension": "ROWS",
                            "startIndex": start_index,
                            "endIndex": end_index,
                        }
                    }
                }
            )
        i = k
    return requests


def apply_requests(sh, requests: List[dict]) -> None:
    if not requests:
        return
    try:
        sh.batch_update({"requests": requests})
    except Exception as e:
        print(f"  Row grouping failed (data was written): {e}")


# --- Hierarchy builders ---


def _sum_cols(df: pd.DataFrame, cols: List[str]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for c in cols:
        out[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).sum()
    return out


def build_bitrix_month_channel(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    for c in ["utm_source", "utm_medium"]:
        if c in d.columns:
            d[c] = d[c].fillna("").astype(str).str.strip()
            d[c] = d[c].replace({"nan": ""})
    d = d.sort_values(["month", "utm_medium", "utm_source"], na_position="first")
    rows = []
    value_cols = [c for c in ["deals", "paid_deals", "revenue"] if c in d.columns]
    for month, g_month in d.groupby("month", sort=False):
        agg_m = _sum_cols(g_month, value_cols)
        rows.append(
            {
                "Level": "Month",
                "month": month,
                "utm_medium": "",
                "utm_source": "",
                **agg_m,
            }
        )
        for med, g_med in g_month.groupby("utm_medium", sort=False):
            med_lbl = med if str(med).strip() else "(нет medium)"
            agg_med = _sum_cols(g_med, value_cols)
            rows.append(
                {
                    "Level": "Medium",
                    "month": month,
                    "utm_medium": med_lbl,
                    "utm_source": "",
                    **agg_med,
                }
            )
            for _, r in g_med.iterrows():
                src = r.get("utm_source", "")
                rows.append(
                    {
                        "Level": "Source",
                        "month": month,
                        "utm_medium": med_lbl,
                        "utm_source": src,
                        **{c: r.get(c, 0) for c in value_cols},
                    }
                )
    out = pd.DataFrame(rows)
    cols = ["Level", "month", "utm_medium", "utm_source"] + value_cols
    return out[[c for c in cols if c in out.columns]]


def build_month_detail_hierarchy(
    df: pd.DataFrame,
    month_col: str,
    metric_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Month summary + one Source row per original line (Level=Source)."""
    d = df.copy()
    if month_col not in d.columns:
        return d
    d[month_col] = d[month_col].fillna("").astype(str).str.strip()
    if metric_cols is None:
        metric_cols = [
            c
            for c in d.columns
            if c != month_col and pd.api.types.is_numeric_dtype(d[c])
        ]
    rows = []
    for mval, g in d.groupby(month_col, sort=False):
        sums = {c: pd.to_numeric(g[c], errors="coerce").fillna(0).sum() for c in metric_cols}
        rows.append({"Level": "Month", month_col: mval, **{c: sums[c] for c in metric_cols}})
        for _, r in g.iterrows():
            row_dict = {"Level": "Source", month_col: mval}
            for c in d.columns:
                if c == month_col:
                    continue
                row_dict[c] = r.get(c, "")
            rows.append(row_dict)
    out = pd.DataFrame(rows)
    front = ["Level", month_col]
    rest = [c for c in out.columns if c not in front]
    return out[front + rest]


def build_funnel_stage(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy().sort_values(["funnel", "stage"], na_position="last")
    rows = []
    value_cols = [c for c in ["deals", "paid_deals", "revenue"] if c in d.columns]
    for funnel, g_f in d.groupby("funnel", sort=False):
        agg_f = _sum_cols(g_f, value_cols)
        rows.append(
            {"Level": "Funnel", "funnel": funnel, "stage": "(все стадии)", **agg_f}
        )
        for _, r in g_f.iterrows():
            rows.append(
                {
                    "Level": "Stage",
                    "funnel": funnel,
                    "stage": r.get("stage", ""),
                    **{c: r.get(c, 0) for c in value_cols},
                }
            )
    out = pd.DataFrame(rows)
    cols = ["Level", "funnel", "stage"] + value_cols
    return out[[c for c in cols if c in out.columns]]


def build_event_course(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy().sort_values(["event_class", "course_code_norm"], na_position="last")
    rows = []
    value_cols = [c for c in ["deals", "paid_deals", "revenue"] if c in d.columns]
    for ev, g_e in d.groupby("event_class", sort=False):
        agg_e = _sum_cols(g_e, value_cols)
        rows.append(
            {
                "Level": "Event",
                "event_class": ev,
                "course_code_norm": "(все курсы)",
                **agg_e,
            }
        )
        for _, r in g_e.iterrows():
            rows.append(
                {
                    "Level": "Course",
                    "event_class": ev,
                    "course_code_norm": r.get("course_code_norm", ""),
                    **{c: r.get(c, 0) for c in value_cols},
                }
            )
    out = pd.DataFrame(rows)
    cols = ["Level", "event_class", "course_code_norm"] + value_cols
    return out[[c for c in cols if c in out.columns]]


def build_yandex_month_project(df: pd.DataFrame) -> pd.DataFrame:
    """month -> project_name rows; aggregate metrics on Month row."""
    d = df.copy()
    if "project_name" not in d.columns or "month" not in d.columns:
        return d
    d["month"] = d["month"].fillna("").astype(str).str.strip()
    d = d.sort_values(["month", "project_name"], na_position="last")
    numeric = [
        c
        for c in d.columns
        if c not in ("month", "project_name") and c != "Level"
    ]
    rows = []
    for mval, g in d.groupby("month", sort=False):
        sums = {c: pd.to_numeric(g[c], errors="coerce").fillna(0).sum() for c in numeric}
        rows.append({"Level": "Month", "month": mval, "project_name": "(все проекты)", **sums})
        for _, r in g.iterrows():
            row = {"Level": "Project", "month": mval, "project_name": r.get("project_name", "")}
            for c in numeric:
                row[c] = r.get(c, "")
            rows.append(row)
    out = pd.DataFrame(rows)
    front = ["Level", "month", "project_name"]
    rest = [c for c in out.columns if c not in front]
    return out[front + rest]


def build_flat(df: pd.DataFrame) -> pd.DataFrame:
    return df.fillna("")


def build_yandex_no_month_sorted(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "revenue_dedup" in d.columns:
        d["_s"] = pd.to_numeric(d["revenue_dedup"], errors="coerce").fillna(0)
        d = d.sort_values("_s", ascending=False).drop(columns=["_s"])
    elif "revenue_raw" in d.columns:
        d["_s"] = pd.to_numeric(d["revenue_raw"], errors="coerce").fillna(0)
        d = d.sort_values("_s", ascending=False).drop(columns=["_s"])
    return d.fillna("")


@dataclass
class ReportSpec:
    report_id: str
    rel_path: str
    worksheet: str
    builder: Callable[[pd.DataFrame], pd.DataFrame]
    group_mode: str  # "month_medium_source" | "parent_child" | "month_source" | "none"


def all_report_specs() -> List[ReportSpec]:
    return [
        ReportSpec(
            "bitrix_month",
            "global/month_channel_bitrix.csv",
            "Slices — Bitrix month×source",
            build_bitrix_month_channel,
            "month_medium_source",
        ),
        ReportSpec(
            "yandex_month",
            "global/month_channel_yandex.csv",
            "Slices — Yandex month",
            lambda df: build_month_detail_hierarchy(
                df,
                "month",
                [c for c in df.columns if c != "month"],
            ),
            "month_source",
        ),
        ReportSpec(
            "sendsay_month",
            "global/month_channel_sendsay.csv",
            "Slices — Sendsay month",
            lambda df: build_month_detail_hierarchy(
                df,
                "month",
                [c for c in df.columns if c != "month"],
            ),
            "month_source",
        ),
        ReportSpec(
            "funnel_stage",
            "global/funnel_stage.csv",
            "Slices — Funnel×stage",
            build_funnel_stage,
            "parent_child",
        ),
        ReportSpec(
            "event_course",
            "global/event_course.csv",
            "Slices — Event×course",
            build_event_course,
            "parent_child",
        ),
        ReportSpec(
            "cohort_contacts",
            "cohorts/attacking_january/cohort_assoc_contacts.csv",
            "Slices — AJ cohort contacts",
            build_flat,
            "none",
        ),
        ReportSpec(
            "cohort_event_course",
            "cohorts/attacking_january/cohort_assoc_event_course.csv",
            "Slices — AJ cohort event×course",
            build_event_course,
            "parent_child",
        ),
        ReportSpec(
            "yandex_raw_vs_dedup",
            "global/yandex_projects_revenue_raw_vs_dedup.csv",
            "Slices — Yandex project revenue raw vs dedup",
            build_yandex_month_project,
            "parent_child",
        ),
        ReportSpec(
            "yandex_by_month",
            "global/yandex_projects_revenue_by_month.csv",
            "Slices — Yandex revenue by month",
            lambda df: build_month_detail_hierarchy(
                df, "month", [c for c in df.columns if c != "month"]
            ),
            "month_source",
        ),
        ReportSpec(
            "yandex_no_month",
            "global/yandex_projects_revenue_no_month.csv",
            "Slices — Yandex project revenue (no month)",
            build_yandex_no_month_sorted,
            "none",
        ),
        ReportSpec(
            "yandex_dedup_summary",
            "global/yandex_dedup_summary.csv",
            "Slices — Yandex dedup summary",
            build_flat,
            "none",
        ),
        ReportSpec(
            "qa_other_share",
            "qa/other_share_global.csv",
            "QA — Other share global",
            build_flat,
            "none",
        ),
        ReportSpec(
            "qa_other_top50",
            "qa/other_top50_cohort.csv",
            "QA — Other top50 cohort",
            build_flat,
            "none",
        ),
        ReportSpec(
            "qa_dedup_check",
            "qa/dedup_check.csv",
            "QA — Dedup check",
            build_flat,
            "none",
        ),
        ReportSpec(
            "qa_yandex_delta",
            "qa/yandex_raw_vs_dedup_delta.csv",
            "QA — Yandex raw vs dedup delta",
            build_flat,
            "none",
        ),
        ReportSpec(
            "qa_yandex_collisions",
            "qa/yandex_dedup_keys_top_collisions.csv",
            "QA — Yandex dedup collisions",
            build_flat,
            "none",
        ),
        ReportSpec(
            "qa_yandex_unmatched",
            "qa/yandex_unmatched_to_bitrix.csv",
            "QA — Yandex unmatched to Bitrix",
            build_flat,
            "none",
        ),
        ReportSpec(
            "qa_mapping_seed",
            "qa/yandex_campaign_mapping_seed.csv",
            "QA — Yandex mapping seed",
            build_flat,
            "none",
        ),
    ]


def build_row_groups_month_source(
    data: List[List[str]], sheet_id_val: int, level_col: int = 0
) -> List[dict]:
    """Month block: group Source/detail rows under Month (like Medium block wrapping Source)."""
    requests: List[dict] = []
    n_rows = len(data)
    if n_rows <= 1:
        return requests
    i = 1
    while i < n_rows:
        level = str(data[i][level_col]).strip() if level_col < len(data[i]) else ""
        if level != "Month":
            i += 1
            continue
        k = i + 1
        while k < n_rows:
            lv = str(data[k][level_col]).strip() if level_col < len(data[k]) else ""
            if lv == "Month":
                break
            k += 1
        if k > i + 1:
            requests.append(
                {
                    "addDimensionGroup": {
                        "range": {
                            "sheetId": sheet_id_val,
                            "dimension": "ROWS",
                            "startIndex": i + 1,
                            "endIndex": k,
                        }
                    }
                }
            )
        i = k
    return requests


def compute_row_groups(
    df: pd.DataFrame, sheet_id_val: int, group_mode: str
) -> List[dict]:
    if group_mode == "none" or "Level" not in df.columns:
        return []
    df2 = df.fillna("").reset_index(drop=True)
    data = [df2.columns.tolist()] + df2.astype(str).values.tolist()
    level_col = df2.columns.get_loc("Level") if "Level" in df2.columns else 0
    if group_mode == "month_medium_source":
        return build_row_groups_month_medium_source(data, sheet_id_val, level_col)
    if group_mode == "parent_child":
        # infer parent/child from data
        levels = df2["Level"].astype(str).str.strip().unique().tolist()
        if "Month" in levels and "Project" in levels:
            return build_row_groups_parent_child(df2, sheet_id_val, "Month", "Project")
        if "Funnel" in levels and "Stage" in levels:
            return build_row_groups_parent_child(df2, sheet_id_val, "Funnel", "Stage")
        if "Event" in levels and "Course" in levels:
            return build_row_groups_parent_child(df2, sheet_id_val, "Event", "Course")
        if len(levels) >= 2:
            parent, child = levels[0], levels[1]
            return build_row_groups_parent_child(df2, sheet_id_val, parent, child)
        return []
    if group_mode == "month_source":
        return build_row_groups_month_source(data, sheet_id_val, level_col)
    return []


def push_report(
    sh,
    sheet_key: str,
    spec: ReportSpec,
    dry_run: bool,
) -> dict:
    path = REPORTS_SLICES / spec.rel_path
    result = {
        "report_id": spec.report_id,
        "path": str(path),
        "worksheet": spec.worksheet,
        "ok": False,
        "rows": 0,
        "groups": 0,
        "error": None,
    }
    if not path.exists():
        result["error"] = "file not found"
        print(f"[skip] {spec.report_id}: missing {path}")
        return result
    df = pd.read_csv(path, encoding="utf-8", low_memory=False)
    try:
        out_df = spec.builder(df)
    except Exception as e:
        result["error"] = f"builder: {e}"
        print(f"[err] {spec.report_id}: {e}")
        return result
    if dry_run:
        result["ok"] = True
        result["rows"] = len(out_df) + 1
        result["groups"] = len(compute_row_groups(out_df, 1, spec.group_mode))
        print(
            f"[dry-run] {spec.report_id}: {len(out_df)} data rows -> tab {spec.worksheet!r}"
        )
        return result
    ws = ensure_worksheet(
        sh, spec.worksheet, len(out_df) + 10, max(10, len(out_df.columns))
    )
    n_rows, _ = write_dataframe(ws, out_df)
    groups = compute_row_groups(out_df, int(ws.id), spec.group_mode)
    apply_requests(sh, groups)
    result["ok"] = True
    result["rows"] = n_rows
    result["groups"] = len(groups)
    print(
        f"[ok] {spec.report_id}: worksheet={spec.worksheet!r} rows={n_rows} groups={len(groups)}"
    )
    try:
        log_sync(sheet_key, spec.worksheet, n_rows)
    except Exception as e:
        print(f"  (log_sync skipped: {e})")
    return result


def parse_args(argv: Optional[Sequence[str]] = None):
    p = argparse.ArgumentParser(description="Push reports/slices CSVs to Google Sheets.")
    p.add_argument(
        "--credentials",
        default=None,
        help="Service account JSON path (else GOOGLE_APPLICATION_CREDENTIALS).",
    )
    p.add_argument(
        "--sheet-id",
        default=None,
        help="Spreadsheet URL or ID (else GOOGLE_SHEET_ID or bitrix_notebook).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Load and build hierarchies only; do not call APIs.",
    )
    p.add_argument(
        "--only",
        default=None,
        help="Comma-separated report_ids (e.g. bitrix_month,yandex_raw_vs_dedup).",
    )
    p.add_argument(
        "--list",
        action="store_true",
        help="Print report ids and exit.",
    )
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    specs = all_report_specs()
    if args.list:
        for s in specs:
            print(f"{s.report_id}\t{s.rel_path}\t{s.worksheet}")
        return 0
    only_set: Optional[set] = None
    if args.only:
        only_set = {x.strip() for x in args.only.split(",") if x.strip()}
        specs = [s for s in specs if s.report_id in only_set]

    if args.dry_run:
        print("Dry run: validating files and builders...")
        for spec in specs:
            path = REPORTS_SLICES / spec.rel_path
            if not path.exists():
                print(f"  MISSING {spec.report_id}: {path}")
                continue
            df = pd.read_csv(path, encoding="utf-8", low_memory=False)
            out = spec.builder(df)
            assert len(out) >= 0
            print(f"  OK {spec.report_id}: {len(df)} -> {len(out)} rows")
        return 0

    creds = resolve_credentials_path(args.credentials)
    if not creds:
        raise SystemExit(
            "Pass --credentials or set GOOGLE_APPLICATION_CREDENTIALS to a service account JSON."
        )
    sheet_ref = resolve_spreadsheet_id(args.sheet_id)
    sh, sheet_key = open_spreadsheet(creds, sheet_ref)

    print(f"Spreadsheet key: {sheet_key}")
    summary = []
    for spec in specs:
        summary.append(push_report(sh, sheet_key, spec, dry_run=False))

    ok = sum(1 for r in summary if r.get("ok"))
    fail = [r for r in summary if not r.get("ok")]
    print(f"\nDone: {ok}/{len(summary)} pushed.")
    if fail:
        for r in fail:
            print(f"  FAIL {r['report_id']}: {r.get('error')}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
