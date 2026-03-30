"""
Read-only Google Sheets: load a worksheet into a DataFrame, optional email-task filter, CSV snapshots.

  python -m db.fetch_sheet_tab --sheet-url URL --worksheet Задачи --out-dir reports/sheets

Environment: GOOGLE_APPLICATION_CREDENTIALS or --credentials path.
"""
from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import pandas as pd
import gspread

DEFAULT_EMAIL_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1Q5KmqXUOVe9hVXpyJ1m1yUqPEaQ_2KXjoH71DHiPrLU/edit"
)


def parse_spreadsheet_key(sheet_id_or_url: str) -> Tuple[str, bool]:
    s = (sheet_id_or_url or "").strip()
    is_url = "/" in s and ("docs.google.com" in s or s.startswith("http"))
    if is_url:
        key = s.split("/d/")[-1].split("/")[0].split("?")[0]
    else:
        key = s
    return key, is_url


def resolve_credentials_path(credentials_path: Optional[str]) -> Optional[str]:
    creds_path = credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        return None
    p = Path(creds_path)
    if p.is_dir():
        jsons = list(p.glob("*.json"))
        if not jsons:
            raise FileNotFoundError(f"No .json file in directory {creds_path}")
        return str(jsons[0])
    if "*" in creds_path:
        import glob

        jsons = glob.glob(creds_path)
        if not jsons:
            raise FileNotFoundError(f"No file matching {creds_path}")
        return jsons[0]
    if not p.exists():
        raise FileNotFoundError(f"Credentials file not found: {creds_path}")
    return str(creds_path)


def authorize_gspread(credentials_path: Optional[str]):
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    path = resolve_credentials_path(credentials_path)
    if not path:
        raise RuntimeError(
            "Set GOOGLE_APPLICATION_CREDENTIALS or pass --credentials path to service account JSON"
        )
    creds = Credentials.from_service_account_file(path, scopes=scopes)
    return gspread.authorize(creds)


def open_spreadsheet(gc, sheet_id_or_url: str):
    key, is_url = parse_spreadsheet_key(sheet_id_or_url)
    if is_url and hasattr(gc, "open_by_url"):
        return gc.open_by_url(sheet_id_or_url.strip())
    return gc.open_by_key(key)


def values_to_dataframe(rows: List[List[str]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    header = rows[0]
    body = rows[1:]
    if not header or all(str(c).strip() == "" for c in header):
        ncols = max((len(r) for r in rows), default=0)
        header = [f"col_{i}" for i in range(ncols)]
        body = rows
    # pad ragged rows
    width = len(header)
    norm: List[List[str]] = []
    for r in body:
        rr = list(r) + [""] * (width - len(r))
        norm.append(rr[:width])
    return pd.DataFrame(norm, columns=header[:width])


# Default: email-channel wording only (avoids matching generic «лид» / Yandex tasks).
STRICT_EMAIL_KEYWORDS: Tuple[str, ...] = (
    "email",
    "e-mail",
    "имейл",
    "mail",
    "mailing",
    "рассыл",
    "sendsay",
    "send say",
    "first line",
    "firstline",
    "utm",
    "массов",
    "почт",
    "ctor",
    "доставлен",
    "отпис",
    "unsub",
    "ads",
)

RELAXED_EMAIL_EXTRA: Tuple[str, ...] = (
    "лид",
    "открыт",
    "кибер",
    "выпуск",
)

DEFAULT_EMAIL_KEYWORDS = STRICT_EMAIL_KEYWORDS

CHANNEL_COLUMN_HINTS = (
    "направление",
    "канал",
    "тег",
    "тип",
    "категория",
)


def _channel_match_column_indices(df: pd.DataFrame) -> List[int]:
    matches: List[int] = []
    for i, c in enumerate(df.columns):
        low = re.sub(r"\s+", " ", str(c).strip().lower())
        for hint in CHANNEL_COLUMN_HINTS:
            if hint in low:
                matches.append(i)
                break
    return matches


def row_text_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=str)
    # axis=1 avoids duplicate column-name issues (empty headers repeat).
    return (
        df.astype(str)
        .replace({"nan": "", "None": ""})
        .apply(lambda r: " ".join(x for x in r if str(x).strip()).lower(), axis=1)
    )


def filter_email_related_rows(
    df: pd.DataFrame,
    keywords: Sequence[str] = DEFAULT_EMAIL_KEYWORDS,
    *,
    extra_channel_substrings: Sequence[str] = ("почт", "email", "рассылк"),
) -> pd.Series:
    """Boolean mask: True if row is email-related (keyword or channel column match)."""
    blob = row_text_series(df)
    pat = "|".join(re.escape(k.lower()) for k in keywords if k.strip())
    mask_kw = blob.str.contains(pat, case=False, na=False) if pat else pd.Series(False, index=df.index)

    mask_ch = pd.Series(False, index=df.index)
    for i in _channel_match_column_indices(df):
        s = df.iloc[:, i].astype(str).str.lower()
        for sub in extra_channel_substrings:
            mask_ch = mask_ch | s.str.contains(re.escape(sub), case=False, na=False)

    return mask_kw | mask_ch


DEFAULT_WORKSHEET_FALLBACKS = ("Copy of Задачи",)


def load_worksheet_dataframe(
    sheet_id_or_url: str,
    worksheet_name: str,
    *,
    credentials_path: Optional[str] = None,
    worksheet_fallbacks: Sequence[str] = DEFAULT_WORKSHEET_FALLBACKS,
) -> Tuple[pd.DataFrame, str]:
    """Returns (dataframe, actual_worksheet_title)."""
    gc = authorize_gspread(credentials_path)
    sh = open_spreadsheet(gc, sheet_id_or_url)
    names_to_try = (worksheet_name,) + tuple(worksheet_fallbacks)
    title_used: Optional[str] = None
    ws = None
    for title in names_to_try:
        try:
            ws = sh.worksheet(title)
            title_used = title
            break
        except gspread.exceptions.WorksheetNotFound:
            continue
    if ws is None:
        names = [w.title for w in sh.worksheets()]
        raise RuntimeError(
            f"Worksheet not found (tried {list(names_to_try)!r}). Available: {names}"
        )
    rows = ws.get_all_values()
    return values_to_dataframe(rows), title_used or worksheet_name


def load_keywords_file(path: Path) -> Tuple[str, ...]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return tuple(
        ln.strip()
        for ln in lines
        if ln.strip() and not ln.strip().startswith("#")
    )


def write_backlog_markdown(
    path: Path,
    filtered: pd.DataFrame,
    *,
    sheet_url: str,
    worksheet: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        "# Email-related tasks (from Задачи)",
        "",
        f"- Source: `{sheet_url}` worksheet `{worksheet}`",
        f"- Generated: `{ts}`",
        f"- Rows: **{len(filtered)}**",
        "",
        "Status legend: `pending` | `in_progress` | `done` | `blocked`",
        "",
    ]
    for i, (_, row) in enumerate(filtered.reset_index(drop=True).iterrows(), start=1):
        preview = " | ".join(
            str(row[c])[:120] for c in filtered.columns[:4] if pd.notna(row[c]) and str(row[c]).strip()
        )
        lines.append(f"{i}. [pending] — {preview}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch Google Sheet worksheet to CSV (+ email filter).")
    ap.add_argument(
        "--sheet-url",
        default=os.environ.get("GOOGLE_SHEET_ID", DEFAULT_EMAIL_SHEET_URL),
        help="Spreadsheet URL or key (default: main marketing sheet or GOOGLE_SHEET_ID)",
    )
    ap.add_argument("--worksheet", default="Задачи", help="Worksheet title")
    ap.add_argument("--credentials", default=None, help="Service account JSON path")
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=Path("reports/sheets"),
        help="Directory for zadachi_full.csv and zadachi_email.csv",
    )
    ap.add_argument(
        "--keywords-file",
        type=Path,
        default=None,
        help="One extra keyword per line (# comments allowed)",
    )
    ap.add_argument(
        "--list-all",
        action="store_true",
        help="Skip email filter; only write zadachi_full.csv",
    )
    ap.add_argument(
        "--no-backlog-md",
        action="store_true",
        help="Do not write zadachi_email_backlog.md",
    )
    ap.add_argument(
        "--relaxed",
        action="store_true",
        help="Also match generic tokens (лид, открыт, кибер, выпуск) — noisier list",
    )
    args = ap.parse_args()

    kw: dict = {}
    base_kw = (
        tuple(STRICT_EMAIL_KEYWORDS) + tuple(RELAXED_EMAIL_EXTRA)
        if args.relaxed
        else tuple(STRICT_EMAIL_KEYWORDS)
    )
    if args.keywords_file:
        extra = load_keywords_file(args.keywords_file)
        kw["keywords"] = base_kw + tuple(extra)
    else:
        kw["keywords"] = base_kw

    df, ws_title = load_worksheet_dataframe(
        args.sheet_url,
        args.worksheet,
        credentials_path=args.credentials,
    )
    print(f"Loaded worksheet: {ws_title!r} ({len(df)} rows)")
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    full_path = out_dir / "zadachi_full.csv"
    df.to_csv(full_path, index=False)
    print(f"Wrote {full_path} ({len(df)} rows)")

    if args.list_all:
        print("Skipped filter (--list-all).")
        return

    mask = filter_email_related_rows(df, **kw)
    email_df = df.loc[mask].reset_index(drop=True)
    email_path = out_dir / "zadachi_email.csv"
    email_df.to_csv(email_path, index=False)
    print(f"Wrote {email_path} ({len(email_df)} email-related rows)")

    if not args.no_backlog_md:
        md_path = out_dir / "zadachi_email_backlog.md"
        write_backlog_markdown(
            md_path,
            email_df,
            sheet_url=args.sheet_url,
            worksheet=ws_title,
        )
        print(f"Wrote {md_path}")


if __name__ == "__main__":
    main()
