from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional

import pandas as pd


def resolve_credentials_path(path: Optional[str]) -> str:
    creds_path = path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        raise SystemExit("Pass --credentials or set GOOGLE_APPLICATION_CREDENTIALS")
    p = Path(creds_path).expanduser()
    if p.is_dir():
        jsons = list(p.glob("*.json"))
        if not jsons:
            raise SystemExit(f"No .json file in directory {p}")
        return str(jsons[0])
    if not p.exists():
        raise SystemExit(f"Credentials file not found: {p}")
    return str(p)


def sheet_key(ref: str) -> str:
    s = ref.strip()
    if "/" in s and ("docs.google.com" in s or s.startswith("http")):
        return s.split("/d/")[-1].split("/")[0].split("?")[0]
    return s


def open_spreadsheet(credentials_path: str, spreadsheet_id_or_url: str):
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    gc = gspread.authorize(creds)
    sid = spreadsheet_id_or_url.strip()
    key = sheet_key(sid)
    is_url = "/" in sid and ("docs.google.com" in sid or sid.startswith("http"))
    if is_url and hasattr(gc, "open_by_url"):
        return gc.open_by_url(sid), key
    return gc.open_by_key(key), key


def delete_worksheets_by_regex(sh, pattern: str, dry_run: bool = False) -> int:
    rx = re.compile(pattern)
    worksheets = sh.worksheets()
    to_delete = [ws for ws in worksheets if rx.search(ws.title)]
    for ws in to_delete:
        if dry_run:
            print(f"[dry-run] delete worksheet: {ws.title}")
        else:
            sh.del_worksheet(ws)
            print(f"[ok] deleted worksheet: {ws.title}")
    return len(to_delete)


def push_dataframe(sh, worksheet_name: str, df: pd.DataFrame) -> None:
    import gspread

    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(
            title=worksheet_name[:100],
            rows=max(1000, len(df) + 10),
            cols=max(26, len(df.columns)),
        )
    data = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    ws.clear()
    ws.update(data, range_name="A1", value_input_option="USER_ENTERED")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Google Sheets ops: delete tabs and push table")
    p.add_argument("--credentials", default=None, help="Service account json path")
    p.add_argument(
        "--sheet-id",
        default=os.environ.get(
            "GOOGLE_SHEET_ID",
            "https://docs.google.com/spreadsheets/d/1Q5KmqXUOVe9hVXpyJ1m1yUqPEaQ_2KXjoH71DHiPrLU/edit",
        ),
        help="Spreadsheet URL or key",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("delete-regex", help="Delete worksheets matching regex")
    d.add_argument("--pattern", required=True, help="Regex for worksheet title")
    d.add_argument("--dry-run", action="store_true")

    pj = sub.add_parser("push-json", help="Push a JSON rows payload to worksheet")
    pj.add_argument("--worksheet", required=True, help="Target worksheet name")
    pj.add_argument("--json-file", required=True, help="JSON file with array of objects")

    pc = sub.add_parser("push-csv", help="Push CSV file to worksheet")
    pc.add_argument("--worksheet", required=True, help="Target worksheet name")
    pc.add_argument("--csv-file", required=True, help="CSV file path")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    creds = resolve_credentials_path(args.credentials)
    sh, _ = open_spreadsheet(creds, args.sheet_id)

    if args.cmd == "delete-regex":
        n = delete_worksheets_by_regex(sh, args.pattern, dry_run=args.dry_run)
        print(f"matched={n}")
        return 0

    if args.cmd == "push-json":
        rows = json.loads(Path(args.json_file).read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            raise SystemExit("JSON must be an array of objects")
        df = pd.DataFrame(rows)
        push_dataframe(sh, args.worksheet, df)
        print(f"[ok] pushed {len(df)} rows to {args.worksheet}")
        return 0

    if args.cmd == "push-csv":
        df = pd.read_csv(args.csv_file, encoding="utf-8", low_memory=False)
        push_dataframe(sh, args.worksheet, df)
        print(f"[ok] pushed {len(df)} rows to {args.worksheet}")
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
