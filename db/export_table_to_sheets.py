from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any


def _load_payload(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"json file not found: {path}")
    payload = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit("payload must be an object")
    return payload


def _resolve_creds(path: str | None) -> str:
    creds = path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not creds:
        raise SystemExit("missing credentials: pass --credentials or set GOOGLE_APPLICATION_CREDENTIALS")
    p = Path(creds).expanduser()
    if not p.exists():
        raise SystemExit(f"credentials file not found: {p}")
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(
            "credentials must be a valid Google service-account JSON file"
        ) from exc
    required = {"type", "client_email", "private_key"}
    if not isinstance(data, dict) or not required.issubset(set(data.keys())):
        raise SystemExit(
            "credentials file is not a Google service-account key (missing type/client_email/private_key)"
        )
    if str(data.get("type", "")).strip() != "service_account":
        raise SystemExit("credentials type must be 'service_account'")
    return str(p)


def _build_row_groups(rows: list[list[str]], sheet_id: int) -> list[dict[str, Any]]:
    # Export format starts with control column "#": "+" / "−" on parent rows.
    # Child rows are encoded in the first data column with leading ">" markers.
    if not rows:
        return []
    reqs: list[dict[str, Any]] = []
    data_col = 1 if rows and rows[0] and rows[0][0] == "#" else 0
    n = len(rows)
    i = 1
    while i < n:
        ctrl = str(rows[i][0]).strip() if rows[i] else ""
        if ctrl not in {"+", "−"}:
            i += 1
            continue
        k = i + 1
        while k < n:
            first_data = str(rows[k][data_col]).strip() if data_col < len(rows[k]) else ""
            if first_data.startswith(">"):
                k += 1
                continue
            break
        if k > i + 1:
            reqs.append(
                {
                    "addDimensionGroup": {
                        "range": {
                            "sheetId": int(sheet_id),
                            "dimension": "ROWS",
                            "startIndex": i + 1,
                            "endIndex": k + 1,
                        }
                    }
                }
            )
        i = k
    return reqs


def main() -> int:
    ap = argparse.ArgumentParser(description="Create Google Sheet from JSON table and share it.")
    ap.add_argument("--json-file", required=True)
    ap.add_argument("--gmail", required=True)
    ap.add_argument("--title", required=True)
    ap.add_argument("--credentials", default=None)
    args = ap.parse_args()

    gmail = args.gmail.strip()
    if not re.match(r"^[^\s@]+@gmail\.com$", gmail, re.IGNORECASE):
        raise SystemExit("gmail must be a valid @gmail.com address")

    payload = _load_payload(args.json_file)
    headers = payload.get("headers")
    rows = payload.get("rows")
    if not isinstance(headers, list) or not all(isinstance(x, str) for x in headers):
        raise SystemExit("headers must be an array of strings")
    if not isinstance(rows, list):
        raise SystemExit("rows must be an array")

    table_rows: list[list[str]] = []
    for r in rows:
        if not isinstance(r, list):
            raise SystemExit("every row must be an array")
        table_rows.append([str(v if v is not None else "") for v in r])

    creds_path = _resolve_creds(args.credentials)

    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    gc = gspread.authorize(creds)

    target_sheet = os.environ.get("GOOGLE_EXPORT_SHEET_ID", "").strip()
    if target_sheet:
        key = target_sheet
        if "/d/" in target_sheet:
            key = target_sheet.split("/d/")[-1].split("/")[0].split("?")[0]
        sh = gc.open_by_key(key)
        ws = sh.add_worksheet(
            title=args.title[:100],
            rows=max(1000, len(table_rows) + 50),
            cols=max(26, len(headers) + 2),
        )
    else:
        sh = gc.create(args.title[:100])
        ws = sh.sheet1
    ws.update([headers] + table_rows, range_name="A1", value_input_option="USER_ENTERED")

    group_reqs = _build_row_groups([headers] + table_rows, int(ws.id))
    if group_reqs:
        sh.batch_update({"requests": group_reqs})

    sh.share(gmail, perm_type="user", role="writer", notify=True)
    print(json.dumps({"ok": True, "url": f"https://docs.google.com/spreadsheets/d/{sh.id}/edit#gid={ws.id}"}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

