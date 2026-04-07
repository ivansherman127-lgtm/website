"""
Build a unique contacts table from Bitrix deals export.

Merge rule:
- Rows are merged into one contact_uid only if they share at least one
  normalized email address.
- Phone numbers are collected and stored for display but do not trigger merges.
- "Контакт: ID" is preserved as metadata and does not constrain merges.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Set

import pandas as pd

from event_classifier import classify_event_from_row


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = PROJECT_ROOT / "DEAL_20260315_aa9ba8d2_69b70d4041fcb.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "bitrix_contacts_uid.csv"
DEFAULT_REPORT = PROJECT_ROOT / "bitrix_contacts_uid_report.json"


EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
PHONE_DIGITS_RE = re.compile(r"\d")
SPLIT_RE = re.compile(r"[;,|]+")


def _cell_to_str(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    s = str(value).strip()
    return "" if s.lower() in {"", "nan", "none", "null"} else s


def _split_values(raw: str) -> List[str]:
    if not raw:
        return []
    parts = [p.strip() for p in SPLIT_RE.split(raw) if p.strip()]
    return parts if parts else [raw.strip()]


def normalize_email(value: str) -> str:
    v = (value or "").strip().lower()
    if not v:
        return ""
    # Pick the first email match from noisy fields.
    m = EMAIL_RE.search(v)
    return m.group(0) if m else ""


def normalize_phone(value: str) -> str:
    if not value:
        return ""
    digits = "".join(PHONE_DIGITS_RE.findall(value))
    if not digits:
        return ""
    # Common RU normalization heuristics.
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    # Keep realistic phone lengths only.
    if not (10 <= len(digits) <= 15):
        return ""
    if is_placeholder_phone(digits):
        return ""
    return digits


def is_placeholder_phone(digits: str) -> bool:
    """Heuristics for fake/test/placeholder phone values."""
    if not digits:
        return True

    # Almost all chars equal: 9999999999, 11111111111, etc.
    if len(set(digits)) <= 2 and max(digits.count(ch) for ch in set(digits)) >= len(digits) - 1:
        return True

    # Common test fragments in local datasets.
    bad_chunks = ("123123", "123456", "999999", "000000", "111111", "222222", "333333", "444444", "555555", "666666", "777777", "888888")
    if any(chunk in digits for chunk in bad_chunks):
        return True

    # Repeated short pattern: 1212121212, 9090909090, etc.
    for size in (2, 3, 4):
        if len(digits) % size == 0:
            pat = digits[:size]
            if pat * (len(digits) // size) == digits:
                return True

    # Sequential runs (ascending or descending), e.g. 1234567890 / 9876543210.
    asc = "0123456789012345"
    desc = asc[::-1]
    if digits in asc or digits in desc:
        return True

    return False


def normalize_name(value: str) -> str:
    return " ".join((value or "").strip().split())


def normalize_contact_id(value: str) -> str:
    v = normalize_name(value)
    if not v:
        return ""
    # Convert float-like IDs from CSV inference (e.g. "109067.0") to "109067".
    if re.fullmatch(r"\d+\.0+", v):
        return v.split(".", 1)[0]
    return v


class UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1


@dataclass
class RowPayload:
    deal_id: str
    names: Set[str]
    phones: Set[str]
    emails: Set[str]
    contact_ids: Set[str]
    deal_date: str   # ISO "YYYY-MM-DD" or empty
    event_class: str  # from classify_event_from_row


def _collect_columns(df: pd.DataFrame) -> Dict[str, List[str]]:
    contact_cols = [c for c in df.columns if c == "Контакт" or c.startswith("Контакт:")]
    phone_cols = [c for c in contact_cols if "телефон" in c.lower()]
    email_cols = [c for c in contact_cols if "e-mail" in c.lower() or "email" in c.lower()]
    name_cols = [c for c in contact_cols if c in {"Контакт", "Контакт: Имя", "Контакт: Фамилия", "Контакт: Отчество"}]
    contact_id_cols = [c for c in contact_cols if c == "Контакт: ID"]
    return {
        "contact_cols": contact_cols,
        "phone_cols": phone_cols,
        "email_cols": email_cols,
        "name_cols": name_cols,
        "contact_id_cols": contact_id_cols,
    }


def _extract_row_payload(row: pd.Series, cols: Dict[str, List[str]]) -> RowPayload:
    names: Set[str] = set()
    phones: Set[str] = set()
    emails: Set[str] = set()
    contact_ids: Set[str] = set()

    for col in cols["name_cols"]:
        val = normalize_name(_cell_to_str(row.get(col, "")))
        if val:
            names.add(val)
    # Build full-name candidate from split fields.
    fn = normalize_name(_cell_to_str(row.get("Контакт: Фамилия", "")))
    nm = normalize_name(_cell_to_str(row.get("Контакт: Имя", "")))
    mn = normalize_name(_cell_to_str(row.get("Контакт: Отчество", "")))
    full = normalize_name(" ".join([x for x in [fn, nm, mn] if x]))
    if full:
        names.add(full)

    for col in cols["phone_cols"]:
        raw = _cell_to_str(row.get(col, ""))
        for part in _split_values(raw):
            p = normalize_phone(part)
            if p:
                phones.add(p)

    for col in cols["email_cols"]:
        raw = _cell_to_str(row.get(col, ""))
        for part in _split_values(raw):
            e = normalize_email(part)
            if e:
                emails.add(e)

    for col in cols["contact_id_cols"]:
        raw = normalize_name(_cell_to_str(row.get(col, "")))
        if raw:
            for part in _split_values(raw):
                cid = normalize_contact_id(part)
                if cid and cid.lower() != "nan":
                    contact_ids.add(cid)

    deal_id = normalize_name(_cell_to_str(row.get("ID", "")))

    raw_date = _cell_to_str(row.get("Дата создания", ""))
    deal_date = ""
    if raw_date:
        try:
            dt = pd.to_datetime(raw_date, dayfirst=True, errors="coerce")
            if pd.notna(dt):
                deal_date = dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    event_class = classify_event_from_row(row.to_dict()).event

    return RowPayload(
        deal_id=deal_id,
        names=names,
        phones=phones,
        emails=emails,
        contact_ids=contact_ids,
        deal_date=deal_date,
        event_class=event_class,
    )


def _join_sorted(values: Iterable[str]) -> str:
    uniq = sorted({v for v in values if v})
    return " | ".join(uniq)


def build_contacts_uid_table(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, int]]:
    if "ID" not in df.columns:
        raise ValueError("Input CSV must contain 'ID' column.")

    cols = _collect_columns(df)
    if not cols["contact_cols"]:
        raise ValueError("Input CSV does not contain columns starting with 'Контакт'.")

    rows_payload: List[RowPayload] = []
    dropped_placeholder_phones = 0
    for _, row in df.iterrows():
        payload = _extract_row_payload(row, cols)
        # Count how many raw phone-like tokens were dropped by normalization.
        raw_phone_tokens = 0
        for col in cols["phone_cols"]:
            raw = _cell_to_str(row.get(col, ""))
            raw_phone_tokens += len([p for p in _split_values(raw) if p.strip()])
        dropped_placeholder_phones += max(0, raw_phone_tokens - len(payload.phones))
        rows_payload.append(payload)

    uf = UnionFind(len(rows_payload))
    token_owner: Dict[str, int] = {}

    # Merge only by shared email token (phone is kept for display but not used for dedup).
    for idx, payload in enumerate(rows_payload):
        tokens = {f"e:{e}" for e in payload.emails}
        for token in tokens:
            prev = token_owner.get(token)
            if prev is None:
                token_owner[token] = idx
            else:
                uf.union(prev, idx)

    groups: Dict[int, List[int]] = {}
    for i in range(len(rows_payload)):
        root = uf.find(i)
        groups.setdefault(root, []).append(i)

    records = []
    for seq, (_, idxs) in enumerate(sorted(groups.items(), key=lambda kv: min(kv[1])), start=1):
        uid = f"C{seq:06d}"
        all_names: Set[str] = set()
        all_phones: Set[str] = set()
        all_emails: Set[str] = set()
        all_contact_ids: Set[str] = set()
        all_deal_ids: Set[str] = set()
        dated_events: list[tuple[str, str]] = []
        for i in idxs:
            payload = rows_payload[i]
            all_names |= payload.names
            all_phones |= payload.phones
            all_emails |= payload.emails
            all_contact_ids |= payload.contact_ids
            if payload.deal_id:
                all_deal_ids.add(payload.deal_id)
            dated_events.append((payload.deal_date or "9999", payload.event_class))
        dated_events.sort(key=lambda x: x[0])
        first_deal_date = dated_events[0][0] if dated_events and dated_events[0][0] != "9999" else ""
        first_touch_event = dated_events[0][1] if dated_events else "Другое"
        all_events = _join_sorted({ev for _, ev in dated_events})
        records.append(
            {
                "contact_uid": uid,
                "all_contact_ids": _join_sorted(all_contact_ids),
                "all_names": _join_sorted(all_names),
                "all_phones": _join_sorted(all_phones),
                "all_emails": _join_sorted(all_emails),
                "all_deal_ids": _join_sorted(all_deal_ids),
                "first_deal_date": first_deal_date,
                "first_touch_event": first_touch_event,
                "all_events": all_events,
                "deals_count": len(all_deal_ids),
                "contact_ids_count": len(all_contact_ids),
                "names_count": len(all_names),
                "phones_count": len(all_phones),
                "emails_count": len(all_emails),
            }
        )

    out = pd.DataFrame(records).sort_values(["deals_count", "contact_uid"], ascending=[False, True]).reset_index(drop=True)

    missing_tokens = 0
    for payload in rows_payload:
        if not payload.phones and not payload.emails:
            missing_tokens += 1

    qa = {
        "input_rows": len(df),
        "unique_contact_uid": len(out),
        "rows_without_phone_and_email": missing_tokens,
        "rows_without_phone_and_email_pct": round((missing_tokens / len(df)) * 100, 2) if len(df) else 0,
        "columns_contact_total": len(cols["contact_cols"]),
        "columns_phone_total": len(cols["phone_cols"]),
        "columns_email_total": len(cols["email_cols"]),
        "dropped_placeholder_or_invalid_phones": dropped_placeholder_phones,
    }
    return out, qa


def run(input_csv: Path, output_csv: Path, report_json: Path) -> None:
    df = pd.read_csv(
        input_csv,
        sep=";",
        encoding="utf-8",
        low_memory=False,
        dtype={"ID": str},
    )
    out, qa = build_contacts_uid_table(df)
    out.to_csv(output_csv, index=False, encoding="utf-8")

    top_groups = out[["contact_uid", "deals_count", "all_names", "all_phones", "all_emails"]].head(20).to_dict(orient="records")
    report = {"qa": qa, "top_groups_preview": top_groups}
    report_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Saved contacts table:", output_csv)
    print("Saved QA report:", report_json)
    print(json.dumps(qa, ensure_ascii=False, indent=2))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build contact_uid table from Bitrix CSV export.")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    p.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.input, args.output, args.report)
