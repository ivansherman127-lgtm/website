from __future__ import annotations

import argparse
import json
import re
import socket
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "sheets" / "bitrix_contact_export.csv"
OUT_JSON = ROOT / "web" / "public" / "data" / "bitrix_contacts_uid.json"
OUT_CSV = ROOT / "bitrix_contacts_uid.csv"
OUT_REPORT = ROOT / "bitrix_contacts_uid_report.json"

TEST_TOKENS = ("test", "тест")
CYBERED_PATTERNS = ("cybered", "cyber ed", "cyber-ed")
BAD_NAME_TOKENS = ("пропущенный", "лид", "имя", "name", "unknown", "user")
BAD_PHONE_CHUNKS = ("123123", "999999", "000000", "111111", "222222", "333333", "444444", "555555", "666666", "777777", "888888")
SPLIT_RE = re.compile(r"[;,|/\n\t ]+")
VOWELS_RU = set("аеёиоуыэюя")
VOWELS_EN = set("aeiouy")


def _norm_phone(raw: str) -> str:
    digits = "".join(ch for ch in str(raw) if ch.isdigit())
    if not digits:
        return ""
    if len(digits) == 10:
        digits = "7" + digits
    elif len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    elif len(digits) != 11:
        return ""
    if digits[1:] == digits[1] * 10:
        return ""
    if any(chunk in digits for chunk in BAD_PHONE_CHUNKS):
        return ""
    # reject alternating repeated pairs like 989898..., 373737...
    tail = digits[1:]
    if len(tail) == 10 and tail[:2] * 5 == tail:
        return ""
    return f"+{digits}"


def _norm_email(raw: str) -> str:
    s = str(raw).strip().lower()
    s = s.replace("mailto:", "").strip(" .;,")
    if not s or s.count("@") != 1:
        return ""
    local, domain = s.split("@")
    if not local or "." not in domain:
        return ""
    return s


def _is_test_contact(row: pd.Series, name_cols: list[str], email_cols: list[str]) -> bool:
    name_text = " ".join(str(row.get(c, "")) for c in name_cols).lower()
    email_text = " ".join(str(row.get(c, "")) for c in email_cols).lower()
    text = f"{name_text} {email_text}"
    return any(tok in text for tok in TEST_TOKENS)


def _is_cybered_contact(row: pd.Series, name_cols: list[str], email_cols: list[str]) -> bool:
    name_text = " ".join(str(row.get(c, "")) for c in name_cols).lower()
    email_text = " ".join(str(row.get(c, "")) for c in email_cols).lower()
    text = f"{name_text} {email_text}"
    return any(tok in text for tok in CYBERED_PATTERNS)


def _is_bad_name(row: pd.Series, name_cols: list[str]) -> bool:
    tokens = []
    for c in name_cols:
        tokens.extend([t for t in SPLIT_RE.split(str(row.get(c, "")).strip()) if t])
    if not tokens:
        return True
    full_text = " ".join(tokens).lower()
    if any(tok in full_text for tok in BAD_NAME_TOKENS):
        return True

    # any token with digits
    if any(any(ch.isdigit() for ch in t) for t in tokens):
        return True

    # "name of one letter" -> first name token length 1
    first_name = str(row.get("Имя", "")).strip()
    if len(first_name) == 1:
        return True

    # token made only of consonants
    for t in tokens:
        letters = [ch.lower() for ch in t if ch.isalpha()]
        if not letters:
            continue
        if all((ch not in VOWELS_RU and ch not in VOWELS_EN) for ch in letters):
            return True
    return False


class DSU:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))
        self.rank = [0] * n

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
            self.parent[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1


def _domain_has_dns(domain: str) -> bool:
    try:
        socket.setdefaulttimeout(0.5)
        socket.getaddrinfo(domain, None)
        return True
    except Exception:
        return False


def run(*, require_dns_domain: bool = False) -> dict:
    df = pd.read_csv(SRC, dtype=str, low_memory=False, sep=";").fillna("")
    all_cols = list(df.columns)
    phone_cols = [c for c in all_cols if "телефон" in c.lower()]
    email_cols = [c for c in all_cols if "e-mail" in c.lower()]
    name_cols = [c for c in all_cols if c in {"Имя", "Фамилия", "Отчество"}]

    base_contacts = len(df)
    mask_test = df.apply(lambda r: _is_test_contact(r, name_cols, email_cols), axis=1)
    mask_cybered = df.apply(lambda r: _is_cybered_contact(r, name_cols, email_cols), axis=1)
    mask_bad_name = df.apply(lambda r: _is_bad_name(r, name_cols), axis=1)
    df = df.loc[~(mask_test | mask_cybered | mask_bad_name)].reset_index(drop=True)

    n = len(df)
    dsu = DSU(n)
    first_by_email: dict[str, int] = {}

    phones_per_row: list[set[str]] = []
    emails_per_row: list[set[str]] = []

    keep_indices: list[int] = []

    dns_cache: dict[str, bool] = {}
    removed_dns_only = 0

    for i, row in df.iterrows():
        pset: set[str] = set()
        for c in phone_cols:
            for token in SPLIT_RE.split(str(row.get(c, ""))):
                v = _norm_phone(token)
                if v:
                    pset.add(v)

        eset: set[str] = set()
        for c in email_cols:
            for token in SPLIT_RE.split(str(row.get(c, ""))):
                v = _norm_email(token)
                if v:
                    eset.add(v)

        phones_per_row.append(pset)
        emails_per_row.append(eset)

        if not pset and not eset:
            continue

        if require_dns_domain and eset:
            domains = {e.split("@", 1)[1] for e in eset}
            has_dns = False
            for d in domains:
                if d not in dns_cache:
                    dns_cache[d] = _domain_has_dns(d)
                if dns_cache[d]:
                    has_dns = True
            if not has_dns:
                removed_dns_only += 1
                continue

        keep_indices.append(i)
        # Deduplicate only by email (case-insensitive). Do not union by phone.
        for e in eset:
            if e in first_by_email:
                dsu.union(i, first_by_email[e])
            else:
                first_by_email[e] = i

    groups: dict[int, list[int]] = defaultdict(list)
    for i in keep_indices:
        groups[dsu.find(i)].append(i)

    records = []
    for idx, members in enumerate(groups.values(), start=1):
        contact_ids = sorted({str(df.iloc[i].get("ID", "")).strip() for i in members if str(df.iloc[i].get("ID", "")).strip()})
        names = sorted(
            {
                " ".join(
                    part for part in [str(df.iloc[i].get("Фамилия", "")).strip(), str(df.iloc[i].get("Имя", "")).strip(), str(df.iloc[i].get("Отчество", "")).strip()] if part
                )
                for i in members
            }
            - {""}
        )
        all_phones = sorted({p for i in members for p in phones_per_row[i]})
        all_emails = sorted({e for i in members for e in emails_per_row[i]})

        records.append(
            {
                "contact_uid": f"C{idx:06d}",
                "all_contact_ids": " | ".join(contact_ids),
                "all_names": " | ".join(names),
                "all_phones": " | ".join(all_phones),
                "all_emails": " | ".join(all_emails),
                "contact_ids_count": len(contact_ids),
                "names_count": len(names),
                "phones_count": len(all_phones),
                "emails_count": len(all_emails),
            }
        )

    out_df = pd.DataFrame(records).sort_values(["contact_ids_count", "phones_count", "emails_count"], ascending=[False, False, False]).reset_index(drop=True)
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out_df.to_dict(orient="records"), ensure_ascii=False), encoding="utf-8")
    out_df.to_csv(OUT_CSV, index=False, encoding="utf-8")

    report = {
        "source": str(SRC),
        "base_contacts": int(base_contacts),
        "removed_test_contacts": int(mask_test.sum()),
        "removed_cybered_contacts": int(mask_cybered.sum()),
        "removed_bad_name_contacts": int(mask_bad_name.sum()),
        "contacts_after_text_filters": int(n),
        "removed_without_phone_and_email": int(n - len(keep_indices)),
        "removed_email_without_dns_domain": int(removed_dns_only),
        "contacts_after_contactability_filter": int(len(keep_indices)),
        "unique_contacts_after_merge": int(len(out_df)),
        "require_dns_domain": bool(require_dns_domain),
        "phone_columns_used": phone_cols,
        "email_columns_used": email_cols,
    }
    OUT_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build unique contacts from Bitrix export")
    parser.add_argument(
        "--require-dns-domain",
        action="store_true",
        help="Keep contacts with email only if at least one email domain resolves via DNS",
    )
    args = parser.parse_args()
    print(json.dumps(run(require_dns_domain=args.require_dns_domain), ensure_ascii=False, indent=2))
