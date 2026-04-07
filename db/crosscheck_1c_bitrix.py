from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional

import pandas as pd

from event_classifier import classify_event_from_row
from utils import _n, _id, _amt

ROOT = Path(__file__).resolve().parent.parent
ONEC_PATH = ROOT / "sheets" / "1c.csv"
BITRIX_PATH = ROOT / "sheets" / "bitrix_19.03.26"
OUT_DIR = ROOT / "reports" / "slices" / "qa"


def _date(v: object) -> Optional[pd.Timestamp]:
    s = _n(v)
    if not s:
        return None
    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    return dt if pd.notna(dt) else None


def _norm_name(s: object) -> str:
    x = _n(s).lower()
    x = re.sub(r"[\n\r\t]+", " ", x)
    x = re.sub(r"[^0-9a-zа-яё ]+", " ", x, flags=re.IGNORECASE)
    x = re.sub(r"\b(основной договор|без договора|договор|операция по платежной карте|поступление|оплата)\b", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x


def _tokenize(text: str) -> set[str]:
    # Tokens likely to be external identifiers/order refs.
    out: set[str] = set()
    for m in re.finditer(r"[A-Za-zА-Яа-я0-9]{2,}[-_/][A-Za-zА-Яа-я0-9]{2,}", text):
        out.add(m.group(0).lower())
    for m in re.finditer(r"\b\d{5,}\b", text):
        out.add(m.group(0))
    return out


def _name_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _is_aj_deal(row: pd.Series) -> bool:
    return classify_event_from_row(row.to_dict()).event == "Attacking January"


def read_1c(path: Path) -> pd.DataFrame:
    rows: list[list[str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.reader(f, delimiter=",", quotechar='"')
        rows = [r for r in rdr]

    header_idx = None
    for i, r in enumerate(rows):
        if len(r) >= 4 and _n(r[0]).lower() == "период" and "документ" in _n(r[1]).lower():
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("1C header row not found")

    header = rows[header_idx]
    data = rows[header_idx + 1 :]
    # skip second service/header row
    if data and _n(data[0][0]) == "" and "счет" in " ".join(_n(x).lower() for x in data[0]):
        data = data[1:]

    width = len(header)
    norm_data = [(r + [""] * (width - len(r)))[:width] for r in data]
    df = pd.DataFrame(norm_data, columns=header)
    # keep rows with date-looking period
    df["period_dt"] = df["Период"].map(_date)
    df = df[df["period_dt"].notna()].copy()

    # parse amount (debit/credit columns are messy in source)
    debit = df.get("Дебет", "").map(_amt)
    credit = df.get("Кредит", "").map(_amt)
    df["amount"] = debit.where(debit > 0, credit)
    df["amount"] = df["amount"].fillna(0.0)

    # client name from Аналитика Кт first meaningful line
    def client_from_kt(v: object) -> str:
        lines = [ln.strip() for ln in _n(v).splitlines() if ln.strip()]
        for ln in lines:
            if "договор" in ln.lower() or "операция" in ln.lower():
                continue
            return ln
        return lines[0] if lines else ""

    df["client_name_raw"] = df.get("Аналитика Кт", "").map(client_from_kt)
    df["client_name_norm"] = df["client_name_raw"].map(_norm_name)
    text_blob = (
        df.get("Документ", "").astype(str)
        + " "
        + df.get("Аналитика Дт", "").astype(str)
        + " "
        + df.get("Аналитика Кт", "").astype(str)
    )
    df["tokens"] = text_blob.map(lambda x: sorted(_tokenize(_n(x))))
    return df.reset_index(drop=True)


def read_bitrix(path: Path) -> pd.DataFrame:
    d = pd.read_csv(path, sep=";", encoding="utf-8", low_memory=False, dtype=str)
    d["ID"] = d["ID"].map(_id)
    d["Контакт: ID"] = d.get("Контакт: ID", "").map(_id)
    d = d[d["ID"] != ""].drop_duplicates("ID", keep="last").copy()
    d["pay_dt"] = d.get("Дата оплаты", "").map(_date)
    d["create_dt"] = d.get("Дата создания", "").map(_date)
    d["match_dt"] = d["pay_dt"].where(d["pay_dt"].notna(), d["create_dt"])
    d["bitrix_amount"] = d.get("Сумма", "").map(_amt)

    contact_name = (
        d.get("Контакт: Фамилия", "").fillna("").astype(str).str.strip()
        + " "
        + d.get("Контакт: Имя", "").fillna("").astype(str).str.strip()
        + " "
        + d.get("Контакт: Отчество", "").fillna("").astype(str).str.strip()
    ).str.replace(r"\s+", " ", regex=True).str.strip()
    d["client_name_raw"] = d.get("Контакт", "").fillna("")
    d["client_name_raw"] = d["client_name_raw"].mask(d["client_name_raw"].astype(str).str.strip().eq(""), contact_name)
    d["client_name_raw"] = d["client_name_raw"].mask(d["client_name_raw"].astype(str).str.strip().eq(""), d.get("Компания", "").fillna(""))
    d["client_name_norm"] = d["client_name_raw"].map(_norm_name)

    id_blob = (
        d.get("ID", "").astype(str)
        + " "
        + d.get("Контакт: ID", "").astype(str)
        + " "
        + d.get("PRM_LEAD_ID", "").astype(str)
        + " "
        + d.get("Название сделки", "").astype(str)
        + " "
        + d.get("Комментарий", "").astype(str)
    )
    d["tokens"] = id_blob.map(lambda x: sorted(_tokenize(_n(x))))
    d["is_attacking_january"] = d.apply(_is_aj_deal, axis=1)
    return d.reset_index(drop=True)


@dataclass
class MatchResult:
    onec_idx: int
    bitrix_id: str
    tier: str
    score: float
    date_delta_days: int
    amount_delta: float
    ambiguous_top: bool
    low_confidence: bool


@dataclass
class BitrixIndexes:
    token_to_idx: dict[str, set[int]]
    exact_name_to_idx: dict[str, set[int]]
    day_to_idx: dict[pd.Timestamp, set[int]]


def build_bitrix_indexes(bitrix: pd.DataFrame) -> BitrixIndexes:
    token_to_idx: dict[str, set[int]] = {}
    exact_name_to_idx: dict[str, set[int]] = {}
    day_to_idx: dict[pd.Timestamp, set[int]] = {}
    for i, b in bitrix.iterrows():
        for t in b["tokens"]:
            token_to_idx.setdefault(t, set()).add(i)
        nm = _n(b["client_name_norm"])
        if nm:
            exact_name_to_idx.setdefault(nm, set()).add(i)
        if pd.notna(b["match_dt"]):
            day = pd.Timestamp(b["match_dt"]).normalize()
            day_to_idx.setdefault(day, set()).add(i)
    return BitrixIndexes(token_to_idx=token_to_idx, exact_name_to_idx=exact_name_to_idx, day_to_idx=day_to_idx)


def _candidate_idx_by_day(indexes: BitrixIndexes, base_day: pd.Timestamp, days: int) -> set[int]:
    out: set[int] = set()
    for d in range(-days, days + 1):
        out |= indexes.day_to_idx.get(base_day + pd.Timedelta(days=d), set())
    return out


def best_match_for_row(
    row: pd.Series,
    bitrix: pd.DataFrame,
    indexes: BitrixIndexes,
) -> tuple[Optional[MatchResult], int]:
    row_tokens = set(row["tokens"])
    row_name = _n(row["client_name_norm"])
    row_dt = row["period_dt"]
    row_amt = float(row["amount"] or 0.0)
    candidates: list[MatchResult] = []

    search_idx: set[int] = set()
    base_day = pd.Timestamp(row_dt).normalize()
    search_idx |= _candidate_idx_by_day(indexes, base_day, 7)

    # Tier A: identifier overlap
    if row_tokens:
        token_hits: set[int] = set()
        for t in row_tokens:
            token_hits |= indexes.token_to_idx.get(t, set())
        search_idx |= token_hits
        for i in token_hits:
            b = bitrix.loc[i]
            dd = abs((row_dt - b["match_dt"]).days) if pd.notna(b["match_dt"]) else 999
            ad = abs(row_amt - float(b["bitrix_amount"] or 0.0))
            candidates.append(
                MatchResult(
                    onec_idx=int(row.name),
                    bitrix_id=_n(b["ID"]),
                    tier="A_identifier",
                    score=1.0,
                    date_delta_days=int(dd),
                    amount_delta=float(ad),
                    ambiguous_top=False,
                    low_confidence=False,
                )
            )

    # Tier B/C: name + date
    name_hits = indexes.exact_name_to_idx.get(row_name, set())
    search_idx |= name_hits
    for i in search_idx:
        b = bitrix.loc[i]
        if pd.isna(b["match_dt"]):
            continue
        dd = abs((row_dt - b["match_dt"]).days)
        if dd > 7:
            continue
        sim = _name_similarity(row_name, _n(b["client_name_norm"]))
        if sim <= 0:
            continue
        ad = abs(row_amt - float(b["bitrix_amount"] or 0.0))
        if sim >= 0.999 and dd <= 3:
            tier = "B_strict_name_date"
            score = 0.95
        elif sim >= 0.82:
            tier = "C_fuzzy_name_date"
            score = 0.70 + min(0.25, sim - 0.82)
        else:
            continue
        candidates.append(
            MatchResult(
                onec_idx=int(row.name),
                bitrix_id=_n(b["ID"]),
                tier=tier,
                score=float(score),
                date_delta_days=int(dd),
                amount_delta=float(ad),
                ambiguous_top=False,
                low_confidence=(tier == "C_fuzzy_name_date" and sim < 0.90),
            )
        )

    if not candidates:
        return None, 0

    candidates.sort(key=lambda x: (-x.score, x.date_delta_days, x.amount_delta, x.bitrix_id))
    best = candidates[0]
    top_count = sum(
        1
        for c in candidates
        if c.score == best.score
        and c.date_delta_days == best.date_delta_days
        and abs(c.amount_delta - best.amount_delta) < 1e-9
    )
    best.ambiguous_top = top_count > 1
    return best, len(candidates)


def run(onec_path: Path, bitrix_path: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    onec = read_1c(onec_path)
    start_dt = pd.Timestamp("2025-09-01")
    end_dt = onec["period_dt"].max()
    onec = onec[onec["period_dt"] >= start_dt].copy().reset_index(drop=True)

    bitrix = read_bitrix(bitrix_path)
    bitrix = bitrix[
        (bitrix["match_dt"].notna())
        & (bitrix["match_dt"] >= start_dt)
        & (bitrix["match_dt"] <= end_dt)
    ].copy()

    bitrix = bitrix.reset_index(drop=True)
    indexes = build_bitrix_indexes(bitrix)

    matches = []
    candidate_counts = []
    used_bitrix_ids = set()
    for _, r in onec.iterrows():
        m, n_cands = best_match_for_row(r, bitrix, indexes)
        candidate_counts.append(n_cands)
        if m is None:
            continue
        used_bitrix_ids.add(m.bitrix_id)
        b = bitrix.loc[bitrix["ID"] == m.bitrix_id].iloc[0]
        matches.append(
            {
                "onec_row_idx": int(m.onec_idx),
                "onec_period": r["period_dt"].strftime("%Y-%m-%d"),
                "onec_document": _n(r.get("Документ", "")),
                "onec_client_name": _n(r["client_name_raw"]),
                "onec_amount": float(r["amount"]),
                "bitrix_id": m.bitrix_id,
                "bitrix_contact_id": _n(b.get("Контакт: ID", "")),
                "bitrix_client_name": _n(b.get("client_name_raw", "")),
                "bitrix_match_date": b["match_dt"].strftime("%Y-%m-%d") if pd.notna(b["match_dt"]) else "",
                "bitrix_amount": float(b.get("bitrix_amount", 0.0)),
                "bitrix_is_attacking_january": bool(b.get("is_attacking_january", False)),
                "tier": m.tier,
                "score": round(m.score, 4),
                "date_delta_days": int(m.date_delta_days),
                "amount_delta": round(m.amount_delta, 2),
                "ambiguous_top": bool(m.ambiguous_top),
                "low_confidence": bool(m.low_confidence),
                "candidate_count": int(n_cands),
            }
        )

    matches_df = pd.DataFrame(matches).sort_values(
        ["tier", "score", "date_delta_days", "amount_delta"],
        ascending=[True, False, True, True],
    )
    matched_onec_idxs = set(matches_df["onec_row_idx"]) if len(matches_df) else set()
    onec_unmatched = onec[~onec.index.isin(matched_onec_idxs)].copy()
    bitrix_unmatched = bitrix[~bitrix["ID"].isin(used_bitrix_ids)].copy()

    m_path = out_dir / "crosscheck_1c_bitrix_matches.csv"
    aj_path = out_dir / "crosscheck_1c_bitrix_matches_aj.csv"
    o_path = out_dir / "crosscheck_1c_unmatched.csv"
    b_path = out_dir / "crosscheck_bitrix_unmatched_in_period.csv"
    s_path = out_dir / "crosscheck_1c_bitrix_summary.json"
    matches_df.to_csv(m_path, index=False, encoding="utf-8")
    if len(matches_df):
        matches_df[matches_df["bitrix_is_attacking_january"]].to_csv(aj_path, index=False, encoding="utf-8")
    else:
        matches_df.to_csv(aj_path, index=False, encoding="utf-8")
    onec_unmatched[
        ["Период", "Документ", "client_name_raw", "amount", "tokens"]
    ].to_csv(o_path, index=False, encoding="utf-8")
    bitrix_unmatched[
        ["ID", "Контакт: ID", "client_name_raw", "match_dt", "bitrix_amount", "Название сделки"]
    ].to_csv(b_path, index=False, encoding="utf-8")

    by_tier = (
        matches_df.groupby("tier")["onec_row_idx"].count().to_dict() if len(matches_df) else {}
    )
    summary = {
        "period_start": "2025-09-01",
        "period_end": end_dt.strftime("%Y-%m-%d") if pd.notna(end_dt) else "",
        "onec_rows_in_period": int(len(onec)),
        "bitrix_rows_in_period": int(len(bitrix)),
        "matched_rows": int(len(matches_df)),
        "matched_rows_aj": int(matches_df["bitrix_is_attacking_january"].sum()) if len(matches_df) else 0,
        "unmatched_onec_rows": int(len(onec_unmatched)),
        "unmatched_bitrix_rows": int(len(bitrix_unmatched)),
        "coverage_onec_pct": round(100.0 * len(matches_df) / max(1, len(onec)), 2),
        "matched_by_tier": by_tier,
        "ambiguous_matches": int(matches_df["ambiguous_top"].sum()) if len(matches_df) else 0,
        "low_confidence_matches": int(matches_df["low_confidence"].sum()) if len(matches_df) else 0,
        "amounts": {
            "onec_matched_sum": round(float(matches_df["onec_amount"].sum()) if len(matches_df) else 0.0, 2),
            "onec_unmatched_sum": round(float(onec_unmatched["amount"].sum()) if len(onec_unmatched) else 0.0, 2),
        },
        "sanity_by_month": {},
        "outputs": {
            "matches": str(m_path),
            "matches_aj": str(aj_path),
            "onec_unmatched": str(o_path),
            "bitrix_unmatched": str(b_path),
        },
    }
    onec_tmp = onec.copy()
    onec_tmp["month"] = onec_tmp["period_dt"].dt.to_period("M").astype(str)
    onec_tmp["is_matched"] = onec_tmp.index.isin(matched_onec_idxs)
    by_month = (
        onec_tmp.groupby("month", dropna=False)
        .apply(
            lambda x: pd.Series(
                {
                    "matched_amount": round(float(x.loc[x["is_matched"], "amount"].sum()), 2),
                    "unmatched_amount": round(float(x.loc[~x["is_matched"], "amount"].sum()), 2),
                    "matched_rows": int(x["is_matched"].sum()),
                    "unmatched_rows": int((~x["is_matched"]).sum()),
                }
            )
        )
        .reset_index()
    )
    summary["sanity_by_month"] = {
        r["month"]: {
            "matched_amount": float(r["matched_amount"]),
            "unmatched_amount": float(r["unmatched_amount"]),
            "matched_rows": int(r["matched_rows"]),
            "unmatched_rows": int(r["unmatched_rows"]),
        }
        for _, r in by_month.iterrows()
    }
    s_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Crosscheck 1C accounting vs Bitrix deals.")
    p.add_argument("--onec", type=Path, default=ONEC_PATH)
    p.add_argument("--bitrix", type=Path, default=BITRIX_PATH)
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    return p.parse_args()


if __name__ == "__main__":
    a = parse_args()
    rep = run(a.onec, a.bitrix, a.out_dir)
    print(json.dumps(rep, ensure_ascii=False, indent=2))

