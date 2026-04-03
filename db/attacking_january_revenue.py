from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd

from bitrix_lead_quality import drop_rows_excluded_funnels
from bitrix_union_io import dedup_bitrix_deals_by_highest_amount, load_bitrix_deals_union

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONTACTS = PROJECT_ROOT / "sheets" / "bitrix_contact_export.csv"
DEFAULT_OUT_CONTACTS = PROJECT_ROOT / "bitrix_attacking_january_contacts_revenue.csv"
DEFAULT_OUT_DEALS = PROJECT_ROOT / "bitrix_attacking_january_all_deals.csv"
DEFAULT_OUT_SUMMARY = PROJECT_ROOT / "bitrix_attacking_january_revenue_breakdown.csv"
DEFAULT_REPORT = PROJECT_ROOT / "bitrix_attacking_january_revenue_report.json"

CAMPAIGN_RE = re.compile(r"(атакующ\w*\s+январ\w*|attacking[_ ]?january)", re.IGNORECASE)
SPLIT_RE = re.compile(r"[;,|]+")
TARGET_EVENTS = [
    "Тренд репорты",
    "Демо Ред",
    "Демо Блю",
    "ПБХ",
    "Blue Team Stepik",
    "Опен дэй",
    "Встреча с экспертом",
]


def _norm_str(v: object) -> str:
    if v is None or pd.isna(v):
        return ""
    s = str(v).strip()
    if s.lower() in {"", "nan", "none", "null"}:
        return ""
    return s


def _norm_id(v: object) -> str:
    s = _norm_str(v)
    if re.fullmatch(r"\d+\.0+", s):
        return s.split(".", 1)[0]
    return s


def _parse_amount(v: object) -> float:
    s = _norm_str(v).replace(" ", "").replace("\xa0", "")
    if not s:
        return 0.0
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _extract_contacts_phones_emails_from_df(df: pd.DataFrame) -> tuple[str, str]:
    phone_cols = [c for c in df.columns if "телефон" in c.lower()]
    email_cols = [c for c in df.columns if "e-mail" in c.lower() or "email" in c.lower()]
    phones = set()
    for _, row in df.iterrows():
        for c in phone_cols:
            raw = _norm_str(row.get(c, ""))
            for p in SPLIT_RE.split(raw):
                pp = _norm_str(p)
                if pp:
                    phones.add(pp)

    emails = set()
    for _, row in df.iterrows():
        for c in email_cols:
            raw = _norm_str(row.get(c, ""))
            for e in SPLIT_RE.split(raw):
                ee = _norm_str(e).lower()
                if ee:
                    emails.add(ee)

    return " | ".join(sorted(phones)), " | ".join(sorted(emails))


def _contains_campaign_token(row: pd.Series, cols: list[str]) -> bool:
    for c in cols:
        val = _norm_str(row.get(c, ""))
        if val and CAMPAIGN_RE.search(val):
            return True
    return False


def _normalize_event_name(raw: str) -> str:
    s = _norm_str(raw).lower()
    if not s:
        return "Другое"
    mapping = [
        ("тренд", "Тренд репорты"),
        ("демо ред", "Демо Ред"),
        ("демо блю", "Демо Блю"),
        ("пбх", "ПБХ"),
        ("blue team stepik", "Blue Team Stepik"),
        ("опен дэй", "Опен дэй"),
        ("open day", "Опен дэй"),
        ("встреча с экспертом", "Встреча с экспертом"),
    ]
    for needle, label in mapping:
        if needle in s:
            return label
    return "Другое"


def run(
    contacts_path: Path,
    deals_path: Path | None,
    out_contacts: Path,
    out_deals: Path,
    out_summary: Path,
    report_path: Path,
) -> None:
    contacts = pd.read_csv(contacts_path, sep=";", encoding="utf-8", low_memory=False, dtype=str)
    if deals_path is None:
        deals = load_bitrix_deals_union()
    else:
        deals = pd.read_csv(deals_path, sep=";", encoding="utf-8", low_memory=False, dtype=str)

    contacts["ID"] = contacts["ID"].map(_norm_id)
    deals["ID"] = deals["ID"].map(_norm_id)
    deals["Контакт: ID"] = deals["Контакт: ID"].map(_norm_id)
    deals["Сумма_num"] = deals["Сумма"].map(_parse_amount) if "Сумма" in deals.columns else 0.0

    preferred_cols = [c for c in ["Название сделки", "UTM Campaign", "Источник (подробно)", "Источник обращения"] if c in deals.columns]
    if not preferred_cols:
        preferred_cols = [c for c in deals.columns if deals[c].dtype == object]

    deals["is_attacking_january"] = deals.apply(lambda r: _contains_campaign_token(r, preferred_cols), axis=1)
    jan_deals = deals[(deals["is_attacking_january"]) & (deals["Контакт: ID"] != "")].copy()

    cohort_ids = sorted(jan_deals["Контакт: ID"].dropna().astype(str).unique())
    cohort_set = set(cohort_ids)

    all_cohort_deals = deals[deals["Контакт: ID"].isin(cohort_set)].copy()
    all_cohort_deals = drop_rows_excluded_funnels(all_cohort_deals)
    all_cohort_deals = all_cohort_deals.sort_values(["Контакт: ID", "Дата создания", "ID"], ascending=[True, True, True])
    # Important: Bitrix exports may contain repeated rows for same deal ID.
    # Keep the row with the highest amount for revenue arithmetic.
    all_cohort_deals = dedup_bitrix_deals_by_highest_amount(all_cohort_deals).copy()

    # Revenue exists only for the canonical revenue stages.
    stage = all_cohort_deals.get("Стадия сделки", "").fillna("").astype(str).str.lower()
    recognized_revenue_mask = (
        stage.str.contains("сделка заключена", na=False)
        | stage.str.contains("постоплат", na=False)
        | stage.str.contains("рассроч", na=False)
    )
    all_cohort_deals["Выручка_учитывается"] = recognized_revenue_mask
    all_cohort_deals["Выручка_для_расчета"] = all_cohort_deals["Сумма_num"].where(recognized_revenue_mask, 0.0)

    all_cohort_deals.to_csv(out_deals, index=False, encoding="utf-8")

    # Contact master + fallback phones/emails from deals-side contact fields.
    if "ID" not in contacts.columns:
        raise ValueError("Contacts export must contain ID column")

    c = contacts[contacts["ID"].isin(cohort_set)].copy()
    c["full_name"] = c[["Фамилия", "Имя", "Отчество"]].fillna("").astype(str).agg(" ".join, axis=1).str.replace(r"\s+", " ", regex=True).str.strip()
    c["full_name"] = c["full_name"].mask(c["full_name"] == "", c.get("Имя", ""))

    # phones/emails from contact export
    contact_phone_cols = [col for col in c.columns if "телефон" in col.lower()]
    contact_email_cols = [col for col in c.columns if "e-mail" in col.lower() or "email" in col.lower()]

    def join_vals(series: pd.Series) -> str:
        vals = set()
        for raw in series:
            s = _norm_str(raw)
            if not s:
                continue
            for part in SPLIT_RE.split(s):
                p = _norm_str(part)
                if p:
                    vals.add(p)
        return " | ".join(sorted(vals))

    c["phones_export"] = c[contact_phone_cols].apply(join_vals, axis=1) if contact_phone_cols else ""
    c["emails_export"] = c[contact_email_cols].apply(join_vals, axis=1) if contact_email_cols else ""

    # Aggregate revenue and deal ids
    def _agg_contact(g: pd.DataFrame) -> pd.Series:
        deal_ids_all = sorted({_norm_id(x) for x in g["ID"] if _norm_id(x)})
        deal_ids_rev = sorted({_norm_id(x) for x in g.loc[g["Выручка_учитывается"], "ID"] if _norm_id(x)})
        deal_ids_jan = sorted({_norm_id(x) for x in g.loc[g["is_attacking_january"], "ID"] if _norm_id(x)})
        return pd.Series(
            {
                "выручка": float(g["Выручка_для_расчета"].sum()),
                "сделок_всего": len(deal_ids_all),
                "id_сделок": " | ".join(deal_ids_all),
                "сделок_атакующий_январь": len(deal_ids_jan),
                "сделок_с_учтенной_выручкой": len(deal_ids_rev),
            }
        )

    agg = (
        all_cohort_deals.groupby("Контакт: ID", dropna=False)
        .apply(_agg_contact)
        .reset_index()
        .rename(columns={"Контакт: ID": "contact_id"})
    )
    agg["средний_чек"] = agg["выручка"] / agg["сделок_с_учтенной_выручкой"].replace(0, pd.NA)
    agg["средний_чек"] = agg["средний_чек"].fillna(0.0)

    # Fallback phones/emails from deals-side contact fields.
    fallback = (
        all_cohort_deals.groupby("Контакт: ID", dropna=False)
        .apply(lambda g: pd.Series({
            "phones_deals_fallback": _extract_contacts_phones_emails_from_df(g)[0],
            "emails_deals_fallback": _extract_contacts_phones_emails_from_df(g)[1],
        }))
        .reset_index()
        .rename(columns={"Контакт: ID": "contact_id"})
    )

    out = agg.merge(c, left_on="contact_id", right_on="ID", how="left").merge(fallback, on="contact_id", how="left")
    out["Телефоны"] = out["phones_export"].fillna("")
    out["Телефоны"] = out["Телефоны"].mask(out["Телефоны"] == "", out["phones_deals_fallback"].fillna(""))
    out["Email"] = out["emails_export"].fillna("")
    out["Email"] = out["Email"].mask(out["Email"] == "", out["emails_deals_fallback"].fillna(""))

    keep_cols = [
        "contact_id",
        "full_name",
        "Телефоны",
        "Email",
        "выручка",
        "средний_чек",
        "сделок_всего",
        "сделок_с_учтенной_выручкой",
        "сделок_атакующий_январь",
        "id_сделок",
    ]
    out = out[keep_cols].rename(
        columns={
            "contact_id": "ID_контакта",
            "full_name": "ФИО",
            "выручка": "Выручка",
            "средний_чек": "Средний_чек",
            "сделок_всего": "Сделок_всего",
            "сделок_с_учтенной_выручкой": "Сделок_с_учтенной_выручкой",
            "сделок_атакующий_январь": "Сделок_АтакующийЯнварь",
            "id_сделок": "ID_сделок",
        }
    )
    out = out.sort_values(["Выручка", "Сделок_всего"], ascending=[False, False]).reset_index(drop=True)
    out.to_csv(out_contacts, index=False, encoding="utf-8")

    # Breakdown: total + by event + by course code.
    def summarize(df: pd.DataFrame, section: str, value: str) -> dict:
        deals_total = int(df["ID"].nunique()) if len(df) else 0
        deals_revenue = int(df.loc[df["Выручка_учитывается"], "ID"].nunique()) if len(df) else 0
        revenue = float(df["Выручка_для_расчета"].sum()) if len(df) else 0.0
        avg = revenue / deals_revenue if deals_revenue else 0.0
        return {
            "Срез": section,
            "Значение": value,
            "Контактов_уник": int(df["Контакт: ID"].nunique()) if len(df) else 0,
            "Сделок_всего": deals_total,
            "Сделок_с_учтенной_выручкой": deals_revenue,
            "Выручка": round(revenue, 2),
            "Средний_чек": round(avg, 2),
        }

    summary_rows = [summarize(all_cohort_deals, "Итого", "Все сделки когорты Атакующий Января")]

    if "Название сделки" in all_cohort_deals.columns:
        tmp = all_cohort_deals.copy()
        tmp["Группа_мероприятия"] = tmp["Название сделки"].map(_normalize_event_name)
        for event_name, g in tmp.groupby("Группа_мероприятия"):
            summary_rows.append(summarize(g, "Мероприятие", event_name))

    course_col = "Код_курса_сайт" if "Код_курса_сайт" in all_cohort_deals.columns else ("Код курса" if "Код курса" in all_cohort_deals.columns else "")
    if course_col:
        tmp = all_cohort_deals.copy()
        tmp[course_col] = tmp[course_col].map(_norm_str).replace("", "(пусто)")
        for course_code, g in tmp.groupby(course_col):
            summary_rows.append(summarize(g, "Код курса", course_code))

    summary_df = pd.DataFrame(summary_rows).sort_values(["Срез", "Выручка"], ascending=[True, False]).reset_index(drop=True)
    summary_df.to_csv(out_summary, index=False, encoding="utf-8")

    report = {
        "cohort_contacts_count": int(len(cohort_ids)),
        "attacking_january_deals_count": int(len(jan_deals)),
        "all_cohort_deals_count": int(len(all_cohort_deals)),
        "total_revenue_all_cohort_deals": round(float(all_cohort_deals["Выручка_для_расчета"].sum()), 2),
        "revenue_eligible_deals_count": int(all_cohort_deals.loc[all_cohort_deals["Выручка_учитывается"], "ID"].nunique()),
        "contacts_with_revenue_deals_count": int(all_cohort_deals.loc[all_cohort_deals["Выручка_учитывается"] & all_cohort_deals["Контакт: ID"].ne(""), "Контакт: ID"].nunique()),
        "average_check_revenue_eligible_deals": round(
            float(all_cohort_deals["Выручка_для_расчета"].sum()) / max(1, int(all_cohort_deals.loc[all_cohort_deals["Выручка_учитывается"], "ID"].nunique())),
            2,
        ),
        "output_contacts_file": str(out_contacts),
        "output_all_deals_file": str(out_deals),
        "output_summary_file": str(out_summary),
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compute total revenue for Attacking January cohort.")
    p.add_argument("--contacts", type=Path, default=DEFAULT_CONTACTS)
    p.add_argument(
        "--deals",
        type=Path,
        default=None,
        help="CSV сделок; по умолчанию объединение sheets/fl_raw_09-03.csv + sheets/bitrix_upd_27.03.csv",
    )
    p.add_argument("--out-contacts", type=Path, default=DEFAULT_OUT_CONTACTS)
    p.add_argument("--out-deals", type=Path, default=DEFAULT_OUT_DEALS)
    p.add_argument("--out-summary", type=Path, default=DEFAULT_OUT_SUMMARY)
    p.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.contacts, args.deals, args.out_contacts, args.out_deals, args.out_summary, args.report)
