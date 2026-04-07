from __future__ import annotations

import json
import re
from difflib import SequenceMatcher
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from bitrix_lead_quality import (
    coalesce_columns,
    deal_funnel_raw_series,
    drop_rows_excluded_funnels,
    funnel_report_bucket_series,
)
from bitrix_union_io import dedup_bitrix_deals_by_highest_amount, load_bitrix_deals_union
from conn import get_engine, ensure_schema
from event_classifier import classify_event_from_row, normalize_course_code
from revenue_variant3 import variant3_revenue_mask
from utils import _n, _id, _amt, _month


ROOT = Path(__file__).resolve().parent.parent
REPORTS_ROOT = ROOT / "reports" / "slices"
GLOBAL_DIR = REPORTS_ROOT / "global"
COHORT_DIR = REPORTS_ROOT / "cohorts" / "attacking_january"
QA_DIR = REPORTS_ROOT / "qa"

# Аналитика по сделкам: DB-first из raw_bitrix_deals, с авто-backfill из
# fl_raw_09-03 + bitrix_upd_27.03 при пустой raw-таблице.
YANDEX_CSV = ROOT / "yandex.csv"
SENDSAY_CSV = ROOT / "sheets" / "mass_email_good.csv"
RAW_BITRIX_TABLE = "raw_bitrix_deals"
RAW_BATCH_TABLE = "raw_source_batches"


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:name LIMIT 1"),
        {"name": table},
    ).first()
    return row is not None


def _persist_raw_bitrix_snapshot(
    engine,
    bitrix: pd.DataFrame,
    *,
    source_batch: str,
    source_type: str,
    source_ref: str,
) -> int:
    if "ID" not in bitrix.columns:
        raise ValueError("Bitrix source frame must contain ID")
    snap = bitrix.copy()
    snap["ID"] = snap["ID"].map(_id)
    snap = dedup_bitrix_deals_by_highest_amount(snap)
    snap = drop_rows_excluded_funnels(snap)
    ingested_at = datetime.now(timezone.utc).isoformat()
    snap["source_batch"] = source_batch
    snap["ingested_at"] = ingested_at
    snap.to_sql(RAW_BITRIX_TABLE, engine, if_exists="replace", index=False, chunksize=100)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT OR REPLACE INTO {RAW_BATCH_TABLE}
                (source_batch, source_type, source_ref, row_count, created_at)
                VALUES (:source_batch, :source_type, :source_ref, :row_count, :created_at)
                """
            ),
            {
                "source_batch": source_batch,
                "source_type": source_type,
                "source_ref": source_ref,
                "row_count": int(len(snap)),
                "created_at": ingested_at,
            },
        )
    return int(len(snap))


def _load_bitrix_from_raw_sql(engine) -> pd.DataFrame | None:
    with engine.connect() as conn:
        if not _table_exists(conn, RAW_BITRIX_TABLE):
            return None
        raw = pd.read_sql_query(
            text(
                f"""
                SELECT *
                FROM {RAW_BITRIX_TABLE}
                ORDER BY ID
                """
            ),
            conn,
        )
    if raw.empty:
        return None

    out = raw.copy()
    for c in ("source_batch", "ingested_at"):
        if c in out.columns:
            out = out.drop(columns=[c])
    if "ID" not in out.columns:
        return None
    out["ID"] = out["ID"].map(_id)
    out = dedup_bitrix_deals_by_highest_amount(out)
    return out.reset_index(drop=True)


def load_bitrix_deals_db_first(engine) -> tuple[pd.DataFrame, str]:
    raw = _load_bitrix_from_raw_sql(engine)
    if raw is not None and not raw.empty:
        return raw, RAW_BITRIX_TABLE

    union = load_bitrix_deals_union()
    batch = f"csv_union_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    rows = _persist_raw_bitrix_snapshot(
        engine,
        union,
        source_batch=batch,
        source_type="csv_union",
        source_ref="bitrix_19.03.26.csv + bitrix_60_days_03.04.2026.csv",
    )
    print(f"Backfilled {RAW_BITRIX_TABLE}: {rows} rows from CSV union", flush=True)
    return union, "csv_union_auto_backfill"


def _normalize_yandex_month(v: object) -> str:
    s = _n(v)
    if not s:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}.*", s):
        return s[:7]

    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    if pd.notna(dt):
        return dt.strftime("%Y-%m")

    lower = s.lower()
    m_year = re.search(r"(20\d{2})", lower)
    if not m_year:
        return ""
    year = m_year.group(1)
    month_map = (
        ("январ", "01"),
        ("феврал", "02"),
        ("март", "03"),
        ("апрел", "04"),
        ("мая", "05"),
        ("май", "05"),
        ("июн", "06"),
        ("июл", "07"),
        ("август", "08"),
        ("сентябр", "09"),
        ("октябр", "10"),
        ("ноябр", "11"),
        ("декабр", "12"),
    )
    for token, mm in month_map:
        if token in lower:
            return f"{year}-{mm}"
    return ""


def _staging_deals_analytics_df(bitrix: pd.DataFrame) -> pd.DataFrame:
    """Narrow staging for Cloudflare rebuild (mirrors columns consumed by web/functions/lib/analytics)."""
    return pd.DataFrame(
        {
            "deal_id": bitrix["ID"].map(_id),
            "contact_id": bitrix["Контакт: ID"].map(_id),
            "created_at": bitrix.get("Дата создания", "").fillna("").astype(str).map(_n),
            "funnel_raw": bitrix["Воронка"].map(_n),
            "stage_raw": coalesce_columns(bitrix, "Стадия сделки").fillna("").astype(str).map(_n),
            "closed_yes": bitrix.get("Сделка закрыта", "").fillna("").astype(str).map(_n),
            "pay_date": bitrix.get("Дата оплаты", "").fillna("").astype(str).map(_n),
            "installment_schedule": bitrix.get("Даты платежей по рассрочке ", "").fillna("").astype(str).map(_n),
            "sum_text": bitrix.get("Сумма", "").fillna("").astype(str).map(_n),
            "utm_source": bitrix.get("UTM Source", "").fillna("").astype(str).map(_n),
            "utm_medium": bitrix.get("UTM Medium", "").fillna("").astype(str).map(_n),
            "utm_campaign": bitrix.get("UTM Campaign", "").fillna("").astype(str).map(_n),
            "utm_content": bitrix.get("UTM Content", "").fillna("").astype(str).map(_n),
            "deal_name": bitrix.get("Название сделки", "").fillna("").astype(str).map(_n),
            "code_site": bitrix.get("Код_курса_сайт", "").fillna("").astype(str).map(_n),
            "code_course": bitrix.get("Код курса", "").fillna("").astype(str).map(_n),
            "source_detail": bitrix.get("Источник (подробно)", "").fillna("").astype(str).map(_n),
            "source_inquiry": bitrix.get("Источник обращения", "").fillna("").astype(str).map(_n),
            "invalid_type_lead": bitrix.get("Типы некачественного лида", "").fillna("").astype(str).map(_n),
        }
    )


def _sqlite_insert_chunksize(n_columns: int) -> int:
    """SQLite SQLITE_MAX_VARIABLE_NUMBER defaults to 999; multi-row INSERT multiplies bind params."""
    return max(1, 998 // max(1, n_columns))


def build_marts(engine) -> dict:
    bitrix, bitrix_source = load_bitrix_deals_db_first(engine)
    bitrix["Контакт: ID"] = bitrix["Контакт: ID"].map(_id)

    bitrix = drop_rows_excluded_funnels(bitrix)
    bitrix["Воронка"] = deal_funnel_raw_series(bitrix)
    bitrix["funnel_group"] = funnel_report_bucket_series(bitrix["Воронка"])

    bitrix["is_revenue_variant3"] = variant3_revenue_mask(bitrix).astype(int)
    bitrix["revenue_amount"] = bitrix["Сумма"].map(_amt).where(bitrix["is_revenue_variant3"].eq(1), 0.0)
    bitrix["month"] = bitrix["Дата создания"].map(_month)

    cls = bitrix.apply(lambda r: classify_event_from_row(r.to_dict()), axis=1)
    bitrix["event_class"] = cls.map(lambda x: x.event)
    bitrix["classification_source"] = cls.map(lambda x: x.source_field)
    bitrix["classification_pattern"] = cls.map(lambda x: x.matched_pattern)
    bitrix["classification_confidence"] = cls.map(lambda x: x.confidence)

    course_raw = bitrix.get("Код_курса_сайт", "").fillna("")
    if "Код курса" in bitrix.columns:
        course_raw = course_raw.mask(course_raw.astype(str).str.strip().eq(""), bitrix["Код курса"].fillna(""))
    bitrix["course_code_norm"] = course_raw.map(normalize_course_code)

    bitrix["is_attacking_january"] = (bitrix["event_class"] == "Attacking January").astype(int)
    if "Типы некачественного лида" not in bitrix.columns:
        bitrix["Типы некачественного лида"] = ""
    if "Типы некачественных лидов" not in bitrix.columns:
        bitrix["Типы некачественных лидов"] = bitrix["Типы некачественного лида"]
    if "Ответственный" not in bitrix.columns:
        bitrix["Ответственный"] = ""

    staging = _staging_deals_analytics_df(bitrix)
    staging.to_sql(
        "stg_deals_analytics",
        engine,
        if_exists="replace",
        index=False,
        chunksize=_sqlite_insert_chunksize(staging.shape[1]),
    )

    # Keep full staging and compact mart.
    bitrix.to_sql(
        "stg_bitrix_deals_wide",
        engine,
        if_exists="replace",
        index=False,
        chunksize=_sqlite_insert_chunksize(bitrix.shape[1]),
    )
    mart_cols = [
        "ID",
        "Контакт: ID",
        "Дата создания",
        "month",
        "Воронка",
        "funnel_group",
        "Стадия сделки",
        "Сделка закрыта",
        "Дата оплаты",
        "Сумма",
        "revenue_amount",
        "is_revenue_variant3",
        "UTM Source",
        "UTM Medium",
        "UTM Campaign",
        "UTM Content",
        "Название сделки",
        "Код_курса_сайт",
        "Код курса",
        "course_code_norm",
        "event_class",
        "classification_source",
        "classification_pattern",
        "classification_confidence",
        "is_attacking_january",
        "Типы некачественного лида",
        "Типы некачественных лидов",
        "Ответственный",
    ]
    bitrix[mart_cols].to_sql(
        "mart_deals_enriched",
        engine,
        if_exists="replace",
        index=False,
        chunksize=_sqlite_insert_chunksize(len(mart_cols)),
    )

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS mart_attacking_january_contacts"))
        conn.execute(
            text(
                """
                CREATE TABLE mart_attacking_january_contacts AS
                SELECT DISTINCT "Контакт: ID" AS contact_id
                FROM mart_deals_enriched
                WHERE event_class = 'Attacking January' AND COALESCE("Контакт: ID", '') <> ''
                """
            )
        )
        conn.execute(text("DROP TABLE IF EXISTS mart_attacking_january_cohort_deals"))
        conn.execute(
            text(
                """
                CREATE TABLE mart_attacking_january_cohort_deals AS
                SELECT d.*
                FROM mart_deals_enriched d
                JOIN mart_attacking_january_contacts c
                  ON d."Контакт: ID" = c.contact_id
                """
            )
        )

    # Yandex/Sendsay staging.
    def _float_col_to_int_str(series: pd.Series) -> pd.Series:
        """Convert float-stored ID columns (e.g. 12345678.0) to clean integer strings.
        Empty/NaN/nan/- -> empty string. Required for plain string equality in D1 SQL matches."""
        def _conv(v):
            if pd.isna(v):
                return ""
            s = str(v).strip()
            if s.lower() in ("", "nan", "-"):
                return ""
            try:
                return str(int(float(s)))
            except (ValueError, OverflowError):
                return s
        return series.map(_conv)

    y_rows = 0
    if YANDEX_CSV.exists():
        y = pd.read_csv(YANDEX_CSV, encoding="utf-8", low_memory=False)
        # Normalize ID columns: float -> clean integer string so D1 string matching works.
        for _yd_id_col in ("№ Объявления", "№ Кампании", "№ Группы"):
            if _yd_id_col in y.columns:
                y[_yd_id_col] = _float_col_to_int_str(y[_yd_id_col])
        # Exact duplicate lines in the export would inflate month totals (SUM spend/clicks).
        _yd_key = [c for c in ("Месяц", "№ Кампании", "№ Группы", "№ Объявления") if c in y.columns]
        if _yd_key:
            y = y.drop_duplicates(subset=_yd_key, keep="first")
        y_rows = len(y)
        if "Месяц" in y.columns:
            y["month"] = y["Месяц"].map(_normalize_yandex_month)
        y.to_sql("stg_yandex_stats", engine, if_exists="replace", index=False)
    if SENDSAY_CSV.exists():
        s = pd.read_csv(SENDSAY_CSV, encoding="utf-8", low_memory=False)
        if "Дата отправки" in s.columns:
            dt = pd.to_datetime(s["Дата отправки"], dayfirst=False, errors="coerce")
            s["month"] = dt.dt.strftime("%Y-%m")
        s.to_sql("stg_email_sends", engine, if_exists="replace", index=False)

    return {
        "bitrix_source": bitrix_source,
        "raw_bitrix_rows": int(len(bitrix)),
        "mart_deals_enriched_rows": int(len(bitrix)),
        "attacking_january_contacts": int(bitrix.loc[bitrix["event_class"] == "Attacking January", "Контакт: ID"].replace("", pd.NA).dropna().nunique()),
        "stg_yandex_rows": int(y_rows),
    }


def build_yandex_dedup_marts(engine) -> dict:
    """Build Yandex marts from raw Yandex stats matched to Bitrix by UTM Content = ad ID."""
    with engine.connect() as conn:
        bitrix = pd.read_sql_query(
            text(
                """
                SELECT
                  ID,
                  "Контакт: ID" AS contact_id,
                  month,
                  "UTM Campaign" AS utm_campaign,
                  "UTM Content" AS utm_content,
                  "UTM Source" AS utm_source,
                  revenue_amount,
                  is_revenue_variant3,
                  "Название сделки" AS deal_name,
                  funnel_group AS funnel,
                  "Стадия сделки" AS stage
                FROM mart_deals_enriched
                """
            ),
            conn,
        )
        yandex = pd.read_sql_query(text('SELECT * FROM stg_yandex_stats'), conn)

    empty_meta = {
        "yandex_raw_rows": 0,
        "yandex_dedup_rows": 0,
        "yandex_projects_raw_rows": 0,
        "yandex_projects_dedup_rows": 0,
    }
    if yandex.empty or bitrix.empty:
        pd.DataFrame().to_sql("mart_yandex_leads_raw", engine, if_exists="replace", index=False)
        pd.DataFrame().to_sql("mart_yandex_leads_dedup", engine, if_exists="replace", index=False)
        pd.DataFrame().to_sql("mart_yandex_revenue_projects_raw", engine, if_exists="replace", index=False)
        pd.DataFrame().to_sql("mart_yandex_revenue_projects_dedup", engine, if_exists="replace", index=False)
        return empty_meta

    bitrix = bitrix.copy()
    bitrix["ID"] = bitrix["ID"].map(_id)
    bitrix["contact_id"] = bitrix["contact_id"].map(_id)
    bitrix["utm_content"] = bitrix["utm_content"].map(_id)
    bitrix["utm_source_norm"] = bitrix["utm_source"].map(_n).str.lower()
    bitrix = bitrix[
        bitrix["utm_content"].ne("")
        & bitrix["utm_source_norm"].str.startswith("y")
        & bitrix["utm_source_norm"].ne("yah")
    ].copy()
    bitrix["lead_key"] = bitrix["contact_id"].where(bitrix["contact_id"].map(bool), bitrix["ID"])
    bitrix["is_paid_deal"] = pd.to_numeric(bitrix["is_revenue_variant3"], errors="coerce").fillna(0).astype(int)
    bitrix["revenue_amount"] = pd.to_numeric(bitrix["revenue_amount"], errors="coerce").fillna(0.0)

    yandex = yandex.copy()
    yandex["ad_id"] = yandex.get("№ Объявления", "").map(_id)
    yandex["campaign_id"] = yandex.get("№ Кампании", "").map(_n)
    yandex["project_name"] = yandex.get("Название кампании", "").map(_n)
    yandex["yandex_month"] = yandex.get("month", yandex.get("Месяц", "")).map(_normalize_yandex_month)
    yandex["yandex_spend"] = pd.to_numeric(yandex.get("Расход, ₽", 0), errors="coerce").fillna(0.0)
    yandex = yandex[yandex["ad_id"].ne("")].copy()

    raw = bitrix.merge(
        yandex[["ad_id", "project_name", "campaign_id", "yandex_month", "yandex_spend"]],
        left_on="utm_content",
        right_on="ad_id",
        how="inner",
    )
    raw["deal_month"] = raw["month"]
    raw["yandex_month"] = raw["yandex_month"].where(
        raw["yandex_month"].astype(str).str.fullmatch(r"\d{4}-\d{2}", na=False),
        raw["deal_month"],
    )

    raw_cols = [
        "ID",
        "contact_id",
        "lead_key",
        "deal_month",
        "utm_campaign",
        "project_name",
        "campaign_id",
        "yandex_month",
        "yandex_spend",
        "deal_name",
        "is_paid_deal",
        "revenue_amount",
        "funnel",
        "stage",
    ]
    if len(raw):
        raw["project_name"] = raw["project_name"].mask(raw["project_name"].astype(str).str.strip().eq(""), "UNMAPPED")
        raw = raw[raw_cols].drop_duplicates(subset=["ID", "campaign_id"], keep="first")
    else:
        raw = pd.DataFrame(columns=raw_cols)
    raw.to_sql("mart_yandex_leads_raw", engine, if_exists="replace", index=False)

    dedup = (
        raw.sort_values(["lead_key", "project_name", "ID"])
        .groupby(["lead_key", "project_name"], dropna=False)
        .agg(
            deals_count=("ID", "nunique"),
            paid_deals=("is_paid_deal", "sum"),
            revenue=("revenue_amount", "sum"),
            contact_id=("contact_id", "first"),
            campaign_id=("campaign_id", "first"),
            yandex_month=("yandex_month", "first"),
        )
        .reset_index()
    )
    dedup.to_sql("mart_yandex_leads_dedup", engine, if_exists="replace", index=False)

    if len(raw):
        project_metrics = (
            raw.groupby(["project_name", "yandex_month"], dropna=False)
            .agg(
                leads_raw=("lead_key", "count"),
                deals_raw=("ID", "nunique"),
                paid_deals_raw=("is_paid_deal", "sum"),
                revenue_raw=("revenue_amount", "sum"),
            )
            .reset_index()
        )
        camp_spend = raw.drop_duplicates(subset=["campaign_id", "yandex_month"], keep="first")[[
            "project_name", "yandex_month", "yandex_spend"
        ]]
        spend_by_proj = (
            camp_spend.groupby(["project_name", "yandex_month"], dropna=False)["yandex_spend"]
            .sum()
            .reset_index()
            .rename(columns={"yandex_spend": "spend"})
        )
        project_raw = project_metrics.merge(spend_by_proj, on=["project_name", "yandex_month"], how="left")
        project_raw["spend"] = project_raw["spend"].fillna(0.0)
    else:
        project_raw = pd.DataFrame(
            columns=[
                "project_name",
                "yandex_month",
                "leads_raw",
                "deals_raw",
                "paid_deals_raw",
                "revenue_raw",
                "spend",
            ]
        )
    project_raw.to_sql("mart_yandex_revenue_projects_raw", engine, if_exists="replace", index=False)

    project_dedup = (
        dedup.groupby(["project_name", "yandex_month"], dropna=False)
        .agg(
            leads_dedup=("lead_key", "nunique"),
            paid_deals_dedup=("paid_deals", "sum"),
            revenue_dedup=("revenue", "sum"),
        )
        .reset_index()
    )
    project_dedup.to_sql("mart_yandex_revenue_projects_dedup", engine, if_exists="replace", index=False)

    return {
        "yandex_raw_rows": int(len(raw)),
        "yandex_dedup_rows": int(len(dedup)),
        "yandex_projects_raw_rows": int(len(project_raw)),
        "yandex_projects_dedup_rows": int(len(project_dedup)),
    }


def _sql_to_csv(engine, query: str, out_path: Path) -> int:
    with engine.connect() as conn:
        df = pd.read_sql_query(text(query), conn)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    return len(df)

def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _group_yandex_projects_no_month(df: pd.DataFrame, threshold: float = 0.6) -> pd.DataFrame:
    """
    Cluster similar Yandex project names by fuzzy similarity threshold.
    The cluster label is the first (highest-revenue) seen project name.
    """
    if df.empty or "project_name" not in df.columns:
        return df

    work = df.copy()
    work["project_name"] = work["project_name"].fillna("").astype(str).str.strip()
    work["__name_norm"] = (
        work["project_name"]
        .str.lower()
        .str.replace(r"[^a-zа-я0-9]+", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    work = work.sort_values(["revenue_raw", "leads_raw"], ascending=[False, False]).reset_index(drop=True)

    cluster_norms: list[str] = []
    cluster_labels: list[str] = []
    mapped: list[str] = []
    for _, row in work.iterrows():
        pn = str(row["project_name"])
        n = str(row["__name_norm"])
        if not n:
            mapped.append("UNMAPPED")
            continue
        best_i = -1
        best_s = 0.0
        for i, cn in enumerate(cluster_norms):
            s = _sim(n, cn)
            if s > best_s:
                best_s = s
                best_i = i
        if best_i >= 0 and best_s >= threshold:
            mapped.append(cluster_labels[best_i])
        else:
            cluster_norms.append(n)
            cluster_labels.append(pn)
            mapped.append(pn)

    work["project_name_grouped"] = mapped
    agg = (
        work.groupby("project_name_grouped", dropna=False)
        .agg(
            leads_raw=("leads_raw", "sum"),
            payments_count=("payments_count", "sum"),
            paid_deals_raw=("paid_deals_raw", "sum"),
            revenue_raw=("revenue_raw", "sum"),
            spend=("spend", "sum"),
        )
        .reset_index()
        .rename(columns={"project_name_grouped": "project_name"})
        .sort_values("revenue_raw", ascending=False)
        .reset_index(drop=True)
    )
    return agg


def export_slices(engine) -> dict:
    exported = {}

    # month_channel: Bitrix revenue + Yandex spend + Sendsay sends
    exported["global_month_channel"] = _sql_to_csv(
        engine,
        """
        SELECT
          month,
          COALESCE("UTM Source", '') AS utm_source,
          COALESCE("UTM Medium", '') AS utm_medium,
          COUNT(DISTINCT ID) AS deals,
          SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
          SUM(revenue_amount) AS revenue
        FROM mart_deals_enriched
        GROUP BY month, COALESCE("UTM Source", ''), COALESCE("UTM Medium", '')
        ORDER BY month, revenue DESC
        """,
        GLOBAL_DIR / "month_channel_bitrix.csv",
    )
    exported["global_month_yandex"] = _sql_to_csv(
        engine,
        """
        SELECT
          month,
          COUNT(*) AS rows_count,
          SUM(COALESCE("Расход, ₽", 0)) AS yandex_spend,
          SUM(COALESCE("Клики", 0)) AS clicks,
          SUM(COALESCE("Конверсии", 0)) AS conversions
        FROM stg_yandex_stats
        GROUP BY month
        ORDER BY month
        """,
        GLOBAL_DIR / "month_channel_yandex.csv",
    )
    exported["global_month_sendsay"] = _sql_to_csv(
        engine,
        """
        SELECT
          month,
          COUNT(*) AS sends,
          SUM(COALESCE("Отправлено", 0)) AS sent_total,
          SUM(COALESCE("Доставлено", 0)) AS delivered_total,
          SUM(COALESCE("Уник. открытий", 0)) AS unique_opens,
          SUM(COALESCE("Уник. кликов", 0)) AS unique_clicks
        FROM stg_email_sends
        GROUP BY month
        ORDER BY month
        """,
        GLOBAL_DIR / "month_channel_sendsay.csv",
    )

    # funnel_stage
    exported["global_funnel_stage"] = _sql_to_csv(
        engine,
        """
        SELECT
          COALESCE(funnel_group, '') AS funnel,
          COALESCE("Стадия сделки", '') AS stage,
          COUNT(DISTINCT ID) AS deals,
          SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
          SUM(revenue_amount) AS revenue
        FROM mart_deals_enriched
        GROUP BY COALESCE(funnel_group, ''), COALESCE("Стадия сделки", '')
        ORDER BY revenue DESC
        """,
        GLOBAL_DIR / "funnel_stage.csv",
    )

    # event_course
    exported["global_event_course"] = _sql_to_csv(
        engine,
        """
        SELECT
          COALESCE(event_class, 'Другое') AS event_class,
          COALESCE(NULLIF(course_code_norm, ''), 'Другое') AS course_code_norm,
          COUNT(DISTINCT ID) AS deals,
          SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
          SUM(revenue_amount) AS revenue
        FROM mart_deals_enriched
        GROUP BY COALESCE(event_class, 'Другое'), COALESCE(NULLIF(course_code_norm, ''), 'Другое')
        ORDER BY revenue DESC
        """,
        GLOBAL_DIR / "event_course.csv",
    )

    # cohort_assoc
    exported["cohort_assoc_contacts"] = _sql_to_csv(
        engine,
        """
        SELECT
          "Контакт: ID" AS contact_id,
          COUNT(DISTINCT ID) AS deals_total,
          SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
          SUM(revenue_amount) AS revenue,
          CASE WHEN SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) = 0 THEN 0
               ELSE SUM(revenue_amount) * 1.0 / SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END)
          END AS avg_check
        FROM mart_attacking_january_cohort_deals
        WHERE COALESCE("Контакт: ID", '') <> ''
        GROUP BY "Контакт: ID"
        ORDER BY revenue DESC
        """,
        COHORT_DIR / "cohort_assoc_contacts.csv",
    )
    exported["cohort_assoc_event_course"] = _sql_to_csv(
        engine,
        """
        SELECT
          COALESCE(event_class, 'Другое') AS event_class,
          COALESCE(NULLIF(course_code_norm, ''), 'Другое') AS course_code_norm,
          COUNT(DISTINCT ID) AS deals,
          SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
          SUM(revenue_amount) AS revenue
        FROM mart_attacking_january_cohort_deals
        GROUP BY COALESCE(event_class, 'Другое'), COALESCE(NULLIF(course_code_norm, ''), 'Другое')
        ORDER BY revenue DESC
        """,
        COHORT_DIR / "cohort_assoc_event_course.csv",
    )

    # Yandex dedup block
    exported["yandex_dedup_summary"] = _sql_to_csv(
        engine,
        """
        SELECT
          (SELECT COUNT(*) FROM mart_yandex_leads_raw) AS leads_raw,
          (SELECT COUNT(*) FROM mart_yandex_leads_dedup) AS leads_dedup,
          (SELECT COALESCE(SUM(is_paid_deal),0) FROM mart_yandex_leads_raw) AS paid_deals_raw,
          (SELECT COALESCE(SUM(paid_deals),0) FROM mart_yandex_leads_dedup) AS paid_deals_dedup,
          (SELECT COALESCE(SUM(revenue_amount),0) FROM mart_yandex_leads_raw) AS revenue_raw,
          (SELECT COALESCE(SUM(revenue),0) FROM mart_yandex_leads_dedup) AS revenue_dedup
        """,
        GLOBAL_DIR / "yandex_dedup_summary.csv",
    )
    exported["yandex_projects_revenue_raw_vs_dedup"] = _sql_to_csv(
        engine,
        """
        SELECT
          r.project_name,
          r.yandex_month AS month,
          r.leads_raw,
          d.leads_dedup,
          r.paid_deals_raw,
          d.paid_deals_dedup,
          r.revenue_raw,
          d.revenue_dedup,
          r.spend
        FROM mart_yandex_revenue_projects_raw r
        LEFT JOIN mart_yandex_revenue_projects_dedup d
          ON r.project_name = d.project_name
         AND r.yandex_month = d.yandex_month
        ORDER BY r.revenue_raw DESC
        """,
        GLOBAL_DIR / "yandex_projects_revenue_raw_vs_dedup.csv",
    )
    with engine.connect() as conn:
        no_month_raw = pd.read_sql_query(
            text(
                """
                SELECT
                  project_name,
                  SUM(leads_raw) AS leads_raw,
                  SUM(paid_deals_raw) AS payments_count,
                  SUM(paid_deals_raw) AS paid_deals_raw,
                  SUM(revenue_raw) AS revenue_raw,
                  SUM(spend) AS spend
                FROM mart_yandex_revenue_projects_raw
                GROUP BY project_name
                ORDER BY revenue_raw DESC
                """
            ),
            conn,
        )
    no_month_grouped = _group_yandex_projects_no_month(no_month_raw, threshold=0.6)
    no_month_path = GLOBAL_DIR / "yandex_projects_revenue_no_month.csv"
    no_month_grouped.to_csv(no_month_path, index=False, encoding="utf-8")
    exported["yandex_projects_revenue_no_month"] = int(len(no_month_grouped))
    exported["yandex_projects_revenue_by_month"] = _sql_to_csv(
        engine,
        """
                WITH matched AS (
                    SELECT
                        yandex_month AS month,
                        SUM(leads_raw) AS leads_raw,
                        SUM(paid_deals_raw) AS paid_deals_raw,
                        SUM(revenue_raw) AS revenue_raw
                    FROM mart_yandex_revenue_projects_raw
                    GROUP BY yandex_month
                ),
                ystats AS (
                    SELECT
                        month,
                        SUM(COALESCE("Клики", 0)) AS clicks,
                        SUM(COALESCE("Расход, ₽", 0)) AS spend
                    FROM stg_yandex_stats
                    WHERE COALESCE(month, '') <> ''
                    GROUP BY month
                ),
                all_months AS (
                    SELECT month FROM matched
                    UNION
                    SELECT month FROM ystats
                )
                SELECT
                    m.month AS month,
                    COALESCE(ma.leads_raw, 0) AS leads_raw,
                    COALESCE(ma.paid_deals_raw, 0) AS paid_deals_raw,
                    COALESCE(ma.revenue_raw, 0) AS revenue_raw,
                    COALESCE(ys.clicks, 0) AS clicks,
                    COALESCE(ys.spend, 0) AS spend
                FROM all_months m
                LEFT JOIN matched ma ON ma.month = m.month
                LEFT JOIN ystats ys ON ys.month = m.month
                ORDER BY m.month
        """,
        GLOBAL_DIR / "yandex_projects_revenue_by_month.csv",
    )

    return exported


def export_qa(engine) -> dict:
    qa = {}
    qa["other_share_global"] = _sql_to_csv(
        engine,
        """
        SELECT
          COUNT(*) AS revenue_deals,
          SUM(CASE WHEN COALESCE(event_class, 'Другое') = 'Другое' THEN 1 ELSE 0 END) AS other_deals,
          CASE WHEN COUNT(*) = 0 THEN 0
               ELSE SUM(CASE WHEN COALESCE(event_class, 'Другое') = 'Другое' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
          END AS other_share
        FROM mart_deals_enriched
        WHERE is_revenue_variant3 = 1
        """,
        QA_DIR / "other_share_global.csv",
    )
    qa["other_top50_cohort"] = _sql_to_csv(
        engine,
        """
        SELECT
          ID,
          "Контакт: ID" AS contact_id,
          "Название сделки" AS deal_name,
          "Код_курса_сайт" AS course_code_site,
          "Код курса" AS course_code,
          "UTM Campaign" AS utm_campaign,
          "Источник (подробно)" AS source_detail,
          "Источник обращения" AS source_ref,
          revenue_amount,
          classification_source
        FROM mart_attacking_january_cohort_deals
        WHERE is_revenue_variant3 = 1
          AND COALESCE(event_class, 'Другое') = 'Другое'
        ORDER BY revenue_amount DESC
        LIMIT 50
        """,
        QA_DIR / "other_top50_cohort.csv",
    )
    qa["dedup_check"] = _sql_to_csv(
        engine,
        """
        SELECT
          COUNT(*) AS rows_in_mart,
          COUNT(DISTINCT ID) AS distinct_ids,
          COUNT(*) - COUNT(DISTINCT ID) AS duplicate_rows
        FROM mart_deals_enriched
        """,
        QA_DIR / "dedup_check.csv",
    )
    qa["yandex_dedup_keys_top_collisions"] = _sql_to_csv(
        engine,
        """
        SELECT
          lead_key,
          COUNT(*) AS rows_count,
          COUNT(DISTINCT project_name) AS projects_count,
          SUM(revenue_amount) AS revenue_raw
        FROM mart_yandex_leads_raw
        GROUP BY lead_key
        HAVING COUNT(*) > 1
        ORDER BY rows_count DESC, revenue_raw DESC
        LIMIT 100
        """,
        QA_DIR / "yandex_dedup_keys_top_collisions.csv",
    )
    qa["yandex_raw_vs_dedup_delta"] = _sql_to_csv(
        engine,
        """
        SELECT
          s.leads_raw,
          s.leads_dedup,
          (s.leads_raw - s.leads_dedup) AS leads_delta,
          s.paid_deals_raw,
          s.paid_deals_dedup,
          (s.paid_deals_raw - s.paid_deals_dedup) AS paid_deals_delta,
          s.revenue_raw,
          s.revenue_dedup,
          (s.revenue_raw - s.revenue_dedup) AS revenue_delta
        FROM (
          SELECT
            (SELECT COUNT(*) FROM mart_yandex_leads_raw) AS leads_raw,
            (SELECT COUNT(*) FROM mart_yandex_leads_dedup) AS leads_dedup,
            (SELECT COALESCE(SUM(is_paid_deal),0) FROM mart_yandex_leads_raw) AS paid_deals_raw,
            (SELECT COALESCE(SUM(paid_deals),0) FROM mart_yandex_leads_dedup) AS paid_deals_dedup,
            (SELECT COALESCE(SUM(revenue_amount),0) FROM mart_yandex_leads_raw) AS revenue_raw,
            (SELECT COALESCE(SUM(revenue),0) FROM mart_yandex_leads_dedup) AS revenue_dedup
        ) s
        """,
        QA_DIR / "yandex_raw_vs_dedup_delta.csv",
    )
    qa["yandex_unmatched_to_bitrix"] = _sql_to_csv(
        engine,
        """
        SELECT
          COALESCE(y."Название кампании", '') AS project_name,
          COALESCE(y."№ Кампании", '') AS campaign_id,
          COALESCE(y.month, '') AS month,
          COUNT(*) AS yandex_rows
        FROM stg_yandex_stats y
        WHERE NOT EXISTS (
          SELECT 1
          FROM mart_yandex_leads_raw r
          WHERE COALESCE(r.campaign_id, '') = COALESCE(y."№ Кампании", '')
            AND COALESCE(r.yandex_month, '') = COALESCE(y.month, '')
        )
        GROUP BY COALESCE(y."Название кампании", ''), COALESCE(y."№ Кампании", ''), COALESCE(y.month, '')
        ORDER BY yandex_rows DESC
        """,
        QA_DIR / "yandex_unmatched_to_bitrix.csv",
    )
    qa["yandex_campaign_mapping_seed"] = _sql_to_csv(
        engine,
        """
        WITH grouped AS (
          SELECT
            COALESCE(project_name, '') AS project_name,
            COALESCE(campaign_id, '') AS campaign_id,
            COALESCE(yandex_month, '') AS month,
            COALESCE(deal_name, '') AS deal_name,
            COUNT(*) AS candidate_rows
          FROM mart_yandex_leads_raw
          GROUP BY COALESCE(project_name, ''), COALESCE(campaign_id, ''), COALESCE(yandex_month, ''), COALESCE(deal_name, '')
        ),
        ranked AS (
          SELECT
            project_name,
            campaign_id,
            month,
            deal_name,
            candidate_rows,
            SUM(candidate_rows) OVER (PARTITION BY project_name, campaign_id, month) AS yandex_rows,
            ROW_NUMBER() OVER (
              PARTITION BY project_name, campaign_id, month
              ORDER BY candidate_rows DESC, deal_name
            ) AS rn
          FROM grouped
        )
        SELECT
          project_name,
          campaign_id,
          month,
          yandex_rows,
          '' AS map_to_utm_campaign,
          deal_name AS map_to_project,
          CASE
            WHEN yandex_rows = 0 THEN 0
            ELSE ROUND(candidate_rows * 1.0 / yandex_rows, 4)
          END AS map_confidence,
          CASE
            WHEN COALESCE(deal_name, '') = '' THEN 'no_deal_name_candidate'
            WHEN candidate_rows = yandex_rows THEN 'single_candidate'
            ELSE 'top_candidate_from_deal_name'
          END AS comment
        FROM ranked
        WHERE rn = 1
        ORDER BY yandex_rows DESC, map_confidence DESC
        """,
        QA_DIR / "yandex_campaign_mapping_seed.csv",
    )
    return qa


def main() -> None:
    for p in [GLOBAL_DIR, COHORT_DIR, QA_DIR]:
        p.mkdir(parents=True, exist_ok=True)

    engine = get_engine()
    ensure_schema(engine)

    marts_meta = build_marts(engine)
    yandex_meta = build_yandex_dedup_marts(engine)
    exported = export_slices(engine)
    qa = export_qa(engine)

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "logic_version": "variant3+smart_event_classification_v1",
        "paths": {
            "root": str(REPORTS_ROOT),
            "global": str(GLOBAL_DIR),
            "cohort": str(COHORT_DIR),
            "qa": str(QA_DIR),
        },
        "marts": marts_meta,
        "yandex_marts": yandex_meta,
        "exports": exported,
        "qa_exports": qa,
        "next_step_integration_hint": "Use yandex_projects_revenue_raw_vs_dedup.csv + month_channel_sendsay.csv as input blocks for final_clean_report",
    }
    (REPORTS_ROOT / "run_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
