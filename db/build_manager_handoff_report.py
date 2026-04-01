from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from bitrix_lead_quality import apply_notebook_lead_flags, coalesce_columns, drop_rows_excluded_funnels, in_work_series
from bitrix_union_io import DEFAULT_BITRIX_UPD, DEFAULT_FL_RAW, load_bitrix_deals_union
from revenue_variant3 import variant3_revenue_mask

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "reports" / "slices" / "qa"
WEB_DIR = ROOT / "web" / "public" / "data"

FIRSTLINE_EXACT = {
    "алена тиханова",
    "георгий воеводин",
}
SALES_EXACT = {
    "анастасия крисанова",
    "василий гореленков",
    "глеб барбазанов",
    "елена лобода",
}
RU_MONTHS = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}


def _n(v: object) -> str:
    if v is None or pd.isna(v):
        return ""
    s = str(v).strip()
    return "" if s.lower() in {"", "nan", "none", "null"} else s


def _amt(v: object) -> float:
    s = _n(v).replace(" ", "").replace("\xa0", "").replace(",", ".")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _norm_person(v: object) -> str:
    s = _n(v).lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _group_for_person(norm_name: str) -> str:
    if norm_name in FIRSTLINE_EXACT:
        return "firstline"
    if norm_name in SALES_EXACT:
        return "sales"
    return ""


def _to_json_records(df: pd.DataFrame) -> str:
    safe = df.where(pd.notna(df), None)
    return json.dumps(safe.to_dict(orient="records"), ensure_ascii=False, allow_nan=False)


def _parse_course_code(v: object) -> str:
    s = _n(v)
    if not s:
        return "—"
    if ":" in s:
        right = s.split(":", 1)[1].strip()
        return right if right else "—"
    return s


def _month_label(v: object) -> str:
    dt = pd.to_datetime(_n(v), errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return "—"
    return f"{RU_MONTHS.get(int(dt.month), str(dt.month))}, {int(dt.year)}"


def _add_conversions(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    leads = out["Лиды"].replace(0, pd.NA)
    out["Конверсия в Квал"] = (out["Квал"] / leads).fillna(0.0)
    out["Конверсия в Неквал"] = (out["Неквал"] / leads).fillna(0.0)
    out["Конверсия в Отказ"] = (out["Отказы"] / leads).fillna(0.0)
    out["Конверсия в работе"] = (out["В работе"] / leads).fillna(0.0)
    out["Средний_чек"] = (out["Выручка"] / out["Сделок_с_выручкой"].replace(0, pd.NA)).fillna(0.0)
    return out


def _build_hierarchy(df: pd.DataFrame, manager_col: str, child_col: str, child_level_name: str) -> pd.DataFrame:
    rows: list[dict] = []
    managers = sorted(df[manager_col].dropna().astype(str).str.strip().unique().tolist())
    for mgr in managers:
        mdf = df[df[manager_col].astype(str).str.strip() == mgr]
        if mdf.empty:
            continue
        mgr_row = {
            "Level": "Manager",
            "Менеджер": mgr,
            child_level_name: "-",
            "Лиды": int(len(mdf)),
            "Квал": int(mdf["is_qual"].sum()),
            "Неквал": int(mdf["is_unqual"].sum()),
            "Неизвестно": int(mdf["is_unknown"].sum()),
            "Отказы": int(mdf["is_refusal"].sum()),
            "В работе": int(mdf["is_in_work"].sum()),
            "Невалидные_лиды": int(mdf["is_invalid_lead"].sum()),
            "Сделок_с_выручкой": int(mdf.loc[mdf["is_revenue"], "id_norm"].nunique()),
            "Выручка": float(mdf.loc[mdf["is_revenue"], "amount"].sum()),
            "fl_IDs": ",".join(sorted({x for x in mdf["id_norm"].astype(str) if _n(x)})),
        }
        rows.append(mgr_row)
        childs = sorted(mdf[child_col].dropna().astype(str).str.strip().unique().tolist())
        for ch in childs:
            cdf = mdf[mdf[child_col].astype(str).str.strip() == ch]
            if cdf.empty:
                continue
            rows.append(
                {
                    "Level": child_level_name,
                    "Менеджер": mgr,
                    child_level_name: ch,
                    "Лиды": int(len(cdf)),
                    "Квал": int(cdf["is_qual"].sum()),
                    "Неквал": int(cdf["is_unqual"].sum()),
                    "Неизвестно": int(cdf["is_unknown"].sum()),
                    "Отказы": int(cdf["is_refusal"].sum()),
                    "В работе": int(cdf["is_in_work"].sum()),
                    "Невалидные_лиды": int(cdf["is_invalid_lead"].sum()),
                    "Сделок_с_выручкой": int(cdf.loc[cdf["is_revenue"], "id_norm"].nunique()),
                    "Выручка": float(cdf.loc[cdf["is_revenue"], "amount"].sum()),
                    "fl_IDs": ",".join(sorted({x for x in cdf["id_norm"].astype(str) if _n(x)})),
                }
            )
    out = pd.DataFrame(rows)
    if len(out):
        out = _add_conversions(out)
    return out


def run() -> dict:
    d = load_bitrix_deals_union()
    d = drop_rows_excluded_funnels(d)
    d["amount"] = d.get("Сумма", "").map(_amt)
    d["id_norm"] = d.get("ID", "").fillna("").astype(str).str.replace(r"\.0+$", "", regex=True).str.strip()
    d["manager_norm"] = d.get("Ответственный", "").map(_norm_person)
    d["creator_norm"] = d.get("Кем создана", "").map(_norm_person)
    d["manager_group"] = d["manager_norm"].map(_group_for_person)
    d["creator_group"] = d["creator_norm"].map(_group_for_person)
    d = apply_notebook_lead_flags(d)
    d["is_invalid_lead"] = d["is_invalid"]
    d["is_in_work"] = in_work_series(d)
    d["is_revenue"] = variant3_revenue_mask(d)
    d["course_code"] = d.get("Код_курса_сайт", "").map(_parse_course_code)
    d["month_label"] = d.get("Дата создания", "").map(_month_label)

    sales_rows = d[d["manager_group"] == "sales"].copy()
    firstline_rows = d[d["manager_group"] == "firstline"].copy()
    sales_by_course = _build_hierarchy(
        sales_rows,
        manager_col="Ответственный",
        child_col="course_code",
        child_level_name="Код курса",
    )
    sales_by_month = _build_hierarchy(
        sales_rows,
        manager_col="Ответственный",
        child_col="month_label",
        child_level_name="Месяц",
    )
    firstline_by_course = _build_hierarchy(
        firstline_rows,
        manager_col="Ответственный",
        child_col="course_code",
        child_level_name="Код курса",
    )
    firstline_by_month = _build_hierarchy(
        firstline_rows,
        manager_col="Ответственный",
        child_col="month_label",
        child_level_name="Месяц",
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    WEB_DIR.mkdir(parents=True, exist_ok=True)
    paths = {
        "sales_course_csv": OUT_DIR / "manager_sales_by_course.csv",
        "sales_month_csv": OUT_DIR / "manager_sales_by_month.csv",
        "firstline_course_csv": OUT_DIR / "manager_firstline_by_course.csv",
        "firstline_month_csv": OUT_DIR / "manager_firstline_by_month.csv",
        "sales_course_json": WEB_DIR / "manager_sales_by_course.json",
        "sales_month_json": WEB_DIR / "manager_sales_by_month.json",
        "firstline_course_json": WEB_DIR / "manager_firstline_by_course.json",
        "firstline_month_json": WEB_DIR / "manager_firstline_by_month.json",
        "summary_json": OUT_DIR / "manager_handoff_summary.json",
    }
    sales_by_course.to_csv(paths["sales_course_csv"], index=False, encoding="utf-8")
    sales_by_month.to_csv(paths["sales_month_csv"], index=False, encoding="utf-8")
    firstline_by_course.to_csv(paths["firstline_course_csv"], index=False, encoding="utf-8")
    firstline_by_month.to_csv(paths["firstline_month_csv"], index=False, encoding="utf-8")
    paths["sales_course_json"].write_text(_to_json_records(sales_by_course), encoding="utf-8")
    paths["sales_month_json"].write_text(_to_json_records(sales_by_month), encoding="utf-8")
    paths["firstline_course_json"].write_text(_to_json_records(firstline_by_course), encoding="utf-8")
    paths["firstline_month_json"].write_text(_to_json_records(firstline_by_month), encoding="utf-8")

    summary = {
        "source": f"{DEFAULT_FL_RAW.name} + {DEFAULT_BITRIX_UPD.name} (union)",
        "rows_total": int(len(d)),
        "rows_sales": int(len(sales_rows)),
        "rows_firstline": int(len(firstline_rows)),
        "non_managers_excluded": int(
            len(d[(d["manager_group"] == "") & (d["creator_group"] == "")])
        ),
        "outputs": {k: str(v) for k, v in paths.items()},
    }
    paths["summary_json"].write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))

