"""
CSV → SQL import (one-time migration and optional re-import).
Run after ensure_schema(). Paths are relative to project root.
"""
from pathlib import Path
from typing import Optional
import pandas as pd

from .conn import get_engine, ensure_schema, DEFAULT_DB_PATH

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Paths to CSVs (configurable)
FIRSTLINE_CSV = PROJECT_ROOT / "sheets" / "stat.uni" / "fl_raw_09-03.csv"
YANDEX_CSV = PROJECT_ROOT / "yandex.csv"
MASS_EMAIL_CSV = PROJECT_ROOT / "sheets" / "mass_email_good.csv"

# Deals: columns we store (must match schema)
DEALS_COLS = [
    "ID",
    "Воронка",
    "Стадия сделки",
    "Дата создания",
    "UTM Source",
    "UTM Medium",
    "UTM Campaign",
    "UTM Content",
    "UTM Term",
]


def _coerce_deals(df: pd.DataFrame) -> pd.DataFrame:
    out = df[DEALS_COLS].copy()
    out["ID"] = out["ID"].astype(str).str.strip()
    return out


def import_firstline_from_csv(
    csv_path: Optional[Path] = None,
    engine=None,
    if_exists: str = "replace",
) -> int:
    csv_path = csv_path or FIRSTLINE_CSV
    if not csv_path.exists():
        return 0
    engine = engine or get_engine()
    kw = dict(sep=";", low_memory=False, dtype={"ID": str}, encoding="utf-8")
    try:
        df = pd.read_csv(csv_path, **kw, on_bad_lines="warn")
    except TypeError:
        df = pd.read_csv(csv_path, **kw, error_bad_lines=False, warn_bad_lines=True)
    df["ID"] = df["ID"].astype(str).str.strip()
    subset = _coerce_deals(df)
    subset.to_sql("deals", engine, if_exists=if_exists, index=False)
    return len(subset)


def import_yandex_from_csv(
    csv_path: Optional[Path] = None,
    engine=None,
    if_exists: str = "replace",
) -> int:
    csv_path = csv_path or YANDEX_CSV
    if not csv_path.exists():
        return 0
    engine = engine or get_engine()
    df = pd.read_csv(csv_path, encoding="utf-8", low_memory=False)
    # Map to schema columns (drop extras like "Путь до изображения", etc.)
    schema_cols = [
        "Месяц",
        "№ Кампании",
        "Название кампании",
        "№ Группы",
        "Название группы",
        "№ Объявления",
        "Статус объявления",
        "Тип объявления",
        "Заголовок",
        "Текст",
        "Ссылка",
        "Расход, ₽",
        "Клики",
        "Конверсии",
        "CR, %",
        "CPA, ₽",
    ]
    # Allow missing columns
    use = [c for c in schema_cols if c in df.columns]
    out = df[use].copy()
    out.to_sql("yandex_stats", engine, if_exists=if_exists, index=False)
    return len(out)


def import_mass_email_from_csv(
    csv_path: Optional[Path] = None,
    engine=None,
    if_exists: str = "replace",
) -> int:
    csv_path = csv_path or MASS_EMAIL_CSV
    if not csv_path.exists():
        return 0
    engine = engine or get_engine()
    df = pd.read_csv(csv_path, low_memory=False, encoding="utf-8")
    schema_cols = [
        "Дата отправки",
        "Название выпуска",
        "Получатели",
        "Тема",
        "Отправлено",
        "Доставлено",
        "Ошибок",
        "Открытий",
        "Уник. открытий",
        "Кликов",
        "Уник. кликов",
        "CTOR, %",
        "Отписок",
        "UTOR, %",
        "ID",
        "Номер задания",
        "utm_campaign",
        "utm_content",
        "utm_medium",
        "utm_source",
        "utm_term",
    ]
    use = [c for c in schema_cols if c in df.columns]
    out = df[use].copy()
    out.to_sql("email_sends", engine, if_exists=if_exists, index=False)
    return len(out)


def run_csv_import(
    firstline_path: Optional[Path] = None,
    yandex_path: Optional[Path] = None,
    mass_email_path: Optional[Path] = None,
    db_path: Optional[str] = None,
) -> dict:
    """Run full CSV → SQL import; ensure schema first. Returns row counts."""
    engine = get_engine(db_path)
    ensure_schema(engine)
    counts = {}
    counts["deals"] = import_firstline_from_csv(firstline_path, engine=engine)
    counts["yandex_stats"] = import_yandex_from_csv(yandex_path, engine=engine)
    counts["email_sends"] = import_mass_email_from_csv(mass_email_path, engine=engine)
    return counts


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Import CSVs into deved SQLite DB")
    p.add_argument("--db", default=None, help="DB path (default: project root / deved.db)")
    p.add_argument("--firstline", type=Path, default=None)
    p.add_argument("--yandex", type=Path, default=None)
    p.add_argument("--mass-email", type=Path, default=None)
    args = p.parse_args()
    counts = run_csv_import(
        firstline_path=args.firstline,
        yandex_path=args.yandex,
        mass_email_path=args.mass_email,
        db_path=args.db,
    )
    print("Imported:", counts)
