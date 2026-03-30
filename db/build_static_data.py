"""
Export reports/slices/*.csv to web/public/data/*.json for the static dashboard.

Run from repo root:
  python db/build_static_data.py
"""
from __future__ import annotations

import argparse
import json
import math
import shutil
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SLICES = ROOT / "reports" / "slices"
OUT_DIR = ROOT / "web" / "public" / "data"
STANDALONE_DATA_DIR = ROOT / "web" / "slices-dashboard" / "data"
DEFAULT_OUT_DIRS = (OUT_DIR, STANDALONE_DATA_DIR)


def _cell(v: Any) -> Any:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return None
    if hasattr(v, "item"):
        try:
            return v.item()
        except (ValueError, AttributeError):
            pass
    return v


def df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    df = df.replace({float("nan"): None})
    records = df.to_dict(orient="records")
    out = []
    for row in records:
        out.append({str(k): _cell(v) for k, v in row.items()})
    return out


# Relative path under reports/slices -> { id, title_ru, category }
DATASET_META: Dict[str, Dict[str, str]] = {
    "global/month_channel_bitrix.csv": {
        "id": "global_month_channel_bitrix",
        "title_ru": "Битрикс: месяц × канал (utm)",
        "category": "global",
    },
    "global/month_channel_yandex.csv": {
        "id": "global_month_channel_yandex",
        "title_ru": "Яндекс: месяц",
        "category": "global",
    },
    "global/month_channel_sendsay.csv": {
        "id": "global_month_channel_sendsay",
        "title_ru": "Sendsay / email: месяц",
        "category": "global",
    },
    "global/funnel_stage.csv": {
        "id": "global_funnel_stage",
        "title_ru": "Воронка × стадия",
        "category": "global",
    },
    "global/event_course.csv": {
        "id": "global_event_course",
        "title_ru": "Мероприятие × код курса",
        "category": "global",
    },
    "global/yandex_dedup_summary.csv": {
        "id": "global_yandex_dedup_summary",
        "title_ru": "Яндекс: сводка дедупа",
        "category": "global",
    },
    "global/yandex_projects_revenue_raw_vs_dedup.csv": {
        "id": "global_yandex_projects_raw_vs_dedup",
        "title_ru": "Яндекс: проекты, raw vs dedup",
        "category": "global",
    },
    "global/yandex_projects_revenue_by_month.csv": {
        "id": "global_yandex_revenue_by_month",
        "title_ru": "Яндекс: выручка по месяцам",
        "category": "global",
    },
    "global/yandex_projects_revenue_no_month.csv": {
        "id": "global_yandex_revenue_no_month",
        "title_ru": "Яндекс: проекты без месяца",
        "category": "global",
    },
    "cohorts/attacking_january/cohort_assoc_contacts.csv": {
        "id": "cohort_aj_contacts",
        "title_ru": "Атакующий январь: контакты, ассоц. выручка",
        "category": "cohort",
    },
    "cohorts/attacking_january/cohort_assoc_event_course.csv": {
        "id": "cohort_aj_event_course",
        "title_ru": "Атакующий январь: мероприятие × курс",
        "category": "cohort",
    },
    "qa/other_share_global.csv": {
        "id": "qa_other_share_global",
        "title_ru": "QA: доля «Другое» (глобал)",
        "category": "qa",
    },
    "qa/other_top50_cohort.csv": {
        "id": "qa_other_top50_cohort",
        "title_ru": "QA: топ-50 «Другое» (когорта)",
        "category": "qa",
    },
    "qa/dedup_check.csv": {
        "id": "qa_dedup_check",
        "title_ru": "QA: проверка дедупа сделок",
        "category": "qa",
    },
    "qa/yandex_raw_vs_dedup_delta.csv": {
        "id": "qa_yandex_delta",
        "title_ru": "QA: дельта raw vs dedup (Яндекс)",
        "category": "qa",
    },
    "qa/yandex_dedup_keys_top_collisions.csv": {
        "id": "qa_yandex_collisions",
        "title_ru": "QA: топ коллизий ключей Яндекс",
        "category": "qa",
    },
    "qa/yandex_unmatched_to_bitrix.csv": {
        "id": "qa_yandex_unmatched",
        "title_ru": "QA: Яндекс без стыковки к Битрикс",
        "category": "qa",
    },
    "qa/yandex_campaign_mapping_seed.csv": {
        "id": "qa_yandex_mapping_seed",
        "title_ru": "QA: сид маппинга кампаний",
        "category": "qa",
    },
}


def build(
    out_dir: Path | None = None,
    slices_dir: Path | None = None,
) -> dict:
    sl = Path(slices_dir or SLICES)
    out_dirs: tuple[Path, ...] = (Path(out_dir),) if out_dir else DEFAULT_OUT_DIRS

    datasets: List[Dict[str, Any]] = []
    slice_json: Dict[str, str] = {}

    for csv_rel, meta in sorted(DATASET_META.items()):
        csv_path = sl / csv_rel
        json_rel = csv_rel.replace(".csv", ".json")

        if not csv_path.exists():
            slice_json[posix_rel(json_rel)] = "[]"
            datasets.append(
                {
                    **meta,
                    "jsonPath": f"data/{posix_rel(json_rel)}",
                    "csvRel": csv_rel.replace("\\", "/"),
                    "rowCount": 0,
                    "missing": True,
                }
            )
            continue

        df = pd.read_csv(csv_path, encoding="utf-8", low_memory=False)
        records = df_to_records(df)
        slice_json[posix_rel(json_rel)] = json.dumps(
            records, ensure_ascii=False, indent=2
        )
        datasets.append(
            {
                **meta,
                "jsonPath": f"data/{posix_rel(json_rel)}",
                "csvRel": csv_rel.replace("\\", "/"),
                "rowCount": len(records),
                "columns": list(df.columns),
                "missing": False,
            }
        )

    datasets_payload = json.dumps(
        {
            "generated_note": "Built by db/build_static_data.py from reports/slices",
            "datasets": sorted(datasets, key=lambda x: (x["category"], x["title_ru"])),
        },
        ensure_ascii=False,
        indent=2,
    )

    manifest_src = sl / "run_manifest.json"
    for out in out_dirs:
        out.mkdir(parents=True, exist_ok=True)
        if manifest_src.exists():
            shutil.copy(manifest_src, out / "run.json")
        else:
            (out / "run.json").write_text("{}", encoding="utf-8")
        (out / "datasets.json").write_text(datasets_payload, encoding="utf-8")
        for rel, body in slice_json.items():
            p = out / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(body, encoding="utf-8")

    return {
        "out_dirs": [str(p) for p in out_dirs],
        "datasets": len(datasets),
        "slices_dir": str(sl),
    }


def posix_rel(path: str) -> str:
    return str(path).replace("\\", "/")


def main() -> None:
    ap = argparse.ArgumentParser(description="Export slice CSVs to web/public/data JSON")
    ap.add_argument("--out", type=Path, default=None, help="Output directory")
    ap.add_argument("--slices", type=Path, default=None, help="reports/slices root")
    args = ap.parse_args()
    info = build(out_dir=args.out, slices_dir=args.slices)
    print(json.dumps(info, indent=2))


if __name__ == "__main__":
    main()
