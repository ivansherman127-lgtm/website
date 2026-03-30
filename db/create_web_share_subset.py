from __future__ import annotations

import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC_WEB = ROOT / "web"
OUT = ROOT / "web_share_subset"

# Only files needed by current webpage app.
WEB_FILES = [
    "index.html",
    "package.json",
    "package-lock.json",
    "tsconfig.json",
    "tsconfig.node.json",
    "vite.config.ts",
    "src/main.ts",
    "src/app.ts",
    "src/style.css",
    "src/vite-env.d.ts",
]

DATA_FILES = [
    "attacking_january_associative_revenue_by_month.json",
    "attacking_january_associative_revenue_by_events.json",
    "attacking_january_associative_revenue_by_course_codes.json",
    "email_hierarchy_by_send.json",
    "yd_hierarchy.json",
    "bitrix_month_total_full.json",
    "manager_sales_by_course.json",
    "manager_sales_by_month.json",
    "manager_firstline_by_course.json",
    "manager_firstline_by_month.json",
    "bitrix_funnel_month_code_full.json",
    "bitrix_contacts_uid.json",
    "member_list_03.26_statuses_categories.json",
]


def _copy_rel(src_root: Path, dst_root: Path, rel: str) -> None:
    src = src_root / rel
    if not src.exists():
        return
    dst = dst_root / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def run() -> dict:
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True, exist_ok=True)

    for rel in WEB_FILES:
        _copy_rel(SRC_WEB, OUT, rel)

    for rel in DATA_FILES:
        _copy_rel(SRC_WEB / "public" / "data", OUT / "public" / "data", rel)

    (OUT / ".gitignore").write_text(
        "node_modules/\n"
        "dist/\n"
        ".DS_Store\n",
        encoding="utf-8",
    )

    (OUT / "README.md").write_text(
        "# Web Share Subset\n\n"
        "This folder is a lightweight repo subset for deploying the webpage.\n\n"
        "## Run locally\n\n"
        "```bash\n"
        "npm install\n"
        "npm run dev\n"
        "```\n\n"
        "## Update workflow (JSON-only)\n\n"
        "1. In the main project, regenerate needed JSON files.\n"
        "2. Copy updated files into `public/data/` here.\n"
        "3. Commit and push this subset repo.\n\n"
        "## Required data files\n\n"
        + "\n".join(f"- `public/data/{f}`" for f in DATA_FILES)
        + "\n",
        encoding="utf-8",
    )

    manifest = {"out_dir": str(OUT), "web_files": WEB_FILES, "data_files": DATA_FILES}
    (OUT / "subset_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
