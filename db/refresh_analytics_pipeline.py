#!/usr/bin/env python3
"""
Refresh all Bitrix-derived analytics JSON for the local web app (and optionally webpush).

Сделки для метрик и воронок берутся из объединения ``bitrix_19.03.26.csv`` +
``bitrix_60_days_03.04.2026.csv`` (см. ``db/bitrix_union_io.py``). Опция ``--merge`` по-прежнему
может собирать full-history base + update для прочих сценариев, но основной веб-пайплайн
смотрит на этот union.

This is the single entry point: it ensures repo-root `.venv` exists, installs deps when
needed, then runs the db scripts in the right order. No manual Python/venv hunting.

From repo root (use ``python3`` so you do not need ``chmod +x``):

  python3 db/refresh_analytics_pipeline.py
  python3 db/refresh_analytics_pipeline.py --merge --update sheets/bitrix_upd_27.03.csv
  python3 db/refresh_analytics_pipeline.py --webpush

**D1 / Cloudflare:** use ``--d1-only`` to skip legacy local CSV→JSON exports while still running
``run_all_slices`` (fills ``raw_bitrix_deals``, ``stg_deals_analytics``, marts, staging). Then push to D1 and
rebuild analytics in the cloud:

  python3 db/refresh_analytics_pipeline.py --d1-only --d1-sync

    # optional incremental raw Bitrix upsert before slices:
    python3 db/refresh_analytics_pipeline.py --upsert-raw-bitrix --d1-only --d1-sync

  # or step-by-step:
  python3 db/refresh_analytics_pipeline.py --d1-only --push-d1 --d1-rebuild

Requires ``CLOUDFLARE_API_TOKEN`` (or ``wrangler login``) for ``--push-d1``. For ``--d1-rebuild``
set ``D1_ANALYTICS_REBUILD_URL`` (full URL to ``POST /api/analytics/rebuild``) and
``ANALYTICS_REBUILD_SECRET`` in the environment.

If you see "permission denied" on ``./db/refresh_analytics_pipeline.py``, either run with
``python3`` as above or ``chmod +x db/refresh_analytics_pipeline.py``.

`--merge` = full-history base CSV + smaller export keyed by ID (see merge_bitrix_csv.py).
"""
from __future__ import annotations

import argparse
import os
import platform
import stat
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
REQUIREMENTS = REPO_ROOT / "requirements.txt"
VENV_DIR = REPO_ROOT / ".venv"
THIS_FILE = Path(__file__).resolve()


def venv_python() -> Path:
    if platform.system() == "Windows":
        return VENV_DIR / "Scripts" / "python.exe"
    p3 = VENV_DIR / "bin" / "python3"
    if p3.exists():
        return p3
    return VENV_DIR / "bin" / "python"


def _ensure_executable(interpreter: Path) -> None:
    try:
        mode = interpreter.stat().st_mode
        if not (mode & stat.S_IXUSR):
            interpreter.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    except OSError:
        pass


def ensure_venv() -> Path:
    py = venv_python()
    if not VENV_DIR.is_dir():
        print("Creating", VENV_DIR, "…", flush=True)
        subprocess.run([sys.executable, "-m", "venv", str(VENV_DIR)], check=True, cwd=str(REPO_ROOT))
        py = venv_python()
    if not py.exists():
        raise SystemExit(f"venv python missing after create: {py}")
    _ensure_executable(py.resolve())
    return py


def delegate_to_venv(py: Path) -> None:
    """Run this script again under venv Python (avoids os.execv permission issues on some setups)."""
    if Path(sys.executable).resolve() == py.resolve():
        return
    r = subprocess.run([str(py), str(THIS_FILE), *sys.argv[1:]], cwd=str(REPO_ROOT))
    raise SystemExit(r.returncode)


def deps_ok(py: Path) -> bool:
    cmd = [
        str(py),
        "-c",
        "import pandas, sqlalchemy",
    ]
    return subprocess.run(cmd, cwd=str(REPO_ROOT)).returncode == 0


def pip_install(py: Path) -> None:
    print("Installing pipeline deps from requirements.txt …", flush=True)
    subprocess.run(
        [str(py), "-m", "pip", "install", "-q", "--upgrade", "pip"],
        check=True,
        cwd=str(REPO_ROOT),
    )
    subprocess.run(
        [str(py), "-m", "pip", "install", "-q", "-r", str(REQUIREMENTS)],
        check=True,
        cwd=str(REPO_ROOT),
    )


def run_step(py: Path, script: str, *args: str) -> None:
    path = REPO_ROOT / "db" / script
    cmd = [str(py), str(path), *args]
    print("\n→", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True, cwd=str(REPO_ROOT))


def run_push_d1(py: Path, *, remote: bool, strict: bool = True) -> None:
    """Push local SQLite + JSON snapshots to Cloudflare D1 (see db/d1/push_from_sqlite.py)."""
    wrangler = REPO_ROOT / "web" / "wrangler.toml"
    args = [
        str(py),
        str(REPO_ROOT / "db" / "d1" / "push_from_sqlite.py"),
        "--wrangler-config",
        str(wrangler),
    ]
    if remote:
        args.append("--remote")
    if strict:
        args.append("--strict")
    print("\n→", " ".join(args), flush=True)
    subprocess.run(args, check=True, cwd=str(REPO_ROOT))


def run_d1_rebuild(*, url: str | None) -> None:
    """POST /api/analytics/rebuild on Pages (Workers rebuild marts + dataset_json)."""
    base = (url or os.environ.get("D1_ANALYTICS_REBUILD_URL", "")).strip()
    secret = os.environ.get("ANALYTICS_REBUILD_SECRET", "").strip()
    if not base or not secret:
        raise SystemExit(
            "D1 rebuild: set D1_ANALYTICS_REBUILD_URL (e.g. https://<project>.pages.dev/api/analytics/rebuild) "
            "and ANALYTICS_REBUILD_SECRET in the environment, or pass --d1-rebuild-url for the URL only."
        )
    req = urllib.request.Request(
        base,
        method="POST",
        headers={"Authorization": f"Bearer {secret}", "Content-Type": "application/json"},
        data=b"{}",
    )
    print("\n→ POST", base, flush=True)
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        raise SystemExit(f"D1 rebuild failed: HTTP {e.code} {e.reason}\n{err_body}") from e
    except urllib.error.URLError as e:
        raise SystemExit(f"D1 rebuild failed: {e}") from e
    print("D1 rebuild OK:", body[:800], flush=True)


def strip_cf_oversized_assets(webpush_data: Path) -> None:
    """Cloudflare ~25 MiB per file; webpush UI does not need deals_base."""
    for name in ("attacking_january_associative_deals_base.json",):
        p = webpush_data / name
        if p.is_file():
            p.unlink()
            print("Removed (CF limit):", p, flush=True)


def sync_webpush(no_delete: bool) -> None:
    src = REPO_ROOT / "web" / "public" / "data"
    dst = REPO_ROOT / "web_share_subset" / "webpush" / "public" / "data"
    if not dst.parent.is_dir():
        print("Skipping --webpush: missing", dst.parent, flush=True)
        return
    dst.mkdir(parents=True, exist_ok=True)
    rsync = ["rsync", "-a"]
    if not no_delete:
        rsync.append("--delete")
    rsync.extend([f"{src}/", f"{dst}/"])
    print("\n→", " ".join(rsync), flush=True)
    subprocess.run(rsync, check=True, cwd=str(REPO_ROOT))
    strip_cf_oversized_assets(dst)


def main() -> None:
    # Run the rest of this function under .venv Python (parent only delegates).
    py = ensure_venv()
    delegate_to_venv(py)

    ap = argparse.ArgumentParser(
        description="Refresh analytics: venv + Bitrix slices + AJ + managers + slices + static JSON.",
    )
    ap.add_argument(
        "--merge",
        action="store_true",
        help="Merge full base CSV with patch export into sheets/bitrix_19.03.26 (see --base/--update).",
    )
    ap.add_argument(
        "--base",
        type=Path,
        default=REPO_ROOT / "sheets" / "bitrix_19.03.26.bak_before_upd_27.03",
        help="Full-history Bitrix export (used with --merge).",
    )
    ap.add_argument(
        "--update",
        type=Path,
        default=REPO_ROOT / "sheets" / "bitrix_upd_27.03.csv",
        help="Patch/export CSV whose rows override same ID in base (used with --merge).",
    )
    ap.add_argument(
        "--out-csv",
        type=Path,
        default=REPO_ROOT / "sheets" / "bitrix_19.03.26",
        dest="out_csv",
        help="Active Bitrix path all db scripts read (--merge output).",
    )
    ap.add_argument(
        "--skip-all-slices",
        action="store_true",
        help="Skip run_all_slices.py (Yandex/global QA slices; needs DB + yandex.csv etc.).",
    )
    ap.add_argument(
        "--upsert-raw-bitrix",
        action="store_true",
        help="Run db/upsert_raw_bitrix_from_union.py before run_all_slices.py.",
    )
    ap.add_argument(
        "--webpush",
        action="store_true",
        help="rsync web/public/data → web_share_subset/webpush/public/data and drop huge JSON.",
    )
    ap.add_argument(
        "--webpush-keep-extra",
        action="store_true",
        help="With --webpush: pass rsync without --delete (keep files only in webpush).",
    )
    ap.add_argument(
        "--pip-always",
        action="store_true",
        help="Always run pip install -r requirements.txt (default: only if imports fail).",
    )
    ap.add_argument(
        "--d1-only",
        action="store_true",
        help="Skip legacy local JSON/report steps (build_bitrix_full_slices, regenerate_associative, "
        "build_manager_handoff, build_static_data). Still runs run_all_slices unless --skip-all-slices "
        "(needed to fill stg_* / marts in SQLite before push_to_d1). Then push and POST /api/analytics/rebuild.",
    )
    ap.add_argument(
        "--push-d1",
        action="store_true",
        help="After other steps: run db/d1/push_from_sqlite.py --remote (needs wrangler + CLOUDFLARE_API_TOKEN or login).",
    )
    ap.add_argument(
        "--push-d1-local",
        action="store_true",
        help="Like --push-d1 but omit --remote (local wrangler D1 / Miniflare testing).",
    )
    ap.add_argument(
        "--d1-rebuild",
        action="store_true",
        help="After other steps: POST /api/analytics/rebuild (env: D1_ANALYTICS_REBUILD_URL, ANALYTICS_REBUILD_SECRET).",
    )
    ap.add_argument(
        "--d1-rebuild-url",
        default=None,
        help="Override D1_ANALYTICS_REBUILD_URL for this run (full URL including /api/analytics/rebuild).",
    )
    ap.add_argument(
        "--d1-sync",
        action="store_true",
        help="Shorthand for --push-d1 --d1-rebuild (remote push + cloud rebuild).",
    )
    ap.add_argument(
        "--no-push-d1-strict",
        action="store_true",
        help="With --push-d1: do not pass --strict to push_from_sqlite (allows sync when marts are empty; not recommended).",
    )
    args = ap.parse_args()
    if args.d1_sync:
        args.push_d1 = True
        args.d1_rebuild = True
    if args.push_d1_local:
        args.push_d1 = True

    if args.pip_always or not deps_ok(py):
        if REQUIREMENTS.is_file():
            pip_install(py)
        elif not deps_ok(py):
            raise SystemExit(f"Missing {REQUIREMENTS} and pandas/sqlalchemy not importable.")

    if args.merge:
        if not args.base.is_file():
            raise SystemExit(f"Missing --base: {args.base}")
        if not args.update.is_file():
            raise SystemExit(f"Missing --update: {args.update}")
        run_step(
            py,
            "merge_bitrix_csv.py",
            "--base",
            str(args.base),
            "--update",
            str(args.update),
            "--out",
            str(args.out_csv),
        )

    if args.upsert_raw_bitrix:
        run_step(py, "upsert_raw_bitrix_from_union.py")

    if not args.d1_only:
        run_step(py, "build_bitrix_full_slices.py")
        run_step(py, "regenerate_associative_variant3.py")
        run_step(py, "build_manager_handoff_report.py")

    if not args.skip_all_slices:
        run_step(py, "run_all_slices.py")

    if not args.d1_only:
        run_step(py, "build_static_data.py")

    if args.webpush:
        sync_webpush(no_delete=args.webpush_keep_extra)

    if args.push_d1:
        run_push_d1(py, remote=not args.push_d1_local, strict=not args.no_push_d1_strict)

    if args.d1_rebuild:
        run_d1_rebuild(url=args.d1_rebuild_url)

    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
