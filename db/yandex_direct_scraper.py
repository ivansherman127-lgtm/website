"""db/yandex_direct_scraper.py
Yandex Direct reports wizard → stg_yandex_stats (Playwright web scraper).

Logs into Yandex, navigates to the pre-configured reports wizard, sets the date
range, downloads the CSV, and ingests it via upsert_yandex_from_csv.

First-time setup (get a clean session, handles 2FA interactively):
    python -m db.yandex_direct_scraper --login

Normal incremental run:
    python -m db.yandex_direct_scraper

Full re-download (last 90 days):
    python -m db.yandex_direct_scraper --full

Daemon mode:
    python -m db.yandex_direct_scraper --watch --interval 180

Required env vars:
    YANDEX_LOGIN    — Yandex login (e.g. ivansherman127@yandex.ru)
    YANDEX_PASSWORD — Yandex account password

Optional env vars:
    YANDEX_REPORT_URL     — override the wizard URL (default: state=2619475)
    ANALYTICS_REBUILD_URL — POST here after each ingest
    ANALYTICS_REBUILD_SECRET
    WEBSITE_DB_PATH
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tempfile
import time
import urllib.request
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = os.environ.get(
    "WEBSITE_DB_PATH", str(ROOT / "website.db")
)
SESSION_FILE = ROOT / "web_share_subset" / "webpush" / ".yandex_session.json"
ENV_FILE     = ROOT / "web_share_subset" / "webpush" / ".env.server.json"

DEFAULT_REPORT_URL = (
    "https://direct.yandex.ru/dna/reports/wizard"
    "?ulogin=ivansherman127&state=2619475"
)
DEFAULT_LOOKBACK_DAYS = 90
DOWNLOAD_TIMEOUT_MS   = 120_000   # 2 minutes for report generation
PAGE_LOAD_TIMEOUT_MS  = 60_000

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Credentials helpers
# ---------------------------------------------------------------------------

def _read_env_file() -> dict:
    if ENV_FILE.exists():
        try:
            return json.loads(ENV_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _get_creds() -> tuple[str, str]:
    secrets = _read_env_file()
    login    = os.environ.get("YANDEX_LOGIN",    secrets.get("YANDEX_LOGIN",    "")).strip()
    password = os.environ.get("YANDEX_PASSWORD", secrets.get("YANDEX_PASSWORD", "")).strip()
    if not login or not password:
        print(
            "ERROR: YANDEX_LOGIN and YANDEX_PASSWORD must be set (env var or .env.server.json).",
            file=sys.stderr,
        )
        sys.exit(1)
    return login, password


# ---------------------------------------------------------------------------
# Sync date helpers (reuse raw_yandex_meta table)
# ---------------------------------------------------------------------------

def _ensure_meta(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_yandex_meta (
            entity         TEXT PRIMARY KEY,
            last_sync_date TEXT NOT NULL
        )
    """)
    conn.commit()


def get_last_sync_date(conn: sqlite3.Connection) -> Optional[str]:
    _ensure_meta(conn)
    row = conn.execute(
        "SELECT last_sync_date FROM raw_yandex_meta WHERE entity = 'scraper'",
    ).fetchone()
    return row[0] if row else None


def set_last_sync_date(conn: sqlite3.Connection, d: str) -> None:
    _ensure_meta(conn)
    conn.execute(
        "INSERT OR REPLACE INTO raw_yandex_meta (entity, last_sync_date) VALUES ('scraper', ?)",
        (d,),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Analytics rebuild trigger (same as yandex_api_sync)
# ---------------------------------------------------------------------------

def trigger_analytics_rebuild(port: int = 3000) -> None:
    url    = os.environ.get("ANALYTICS_REBUILD_URL", f"http://127.0.0.1:{port}/api/analytics/rebuild")
    secret = os.environ.get("ANALYTICS_REBUILD_SECRET", _read_env_file().get("ANALYTICS_REBUILD_SECRET", ""))
    try:
        payload = json.dumps({"force": False}).encode("utf-8")
        req = urllib.request.Request(url, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        if secret:
            req.add_header("Authorization", f"Bearer {secret}")
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read().decode())
        skipped = result.get("skipped") or (result.get("result") or {}).get("skipped")
        if skipped:
            print("  [rebuild] Analytics up to date — skipped.")
        else:
            paths = (result.get("result") or {}).get("dataset_paths", "?")
            print(f"  [rebuild] Analytics rebuild complete — {paths} paths materialized.")
    except Exception as exc:
        print(f"  [rebuild] Warning: analytics rebuild call failed: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Playwright browser context helpers
# ---------------------------------------------------------------------------

def _make_context(p, headless: bool = True):
    """Create a browser context, loading saved session if available."""
    browser = p.chromium.launch(headless=headless)
    ctx_args: dict = {"user_agent": _USER_AGENT}
    if SESSION_FILE.exists():
        ctx_args["storage_state"] = str(SESSION_FILE)
        print("  [browser] Loaded saved session from", SESSION_FILE)
    return browser, browser.new_context(**ctx_args)


def _save_session(ctx) -> None:
    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    ctx.storage_state(path=str(SESSION_FILE))
    print("  [browser] Session saved to", SESSION_FILE)


def _do_login(page, login: str, password: str) -> bool:
    """
    Perform Yandex Passport login. Returns True on success.
    Expects page to already be on a passport auth URL.
    """
    print("  [login] Filling login field…")
    # Yandex passport login: two-step form (login → Next → password → Sign in)
    try:
        page.wait_for_selector(
            "#passp-field-login, input[autocomplete='username'], input[name='login']",
            timeout=15000,
        )
    except Exception:
        print("  [login] ERROR: login input not found. Page:", page.url[:100], file=sys.stderr)
        page.screenshot(path="/tmp/yd_login_fail.png")
        print("  [login] Screenshot saved to /tmp/yd_login_fail.png", file=sys.stderr)
        return False

    page.fill(
        "#passp-field-login, input[autocomplete='username'], input[name='login']",
        login,
    )
    # Click the "Next" / "Войти" button
    page.click("button[type=submit]")
    page.wait_for_timeout(2500)

    # Wait for password field
    print("  [login] Waiting for password field…")
    try:
        page.wait_for_selector(
            "#passp-field-passwd, input[type='password'], input[name='passwd']",
            timeout=10000,
        )
    except Exception:
        # May already be on the Direct page (SSO / remembered session)
        if "direct.yandex.ru" in page.url:
            print("  [login] SSO succeeded without password step.")
            return True
        # May be captcha
        if "captcha" in page.url or "showcaptcha" in page.url:
            print("  [login] CAPTCHA detected — cannot proceed headlessly.", file=sys.stderr)
            print("  [login] Run with --login to handle CAPTCHA manually.", file=sys.stderr)
            return False
        print("  [login] ERROR: password field not found. Page:", page.url[:100], file=sys.stderr)
        page.screenshot(path="/tmp/yd_passwd_fail.png")
        return False

    page.fill(
        "#passp-field-passwd, input[type='password'], input[name='passwd']",
        password,
    )
    page.click("button[type=submit]")

    # Wait for redirect away from passport
    print("  [login] Waiting for redirect after password…")
    try:
        page.wait_for_url("**/direct.yandex.ru/**", timeout=15000)
        print("  [login] Login succeeded.")
        return True
    except Exception:
        # Check for 2FA
        if "passport" in page.url and ("sms" in page.url or "code" in page.url or "challenge" in page.url):
            print("  [login] 2FA challenge detected.", file=sys.stderr)
            if not sys.stdout.isatty():
                print("  [login] Running non-interactively — cannot complete 2FA.", file=sys.stderr)
                print("  [login] Run 'python -m db.yandex_direct_scraper --login' from a terminal.", file=sys.stderr)
                return False
            # Interactive mode: wait for user to complete 2FA
            print("  [login] Please complete 2FA in the browser window, then press Enter here.")
            input()
            page.wait_for_timeout(2000)
            return True
        print("  [login] ERROR: unexpected URL after login:", page.url[:100], file=sys.stderr)
        page.screenshot(path="/tmp/yd_redirect_fail.png")
        return False


def _is_logged_in(page) -> bool:
    """Navigate to Direct root and check we're not on passport."""
    page.goto(
        "https://direct.yandex.ru/registered/main.pl",
        wait_until="domcontentloaded",
        timeout=PAGE_LOAD_TIMEOUT_MS,
    )
    return "passport" not in page.url and "auth" not in page.url


# ---------------------------------------------------------------------------
# Report download
# ---------------------------------------------------------------------------

def _build_report_url(base_url: str, date_from: str, date_to: str) -> str:
    """Append date range parameters to the wizard URL."""
    sep = "&" if "?" in base_url else "?"
    return (
        f"{base_url}{sep}"
        f"dateRange=CUSTOM_DATE&dateFrom={date_from}&dateTo={date_to}"
    )


def _wait_for_data_and_download(page, download_dir: Path) -> Optional[Path]:
    """
    On the reports wizard page: wait until data is loaded, click "Скачать",
    capture the download. Returns the path of the downloaded CSV file.
    """
    print("  [report] Waiting for report data to load…")

    # The wizard needs time to generate the report after page load.
    # Wait for the download button to appear and become enabled.
    download_btn_sel = (
        "button:has-text('Скачать'), "
        "button[aria-label*='качать'], "
        "button[data-testid*='download'], "
        "a[href*='.csv']"
    )
    try:
        page.wait_for_selector(download_btn_sel, state="visible", timeout=60000)
    except Exception:
        # Try finding any element with download text
        print("  [report] Download button not found via primary selector, trying fallback…")
        try:
            page.wait_for_selector(
                "*:has-text('Скачать')",
                state="visible",
                timeout=30000,
            )
        except Exception:
            print("  [report] ERROR: Cannot locate download button.", file=sys.stderr)
            page.screenshot(path="/tmp/yd_report_fail.png")
            print("  [report] Screenshot saved to /tmp/yd_report_fail.png", file=sys.stderr)
            return None

    print("  [report] Download button visible. Starting download…")
    with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl_info:
        # Try the primary selector first, fall back to text search
        try:
            page.click(download_btn_sel)
        except Exception:
            page.locator("*:has-text('Скачать')").first.click()

    download = dl_info.value
    out_path = download_dir / f"yandex_report_{date.today().isoformat()}.csv"
    download.save_as(str(out_path))
    print(f"  [report] Downloaded to {out_path} ({out_path.stat().st_size} bytes)")
    return out_path


# ---------------------------------------------------------------------------
# Ingest helpers (delegate to upsert_yandex_from_csv)
# ---------------------------------------------------------------------------

def _ingest_csv(csv_path: Path, db_path: str) -> int:
    """Load the CSV and upsert into stg_yandex_stats. Returns row count."""
    try:
        from db.upsert_yandex_from_csv import load_and_normalize, upsert_to_sqlite
        from sqlalchemy import create_engine
    except ImportError as e:
        print(f"  [ingest] ERROR: {e}", file=sys.stderr)
        return 0

    engine = create_engine(f"sqlite:///{db_path}")
    df = load_and_normalize(csv_path)
    if df.empty:
        print("  [ingest] WARNING: CSV produced 0 rows after normalization.")
        return 0
    n = upsert_to_sqlite(df, engine)
    return n


def _rebuild_stg(db_path: str) -> int:
    """Delegate to yandex_api_sync.rebuild_stg_yandex_stats."""
    try:
        from db.yandex_api_sync import rebuild_stg_yandex_stats
        conn = sqlite3.connect(db_path)
        n = rebuild_stg_yandex_stats(conn)
        conn.close()
        return n
    except Exception as e:
        # stg_yandex_stats is already populated by _ingest_csv (upsert writes it directly)
        print(f"  [stg] Note: rebuild_stg_yandex_stats: {e}", file=sys.stderr)
        return 0


# ---------------------------------------------------------------------------
# Core orchestration
# ---------------------------------------------------------------------------

def interactive_login(login: str, password: str) -> None:
    """
    Open a visible browser window for first-time login / 2FA.
    Saves session on completion.
    """
    from playwright.sync_api import sync_playwright
    print("Opening browser for interactive login (non-headless)…")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context(user_agent=_USER_AGENT)
        page = ctx.new_page()

        page.goto(
            "https://passport.yandex.ru/auth?origin=direct"
            "&retpath=https://direct.yandex.ru/",
            wait_until="domcontentloaded",
            timeout=PAGE_LOAD_TIMEOUT_MS,
        )
        ok = _do_login(page, login, password)
        if not ok:
            # Let user complete manually
            print("Please complete login in the browser. Press Enter when Done.")
            input()

        _save_session(ctx)
        print("Login complete. You can now run the scraper without --login.")
        browser.close()


def run_once(
    login: str,
    password: str,
    db_path: str,
    full: bool = False,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    base_url: str = DEFAULT_REPORT_URL,
) -> bool:
    """
    One sync cycle: login → navigate to report → set dates → download CSV → ingest.
    Returns True on success.
    """
    from playwright.sync_api import sync_playwright

    t0 = time.time()

    conn = sqlite3.connect(db_path)
    today = date.today().isoformat()

    if date_to is None:
        date_to = today
    if date_from is None:
        if full:
            date_from = (date.today() - timedelta(days=DEFAULT_LOOKBACK_DAYS)).isoformat()
        else:
            last = get_last_sync_date(conn)
            if last:
                date_from = (date.fromisoformat(last) - timedelta(days=2)).isoformat()
            else:
                print("No prior sync — performing full lookback.")
                date_from = (date.today() - timedelta(days=DEFAULT_LOOKBACK_DAYS)).isoformat()
    conn.close()

    print(f"── Yandex Direct scraper ────────────────────────────────")
    print(f"  Date range : {date_from} → {date_to}")

    report_url = _build_report_url(base_url, date_from, date_to)

    with sync_playwright() as p:
        browser, ctx = _make_context(p, headless=True)
        page = ctx.new_page()

        # Verify session / login
        try:
            if not _is_logged_in(page):
                print("  [browser] Session expired or missing — logging in…")
                page.goto(
                    "https://passport.yandex.ru/auth?origin=direct"
                    "&retpath=https://direct.yandex.ru/",
                    wait_until="domcontentloaded",
                    timeout=PAGE_LOAD_TIMEOUT_MS,
                )
                if not _do_login(page, login, password):
                    browser.close()
                    return False
                _save_session(ctx)
        except Exception as exc:
            print(f"  [browser] Login check failed: {exc}", file=sys.stderr)
            browser.close()
            return False

        # Navigate to report
        print(f"  [report] Navigating to report…")
        try:
            page.goto(report_url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT_MS)
        except Exception as exc:
            print(f"  [report] Navigation error: {exc}", file=sys.stderr)
            browser.close()
            return False

        if "passport" in page.url:
            print("  [report] Redirected to login — session invalid, re-logging in…")
            if not _do_login(page, login, password):
                browser.close()
                return False
            _save_session(ctx)
            page.goto(report_url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT_MS)

        # Download CSV
        with tempfile.TemporaryDirectory() as tmpdir:
            dl_path = _wait_for_data_and_download(page, Path(tmpdir))
            if dl_path is None:
                browser.close()
                return False

            _save_session(ctx)
            browser.close()

            # Ingest
            print("── Ingesting CSV ────────────────────────────────────────")
            n = _ingest_csv(dl_path, db_path)
            if n == 0:
                print("  WARNING: 0 rows ingested.", file=sys.stderr)
                return False
            print(f"  stg_yandex_stats: {n} rows upserted")

    # Update last sync date
    conn = sqlite3.connect(db_path)
    set_last_sync_date(conn, date_to)
    conn.close()

    # Trigger analytics rebuild
    print("── Triggering analytics cache rebuild ──────────────────")
    trigger_analytics_rebuild()

    print(f"\nSync complete in {time.time() - t0:.1f}s")
    return True


def run_watch(
    login: str,
    password: str,
    db_path: str,
    interval_minutes: int,
    full: bool = False,
    base_url: str = DEFAULT_REPORT_URL,
) -> None:
    """Daemon loop: run_once every interval_minutes."""
    print(f"[watch] Starting daemon (interval: {interval_minutes} min)", flush=True)
    first = True
    while True:
        try:
            run_once(login, password, db_path, full=full if first else False, base_url=base_url)
        except Exception as exc:
            print(f"[watch] ERROR during sync: {exc}", file=sys.stderr, flush=True)
        first = False
        sleep = interval_minutes * 60
        print(f"[watch] Sleeping {interval_minutes} min…", flush=True)
        time.sleep(sleep)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Yandex Direct reports wizard → stg_yandex_stats (Playwright scraper)"
    )
    parser.add_argument(
        "--login",
        action="store_true",
        help="Open visible browser for first-time / 2FA login and save session",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=f"Re-download last {DEFAULT_LOOKBACK_DAYS} days",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Run continuously on --interval schedule (daemon mode)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=180,
        metavar="MINUTES",
        help="Minutes between syncs in --watch mode (default: 180)",
    )
    parser.add_argument(
        "--date-from",
        metavar="YYYY-MM-DD",
        help="Override start date",
    )
    parser.add_argument(
        "--date-to",
        metavar="YYYY-MM-DD",
        help="Override end date (default: today)",
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite database path (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--report-url",
        default=os.environ.get("YANDEX_REPORT_URL", DEFAULT_REPORT_URL),
        metavar="URL",
        help="Override the reports wizard URL",
    )
    args = parser.parse_args()

    login_val, password_val = _get_creds()

    if args.login:
        interactive_login(login_val, password_val)
        return

    if args.watch:
        run_watch(
            login_val, password_val, args.db,
            interval_minutes=args.interval,
            full=args.full,
            base_url=args.report_url,
        )
    else:
        ok = run_once(
            login_val, password_val, args.db,
            full=args.full,
            date_from=args.date_from,
            date_to=args.date_to,
            base_url=args.report_url,
        )
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
