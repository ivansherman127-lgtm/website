"""db/yandex_direct_scraper.py
Yandex Direct reports wizard -> stg_yandex_stats (Playwright web scraper).

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
# Persistent browser profile — survives across runs so Yandex remembers the device
PROFILE_DIR     = ROOT / "web_share_subset" / "webpush" / ".yandex_browser_profile"
SESSION_FILE    = ROOT / "web_share_subset" / "webpush" / ".yandex_session.json"  # legacy, kept for compat
LOCAL_SYNC_FILE = ROOT / "web_share_subset" / "webpush" / ".yandex_last_sync"
ENV_FILE        = ROOT / "web_share_subset" / "webpush" / ".env.server.json"

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
# Remote push helpers (Mac-local scraper -> server ingest)
# ---------------------------------------------------------------------------

def _get_local_last_sync() -> Optional[str]:
    try:
        return LOCAL_SYNC_FILE.read_text(encoding="utf-8").strip() or None
    except Exception:
        return None


def _set_local_last_sync(d: str) -> None:
    LOCAL_SYNC_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_SYNC_FILE.write_text(d, encoding="utf-8")


def _push_csv_to_server(csv_path: Path, ingest_url: str, secret: str, fallback_month: str | None = None) -> bool:
    """POST a CSV file to the server's /api/yandex/ingest endpoint."""
    with open(csv_path, "rb") as fh:
        csv_data = fh.read()
    url = ingest_url
    if fallback_month:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}month={fallback_month}"
    req = urllib.request.Request(url, data=csv_data, method="POST")
    req.add_header("Content-Type", "text/csv")
    if secret:
        req.add_header("Authorization", f"Bearer {secret}")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode())
        if result.get("ok"):
            print(f"  [push] Server ingested {len(csv_data):,} bytes — OK")
            return True
        print(f"  [push] Server error: {result}", file=sys.stderr)
        return False
    except Exception as exc:
        print(f"  [push] Failed to push CSV to server: {exc}", file=sys.stderr)
        return False


# ---------------------------------------------------------------------------
# Playwright browser context helpers
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Playwright browser context helpers
# ---------------------------------------------------------------------------

def _launch_context(p, headless: bool = True):
    """
    Launch a fresh Chromium context, loading saved storage_state (cookies) from
    SESSION_FILE if it exists. Returns (browser, context).
    Uses channel='chrome' (real Chrome) when available to reduce bot detection.
    """
    launch_kwargs: dict = {
        "headless": headless,
        "args": ["--disable-blink-features=AutomationControlled"],
    }
    try:
        browser = p.chromium.launch(channel="chrome", **launch_kwargs)
    except Exception:
        browser = p.chromium.launch(**launch_kwargs)

    ctx_kwargs: dict = {"user_agent": _USER_AGENT}
    if SESSION_FILE.exists():
        ctx_kwargs["storage_state"] = str(SESSION_FILE)
        print(f"  [browser] Loaded session from {SESSION_FILE}")
    ctx = browser.new_context(**ctx_kwargs)
    ctx.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return browser, ctx


def _do_login(page, login: str, password: str) -> bool:
    """
    Perform Yandex Passport login. Returns True on success.
    Expects page to already be on a passport auth URL.
    """
    # Handle account picker (auth/list) — session cookies loaded, Yandex shows the
    # account selection page; just click the account to continue.
    if "auth/list" in page.url or "pwl-yandex" in page.url:
        print("  [login] Account picker detected — clicking account…")
        page.wait_for_timeout(1500)
        short_login = login.split("@")[0]
        selectors = [
            f"[data-login='{short_login}']",
            f"[title*='{login}']",
            f"[title*='{short_login}']",
            # Common Yandex account list item classes
            ".user-account",
            "[class*='UserAccount']",
            "[class*='user-account']",
            "[class*='AccountItem']",
            "a[class*='account']",
        ]
        clicked = False
        for sel in selectors:
            try:
                el = page.query_selector(sel)
                if el:
                    el.click()
                    clicked = True
                    print(f"  [login] Clicked account with selector: {sel}")
                    break
            except Exception:
                continue
        if not clicked:
            # Fallback: click the first clickable account row
            try:
                page.locator("//div[@role='button' or @role='link'] | //a[contains(@href,'select')]").first.click()
                clicked = True
                print("  [login] Clicked first account row (fallback)")
            except Exception as e:
                print(f"  [login] ERROR: could not click account: {e}", file=sys.stderr)
                page.screenshot(path="/tmp/yd_login_fail.png")
                return False
        # Wait for redirect to Direct
        try:
            page.wait_for_url("**/direct.yandex.ru/**", timeout=20000)
            print("  [login] Account selected — redirected to Direct.")
            return True
        except Exception:
            # May need password step after account selection
            if "passwd" in page.url or "password" in page.url or page.query_selector("input[type='password']"):
                print("  [login] Password prompt after account selection…")
                # Fall through to password entry below
            elif page.url.startswith("https://direct.yandex.ru/"):
                return True
            else:
                print("  [login] Unexpected URL after account click:", page.url[:120], file=sys.stderr)
                page.screenshot(path="/tmp/yd_login_fail.png")
                return False

    print("  [login] Filling login field…")
    # Yandex passport login: two-step form (login -> Next -> password -> Sign in)
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
    # Submit login (press Enter — Yandex uses type='button' on the Next button)
    page.keyboard.press("Enter")
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
        if page.url.startswith("https://direct.yandex.ru/"):
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
    page.keyboard.press("Enter")

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
    """Navigate to Direct root and check we're not on passport or captcha."""
    try:
        page.goto(
            "https://direct.yandex.ru/registered/main.pl",
            wait_until="domcontentloaded",
            timeout=PAGE_LOAD_TIMEOUT_MS,
        )
    except Exception:
        return False
    # Direct may redirect to /dna/grid/campaigns/ or /registered/ — both mean logged in
    if page.url.startswith("https://direct.yandex.ru/"):
        return True
    # Account picker — session cookies are valid but need account selection click
    if "auth/list" in page.url or "pwl-yandex" in page.url:
        return False  # _do_login will handle the picker
    return False


# ---------------------------------------------------------------------------
# Report download
# ---------------------------------------------------------------------------

def _build_report_url(base_url: str, date_from: str, date_to: str) -> str:
    """Return the wizard base URL (date range is set via UI, not URL params)."""
    # The Yandex Direct wizard SPA does not apply dateFrom/dateTo URL params —
    # it uses its saved state. We strip them and set the date range via UI instead.
    return base_url


def _set_date_range_ui(page, date_from: str, date_to: str) -> bool:
    """
    Attempt to set the date range in the wizard UI. Uses presets if the range
    matches; falls back to the custom date picker. Returns True on success.
    """
    from datetime import date as _date, timedelta

    try:
        df = _date.fromisoformat(date_from)
        dt = _date.fromisoformat(date_to)
    except ValueError:
        return False

    today = _date.today()
    days = (dt - df).days + 1  # inclusive range

    # Check if date_to is today and range matches a standard preset
    if dt == today:
        if days == 1 and df == today:
            preset_testid = "DateRangeSelect.Presets.TODAY"
        elif days == 1 and df == today - timedelta(days=1):
            preset_testid = "DateRangeSelect.Presets.YESTERDAY"
        elif days == 7:
            preset_testid = "DateRangeSelect.Presets.LAST_7DAYS"
        elif days == 30:
            preset_testid = "DateRangeSelect.Presets.LAST_30DAYS"
        else:
            preset_testid = None
    else:
        preset_testid = None

    if preset_testid:
        print(f"  [date] Clicking preset {preset_testid}…")
        try:
            # Wait for the Скачать button FIRST (ensures wizard is fully loaded)
            page.wait_for_selector("button:has-text('Скачать')", state="visible", timeout=180000)
            # Then click the date preset
            page.click(f"[data-testid='{preset_testid}']", timeout=10000)
            page.wait_for_timeout(2000)
            return True
        except Exception as e:
            print(f"  [date] Preset click failed: {e}")

    # Custom date range — click the date display to open picker
    print(f"  [date] Setting custom date range {date_from} -> {date_to}…")
    try:
        import re as _re
        # Wait for wizard to load first
        page.wait_for_selector("button:has-text('Скачать')", state="visible", timeout=180000)

        date_from_dmy = date_from[8:10] + "." + date_from[5:7] + "." + date_from[:4]
        date_to_dmy   = date_to[8:10]   + "." + date_to[5:7]   + "." + date_to[:4]

        # STEP 1: Dismiss any modal overlays (e.g. "Новости Директа" news popup).
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(300)
        except Exception:
            pass

        # STEP 2: Open the calendar popup using JS (bypasses overlay/actionability restrictions).
        opened = False
        for _attempt in range(3):
            r = page.evaluate("""() => {
                const dp = /\\d{1,2}\\s*[\\u2013\\-]\\s*\\d{1,2}\\s+[\\u0430-\\u044f\\u0410-\\u042f\\u0451\\u0401]{3,}/;
                for (const el of document.querySelectorAll('button,[role="button"]')) {
                    const tid = el.getAttribute('data-testid') || '';
                    if (tid.includes('IncludeToday') || tid.includes('Preset')) continue;
                    const t = (el.innerText || '').trim();
                    if (dp.test(t) && t.length < 45) {
                        el.scrollIntoView({block:'nearest'});
                        el.click();
                        return 'ok:' + tid + ':' + t.substr(0, 20);
                    }
                }
                return 'not_found';
            }""")
            print(f"  [date] JS click [{_attempt+1}]: {r}")
            if r.startswith("not_found"):
                page.wait_for_timeout(1000)
                continue
            # Confirm popup appeared
            try:
                page.wait_for_selector('[data-testid="Popup"]', timeout=2000)
                opened = True
                print("  [date] Popup confirmed")
                break
            except Exception:
                print("  [date] Popup not confirmed, retrying…")
                page.wait_for_timeout(800)

        if not opened:
            print("  [date] Could not open calendar popup — using wizard default.")
            return False

        page.wait_for_timeout(400)

        page.wait_for_timeout(500)

        # Use page.locator().all() — returns Locator objects that support triple_click/fill.
        # Match inputs by: date-value format  OR  placeholder text indicating "from/to" date fields.
        _from_ph = ("от", "с", "начало", "from", "дд.мм.гггг", "dd.mm.yyyy")
        _to_ph   = ("до", "по", "конец", "to", "дд.мм.гггг", "dd.mm.yyyy")
        all_locs = page.locator("input").all()

        date_locs: list = []
        from_loc = None
        to_loc = None
        for loc in all_locs:
            val = loc.input_value() or ""
            ph  = (loc.get_attribute("placeholder") or "").lower().strip()
            is_date_val = bool(_re.match(r"^\d{1,2}\.\d{1,2}\.\d{4}$", val))
            is_from_ph  = ph in _from_ph
            is_to_ph    = ph in _to_ph
            if is_date_val or is_from_ph:
                if from_loc is None:
                    from_loc = loc
                elif to_loc is None:
                    to_loc = loc
            elif is_to_ph and to_loc is None:
                to_loc = loc

        if from_loc:
            date_locs.append(from_loc)
        if to_loc:
            date_locs.append(to_loc)
        # Fallback: if still nothing found but there are exactly 2 date-value inputs
        if not date_locs:
            by_val = [
                loc for loc in all_locs
                if _re.match(r"^\d{1,2}\.\d{1,2}\.\d{4}$", loc.input_value() or "")
            ]
            date_locs = by_val[:2]

        print(f"  [date] Date-format inputs found: {len(date_locs)}")

        if len(date_locs) >= 2:
            # filling the start input closes the popup; then reopen and fill end input.
            start_loc = page.locator(
                "[data-testid='DateRangeSelect.RangeCalendarWithButton.RangeCalendar.StartInput']"
            )
            end_loc = page.locator(
                "[data-testid='DateRangeSelect.RangeCalendarWithButton.RangeCalendar.EndInput']"
            )

            # Fill both inputs in quick succession (avoid any wait that lets popup close).
            start_loc.fill(date_from_dmy, timeout=10000)
            print(f"  [date] Start filled: {date_from_dmy}")
            # Immediately fill end (no wait) so popup doesn't close between fills
            try:
                end_loc.fill(date_to_dmy, timeout=5000)
                print(f"  [date] End filled:   {date_to_dmy}")
                page.keyboard.press("Enter")
                page.wait_for_timeout(2500)
                page.screenshot(path="/tmp/yd_after_date_set.png")
                return True
            except Exception as _e:
                print(f"  [date] End immediate fill failed: {_e}, trying with reopen")

            # Fallback: wait, reopen popup, fill end date
            page.wait_for_timeout(600)
            for _try in range(3):
                page.evaluate(
                    """() => { const el = document.querySelector(
                        '[data-testid="DateRangeSelect.RangeCalendarWithButton.Calendar"]');
                       if (el) el.click(); }"""
                )
                reopen_ok = False
                try:
                    page.wait_for_selector('[data-testid="Popup"]', timeout=2000)
                    reopen_ok = True
                except Exception:
                    pass
                if reopen_ok:
                    try:
                        end_loc.fill(date_to_dmy, timeout=5000)
                        print(f"  [date] End filled after reopen: {date_to_dmy}")
                        page.keyboard.press("Enter")
                        page.wait_for_timeout(2500)
                        page.screenshot(path="/tmp/yd_after_date_set.png")
                        return True
                    except Exception as _e2:
                        print(f"  [date] End fill try {_try+1} failed: {_e2}")
                page.wait_for_timeout(700)
            print("  [date] Could not fill end date after 3 attempts.")
        elif len(date_locs) == 1:
            start_loc = page.locator(
                "[data-testid='DateRangeSelect.RangeCalendarWithButton.RangeCalendar.StartInput']"
            )
            start_loc.click()
            page.keyboard.press("Control+a")
            page.keyboard.type(date_from_dmy)
            start_loc.press("Enter")
            print(f"  [date] Only 1 date input found, typed start date: {date_from_dmy}")

        # No date inputs found — dump screenshot for debugging
        print("  [date] No date-format inputs found after calendar open.")
        page.screenshot(path="/tmp/yd_no_inputs.png")
        return False
    except Exception as e:
        print(f"  [date] Custom date picker failed: {e} — using wizard default range.")
        return False



def _wait_for_data_and_download(page, download_dir: Path) -> Optional[Path]:
    """
    On the reports wizard page: wait for the 'Скачать' button to appear (the
    definitive signal that the wizard has loaded and data is ready), then trigger
    the 2-step download: click 'Скачать' (opens format dropdown) -> 'Скачать CSV'.
    """
    print("  [report] Waiting for wizard to finish loading…")

    # Wait for the Скачать button — the definitive "wizard is ready" signal.
    # This covers spinner + data loading in one check.
    try:
        page.wait_for_selector(
            "button:has-text('Скачать')",
            state="visible",
            timeout=180000,  # 3 minutes — wizard can be slow
        )
    except Exception:
        # Older UI might not have this button or it might be labeled differently
        print("  [report] WARNING: 'Скачать' button not found — checking for alternatives.")
        try:
            page.wait_for_selector("a[href*='.csv'], button[aria-label*='качать']",
                                   state="visible", timeout=30000)
        except Exception:
            print("  [report] ERROR: Cannot locate download button.", file=sys.stderr)
            page.screenshot(path="/tmp/yd_report_fail.png")
            return None

    page.wait_for_timeout(1500)  # Let UI settle after data load
    page.screenshot(path="/tmp/yd_before_download.png")
    print("  [report] Wizard ready. Opening download menu…")

    # Click "Скачать" to open the format dropdown (already confirmed visible above)
    try:
        page.click("button:has-text('Скачать')")
    except Exception:
        page.locator("button:has-text('Скачать')").first.click(force=True)

    page.wait_for_timeout(500)
    page.screenshot(path="/tmp/yd_ready_to_click.png")

    # Click "Скачать CSV" from the dropdown — INSIDE expect_download context
    print("  [report] Clicking 'Скачать CSV'…")
    try:
        with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl_info:
            page.click("text='Скачать CSV'")
    except Exception as e:
        print(f"  [report] Download event failed: {e}", file=sys.stderr)
        page.screenshot(path="/tmp/yd_report_fail.png")
        return None

    download = dl_info.value
    out_path = download_dir / f"yandex_report_{date.today().isoformat()}.csv"
    download.save_as(str(out_path))
    print(f"  [report] Downloaded to {out_path} ({out_path.stat().st_size} bytes)")
    return out_path


# ---------------------------------------------------------------------------
# Ingest helpers (delegate to upsert_yandex_from_csv)
# ---------------------------------------------------------------------------

def _ingest_csv(csv_path: Path, db_path: str, fallback_month: str | None = None) -> int:
    """Load the CSV and upsert into stg_yandex_stats. Returns row count."""
    try:
        from db.upsert_yandex_from_csv import load_and_normalize, upsert_to_sqlite
        from sqlalchemy import create_engine
    except ImportError as e:
        print(f"  [ingest] ERROR: {e}", file=sys.stderr)
        return 0

    engine = create_engine(f"sqlite:///{db_path}")
    df = load_and_normalize(csv_path, fallback_month=fallback_month)
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
    Open a visible Chrome window. User logs in manually.
    Saves cookies to SESSION_FILE once direct.yandex.ru/registered is reached.
    """
    import re
    from playwright.sync_api import sync_playwright

    print("Opening browser for interactive login…")
    print("Log in to Yandex in the browser window. The browser closes automatically.\n")

    # Do NOT load existing session — start fresh so user can log in cleanly
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(
                headless=False,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception:
            browser = p.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled"],
            )

        ctx = browser.new_context(user_agent=_USER_AGENT)
        ctx.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = ctx.new_page()
        page.goto(
            "https://direct.yandex.ru/registered/main.pl",
            wait_until="domcontentloaded",
            timeout=PAGE_LOAD_TIMEOUT_MS,
        )

        print("  Waiting for login (up to 5 min)…")
        try:
            page.wait_for_url(
                re.compile(r"^https://direct\.yandex\.ru/registered"),
                timeout=300_000,
            )
            print("  Logged in!")
        except Exception:
            print(f"  Timed out — URL: {page.url[:100]}", file=sys.stderr)

        SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
        ctx.storage_state(path=str(SESSION_FILE))
        print(f"  Session saved to {SESSION_FILE}")
        browser.close()

    print("\nDone. You can now run the scraper without --login.")


def run_once(
    login: str,
    password: str,
    db_path: str,
    full: bool = False,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    base_url: str = DEFAULT_REPORT_URL,
    ingest_url: Optional[str] = None,
) -> bool:
    """
    One sync cycle: login -> navigate to report -> set dates -> download CSV -> ingest.
    Returns True on success.

    ingest_url: if set, POST CSV to this URL (server push mode) instead of local ingest.
                Reads/writes last_sync_date from a local file instead of SQLite.
    """
    from playwright.sync_api import sync_playwright

    t0 = time.time()
    today = date.today().isoformat()
    secret = os.environ.get("ANALYTICS_REBUILD_SECRET", _read_env_file().get("ANALYTICS_REBUILD_SECRET", ""))

    if date_to is None:
        date_to = today
    if date_from is None:
        if full:
            date_from = (date.today() - timedelta(days=DEFAULT_LOOKBACK_DAYS)).isoformat()
        else:
            if ingest_url:
                last = _get_local_last_sync()
            else:
                conn = sqlite3.connect(db_path)
                last = get_last_sync_date(conn)
                conn.close()
            if last:
                date_from = (date.fromisoformat(last) - timedelta(days=2)).isoformat()
            else:
                print("No prior sync — performing full lookback.")
                date_from = (date.today() - timedelta(days=DEFAULT_LOOKBACK_DAYS)).isoformat()

    print(f"── Yandex Direct scraper ────────────────────────────────")
    print(f"  Date range : {date_from} -> {date_to}")
    if ingest_url:
        print(f"  Mode       : push to server ({ingest_url})")

    report_url = _build_report_url(base_url, date_from, date_to)

    with sync_playwright() as p:
        browser, ctx = _launch_context(p, headless=False)
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
            page.goto(report_url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT_MS)

        # Set date range via UI (URL params are ignored by the wizard SPA)
        print(f"  [report] Current URL: {page.url[:120]}")
        page.screenshot(path="/tmp/yd_after_nav.png")
        _set_date_range_ui(page, date_from, date_to)

        # Download CSV
        with tempfile.TemporaryDirectory() as tmpdir:
            dl_path = _wait_for_data_and_download(page, Path(tmpdir))
            if dl_path is None:
                browser.close()
                return False

            browser.close()

            if ingest_url:
                # Push mode: POST CSV to server; server upserts and triggers analytics rebuild
                print("── Pushing CSV to server ────────────────────────────")
                ok = _push_csv_to_server(dl_path, ingest_url, secret, fallback_month=date_from[:7])
                if not ok:
                    return False
                _set_local_last_sync(date_to)
            else:
                # Local mode: ingest directly into SQLite
                print("── Ingesting CSV ────────────────────────────────────────")
                n = _ingest_csv(dl_path, db_path, fallback_month=date_from[:7])
                if n == 0:
                    print("  WARNING: 0 rows ingested.", file=sys.stderr)
                    return False
                print(f"  stg_yandex_stats: {n} rows upserted")
                conn = sqlite3.connect(db_path)
                set_last_sync_date(conn, date_to)
                conn.close()
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
    ingest_url: Optional[str] = None,
) -> None:
    """Daemon loop: run_once every interval_minutes."""
    print(f"[watch] Starting daemon (interval: {interval_minutes} min)", flush=True)
    first = True
    while True:
        try:
            run_once(
                login, password, db_path,
                full=full if first else False,
                base_url=base_url,
                ingest_url=ingest_url,
            )
        except Exception as exc:
            print(f"[watch] ERROR during sync: {exc}", file=sys.stderr, flush=True)
        first = False
        sleep = interval_minutes * 60
        print(f"[watch] Sleeping {interval_minutes} min…", flush=True)
        time.sleep(sleep)


def run_batch_months(
    login: str,
    password: str,
    db_path: str,
    batch_months: list,
    base_url: str = DEFAULT_REPORT_URL,
    ingest_url: Optional[str] = None,
) -> None:
    """
    Single-session batch: download one CSV per month in one browser session.
    Keeps the browser open between months to avoid repeated login overhead.
    Each month uses its own date range (YYYY-MM-01 -> YYYY-MM-last).

    batch_months: list of 'YYYY-MM' strings in ascending order.
    ingest_url:   if set, each CSV is POSTed to the server (push mode).
    """
    from calendar import monthrange
    from playwright.sync_api import sync_playwright

    secret = os.environ.get("ANALYTICS_REBUILD_SECRET", _read_env_file().get("ANALYTICS_REBUILD_SECRET", ""))

    print("── Yandex batch-months ingest ───────────────────────────────")
    print(f"  Months : {batch_months[0]} -> {batch_months[-1]} ({len(batch_months)} months)")
    if ingest_url:
        print(f"  Mode   : push to server ({ingest_url})")
    else:
        print(f"  Mode   : local SQLite ({db_path})")

    with sync_playwright() as p:
        browser, ctx = _launch_context(p, headless=False)
        page = ctx.new_page()

        # Login check
        try:
            if not _is_logged_in(page):
                print("  [browser] Session missing/expired — logging in…")
                page.goto(
                    "https://passport.yandex.ru/auth?origin=direct"
                    "&retpath=https://direct.yandex.ru/",
                    wait_until="domcontentloaded",
                    timeout=PAGE_LOAD_TIMEOUT_MS,
                )
                if not _do_login(page, login, password):
                    browser.close()
                    return
        except Exception as exc:
            print(f"  [browser] Login check failed: {exc}", file=sys.stderr)
            browser.close()
            return

        success_count = 0
        for month in batch_months:
            y, m = int(month[:4]), int(month[5:7])
            date_from = f"{y:04d}-{m:02d}-01"
            last_day = monthrange(y, m)[1]
            date_to = f"{y:04d}-{m:02d}-{last_day:02d}"

            print(f"\n── {month} ─── ({date_from} -> {date_to}) ──────")

            # Navigate to wizard (fresh page load per month)
            try:
                page.goto(base_url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT_MS)
            except Exception as exc:
                print(f"  [nav] Error: {exc} — skipping {month}", file=sys.stderr)
                continue

            if "passport" in page.url:
                print("  [auth] Session expired mid-batch — stopping.", file=sys.stderr)
                break

            # Set date range in wizard UI
            _set_date_range_ui(page, date_from, date_to)

            # Download CSV for this month
            with tempfile.TemporaryDirectory() as tmpdir:
                dl_path = _wait_for_data_and_download(page, Path(tmpdir))
                if dl_path is None:
                    print(f"  WARNING: Download failed for {month} — skipping.")
                    continue

                if ingest_url:
                    ok = _push_csv_to_server(dl_path, ingest_url, secret, fallback_month=month)
                    if ok:
                        _set_local_last_sync(date_to)
                        success_count += 1
                else:
                    n = _ingest_csv(dl_path, db_path, fallback_month=month)
                    if n == 0:
                        print(f"  WARNING: 0 rows ingested for {month}.")
                    else:
                        print(f"  stg_yandex_stats: {n} rows upserted for {month}")
                        conn = sqlite3.connect(db_path)
                        set_last_sync_date(conn, date_to)
                        conn.close()
                        success_count += 1

        browser.close()

    if not ingest_url and success_count > 0:
        print("\n── Triggering analytics rebuild ─────────────────────────────")
        trigger_analytics_rebuild()

    print(f"\nBatch done: {success_count}/{len(batch_months)} months processed.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Yandex Direct reports wizard -> stg_yandex_stats (Playwright scraper)"
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
    parser.add_argument(
        "--batch-months",
        metavar="YYYY-MM:YYYY-MM",
        help="Batch mode: download every month in START:END range in one browser session",
    )
    args = parser.parse_args()

    login_val, password_val = _get_creds()
    ingest_url_val = os.environ.get("YANDEX_INGEST_URL", "").strip() or None

    if args.login:
        interactive_login(login_val, password_val)
        return

    if args.batch_months:
        start_m, end_m = args.batch_months.split(":")
        months: list = []
        y, m = int(start_m[:4]), int(start_m[5:7])
        ey, em = int(end_m[:4]), int(end_m[5:7])
        while (y, m) <= (ey, em):
            months.append(f"{y:04d}-{m:02d}")
            m += 1
            if m > 12:
                m = 1
                y += 1
        run_batch_months(
            login_val, password_val, args.db, months,
            base_url=args.report_url,
            ingest_url=ingest_url_val,
        )
        return

    if args.watch:
        run_watch(
            login_val, password_val, args.db,
            interval_minutes=args.interval,
            full=args.full,
            base_url=args.report_url,
            ingest_url=ingest_url_val,
        )
    else:
        ok = run_once(
            login_val, password_val, args.db,
            full=args.full,
            date_from=args.date_from,
            date_to=args.date_to,
            base_url=args.report_url,
            ingest_url=ingest_url_val,
        )
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
