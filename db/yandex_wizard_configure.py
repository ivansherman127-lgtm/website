"""
One-time helper: open the Yandex Direct wizard, add «День» (Day) as a grouping
dimension, wait for the URL to update with the new state ID, and print it.

Run: python -m db.yandex_wizard_configure
"""
from __future__ import annotations
import json
import sys
import time
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
SESSION_FILE = ROOT / "web_share_subset" / "webpush" / ".yandex_session.json"
WIZARD_URL = "https://direct.yandex.ru/dna/reports/wizard?ulogin=ivansherman127&state=2619475"

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _get_state(url: str) -> str | None:
    qs = parse_qs(urlparse(url).query)
    return qs.get("state", [None])[0]


def configure():
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(channel="chrome", headless=False)
        except Exception:
            browser = p.chromium.launch(headless=False)

        ctx_kwargs: dict = {"user_agent": _USER_AGENT}
        if SESSION_FILE.exists():
            ctx_kwargs["storage_state"] = str(SESSION_FILE)
            print(f"Loaded session from {SESSION_FILE}")

        ctx = browser.new_context(**ctx_kwargs)
        ctx.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = ctx.new_page()

        print("Navigating to wizard…")
        page.goto(WIZARD_URL, wait_until="domcontentloaded", timeout=60000)

        # Handle account picker if redirected
        time.sleep(2)
        if "auth/list" in page.url or "pwl-yandex" in page.url:
            print("Account picker — clicking account…")
            for sel in ["[data-login='ivansherman127']", "[class*='UserAccount']"]:
                el = page.query_selector(sel)
                if el:
                    el.click()
                    break
            page.wait_for_url("**/direct.yandex.ru/**", timeout=20000)
            page.goto(WIZARD_URL, wait_until="domcontentloaded", timeout=60000)

        print(f"On page: {page.url}")
        initial_state = _get_state(page.url) or "2619475"

        # Wait for the wizard to load (Скачать button appears)
        print("Waiting for wizard to load (up to 3 min)…")
        try:
            page.wait_for_selector("button:has-text('Скачать')", state="visible", timeout=180000)
        except Exception as e:
            print(f"Wizard did not load: {e}")
            input("Press Enter to quit…")
            browser.close()
            return

        print("Wizard loaded!")
        page.screenshot(path="/tmp/yd_configure_loaded.png")

        # Click "Группировки и метрики" to open the groupings panel
        print("Opening 'Группировки и метрики' panel…")
        try:
            page.click("button:has-text('Группировки и метрики'), *:has-text('Группировки и метрики')",
                       timeout=5000)
            page.wait_for_timeout(1500)
            page.screenshot(path="/tmp/yd_configure_panel.png")
        except Exception as e:
            print(f"Could not open groupings panel: {e}")
            print("Please manually open 'Группировки и метрики' and enable 'День'.")
            input("Press Enter when done, then let URL update with new state…")

        # Look for "День" checkbox/option in the panel
        day_added = False
        for sel in [
            "label:has-text('День')",
            "div:has-text('День'):not(:has(*))",
            "[role='checkbox'][aria-label*='День']",
            "input[type='checkbox']:near(:has-text('День'))",
        ]:
            try:
                el = page.query_selector(sel)
                if el:
                    # Check if it's already checked
                    checked = el.get_attribute("aria-checked") or el.get_attribute("data-checked")
                    if checked == "true":
                        print("  'День' grouping was already enabled.")
                        day_added = True
                        break
                    el.click()
                    print(f"  Clicked 'День' option via {sel}")
                    day_added = True
                    break
            except Exception:
                continue

        if not day_added:
            print("Could not automatically find 'День' checkbox.")
            print("Please enable 'День' grouping manually in the browser.")

        # Close the panel / apply
        try:
            # Press Escape or click outside to close
            page.keyboard.press("Escape")
            page.wait_for_timeout(1000)
        except Exception:
            pass

        # Wait for URL to update with new state
        print("Waiting for URL to update with new state…")
        for _ in range(30):
            time.sleep(1)
            new_state = _get_state(page.url)
            if new_state and new_state != initial_state:
                print(f"\n✅ NEW STATE ID: {new_state}")
                print(f"New URL: {page.url}")
                print(f"\nUpdate DEFAULT_REPORT_URL in db/yandex_direct_scraper.py to:")
                new_url = f"https://direct.yandex.ru/dna/reports/wizard?ulogin=ivansherman127&state={new_state}"
                print(f"  {new_url}")
                page.screenshot(path="/tmp/yd_configure_new_state.png")
                break
        else:
            print(f"URL did not change from initial state. Current URL: {page.url}")
            print("Please make sure 'День' was added as a grouping dimension.")

        input("\nPress Enter to close browser…")
        browser.close()


if __name__ == "__main__":
    configure()
