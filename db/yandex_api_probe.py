"""
Probe Yandex Direct's internal API — capture network requests made by the
wizard when it loads data, particularly the data/export endpoint.

Run: python -m db.yandex_api_probe
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
SESSION_FILE = ROOT / "web_share_subset" / "webpush" / ".yandex_session.json"

WIZARD_URL = "https://direct.yandex.ru/dna/reports/wizard?ulogin=ivansherman127&state=2619475"

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def probe():
    captured = []

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

        # Capture all requests/responses from direct.yandex.ru
        def on_request(req):
            if "direct.yandex.ru" in req.url and any(
                k in req.url for k in ["report", "stat", "csv", "export", "data", "wizard"]
            ):
                entry = {
                    "type": "request",
                    "method": req.method,
                    "url": req.url,
                    "post_data": req.post_data,
                }
                captured.append(entry)
                print(f">> {req.method} {req.url[:120]}")
                if req.post_data:
                    print(f"   body: {req.post_data[:200]}")

        def on_response(resp):
            if "direct.yandex.ru" in resp.url and any(
                k in resp.url for k in ["report", "stat", "csv", "export", "data", "wizard"]
            ):
                ct = resp.headers.get("content-type", "")
                entry = {
                    "type": "response",
                    "status": resp.status,
                    "url": resp.url,
                    "content_type": ct,
                }
                # Capture response body for data endpoints
                if any(t in ct for t in ["csv", "octet-stream", "json"]) or resp.status == 200:
                    try:
                        body = resp.text()
                        entry["body_preview"] = body[:500]
                        if "csv" in ct or "octet-stream" in ct:
                            entry["full_body"] = body
                    except Exception:
                        pass
                captured.append(entry)
                print(f"<< {resp.status} {resp.url[:120]} [{ct[:40]}]")

        page.on("request", on_request)
        page.on("response", on_response)

        print(f"\nNavigating to wizard…")
        page.goto(WIZARD_URL, wait_until="domcontentloaded", timeout=30000)

        # Handle account picker if needed
        time.sleep(2)
        if "auth/list" in page.url or "pwl-yandex" in page.url:
            print("Account picker — clicking account…")
            for sel in ["[data-login='ivansherman127']", "[class*='UserAccount']", "[class*='user-account']"]:
                el = page.query_selector(sel)
                if el:
                    el.click()
                    print(f"Clicked {sel}")
                    break
            page.wait_for_url("**/direct.yandex.ru/**", timeout=20000)
            # Now navigate to wizard
            page.goto(WIZARD_URL, wait_until="domcontentloaded", timeout=30000)

        print(f"\nOn page: {page.url}")
        print("Waiting 15s for data to load…")
        page.wait_for_timeout(5000)

        # Take screenshot
        page.screenshot(path="/tmp/yd_probe_initial.png")
        print("Screenshot: /tmp/yd_probe_initial.png")

        # Try clicking "7 дней" to ensure data loads
        print("\nClicking '7 дней' preset…")
        try:
            page.click("button:has-text('7 дней'), [role='button']:has-text('7 дней')")
            page.wait_for_timeout(5000)
            page.screenshot(path="/tmp/yd_probe_after_7days.png")
            print("Screenshot: /tmp/yd_probe_after_7days.png")
        except Exception as e:
            print(f"Could not click 7 дней: {e}")

        # Now click Скачать
        print("\nLooking for Скачать button…")
        page.wait_for_timeout(3000)
        btn = page.query_selector("button:has-text('Скачать')")
        if btn:
            print(f"Found button: {btn.inner_text()[:50]}")
            print("Clicking download button…")
            # Listen for download
            try:
                with page.expect_download(timeout=15000) as dl:
                    btn.click()
                dl_path = "/tmp/yd_probe_download.csv"
                dl.value.save_as(dl_path)
                print(f"DOWNLOAD SUCCESS → {dl_path}")
            except Exception as e:
                print(f"Download event not fired: {e}")
                page.screenshot(path="/tmp/yd_probe_after_click.png")
                print("Screenshot: /tmp/yd_probe_after_click.png")
        else:
            print("Скачать button not found")

        print("\n=== Captured network entries ===")
        for i, e in enumerate(captured):
            print(f"\n[{i}] {e.get('type','?')} {e.get('method','')} {e.get('url','')[:120]}")
            if e.get("body_preview"):
                print(f"    preview: {e['body_preview'][:200]}")

        # Save captured data
        out = ROOT / "db" / "yandex_api_probe_result.json"
        with open(out, "w") as f:
            # Don't include full_body in JSON — too large
            safe = [{k: v for k, v in e.items() if k != "full_body"} for e in captured]
            json.dump(safe, f, indent=2, ensure_ascii=False)
        print(f"\nSaved capture to {out}")

        input("\nPress Enter to close browser…")
        browser.close()


if __name__ == "__main__":
    probe()
