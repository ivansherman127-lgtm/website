/**
 * Local Mac PM2 config for the Yandex Direct scraper.
 *
 * Runs on the local Mac (residential IP) to bypass Yandex's datacenter IP blocks.
 * Downloads CSV via Playwright and POSTs it to the branch/production server for ingestion.
 *
 * First-time setup:
 *   1. python -m db.yandex_direct_scraper --login   (creates session file)
 *   2. pm2 start ecosystem.config.yandex-local.cjs
 *   3. pm2 save && pm2 startup
 *
 * The session file (.yandex_session.json) is used for headless runs.
 * Re-run --login if the session expires (Yandex sessions are typically valid for 7-30 days).
 */

const path = require("path");

const repoRoot = path.resolve(__dirname, "..", "..");
const venvPython = path.join(repoRoot, ".venv", "bin", "python3");

let serverSecrets = {};
try { serverSecrets = require("./.env.server.json"); } catch (_) {}

const rebuildSecret = serverSecrets.ANALYTICS_REBUILD_SECRET || process.env.ANALYTICS_REBUILD_SECRET || "";

module.exports = {
  apps: [
    {
      name: "yandex-scraper-local",
      script: venvPython,
      args: "-m db.yandex_direct_scraper --watch --interval 180",
      interpreter: "none",
      cwd: repoRoot,
      watch: false,
      autorestart: true,
      restart_delay: 60000,
      env: {
        YANDEX_LOGIN: serverSecrets.YANDEX_LOGIN || process.env.YANDEX_LOGIN || "",
        YANDEX_PASSWORD: serverSecrets.YANDEX_PASSWORD || process.env.YANDEX_PASSWORD || "",
        // POST downloaded CSV to branch server for ingestion.
        // Change port 3010 → 3000 if targeting production server.
        YANDEX_INGEST_URL: `http://130.49.149.212:3010/api/yandex/ingest`,
        ANALYTICS_REBUILD_SECRET: rebuildSecret,
        // WEBSITE_DB_PATH is not used in push mode (ingest happens on server)
        WEBSITE_DB_PATH: path.join(repoRoot, "website.db"),
      },
    },
  ],
};
