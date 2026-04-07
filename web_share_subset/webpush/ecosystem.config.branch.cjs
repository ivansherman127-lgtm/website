// PM2 ecosystem config for the branch-preview server.
// Runs on port 3001, serves dist-branch-analytics/ at /analytics/ internally,
// while nginx proxies new.cyber-ed.ru/branch/* → localhost:3001/*.
//
// Usage (from /opt/utm-app-branch/web_share_subset/webpush):
//   pm2 start ecosystem.config.branch.cjs
//   pm2 restart utm-server-branch

const path = require("path");
const tsxEsmPath = require.resolve("tsx/esm", { paths: [__dirname] });
const tsxEsmUrl = `file://${tsxEsmPath}`;

let serverSecrets = {};
try { serverSecrets = require("./.env.server.json"); } catch (_) {}

const repoRoot = path.resolve(__dirname, "..", "..");
const venvPython = path.join(repoRoot, ".venv", "bin", "python");
const fs = require("fs");
const pythonBin = fs.existsSync(venvPython) ? venvPython : "python3";

module.exports = {
  apps: [
    {
      name: "utm-server-branch",
      script: "node",
      args: `--no-warnings --import ${tsxEsmUrl} server/index.ts`,
      interpreter: "none",
      cwd: __dirname,
      watch: false,
      autorestart: true,
      restart_delay: 3000,
      kill_signal: "SIGTERM",
      kill_timeout: 5000,
      env: {
        NODE_ENV: "production",
        PORT: "3010",
        UTM_DB_PATH: process.env.UTM_DB_PATH || path.join(__dirname, "..", "..", "utm.db"),
        DIST_DIR: process.env.DIST_DIR || path.join(__dirname, "dist-utm"),
        UTM_PASSWORD: serverSecrets.UTM_PASSWORD || process.env.UTM_PASSWORD || "",
        ANALYTICS_PASSWORD: serverSecrets.ANALYTICS_PASSWORD || process.env.ANALYTICS_PASSWORD || "",
        // Uses a per-branch copy of website.db (copied from production during setup).
        // Allows branch schema migrations to run without touching production DB.
        WEBSITE_DB_PATH: process.env.WEBSITE_DB_PATH || path.join(repoRoot, "website.db"),
        ANALYTICS_DIST_DIR: process.env.ANALYTICS_DIST_DIR || path.join(__dirname, "dist-branch-analytics"),
        ANALYTICS_REBUILD_SECRET: serverSecrets.ANALYTICS_REBUILD_SECRET || process.env.ANALYTICS_REBUILD_SECRET || "",
      },
    },
    {
      name: "yandex-sync-branch",
      script: pythonBin,
      args: "-m db.yandex_direct_scraper --watch --interval 180",
      interpreter: "none",
      cwd: repoRoot,
      watch: false,
      // NOTE: Server IP (reg.ru datacenter) is blocked by Yandex web access.
      // The scraper must run on a residential IP (local Mac). This process is
      // disabled on the server. To receive ingested data, the Mac scraper POSTs
      // CSV to /api/yandex/ingest, which runs upsert_yandex_from_csv locally.
      autorestart: false,
      env: {
        WEBSITE_DB_PATH: process.env.WEBSITE_DB_PATH || path.join(repoRoot, "website.db"),
        YANDEX_LOGIN: serverSecrets.YANDEX_LOGIN || process.env.YANDEX_LOGIN || "",
        YANDEX_PASSWORD: serverSecrets.YANDEX_PASSWORD || process.env.YANDEX_PASSWORD || "",
        ANALYTICS_REBUILD_URL: "http://127.0.0.1:3010/api/analytics/rebuild",
        ANALYTICS_REBUILD_SECRET: serverSecrets.ANALYTICS_REBUILD_SECRET || process.env.ANALYTICS_REBUILD_SECRET || "",
      },
    },
  ],
};
