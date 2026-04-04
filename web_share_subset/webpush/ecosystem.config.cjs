// PM2 ecosystem config for the UTM server.
// CommonJS (.cjs) so PM2 can load it even when package.json has "type":"module".
//
// Usage:
//   pm2 start ecosystem.config.cjs          — start all apps
//   pm2 start ecosystem.config.cjs --only utm-server
//   pm2 start ecosystem.config.cjs --only b24-sync

const path = require("path");

// tsx v4 uses spawnSync internally, so PM2 kills tsx but its child node process
// (which holds the HTTP port) survives as an orphan. Fix: run node directly with
// tsx registered as an ESM loader via --import, so PM2 manages node itself.
const tsxEsmPath = require.resolve("tsx/esm", { paths: [__dirname] });
const tsxEsmUrl = `file://${tsxEsmPath}`;

// Load secrets from a local file not tracked by git.
// Create .env.server.json on the server:
//   { "UTM_PASSWORD": "...", "B24_WEBHOOK_URL": "https://your.bitrix24.ru/rest/1/TOKEN/" }
let serverSecrets = {};
try { serverSecrets = require("./.env.server.json"); } catch (_) {}

// Repo root is two levels up from webpush/
const repoRoot = path.resolve(__dirname, "..", "..");

// Python interpreter — use virtualenv if present, otherwise system python3
const venvPython = path.join(repoRoot, ".venv", "bin", "python");
const fs = require("fs");
const pythonBin = fs.existsSync(venvPython) ? venvPython : "python3";

module.exports = {
  apps: [
    {
      name: "utm-server",
      // Run node directly with tsx as ESM loader so PM2 manages the node
      // process itself. SIGTERM goes straight to node → our shutdown() handler.
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
        PORT: process.env.PORT || "3000",
        UTM_DB_PATH: process.env.UTM_DB_PATH || path.join(__dirname, "..", "..", "utm.db"),
        DIST_DIR: process.env.DIST_DIR || path.join(__dirname, "dist-utm"),
        UTM_PASSWORD: serverSecrets.UTM_PASSWORD || process.env.UTM_PASSWORD || "",
      },
    },
    {
      name: "b24-sync",
      // PM2: set script to the binary, args to the arguments, interpreter to "none"
      script: pythonBin,
      args: "-m db.b24_fetch_crm --watch --interval 60",
      interpreter: "none",
      cwd: repoRoot,
      watch: false,
      autorestart: true,
      restart_delay: 10000,
      env: {
        WEBSITE_DB_PATH: process.env.WEBSITE_DB_PATH || path.join(repoRoot, "website.db"),
        B24_WEBHOOK_URL: serverSecrets.B24_WEBHOOK_URL || process.env.B24_WEBHOOK_URL || "",
      },
    },
    {
      name: "yandex-sync",
      script: pythonBin,
      args: "-m db.yandex_api_sync --watch --interval 180",
      interpreter: "none",
      cwd: repoRoot,
      watch: false,
      autorestart: true,
      restart_delay: 30000,
      env: {
        WEBSITE_DB_PATH: process.env.WEBSITE_DB_PATH || path.join(repoRoot, "website.db"),
        YANDEX_TOKEN: serverSecrets.YANDEX_TOKEN || process.env.YANDEX_TOKEN || "",
      },
    },
  ],
};
