// PM2 ecosystem config for the UTM server.
// CommonJS (.cjs) so PM2 can load it even when package.json has "type":"module".
//
// Usage:
//   pm2 start ecosystem.config.cjs          — start all apps
//   pm2 start ecosystem.config.cjs --only utm-server
//   pm2 start ecosystem.config.cjs --only b24-sync

const path = require("path");

// Resolve tsx binary relative to this file so it works regardless of cwd.
const tsxBin = path.join(__dirname, "node_modules", ".bin", "tsx");

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
      script: "server/index.ts",
      interpreter: tsxBin,
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
  ],
};
