/**
 * Standalone Node.js HTTP server for the UTM tool.
 *
 * Replaces the Cloudflare Worker on a plain Ubuntu server.
 * - GET/POST /api/utm  → utm.ts handler (unchanged) via D1 compat adapter
 * - GET /              → serves dist-utm/ (SPA fallback to index.html)
 *
 * Run with:  node --import tsx/esm server/index.ts
 * Or via PM2: pm2 start server/index.ts --interpreter tsx
 */

import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { createReadStream, existsSync, readFileSync, writeFileSync, unlinkSync } from "node:fs";
import { join, extname, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash, randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import Database from "better-sqlite3";

import { createD1Compat } from "./d1-compat.js";
import {
  onRequestGet as utmGet,
  onRequestPost as utmPost,
} from "../functions/api/utm.js";
import {
  onRequestGet as dataGet,
} from "../functions/api/data.js";
import {
  onRequestGet as assocRevenueGet,
} from "../functions/api/assoc-revenue.js";
import {
  onRequestGet as cohortDealsGet,
} from "../functions/api/cohort-deals.js";
import {
  onRequestPost as analyticsRebuildPost,
} from "../functions/api/analytics/rebuild.js";
import {
  onRequestPost as analyticsMaterializePost,
} from "../functions/api/analytics/materialize.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const PORT = parseInt(process.env.PORT ?? "3000", 10);
const DB_PATH = process.env.UTM_DB_PATH ?? join(process.cwd(), "utm.db");
const DIST_DIR = process.env.DIST_DIR ?? join(process.cwd(), "dist-utm");
const SCHEMA_PATH = join(process.cwd(), "server", "utm-schema.sql");

// Analytics DB and SPA dist dir (website.db + dist-analytics/)
const WEBSITE_DB_PATH = process.env.WEBSITE_DB_PATH ?? join(process.cwd(), "website.db");
const ANALYTICS_DIST_DIR = process.env.ANALYTICS_DIST_DIR ?? join(process.cwd(), "dist-analytics");
const ANALYTICS_REBUILD_SECRET = process.env.ANALYTICS_REBUILD_SECRET ?? "";

// Python binary — prefer venv if available
const repoRoot = join(__dirname, "..", "..");
const _venvPython = join(repoRoot, ".venv", "bin", "python");
const PYTHON_BIN = existsSync(_venvPython) ? _venvPython : "python3";

// Read secrets from .env.server.json if present (takes priority over process.env)
let _serverSecrets: Record<string, string> = {};
try {
  const secretsPath = join(process.cwd(), ".env.server.json");
  _serverSecrets = JSON.parse(readFileSync(secretsPath, "utf8"));
} catch { /* file optional */ }
const UTM_PASSWORD: string = _serverSecrets["UTM_PASSWORD"] ?? process.env.UTM_PASSWORD ?? "";
console.log(`[auth] UTM_PASSWORD set: ${UTM_PASSWORD ? "yes" : "NO - open access"}`);
const ANALYTICS_PASSWORD: string = _serverSecrets["ANALYTICS_PASSWORD"] ?? process.env.ANALYTICS_PASSWORD ?? UTM_PASSWORD;
const _analyticsRebuildSecret: string = _serverSecrets["ANALYTICS_REBUILD_SECRET"] ?? ANALYTICS_REBUILD_SECRET;

// In-memory cache for dataset_json API responses (path → JSON body string).
// Populated lazily on first read; cleared whenever rebuild or materialize succeeds.
// This keeps repeat page loads fast without any DB round-trips.
const datasetCache = new Map<string, string>();

// Auth helpers
const COOKIE_NAME = "utm_auth";
const ANALYTICS_COOKIE_NAME = "analytics_auth";

function cookieToken(): string {
  return createHash("sha256").update("utm:" + UTM_PASSWORD).digest("hex");
}

function analyticsCookieToken(): string {
  return createHash("sha256").update("analytics:" + ANALYTICS_PASSWORD).digest("hex");
}

function parseCookies(req: IncomingMessage): Record<string, string> {
  const header = req.headers.cookie ?? "";
  return Object.fromEntries(
    header.split(";").flatMap((s) => {
      const eq = s.indexOf("=");
      if (eq === -1) return [];
      return [[s.slice(0, eq).trim(), s.slice(eq + 1).trim()]];
    })
  );
}

function isAuthenticated(req: IncomingMessage): boolean {
  if (!UTM_PASSWORD) return true; // no password configured → open
  return parseCookies(req)[COOKIE_NAME] === cookieToken();
}

function isAnalyticsAuthenticated(req: IncomingMessage): boolean {
  if (!ANALYTICS_PASSWORD) return true;
  const cookies = parseCookies(req);
  return (
    cookies[ANALYTICS_COOKIE_NAME] === analyticsCookieToken() ||
    // Also accept utm_auth cookie so users already logged into the UTM tool
    // don't need to log in again for analytics.
    cookies[COOKIE_NAME] === cookieToken()
  );
}

const LOGIN_HTML = (error: boolean) => `<!DOCTYPE html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>UTM Builder — вход</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; background: #f5f7fa; }
    form { background: #fff; padding: 2rem; border-radius: 10px; box-shadow: 0 2px 12px rgba(0,0,0,.1); display: flex; flex-direction: column; gap: 1rem; width: 300px; }
    h2 { margin: 0; font-size: 1.25rem; color: #111; }
    input[type=password] { padding: .65rem .85rem; border: 1px solid #d1d5db; border-radius: 6px; font-size: 1rem; outline: none; }
    input[type=password]:focus { border-color: #2563eb; box-shadow: 0 0 0 2px rgba(37,99,235,.2); }
    button { padding: .65rem; background: #2563eb; color: #fff; border: none; border-radius: 6px; font-size: 1rem; cursor: pointer; font-weight: 600; }
    button:hover { background: #1d4ed8; }
    .err { color: #dc2626; font-size: .875rem; margin: 0; }
  </style>
</head>
<body>
  <form method="POST" action="/utm/login">
    <h2>UTM Builder</h2>
    ${error ? "<p class=\"err\">Неверный пароль</p>" : ""}
    <input type="password" name="password" placeholder="Пароль" autofocus required />
    <button type="submit">Войти</button>
  </form>
</body>
</html>`;

const ANALYTICS_LOGIN_HTML = (error: boolean) => `<!DOCTYPE html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Аналитика — вход</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; background: #f5f7fa; }
    form { background: #fff; padding: 2rem; border-radius: 10px; box-shadow: 0 2px 12px rgba(0,0,0,.1); display: flex; flex-direction: column; gap: 1rem; width: 300px; }
    h2 { margin: 0; font-size: 1.25rem; color: #111; }
    input[type=password] { padding: .65rem .85rem; border: 1px solid #d1d5db; border-radius: 6px; font-size: 1rem; outline: none; }
    input[type=password]:focus { border-color: #2563eb; box-shadow: 0 0 0 2px rgba(37,99,235,.2); }
    button { padding: .65rem; background: #2563eb; color: #fff; border: none; border-radius: 6px; font-size: 1rem; cursor: pointer; font-weight: 600; }
    button:hover { background: #1d4ed8; }
    .err { color: #dc2626; font-size: .875rem; margin: 0; }
  </style>
</head>
<body>
  <form method="POST" action="/analytics/login">
    <h2>Аналитика</h2>
    ${error ? "<p class=\"err\">Неверный пароль</p>" : ""}
    <input type="password" name="password" placeholder="Пароль" autofocus required />
    <button type="submit">Войти</button>
  </form>
</body>
</html>`;

// Initialise database and apply schema if present
const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
if (existsSync(SCHEMA_PATH)) {
  db.exec(readFileSync(SCHEMA_PATH, "utf8"));
}

// Apply incremental column additions (idempotent — ignore "duplicate column" errors)
const COLUMN_MIGRATIONS: string[] = [
  "ALTER TABLE utm_tags ADD COLUMN created_by TEXT NOT NULL DEFAULT ''",
];
for (const stmt of COLUMN_MIGRATIONS) {
  try { db.prepare(stmt).run(); } catch { /* column already exists */ }
}

const UTM = createD1Compat(db);

// Analytics DB (website.db) — opened read-write for rebuild endpoint, WAL for concurrency.
let websiteDb: Database.Database | null = null;
let WEBSITEDB: ReturnType<typeof createD1Compat> | null = null;
try {
  if (existsSync(WEBSITE_DB_PATH)) {
    websiteDb = new Database(WEBSITE_DB_PATH);
    websiteDb.pragma("journal_mode = WAL");
    // Ensure analytics_build_meta table exists (needed by rebuild/materialize)
    websiteDb.exec(`CREATE TABLE IF NOT EXISTS analytics_build_meta (
      k TEXT PRIMARY KEY NOT NULL,
      v TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`);
    websiteDb.exec(`CREATE TABLE IF NOT EXISTS dataset_json (
      path TEXT NOT NULL,
      chunk INTEGER NOT NULL DEFAULT 0,
      body TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (path, chunk)
    )`);
    WEBSITEDB = createD1Compat(websiteDb);
    console.log(`[analytics] website.db opened: ${WEBSITE_DB_PATH}`);
  } else {
    console.log(`[analytics] website.db not found at ${WEBSITE_DB_PATH} — analytics disabled`);
  }
} catch (e) {
  console.error("[analytics] Failed to open website.db:", e);
}

const MIME: Record<string, string> = {
  ".html":  "text/html; charset=utf-8",
  ".js":    "application/javascript; charset=utf-8",
  ".css":   "text/css; charset=utf-8",
  ".json":  "application/json; charset=utf-8",
  ".png":   "image/png",
  ".ico":   "image/x-icon",
  ".svg":   "image/svg+xml",
  ".woff2": "font/woff2",
  ".woff":  "font/woff",
  ".ttf":   "font/ttf",
  ".map":   "application/json",
};

function jsonRes(res: ServerResponse, status: number, body: unknown): void {
  const data = JSON.stringify(body);
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(data);
}

function serveStatic(res: ServerResponse, urlPath: string): void {
  // Strip the /utm prefix so we can look up files in dist-utm/
  const stripped = urlPath.replace(/^\/utm(\/|$)/, "/") || "/";
  const cleanPath = stripped.split("?")[0] ?? "/";
  let filePath = join(DIST_DIR, cleanPath);

  if (!existsSync(filePath) || filePath.endsWith("/")) {
    filePath = join(DIST_DIR, "index.html");
  }

  if (!existsSync(filePath)) {
    res.writeHead(404);
    res.end("Not Found");
    return;
  }

  const mime = MIME[extname(filePath)] ?? "application/octet-stream";
  res.writeHead(200, { "content-type": mime });
  createReadStream(filePath).pipe(res);
}

function serveAnalytics(res: ServerResponse, urlPath: string): void {
  const stripped = urlPath.replace(/^\/analytics(\/|$)/, "/") || "/";
  const cleanPath = stripped.split("?")[0] ?? "/";
  let filePath = join(ANALYTICS_DIST_DIR, cleanPath);
  if (!existsSync(filePath) || filePath.endsWith("/")) filePath = join(ANALYTICS_DIST_DIR, "index.html");
  if (!existsSync(filePath)) { res.writeHead(404); res.end("Not Found"); return; }
  const mime = MIME[extname(filePath)] ?? "application/octet-stream";
  res.writeHead(200, { "content-type": mime });
  createReadStream(filePath).pipe(res);
}

function toWebRequest(req: IncomingMessage, body?: Buffer): Request {
  const host = req.headers.host ?? "localhost";
  const url = `http://${host}${req.url ?? "/"}`;
  const headers = new Headers();
  for (const [k, v] of Object.entries(req.headers)) {
    if (typeof v === "string") headers.set(k, v);
  }
  return new Request(url, {
    method: req.method ?? "GET",
    headers,
    body: body?.length ? body : undefined,
  });
}

function readBody(req: IncomingMessage): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

async function sendWebResponse(webRes: Response, res: ServerResponse): Promise<void> {
  const headers: Record<string, string> = {};
  webRes.headers.forEach((v, k) => { headers[k] = v; });
  res.writeHead(webRes.status, headers);
  res.end(await webRes.text());
}

const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
  const rawUrl = req.url ?? "/";
  const pathname = rawUrl.split("?")[0] ?? "/";

  try {
    // ── Analytics login (no auth required) ──────────────────────────────────
    if (pathname === "/analytics/login") {
      if (req.method === "GET") {
        res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        res.end(ANALYTICS_LOGIN_HTML(false));
      } else if (req.method === "POST") {
        const body = await readBody(req);
        const params = new URLSearchParams(body.toString());
        const submitted = params.get("password") ?? "";
        if (ANALYTICS_PASSWORD && submitted === ANALYTICS_PASSWORD) {
          const expires = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toUTCString();
          res.writeHead(302, {
            "set-cookie": `${ANALYTICS_COOKIE_NAME}=${analyticsCookieToken()}; HttpOnly; SameSite=Lax; Path=/; Expires=${expires}`,
            "location": "/analytics/",
          });
          res.end();
        } else {
          res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
          res.end(ANALYTICS_LOGIN_HTML(true));
        }
      } else {
        res.writeHead(405); res.end();
      }
      return;
    }

    // ── Analytics auth gate ───────────────────────────────────────────────────
    if (
      (pathname.startsWith("/analytics") || pathname === "/analytics") &&
      !isAnalyticsAuthenticated(req)
    ) {
      if (pathname.startsWith("/api/")) {
        jsonRes(res, 401, { ok: false, error: "unauthorized" });
      } else {
        res.writeHead(302, { "location": "/analytics/login" });
        res.end();
      }
      return;
    }

    // ── Analytics API routes (authenticated) ─────────────────────────────────
    if (pathname === "/api/data" || pathname.startsWith("/api/data?")) {
      if (!WEBSITEDB) { jsonRes(res, 503, { ok: false, error: "analytics_db_unavailable" }); return; }
      if (!isAnalyticsAuthenticated(req)) { jsonRes(res, 401, { ok: false, error: "unauthorized" }); return; }
      const cacheKey = rawUrl;
      const cached = datasetCache.get(cacheKey);
      if (cached !== undefined) {
        res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
        res.end(cached);
        return;
      }
      const webReq = toWebRequest(req);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const webRes = await dataGet({ request: webReq, env: { DB: WEBSITEDB } as any });
      if (webRes.status === 200) {
        const body = await webRes.text();
        datasetCache.set(cacheKey, body);
        res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
        res.end(body);
      } else {
        await sendWebResponse(webRes, res);
      }
      return;
    }

    if (pathname === "/api/assoc-revenue") {
      if (!WEBSITEDB) { jsonRes(res, 503, { ok: false, error: "analytics_db_unavailable" }); return; }
      if (!isAnalyticsAuthenticated(req)) { jsonRes(res, 401, { ok: false, error: "unauthorized" }); return; }
      // Cache key must include query params — assoc-revenue accepts dims/cohort/pnlmode/from/to
      const cacheKey = rawUrl;
      const cached = datasetCache.get(cacheKey);
      if (cached !== undefined) {
        res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
        res.end(cached);
        return;
      }
      const webReq = toWebRequest(req);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const webRes = await assocRevenueGet({ request: webReq, env: { DB: WEBSITEDB } as any });
      if (webRes.status === 200) {
        const body = await webRes.text();
        datasetCache.set(cacheKey, body);
        res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
        res.end(body);
      } else {
        await sendWebResponse(webRes, res);
      }
      return;
    }

    if (pathname === "/api/cohort-deals") {
      if (!WEBSITEDB) { jsonRes(res, 503, { ok: false, error: "analytics_db_unavailable" }); return; }
      if (!isAnalyticsAuthenticated(req)) { jsonRes(res, 401, { ok: false, error: "unauthorized" }); return; }
      const cached = datasetCache.get("/api/cohort-deals");
      if (cached !== undefined) {
        res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
        res.end(cached);
        return;
      }
      const webReq = toWebRequest(req);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const webRes = await cohortDealsGet({ request: webReq, env: { DB: WEBSITEDB } as any });
      if (webRes.status === 200) {
        const body = await webRes.text();
        datasetCache.set("/api/cohort-deals", body);
        res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
        res.end(body);
      } else {
        await sendWebResponse(webRes, res);
      }
      return;
    }

    if (pathname === "/api/yandex/ingest") {
      if (req.method !== "POST") { jsonRes(res, 405, { ok: false, error: "method_not_allowed" }); return; }
      // Auth: same bearer secret as analytics rebuild
      const ingestAuth = req.headers["authorization"] ?? "";
      const ingestMatch = /^Bearer\s+(.+)$/i.exec(ingestAuth.trim());
      if (!_analyticsRebuildSecret || !ingestMatch || ingestMatch[1] !== _analyticsRebuildSecret) {
        jsonRes(res, 401, { ok: false, error: "unauthorized" }); return;
      }
      const csvBody = await readBody(req);
      const tmpCsv = `/tmp/yandex_ingest_${randomUUID()}.csv`;
      // Optional ?month=YYYY-MM for wizard exports without a date column
      const urlObj = new URL(req.url || "/", "http://localhost");
      const monthArg = urlObj.searchParams.get("month");
      const extraArgs = monthArg ? ["--month", monthArg] : [];
      try {
        writeFileSync(tmpCsv, csvBody);
        await new Promise<void>((resolve, reject) => {
          execFile(
            PYTHON_BIN,
            ["-m", "db.upsert_yandex_from_csv", tmpCsv, ...extraArgs],
            { cwd: repoRoot, timeout: 120_000 },
            (err, stdout, stderr) => {
              if (stdout) console.log("[yandex-ingest]", stdout.trim());
              if (stderr) console.error("[yandex-ingest]", stderr.trim());
              if (err) reject(err); else resolve();
            }
          );
        });
        jsonRes(res, 200, { ok: true });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        console.error("[yandex-ingest] error:", msg);
        jsonRes(res, 500, { ok: false, error: msg });
      } finally {
        try { unlinkSync(tmpCsv); } catch { /* ignore */ }
      }
      return;
    }

    if (pathname === "/api/analytics/rebuild") {
      if (!WEBSITEDB) { jsonRes(res, 503, { ok: false, error: "analytics_db_unavailable" }); return; }
      if (req.method !== "POST") { jsonRes(res, 405, { ok: false, error: "method_not_allowed" }); return; }
      const body = await readBody(req);
      const webReq = toWebRequest(req, body);
      // Override secret so the handler can verify the bearer token
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const webRes = await analyticsRebuildPost({ request: webReq, env: { DB: WEBSITEDB, ANALYTICS_REBUILD_SECRET: _analyticsRebuildSecret } as any });
      if (webRes.status === 200) datasetCache.clear();
      await sendWebResponse(webRes, res);
      return;
    }

    if (pathname === "/api/analytics/materialize") {
      if (!WEBSITEDB) { jsonRes(res, 503, { ok: false, error: "analytics_db_unavailable" }); return; }
      if (req.method !== "POST") { jsonRes(res, 405, { ok: false, error: "method_not_allowed" }); return; }
      if (!isAnalyticsAuthenticated(req)) { jsonRes(res, 401, { ok: false, error: "unauthorized" }); return; }
      const body = await readBody(req);
      const webReq = toWebRequest(req, body);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const webRes = await analyticsMaterializePost({ request: webReq, env: { DB: WEBSITEDB } as any });
      if (webRes.status === 200) datasetCache.clear();
      await sendWebResponse(webRes, res);
      return;
    }

    // ── Analytics SPA static files ────────────────────────────────────────────
    if (pathname === "/analytics" || pathname.startsWith("/analytics/")) {
      if (!isAnalyticsAuthenticated(req)) {
        res.writeHead(302, { "location": "/analytics/login" }); res.end(); return;
      }
      if (pathname === "/analytics") {
        res.writeHead(302, { "location": "/analytics/" }); res.end(); return;
      }
      serveAnalytics(res, pathname);
      return;
    }

    // Login endpoints — always accessible
    if (pathname === "/utm/login") {
      if (req.method === "GET") {
        res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        res.end(LOGIN_HTML(false));
      } else if (req.method === "POST") {
        const body = await readBody(req);
        const params = new URLSearchParams(body.toString());
        const submitted = params.get("password") ?? "";
        if (UTM_PASSWORD && submitted === UTM_PASSWORD) {
          const expires = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toUTCString();
          res.writeHead(302, {
            "set-cookie": `${COOKIE_NAME}=${cookieToken()}; HttpOnly; SameSite=Lax; Path=/; Expires=${expires}`,
            "location": "/utm/",
          });
          res.end();
        } else {
          res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
          res.end(LOGIN_HTML(true));
        }
      } else {
        res.writeHead(405); res.end();
      }
      return;
    }

    // Auth gate — redirect unauthenticated requests to login
    if (!isAuthenticated(req)) {
      if (pathname.startsWith("/api/")) {
        jsonRes(res, 401, { ok: false, error: "unauthorized" });
      } else {
        res.writeHead(302, { "location": "/utm/login" });
        res.end();
      }
      return;
    }

    if (pathname === "/api/utm") {
      if (req.method === "GET") {
        const webReq = toWebRequest(req);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const webRes = await utmGet({ request: webReq, env: { UTM } as any });
        await sendWebResponse(webRes, res);
      } else if (req.method === "POST") {
        const body = await readBody(req);
        const webReq = toWebRequest(req, body);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const webRes = await utmPost({ request: webReq, env: { UTM } as any });
        await sendWebResponse(webRes, res);
      } else {
        jsonRes(res, 405, { ok: false, error: "method_not_allowed" });
      }
      return;
    }

    if (pathname.startsWith("/api/")) {
      jsonRes(res, 404, { ok: false, error: "not_found" });
      return;
    }

    if (!pathname.startsWith("/utm/") && pathname !== "/utm") {
      res.writeHead(404);
      res.end("Not Found");
      return;
    }

    serveStatic(res, pathname);
  } catch (err) {
    console.error("Request error:", err);
    jsonRes(res, 500, { ok: false, error: "internal_server_error" });
  }
});

server.listen(PORT, () => {
  console.log(`UTM server listening on port ${PORT}`);
  console.log(`  DB:   ${DB_PATH}`);
  console.log(`  Dist: ${DIST_DIR}`);
  console.log(`  cwd:  ${process.cwd()}`);

  // Pre-warm the assoc-revenue cache after startup so the first user request
  // is served from cache instead of running the heavy SQL query cold.
  if (WEBSITEDB) {
    const warmupDims = ["event", "yandex_campaign", "email_campaign"];
    for (const dims of warmupDims) {
      const warmReq = new Request(`http://localhost:${PORT}/api/assoc-revenue?dims=${dims}`, {
        headers: { cookie: `analytics_auth=${analyticsCookieToken()}` },
      });
      assocRevenueGet({ request: warmReq, env: { DB: WEBSITEDB } as never })
        .then((r) => {
          if (r.status === 200) {
            return r.text().then((body) => {
              datasetCache.set(`/api/assoc-revenue?dims=${dims}`, body);
              console.log(`[warmup] assoc-revenue?dims=${dims} cached (${body.length} bytes)`);
            });
          }
        })
        .catch((e: unknown) => {
          console.warn(`[warmup] assoc-revenue?dims=${dims} failed:`, e);
        });
    }
  }
});

function shutdown() {
  server.close(() => {
    db.close();
    try { websiteDb?.close(); } catch { /* ignore */ }
    process.exit(0);
  });
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
