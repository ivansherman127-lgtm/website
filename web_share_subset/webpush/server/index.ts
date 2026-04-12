/**
 * Standalone Node.js HTTP server for the analytics dashboard.
 *
 * - GET /analytics/*  → serves dist-analytics/ (SPA)
 * - All API routes    → analytics handlers
 *
 * Run with:  node --import tsx/esm server/index.ts
 * Or via PM2: pm2 start server/index.ts --interpreter tsx
 */

import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { createReadStream, existsSync, readFileSync, writeFileSync, unlinkSync } from "node:fs";
import { join, extname, dirname, basename } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash, randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import Database from "better-sqlite3";

import { createD1Compat } from "./d1-compat.js";
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
  onRequestGet as leadsBreakdownGet,
} from "../functions/api/leads-breakdown.js";
import {
  onRequestPost as analyticsRebuildPost,
} from "../functions/api/analytics/rebuild.js";
import {
  onRequestPost as analyticsMaterializePost,
} from "../functions/api/analytics/materialize.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const PORT = parseInt(process.env.PORT ?? "3000", 10);

// Analytics DB and SPA dist dir (website.db + dist-analytics/)
const WEBSITE_DB_PATH = process.env.WEBSITE_DB_PATH ?? join(process.cwd(), "website.db");
const ANALYTICS_DIST_DIR = process.env.ANALYTICS_DIST_DIR ?? join(process.cwd(), "dist-analytics");
const ANALYTICS_REBUILD_SECRET = process.env.ANALYTICS_REBUILD_SECRET ?? "";

// Python binary — prefer venv if available
// __dirname = webpush/server/ → go up 3 levels to reach repo root
const repoRoot = join(__dirname, "..", "..", "..");
const _venvPython = join(repoRoot, ".venv", "bin", "python");
const PYTHON_BIN = existsSync(_venvPython) ? _venvPython : "python3";

// Read secrets from .env.server.json if present (takes priority over process.env)
let _serverSecrets: Record<string, string> = {};
try {
  const secretsPath = join(process.cwd(), ".env.server.json");
  _serverSecrets = JSON.parse(readFileSync(secretsPath, "utf8"));
} catch { /* file optional */ }
const ANALYTICS_PASSWORD: string = _serverSecrets["ANALYTICS_PASSWORD"] ?? process.env.ANALYTICS_PASSWORD ?? "";
const _analyticsRebuildSecret: string = _serverSecrets["ANALYTICS_REBUILD_SECRET"] ?? ANALYTICS_REBUILD_SECRET;
const SENDSAY_LOGIN = _serverSecrets["SENDSAY_LOGIN"] ?? process.env.SENDSAY_LOGIN ?? "";
const SENDSAY_PASSWORD = _serverSecrets["SENDSAY_PASSWORD"] ?? process.env.SENDSAY_PASSWORD ?? "";
const SENDSAY_SUBLOGIN = _serverSecrets["SENDSAY_SUBLOGIN"] ?? process.env.SENDSAY_SUBLOGIN ?? "";
const GOOGLE_APPLICATION_CREDENTIALS = _serverSecrets["GOOGLE_APPLICATION_CREDENTIALS"] ?? process.env.GOOGLE_APPLICATION_CREDENTIALS ?? "";
const GOOGLE_EXPORT_SHEET_ID = _serverSecrets["GOOGLE_EXPORT_SHEET_ID"] ?? process.env.GOOGLE_EXPORT_SHEET_ID ?? "";

// In-memory cache for dataset_json API responses (path → JSON body string).
// Populated lazily on first read; cleared whenever rebuild or materialize succeeds.
// This keeps repeat page loads fast without any DB round-trips.
const datasetCache = new Map<string, string>();

// Auth helpers
const ANALYTICS_COOKIE_NAME = "analytics_auth";

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

function isAnalyticsAuthenticated(req: IncomingMessage): boolean {
  if (!ANALYTICS_PASSWORD) return true;
  const cookies = parseCookies(req);
  return cookies[ANALYTICS_COOKIE_NAME] === analyticsCookieToken();
}

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

function serveAnalytics(res: ServerResponse, urlPath: string): void {
  const stripped = urlPath.replace(/^\/analytics(\/|$)/, "/") || "/";
  const cleanPath = stripped.split("?")[0] ?? "/";
  let filePath = join(ANALYTICS_DIST_DIR, cleanPath);
  if (!existsSync(filePath) || filePath.endsWith("/")) filePath = join(ANALYTICS_DIST_DIR, "index.html");
  if (!existsSync(filePath)) { res.writeHead(404); res.end("Not Found"); return; }
  const mime = MIME[extname(filePath)] ?? "application/octet-stream";
  const headers: Record<string, string> = { "content-type": mime };
  // Avoid stale index.html referencing an old hashed chunk after deploy (hard refresh was loading cached HTML).
  if (basename(filePath) === "index.html") {
    headers["cache-control"] = "no-store, no-cache, must-revalidate";
  }
  res.writeHead(200, headers);
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
      const apiDataUrl = new URL(rawUrl, "http://localhost");
      const skipDataCache =
        apiDataUrl.pathname === "/api/data" &&
        apiDataUrl.searchParams.get("path") === "dashboard_summary_dynamic.json" &&
        apiDataUrl.searchParams.get("preset") === "last_week";
      if (!skipDataCache) {
        const cached = datasetCache.get(cacheKey);
        if (cached !== undefined) {
          res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
          res.end(cached);
          return;
        }
      }
      const webReq = toWebRequest(req);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const webRes = await dataGet({ request: webReq, env: { DB: WEBSITEDB } as any });
      if (webRes.status === 200) {
        const body = await webRes.text();
        if (!skipDataCache) datasetCache.set(cacheKey, body);
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
      const assocUrl = new URL(`http://localhost${rawUrl}`);
      const recalc = String(assocUrl.searchParams.get("recalc") || "").toLowerCase() === "true";
      if (!recalc && websiteDb) {
        const dims = (assocUrl.searchParams.get("dims") || "event").trim();
        const cohort = (assocUrl.searchParams.get("cohort") || "all").trim();
        const pnlMode = (assocUrl.searchParams.get("pnlmode") || "cohort").trim();
        const from = (assocUrl.searchParams.get("from") || "").trim();
        const to = (assocUrl.searchParams.get("to") || "").trim();
        const persistedKey = `v14|dims=${dims}|cohort=${cohort}|pnlmode=${pnlMode}|from=${from}|to=${to}`;
        try {
          const row = websiteDb.prepare(
            "SELECT response_json FROM assoc_revenue_reports_cache WHERE cache_key = ? LIMIT 1",
          ).get(persistedKey) as { response_json?: string } | undefined;
          if (row?.response_json) {
            datasetCache.set(rawUrl, row.response_json);
            res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
            res.end(row.response_json);
            return;
          }
        } catch {
          // If persisted cache read fails, continue to handler path below.
        }
      }
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

    if (pathname === "/api/leads-breakdown") {
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
      const webRes = await leadsBreakdownGet({ request: webReq, env: { DB: WEBSITEDB } as any });
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
      const ingestArgs = ["-m", "db.upsert_yandex_from_csv", tmpCsv, ...extraArgs, "--skip-push", "--skip-rebuild"];
      try {
        writeFileSync(tmpCsv, csvBody);
        await new Promise<void>((resolve, reject) => {
          execFile(
            PYTHON_BIN,
            ingestArgs,
            { cwd: repoRoot, timeout: 120_000 },
            (err, stdout, stderr) => {
              if (stdout) console.log("[yandex-ingest]", stdout.trim());
              if (stderr) console.error("[yandex-ingest]", stderr.trim());
              if (err) reject(err); else resolve();
            }
          );
        });
        jsonRes(res, 200, { ok: true });
        // Non-blocking: trigger analytics rebuild so Yandex data appears immediately
        if (WEBSITEDB) {
          setImmediate(async () => {
            try {
              const rebuildReq = new Request("http://localhost/api/analytics/rebuild", {
                method: "POST",
                headers: { authorization: `Bearer ${_analyticsRebuildSecret}`, "content-type": "application/json" },
                body: JSON.stringify({ force: false }),
              });
              const rbRes = await analyticsRebuildPost({ request: rebuildReq, env: { DB: WEBSITEDB!, ANALYTICS_REBUILD_SECRET: _analyticsRebuildSecret } as any });
              if (rbRes.status === 200) datasetCache.clear();
              const rbResult = await rbRes.json() as Record<string, unknown>;
              console.log("[yandex-ingest] analytics rebuild:", JSON.stringify(rbResult));
            } catch (rbErr) {
              console.error("[yandex-ingest] rebuild trigger failed:", rbErr);
            }
          });
        }
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        console.error("[yandex-ingest] error:", msg);
        jsonRes(res, 500, { ok: false, error: msg });
      } finally {
        try { unlinkSync(tmpCsv); } catch { /* ignore */ }
      }
      return;
    }

    if (pathname === "/api/email/ingest") {
      if (req.method !== "POST") { jsonRes(res, 405, { ok: false, error: "method_not_allowed" }); return; }
      const ingestAuth = req.headers["authorization"] ?? "";
      const ingestMatch = /^Bearer\s+(.+)$/i.exec(ingestAuth.trim());
      if (!_analyticsRebuildSecret || !ingestMatch || ingestMatch[1] !== _analyticsRebuildSecret) {
        jsonRes(res, 401, { ok: false, error: "unauthorized" }); return;
      }
      if (!SENDSAY_LOGIN || !SENDSAY_PASSWORD) {
        jsonRes(res, 503, { ok: false, error: "sendsay_credentials_not_configured" }); return;
      }
      const urlObj = new URL(req.url || "/", "http://localhost");
      const dateFrom = urlObj.searchParams.get("from");
      // Default to "append" (incremental, deduplicates by ID) unless ?if_exists=replace is passed
      const ifExists = urlObj.searchParams.get("if_exists") === "replace" ? "replace" : "append";
      const extraArgs: string[] = ["--if-exists", ifExists];
      if (dateFrom) extraArgs.push("--from", dateFrom);
      try {
        await new Promise<void>((resolve, reject) => {
          execFile(
            PYTHON_BIN,
            ["-m", "db.fetch_sendsay_emails", ...extraArgs],
            {
              cwd: repoRoot,
              timeout: 180_000,
              env: {
                ...process.env,
                SENDSAY_LOGIN,
                SENDSAY_PASSWORD,
                SENDSAY_SUBLOGIN,
                WEBSITE_DB_PATH,
              },
            },
            (err, stdout, stderr) => {
              if (stdout) console.log("[email-ingest]", stdout.trim());
              if (stderr) console.error("[email-ingest]", stderr.trim());
              if (err) reject(err); else resolve();
            }
          );
        });
        jsonRes(res, 200, { ok: true });
        if (WEBSITEDB) {
          setImmediate(async () => {
            try {
              const rebuildReq = new Request("http://localhost/api/analytics/rebuild", {
                method: "POST",
                headers: { authorization: `Bearer ${_analyticsRebuildSecret}`, "content-type": "application/json" },
                body: JSON.stringify({ force: false }),
              });
              const rbRes = await analyticsRebuildPost({ request: rebuildReq, env: { DB: WEBSITEDB!, ANALYTICS_REBUILD_SECRET: _analyticsRebuildSecret } as any });
              if (rbRes.status === 200) datasetCache.clear();
              const rbResult = await rbRes.json() as Record<string, unknown>;
              console.log("[email-ingest] analytics rebuild:", JSON.stringify(rbResult));
            } catch (rbErr) {
              console.error("[email-ingest] rebuild trigger failed:", rbErr);
            }
          });
        }
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        console.error("[email-ingest] error:", msg);
        jsonRes(res, 500, { ok: false, error: msg });
      }
      return;
    }

    if (pathname === "/api/export-sheets") {
      if (req.method !== "POST") { jsonRes(res, 405, { ok: false, error: "method_not_allowed" }); return; }
      if (!isAnalyticsAuthenticated(req)) { jsonRes(res, 401, { ok: false, error: "unauthorized" }); return; }
      const body = await readBody(req);
      let payload: { gmail?: string; title?: string; headers?: string[]; rows?: string[][] } = {};
      try {
        payload = JSON.parse(body.toString("utf-8"));
      } catch {
        jsonRes(res, 400, { ok: false, error: "invalid_json" });
        return;
      }
      const gmail = String(payload.gmail ?? "").trim();
      const title = String(payload.title ?? "").trim() || `Analytics export ${new Date().toISOString().slice(0, 10)}`;
      const headers = Array.isArray(payload.headers) ? payload.headers.map((v) => String(v ?? "")) : [];
      const rows = Array.isArray(payload.rows) ? payload.rows.map((r) => (Array.isArray(r) ? r.map((v) => String(v ?? "")) : [])) : [];
      if (!gmail || !/^[^\s@]+@gmail\.com$/i.test(gmail)) {
        jsonRes(res, 400, { ok: false, error: "invalid_gmail", message: "Please provide a valid @gmail.com address" });
        return;
      }
      if (!headers.length) {
        jsonRes(res, 400, { ok: false, error: "empty_headers" });
        return;
      }
      const tmpJson = `/tmp/sheets_export_${randomUUID()}.json`;
      try {
        writeFileSync(tmpJson, JSON.stringify({ headers, rows }), "utf8");
        const out = await new Promise<string>((resolve, reject) => {
          execFile(
            PYTHON_BIN,
            [
              "-m",
              "db.export_table_to_sheets",
              "--json-file",
              tmpJson,
              "--gmail",
              gmail,
              "--title",
              title,
            ],
            {
              cwd: repoRoot,
              timeout: 180_000,
              env: {
                ...process.env,
                GOOGLE_APPLICATION_CREDENTIALS,
                GOOGLE_EXPORT_SHEET_ID,
              },
            },
            (err, stdout, stderr) => {
              if (stderr) console.error("[export-sheets]", stderr.trim());
              if (err) reject(new Error(stderr?.trim() || err.message));
              else resolve((stdout || "").trim());
            }
          );
        });
        let result: { ok?: boolean; url?: string } = {};
        try {
          result = JSON.parse(out || "{}");
        } catch {
          // Keep going with generic response if python did not emit JSON.
        }
        jsonRes(res, 200, {
          ok: true,
          url: result.url ?? null,
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        jsonRes(res, 500, { ok: false, error: "sheets_export_failed", message: msg });
      } finally {
        try { unlinkSync(tmpJson); } catch { /* ignore */ }
      }
      return;
    }

    if (pathname === "/api/dashboard-summary") {
      if (!WEBSITEDB || !websiteDb) { jsonRes(res, 503, { ok: false, error: "analytics_db_unavailable" }); return; }
      if (!isAnalyticsAuthenticated(req)) { jsonRes(res, 401, { ok: false, error: "unauthorized" }); return; }
      const q = new URL(req.url ?? "/", "http://localhost");
      const dFrom = String(q.searchParams.get("from") ?? "").trim();
      const dTo = String(q.searchParams.get("to") ?? "").trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(dFrom) || !/^\d{4}-\d{2}-\d{2}$/.test(dTo)) {
        jsonRes(res, 400, { ok: false, error: "invalid_date_range", message: "Use from/to in YYYY-MM-DD format" });
        return;
      }
      if (dFrom > dTo) {
        jsonRes(res, 400, { ok: false, error: "invalid_date_range", message: "from must be <= to" });
        return;
      }
      try {
        const row = websiteDb.prepare(
          `WITH range AS (
             SELECT @dFrom AS d_from, @dTo AS d_to
           ),
           bitrix_src AS (
             SELECT
               CASE
                 WHEN COALESCE("Дата создания", '') LIKE '____-__-__%' THEN SUBSTR("Дата создания", 1, 10)
                 WHEN COALESCE("Дата создания", '') LIKE '__.__.____%' THEN SUBSTR("Дата создания", 7, 4) || '-' || SUBSTR("Дата создания", 4, 2) || '-' || SUBSTR("Дата создания", 1, 2)
                 ELSE ''
               END AS created_date,
               COALESCE("Стадия", '') AS stage_name,
               COALESCE("Стадия сделки", '') AS stage_name_alt,
               COALESCE("Стадия лида", '') AS stage_name_lead,
               COALESCE(CAST(is_revenue_variant3 AS INTEGER), 0) AS is_revenue,
               COALESCE(revenue_amount, 0) AS revenue_amount,
               LOWER(COALESCE("UTM Source", '')) AS utm_source,
               LOWER(COALESCE("UTM Medium", '')) AS utm_medium,
               LOWER(COALESCE(event_class, '')) AS event_class_lc,
               LOWER(COALESCE("Название сделки", '')) AS deal_name_lower
             FROM mart_deals_enriched
           ),
           bitrix_agg AS (
             SELECT
               COUNT(*) AS total_leads,
               SUM(CASE
                 WHEN LOWER(TRIM(COALESCE(stage_name, stage_name_alt, stage_name_lead, ''))) LIKE '%квал%' THEN 1
                 WHEN LOWER(TRIM(COALESCE(stage_name, stage_name_alt, stage_name_lead, ''))) LIKE '%qual%' THEN 1
                 ELSE 0
               END) AS qual_leads,
               SUM(is_revenue) AS payments,
               SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS revenue,
               SUM(CASE WHEN utm_source IN ('email', 'sendsay') OR utm_medium = 'email' THEN 1 ELSE 0 END) AS email_leads,
               SUM(CASE WHEN event_class_lc = 'пбх' THEN 1 ELSE 0 END) AS pbh_regs,
               SUM(CASE
                 WHEN event_class_lc = 'старт карьеры в иб'
                   OR event_class_lc LIKE 'старт карьеры в иб (%'
                 THEN 1 ELSE 0
               END) AS start_ib_regs
             FROM bitrix_src
             WHERE created_date BETWEEN (SELECT d_from FROM range) AND (SELECT d_to FROM range)
           ),
           yandex_spend AS (
             SELECT COALESCE(SUM("Расход, ₽"), 0) AS budget
             FROM stg_yandex_stats
             WHERE COALESCE(NULLIF(TRIM(COALESCE("День", '')), ''), '') <> ''
               AND date(NULLIF(TRIM(COALESCE("День", '')), '')) BETWEEN (SELECT d_from FROM range) AND (SELECT d_to FROM range)
           ),
           yandex_clicks AS (
             SELECT COALESCE(SUM(COALESCE("Клики", 0)), 0) AS clicks
             FROM stg_yandex_stats
             WHERE COALESCE(NULLIF(TRIM(COALESCE("День", '')), ''), '') <> ''
               AND date(NULLIF(TRIM(COALESCE("День", '')), '')) BETWEEN (SELECT d_from FROM range) AND (SELECT d_to FROM range)
           ),
           yandex_leads AS (
             SELECT COUNT(*) AS leads
             FROM mart_yandex_leads_raw l
             LEFT JOIN mart_deals_enriched m ON m."ID" = l."ID"
             WHERE (
               CASE
                 WHEN COALESCE(m."Дата создания", '') LIKE '____-__-__%' THEN SUBSTR(m."Дата создания", 1, 10)
                 WHEN COALESCE(m."Дата создания", '') LIKE '__.__.____%' THEN SUBSTR(m."Дата создания", 7, 4) || '-' || SUBSTR(m."Дата создания", 4, 2) || '-' || SUBSTR(m."Дата создания", 1, 2)
                 ELSE ''
               END
             ) BETWEEN (SELECT d_from FROM range) AND (SELECT d_to FROM range)
           ),
           email_agg AS (
             SELECT
               COUNT(*) AS campaigns,
               COALESCE(SUM("Открытий"), 0) AS opens
             FROM stg_email_sends
             WHERE date("Дата отправки") BETWEEN (SELECT d_from FROM range) AND (SELECT d_to FROM range)
           )
           SELECT
             (SELECT d_from FROM range) AS date_from,
             (SELECT d_to FROM range) AS date_to,
             ba.total_leads AS "Всего заявок",
             ba.qual_leads AS "Квал лидов",
             CASE WHEN ba.total_leads > 0 THEN ROUND(ba.qual_leads * 100.0 / ba.total_leads, 1) ELSE 0 END AS "Конверсия в квал %",
             ba.payments AS "Оплат",
             CASE WHEN ba.qual_leads > 0 THEN ROUND(ba.payments * 100.0 / ba.qual_leads, 1) ELSE 0 END AS "Конверсия в оплату из квал %",
             ba.revenue AS "Выручка",
             CASE WHEN ba.payments > 0 THEN ROUND(ba.revenue * 1.0 / ba.payments, 0) ELSE 0 END AS "Средний чек",
             ys.budget AS "Бюджет на рекламу",
             yc.clicks AS "Кликов из Яндекса",
             yl.leads AS "Лидов с рекламы",
             CASE WHEN yl.leads > 0 THEN ROUND(ys.budget * 1.0 / yl.leads, 0) ELSE 0 END AS "Стоимость лида",
             ea.campaigns AS "Рассылок",
             ea.opens AS "Открытий email",
             ba.email_leads AS "Заявок email",
             ba.pbh_regs AS "Рег на ПБХ",
             ba.start_ib_regs AS "Рег на Старт в ИБ",
             ba.start_ib_regs AS "Рег на ИБ"
           FROM bitrix_agg ba, yandex_spend ys, yandex_clicks yc, yandex_leads yl, email_agg ea`
        ).get({ dFrom, dTo }) as Record<string, unknown> | undefined;
        jsonRes(res, 200, { ok: true, row: row ?? null });
      } catch (e) {
        jsonRes(res, 500, { ok: false, error: String(e) });
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

    // Root → analytics dashboard
    if (pathname === "/") {
      res.writeHead(302, { "location": "/analytics/" }); res.end(); return;
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

    if (pathname.startsWith("/api/")) {
      jsonRes(res, 404, { ok: false, error: "not_found" });
      return;
    }

    res.writeHead(404);
    res.end("Not Found");
  } catch (err) {
    console.error("Request error:", err);
    jsonRes(res, 500, { ok: false, error: "internal_server_error" });
  }
});

server.listen(PORT, () => {
  console.log(`Analytics server listening on port ${PORT}`);
  console.log(`  DB:   ${WEBSITE_DB_PATH}`);
  console.log(`  Dist: ${ANALYTICS_DIST_DIR}`);
  console.log(`  cwd:  ${process.cwd()}`);

  // Startup stays lightweight; caches are built lazily per-request.
});

function shutdown() {
  server.close(() => {
    try { websiteDb?.close(); } catch { /* ignore */ }
    process.exit(0);
  });
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
