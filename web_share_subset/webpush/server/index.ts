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
import { createReadStream, existsSync, readFileSync } from "node:fs";
import { join, extname, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import Database from "better-sqlite3";

import { createD1Compat } from "./d1-compat.js";
import {
  onRequestGet as utmGet,
  onRequestPost as utmPost,
} from "../functions/api/utm.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const PORT = parseInt(process.env.PORT ?? "3000", 10);
const DB_PATH = process.env.UTM_DB_PATH ?? join(process.cwd(), "utm.db");
const DIST_DIR = process.env.DIST_DIR ?? join(process.cwd(), "dist-utm");
const SCHEMA_PATH = join(process.cwd(), "server", "utm-schema.sql");
const UTM_PASSWORD = process.env.UTM_PASSWORD ?? "";

// Auth helpers
const COOKIE_NAME = "utm_auth";

function cookieToken(): string {
  return createHash("sha256").update("utm:" + UTM_PASSWORD).digest("hex");
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

const LOGIN_HTML = (error: boolean) => `<!DOCTYPE html>
<html lang="ru">
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
});

process.on("SIGTERM", () => {
  server.close(() => {
    db.close();
    process.exit(0);
  });
});
