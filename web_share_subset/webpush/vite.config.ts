import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig, type Plugin } from "vite";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const ROOT = resolve(__dirname, "..");
const DEFAULT_SHEET_URL =
  "https://docs.google.com/spreadsheets/d/1Q5KmqXUOVe9hVXpyJ1m1yUqPEaQ_2KXjoH71DHiPrLU/edit";

function readBody(req: NodeJS.ReadableStream): Promise<string> {
  return new Promise((resolveBody, reject) => {
    let raw = "";
    req.on("data", (chunk) => {
      raw += String(chunk);
    });
    req.on("end", () => resolveBody(raw));
    req.on("error", reject);
  });
}

function sheetsPushPlugin(): Plugin {
  return {
    name: "local-sheets-push-api",
    configureServer(server) {
      server.middlewares.use("/api/save-view-json", async (req, res) => {
        if (req.method !== "POST") {
          res.statusCode = 405;
          res.setHeader("content-type", "application/json");
          res.end(JSON.stringify({ ok: false, error: "Method not allowed" }));
          return;
        }
        try {
          const body = await readBody(req);
          const payload = JSON.parse(body) as {
            path?: string;
            rows?: Array<Record<string, unknown>>;
          };
          const relPath = String(payload.path || "").trim();
          const rows = payload.rows || [];
          if (!relPath.startsWith("data/")) {
            throw new Error("Invalid path: expected data/*");
          }
          const relToPublic = relPath.replace(/^data\//, "");
          const target = resolve(__dirname, "public", "data", relToPublic);
          const publicDataRoot = resolve(__dirname, "public", "data");
          if (!target.startsWith(publicDataRoot)) {
            throw new Error("Invalid target path");
          }
          await mkdir(resolve(target, ".."), { recursive: true });
          await writeFile(target, JSON.stringify(rows, null, 2), "utf-8");
          res.setHeader("content-type", "application/json");
          res.end(JSON.stringify({ ok: true, path: relPath, rows: rows.length }));
        } catch (e) {
          res.statusCode = 400;
          res.setHeader("content-type", "application/json");
          res.end(JSON.stringify({ ok: false, error: String(e) }));
        }
      });
      server.middlewares.use("/api/push-table", async (req, res) => {
        if (req.method !== "POST") {
          res.statusCode = 405;
          res.setHeader("content-type", "application/json");
          res.end(JSON.stringify({ ok: false, error: "Method not allowed" }));
          return;
        }
        try {
          const body = await readBody(req);
          const payload = JSON.parse(body) as {
            worksheet?: string;
            rows?: Array<Record<string, unknown>>;
          };
          const rows = payload.rows || [];
          const worksheet = (payload.worksheet || "App export").trim();
          if (!rows.length) {
            throw new Error("No rows to push");
          }

          const tempPath = join(tmpdir(), `deved-push-${Date.now()}.json`);
          await writeFile(tempPath, JSON.stringify(rows), "utf-8");

          const credsCandidate = resolve(ROOT, "keys", "cybered-490317-a7b083e70c85.json");
          const creds =
            process.env.GOOGLE_APPLICATION_CREDENTIALS ||
            (existsSync(credsCandidate) ? credsCandidate : "");
          const sheetRef = process.env.GOOGLE_SHEET_ID || DEFAULT_SHEET_URL;
          if (!creds) {
            throw new Error("Missing GOOGLE_APPLICATION_CREDENTIALS (or keys/*.json)");
          }

          const script = resolve(ROOT, "db", "sheets_ops.py");
          const args = [
            script,
            "--credentials",
            creds,
            "--sheet-id",
            sheetRef,
            "push-json",
            "--worksheet",
            worksheet,
            "--json-file",
            tempPath,
          ];
          const py = spawn("python", args, { cwd: ROOT });
          let out = "";
          let err = "";
          py.stdout.on("data", (d) => (out += String(d)));
          py.stderr.on("data", (d) => (err += String(d)));
          const code: number = await new Promise((resolveCode) =>
            py.on("close", (c) => resolveCode(c ?? 1)),
          );
          await unlink(tempPath).catch(() => {});

          res.setHeader("content-type", "application/json");
          if (code !== 0) {
            res.statusCode = 500;
            res.end(JSON.stringify({ ok: false, error: err || out || "push failed" }));
            return;
          }
          res.end(JSON.stringify({ ok: true, message: out.trim() || "pushed" }));
        } catch (e) {
          res.statusCode = 400;
          res.setHeader("content-type", "application/json");
          res.end(JSON.stringify({ ok: false, error: String(e) }));
        }
      });
      server.middlewares.use("/api/delete-slices", async (req, res) => {
        if (req.method !== "POST") {
          res.statusCode = 405;
          res.setHeader("content-type", "application/json");
          res.end(JSON.stringify({ ok: false, error: "Method not allowed" }));
          return;
        }
        try {
          const credsCandidate = resolve(ROOT, "keys", "cybered-490317-a7b083e70c85.json");
          const creds =
            process.env.GOOGLE_APPLICATION_CREDENTIALS ||
            (existsSync(credsCandidate) ? credsCandidate : "");
          const sheetRef = process.env.GOOGLE_SHEET_ID || DEFAULT_SHEET_URL;
          if (!creds) {
            throw new Error("Missing GOOGLE_APPLICATION_CREDENTIALS (or keys/*.json)");
          }

          const script = resolve(ROOT, "db", "sheets_ops.py");
          const args = [
            script,
            "--credentials",
            creds,
            "--sheet-id",
            sheetRef,
            "delete-regex",
            "--pattern",
            "^Slices",
          ];
          const py = spawn("python", args, { cwd: ROOT });
          let out = "";
          let err = "";
          py.stdout.on("data", (d) => (out += String(d)));
          py.stderr.on("data", (d) => (err += String(d)));
          const code: number = await new Promise((resolveCode) =>
            py.on("close", (c) => resolveCode(c ?? 1)),
          );

          res.setHeader("content-type", "application/json");
          if (code !== 0) {
            res.statusCode = 500;
            res.end(JSON.stringify({ ok: false, error: err || out || "delete failed" }));
            return;
          }
          res.end(JSON.stringify({ ok: true, message: out.trim() || "deleted" }));
        } catch (e) {
          res.statusCode = 400;
          res.setHeader("content-type", "application/json");
          res.end(JSON.stringify({ ok: false, error: String(e) }));
        }
      });
    },
  };
}

export default defineConfig({
  base: "./",
  publicDir: "public",
  plugins: [sheetsPushPlugin()],
});
