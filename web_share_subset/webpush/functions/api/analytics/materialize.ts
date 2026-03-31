/**
 * POST /api/analytics/materialize
 * No auth required. Clears all dataset_json rows then regenerates them from existing mart tables
 * using the current campaign-group mapping. Rate-limited to once per 90 seconds to prevent abuse.
 */
/// <reference types="@cloudflare/workers-types" />

import { materializeSliceDatasets } from "../../lib/analytics/materializeDatasets";

interface Env {
  DB: D1Database;
}

const RATE_LIMIT_MS = 90_000;
const RATE_LIMIT_KEY = "last_materialize_utc";

export async function onRequestPost(context: {
  request: Request;
  env: Env;
}): Promise<Response> {
  const db = context.env.DB;

  // Rate-limit using a separate key from full rebuild so they don't interfere.
  try {
    const meta = await db
      .prepare(`SELECT v FROM analytics_build_meta WHERE k = ? LIMIT 1`)
      .bind(RATE_LIMIT_KEY)
      .first<{ v: string }>();
    if (meta?.v) {
      const lastMs = new Date(meta.v).getTime();
      if (!Number.isNaN(lastMs) && Date.now() - lastMs < RATE_LIMIT_MS) {
        return new Response(JSON.stringify({ ok: true, skipped: true, reason: "rate_limited" }), {
          status: 200,
          headers: { "content-type": "application/json; charset=utf-8" },
        });
      }
    }
  } catch {
    // If metadata check fails, proceed with materialization anyway.
  }

  try {
    // Run materialization FIRST — only delete stale paths after success.
    // Never delete upfront: if materialization fails mid-way, old data is
    // still readable instead of returning 404 for every path.
    const result = await materializeSliceDatasets(db);

    // Delete paths that were NOT written in this run (cleanup stale entries).
    const written = result.paths;
    if (written.length) {
      const placeholders = written.map(() => "?").join(", ");
      await db
        .prepare(`DELETE FROM dataset_json WHERE path NOT IN (${placeholders})`)
        .bind(...written)
        .run();
    }

    await db
      .prepare(
        `INSERT OR REPLACE INTO analytics_build_meta (k, v, updated_at) VALUES (?, ?, datetime('now'))`,
      )
      .bind(RATE_LIMIT_KEY, new Date().toISOString())
      .run();
    return new Response(JSON.stringify({ ok: true, datasets: written.length }), {
      status: 200,
      headers: { "content-type": "application/json; charset=utf-8" },
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return new Response(JSON.stringify({ ok: false, error: msg }), {
      status: 500,
      headers: { "content-type": "application/json; charset=utf-8" },
    });
  }
}
