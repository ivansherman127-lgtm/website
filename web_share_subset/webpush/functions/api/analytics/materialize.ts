/**
 * POST /api/analytics/materialize
 * No auth required. Regenerates all dataset_json blobs from existing mart tables using the
 * current campaign-group mapping. Rate-limited to once per 90 seconds to prevent abuse.
 */
/// <reference types="@cloudflare/workers-types" />

import { materializeSliceDatasets } from "../../lib/analytics/materializeDatasets";

interface Env {
  DB: D1Database;
}

const RATE_LIMIT_MS = 90_000;

export async function onRequestPost(context: {
  request: Request;
  env: Env;
}): Promise<Response> {
  const db = context.env.DB;

  // Rate-limit: skip if last rebuild finished < RATE_LIMIT_MS ago.
  try {
    const meta = await db
      .prepare(`SELECT v FROM analytics_build_meta WHERE k = 'last_rebuild_utc' LIMIT 1`)
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
    const result = await materializeSliceDatasets(db);
    await db
      .prepare(
        `INSERT OR REPLACE INTO analytics_build_meta (k, v, updated_at) VALUES ('last_rebuild_utc', ?, datetime('now'))`,
      )
      .bind(new Date().toISOString())
      .run();
    return new Response(JSON.stringify({ ok: true, datasets: result.paths }), {
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
