/**
 * POST /api/analytics/materialize
 * No auth required. Clears all dataset_json rows then regenerates them from existing mart tables
 * using the current campaign-group mapping. Rate-limited to once per 90 seconds to prevent abuse.
 */
/// <reference types="@cloudflare/workers-types" />

import { materializeSliceDatasets } from "../../lib/analytics/materializeDatasets";
import { evaluateFreshness, saveFreshnessMeta, saveAttemptTimestamp } from "../../lib/analytics/sourceFreshness";

interface Env {
  DB: D1Database;
}

export async function onRequestPost(context: {
  request: Request;
  env: Env;
}): Promise<Response> {
  const db = context.env.DB;
  const url = new URL(context.request.url);
  const force = (url.searchParams.get("force") || "").toLowerCase() === "true";

  try {
    if (!force) {
      const freshness = await evaluateFreshness(db);
      if (!freshness.stale) {
        return new Response(
          JSON.stringify({
            ok: true,
            skipped: true,
            reason: freshness.reason,
            fingerprint: freshness.fingerprint,
            last_materialize_utc: freshness.meta.lastMaterializeUtc,
          }),
          {
            status: 200,
            headers: { "content-type": "application/json; charset=utf-8" },
          },
        );
      }
    }
  } catch {
    // If freshness check fails, continue with recomputation for correctness.
  }

  try {
    // Record attempt timestamp BEFORE running — so if D1 hits its CPU limit and this
    // worker is killed, the cooldown prevents another immediate retry that would repeat
    // the same partial-delete/no-insert corruption cycle.
    await saveAttemptTimestamp(db).catch(() => {});

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

    const freshnessAfter = await evaluateFreshness(db);
    await saveFreshnessMeta(db, freshnessAfter.fingerprint);

    return new Response(JSON.stringify({ ok: true, skipped: false, datasets: written.length }), {
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
