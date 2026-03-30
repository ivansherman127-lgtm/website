/**
 * POST /api/analytics/rebuild
 * Auth: Authorization: Bearer <ANALYTICS_REBUILD_SECRET>
 * Runs mart rebuild (TS), cohort SQL, Yandex marts, and materializes dataset_json slice blobs.
 */
/// <reference types="@cloudflare/workers-types" />

import { runAnalyticsRebuild } from "../../lib/analytics/analyticsRebuild";

interface Env {
  DB: D1Database;
  ANALYTICS_REBUILD_SECRET?: string;
}

function unauthorized(): Response {
  return new Response(JSON.stringify({ ok: false, error: "unauthorized" }), {
    status: 401,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function authOk(request: Request, env: Env): boolean {
  const secret = env.ANALYTICS_REBUILD_SECRET;
  if (!secret) return false;
  const h = request.headers.get("Authorization") ?? "";
  const m = /^Bearer\s+(.+)$/i.exec(h.trim());
  return Boolean(m && m[1] === secret);
}

export async function onRequestPost(context: {
  request: Request;
  env: Env;
}): Promise<Response> {
  if (!authOk(context.request, context.env)) {
    return unauthorized();
  }
  try {
    const result = await runAnalyticsRebuild(context.env.DB);
    return new Response(JSON.stringify({ ok: true, result }), {
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
