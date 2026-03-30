/**
 * GET /api/data?path=<relative path under public/data>
 */
import { buildYdHierarchyRows } from "../lib/analytics/ydHierarchy";

interface Env {
  DB: D1Database;
}

export async function onRequestGet(context: {
  request: Request;
  env: Env;
}): Promise<Response> {
  const url = new URL(context.request.url);
  const path = url.searchParams.get("path");
  if (!path || path.includes("..")) {
    return new Response(JSON.stringify({ error: "invalid or missing path" }), {
      status: 400,
      headers: { "content-type": "application/json; charset=utf-8" },
    });
  }
  let parts: { body: string }[] = [];
  try {
    const rows = await context.env.DB.prepare(
      "SELECT body FROM dataset_json WHERE path = ? ORDER BY chunk ASC",
    )
      .bind(path)
      .all<{ body: string }>();
    parts = rows.results ?? [];
  } catch (error) {
    if (path !== "yd_hierarchy.json") throw error;
  }
  if (!parts.length) {
    if (path === "yd_hierarchy.json") {
      const rows = await buildYdHierarchyRows(context.env.DB);
      return new Response(JSON.stringify(rows), {
        status: 200,
        headers: {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "public, max-age=60",
        },
      });
    }
    return new Response(JSON.stringify({ error: "not_found", path }), {
      status: 404,
      headers: { "content-type": "application/json; charset=utf-8" },
    });
  }
  const body = parts.map((r) => r.body).join("");
  return new Response(body, {
    status: 200,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "public, max-age=60",
    },
  });
}
