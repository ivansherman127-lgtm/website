import { onRequestGet as utmGet, onRequestPost as utmPost } from "./functions/api/utm";

interface Env {
  UTM: D1Database;
  ASSETS: Fetcher;
}

function methodNotAllowed(): Response {
  return new Response(JSON.stringify({ ok: false, error: "method_not_allowed" }), {
    status: 405,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const { pathname } = url;

    if (pathname === "/api/utm") {
      if (request.method === "GET") return utmGet({ request, env });
      if (request.method === "POST") return utmPost({ request, env });
      return methodNotAllowed();
    }

    // UTM-only worker intentionally exposes no other API routes.
    if (pathname.startsWith("/api/")) {
      return json(404, { ok: false, error: "not_found" });
    }

    return env.ASSETS.fetch(request);
  },
};
