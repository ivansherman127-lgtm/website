import { onRequestGet as assocRevenueGet } from "./functions/api/assoc-revenue";
import { onRequestGet as dataGet } from "./functions/api/data";
import { onRequestGet as cohortDealsGet } from "./functions/api/cohort-deals";
import { onRequestGet as protectedTableGet } from "./functions/api/protected-table";
import { onRequestPost as saveToGithubPost } from "./functions/api/save-to-github";
import { onRequestPost as analyticsRebuildPost } from "./functions/api/analytics/rebuild";

interface Env {
  DB: D1Database;
  ASSETS: Fetcher;
  GITHUB_TOKEN?: string;
  GITHUB_OWNER?: string;
  GITHUB_REPO?: string;
  GITHUB_BRANCH?: string;
  ANALYTICS_REBUILD_SECRET?: string;
}

function methodNotAllowed(): Response {
  return new Response(JSON.stringify({ ok: false, error: "method_not_allowed" }), {
    status: 405,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const { pathname } = url;

    if (pathname === "/api/assoc-revenue") {
      if (request.method !== "GET") return methodNotAllowed();
      return assocRevenueGet({ request, env });
    }

    if (pathname === "/api/data") {
      if (request.method !== "GET") return methodNotAllowed();
      return dataGet({ request, env });
    }

    if (pathname === "/api/cohort-deals") {
      if (request.method !== "GET") return methodNotAllowed();
      return cohortDealsGet({ request, env });
    }

    if (pathname === "/api/protected-table") {
      if (request.method !== "GET") return methodNotAllowed();
      return protectedTableGet({ request, env } as never);
    }

    if (pathname === "/api/save-to-github") {
      if (request.method !== "POST") return methodNotAllowed();
      return saveToGithubPost({ request, env } as never);
    }

    if (pathname === "/api/analytics/rebuild") {
      if (request.method !== "POST") return methodNotAllowed();
      return analyticsRebuildPost({ request, env });
    }

    return env.ASSETS.fetch(request);
  },
};
