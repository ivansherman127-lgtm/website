interface Env {
  ASSETS: Fetcher;
}

const TABLES: Record<string, string> = {
  assoc_contacts: "/data/cohorts/attacking_january/cohort_assoc_contacts.json",
};

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export const onRequestGet: PagesFunction<Env> = async (ctx) => {
  const email = ctx.request.headers.get("cf-access-authenticated-user-email");
  if (!email) {
    return json(401, {
      ok: false,
      error: "unauthorized",
      message: "This table is protected. Sign in via Cloudflare Access.",
    });
  }

  const url = new URL(ctx.request.url);
  const name = (url.searchParams.get("name") || "").trim();
  const assetPath = TABLES[name];
  if (!assetPath) {
    return json(400, { ok: false, error: "unknown_table_name" });
  }

  const assetUrl = new URL(assetPath, url.origin);
  const assetResp = await ctx.env.ASSETS.fetch(new Request(assetUrl.toString(), { method: "GET" }));
  if (!assetResp.ok) {
    return json(500, { ok: false, error: `asset_fetch_failed_${assetResp.status}` });
  }
  const body = await assetResp.text();
  return new Response(body, {
    status: 200,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
};

