type D1Prepared = {
  bind: (...args: unknown[]) => D1Prepared;
  all: <T = Record<string, unknown>>() => Promise<{ results?: T[] }>;
  first: <T = Record<string, unknown>>() => Promise<T | null>;
  run: () => Promise<unknown>;
};

type D1Database = {
  prepare: (query: string) => D1Prepared;
};

interface Env {
  UTM: D1Database;
}

type UtmMedium = "cpc" | "email" | "tg";

const SOURCES_BY_MEDIUM: Record<UtmMedium, string[]> = {
  cpc: ["yandexd", "headhunter"],
  email: ["sendsay", "unisender"],
  tg: ["social", "cybered"],
};

type UtmRow = {
  "Дата создания": string;
  "UTM Source": string;
  "UTM Medium": string;
  "UTM Campaign": string;
  "UTM Content": string;
  "UTM Term": string;
  "UTM Tag": string;
};

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}

function asTrimmedString(value: unknown): string {
  return String(value ?? "").trim();
}

function buildUtmTag(values: {
  source: string;
  medium: string;
  campaign: string;
  content: string;
  term: string;
}): string {
  return [
    `utm_source=${encodeURIComponent(values.source)}`,
    `utm_medium=${encodeURIComponent(values.medium)}`,
    `utm_campaign=${encodeURIComponent(values.campaign)}`,
    `utm_content=${encodeURIComponent(values.content)}`,
    `utm_term=${encodeURIComponent(values.term)}`,
  ].join("&");
}

async function fetchRows(db: D1Database): Promise<UtmRow[]> {
  const result = await db
    .prepare(
      `SELECT
         created_at AS "Дата создания",
         utm_source AS "UTM Source",
         utm_medium AS "UTM Medium",
         utm_campaign AS "UTM Campaign",
         utm_content AS "UTM Content",
         utm_term AS "UTM Term",
         utm_tag AS "UTM Tag"
       FROM utm_tags
       ORDER BY id DESC
       LIMIT 200`,
    )
    .all<UtmRow>();
  return result.results ?? [];
}

export async function onRequestGet(context: { request: Request; env: Env }): Promise<Response> {
  const url = new URL(context.request.url);
  if ((url.searchParams.get("mode") || "").trim() === "config") {
    return json(200, {
      ok: true,
      mapping: Object.entries(SOURCES_BY_MEDIUM).map(([medium, sources]) => ({ medium, sources })),
    });
  }

  try {
    const rows = await fetchRows(context.env.UTM);
    return json(200, rows);
  } catch (e) {
    return json(500, { ok: false, error: e instanceof Error ? e.message : String(e) });
  }
}

export async function onRequestPost(context: { request: Request; env: Env }): Promise<Response> {
  let body: Record<string, unknown>;
  try {
    body = (await context.request.json()) as Record<string, unknown>;
  } catch {
    return json(400, { ok: false, error: "invalid_json" });
  }

  const mediumRaw = asTrimmedString(body.utm_medium).toLowerCase();
  if (!(mediumRaw in SOURCES_BY_MEDIUM)) {
    return json(400, { ok: false, error: "invalid_medium" });
  }
  const medium = mediumRaw as UtmMedium;

  const source = asTrimmedString(body.utm_source).toLowerCase();
  if (!SOURCES_BY_MEDIUM[medium].includes(source)) {
    return json(400, { ok: false, error: "invalid_source_for_medium" });
  }

  const campaign = asTrimmedString(body.utm_campaign);
  const content = asTrimmedString(body.utm_content);
  const term = asTrimmedString(body.utm_term);

  if (!campaign || !content || !term) {
    return json(400, { ok: false, error: "campaign_content_term_required" });
  }

  const utmTag = buildUtmTag({ source, medium, campaign, content, term });

  try {
    await context.env.UTM
      .prepare(
        `INSERT INTO utm_tags (
          created_at,
          utm_source,
          utm_medium,
          utm_campaign,
          utm_content,
          utm_term,
          utm_tag
        ) VALUES (datetime('now'), ?, ?, ?, ?, ?, ?)`,
      )
      .bind(source, medium, campaign, content, term, utmTag)
      .run();

    const latest = await context.env.UTM
      .prepare(
        `SELECT
           created_at AS "Дата создания",
           utm_source AS "UTM Source",
           utm_medium AS "UTM Medium",
           utm_campaign AS "UTM Campaign",
           utm_content AS "UTM Content",
           utm_term AS "UTM Term",
           utm_tag AS "UTM Tag"
         FROM utm_tags
         ORDER BY id DESC
         LIMIT 1`,
      )
      .first<UtmRow>();

    return json(200, { ok: true, row: latest ?? null, utm_tag: utmTag });
  } catch (e) {
    return json(500, { ok: false, error: e instanceof Error ? e.message : String(e) });
  }
}
