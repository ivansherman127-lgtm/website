import mediumConfig from "./utm_medium_sources.json";

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

type MediumEntry = { value: string; label: string; sourceType: "select" | "freetext"; sources: string[] };

const MEDIUM_CONFIG: MediumEntry[] = (mediumConfig as { mediums: MediumEntry[] }).mediums;
const MEDIUM_MAP = new Map<string, MediumEntry>(MEDIUM_CONFIG.map(m => [m.value, m]));

type UtmRow = {
  "Дата создания": string;
  "UTM Source": string;
  "UTM Medium": string;
  "UTM Campaign": string;
  "Link": string;
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
  link: string;
  source: string;
  medium: string;
  campaign: string;
  content: string;
  term: string;
}): string {
  const params = [
    `utm_source=${encodeURIComponent(values.source)}`,
    `utm_medium=${encodeURIComponent(values.medium)}`,
    `utm_campaign=${encodeURIComponent(values.campaign)}`,
    `utm_content=${encodeURIComponent(values.content)}`,
    `utm_term=${encodeURIComponent(values.term)}`,
  ].join("&");

  const link = values.link.trim();
  if (!link) return params;

  const hashIndex = link.indexOf("#");
  const beforeHash = hashIndex >= 0 ? link.slice(0, hashIndex) : link;
  const hashPart = hashIndex >= 0 ? link.slice(hashIndex) : "";
  const separator = beforeHash.includes("?") ? (beforeHash.endsWith("?") || beforeHash.endsWith("&") ? "" : "&") : "?";
  return `${beforeHash}${separator}${params}${hashPart}`;
}

async function fetchRows(db: D1Database): Promise<UtmRow[]> {
  const result = await db
    .prepare(
      `SELECT
         created_at AS "Дата создания",
         utm_source AS "UTM Source",
         utm_medium AS "UTM Medium",
         utm_campaign AS "UTM Campaign",
         campaign_link AS "Link",
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
    return json(200, { ok: true, mediums: MEDIUM_CONFIG });
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
  const mediumEntry = MEDIUM_MAP.get(mediumRaw);
  if (!mediumEntry) {
    return json(400, { ok: false, error: "invalid_medium" });
  }
  const medium = mediumRaw;

  const source = asTrimmedString(body.utm_source);
  if (mediumEntry.sourceType === "select") {
    if (!mediumEntry.sources.includes(source.toLowerCase())) {
      return json(400, { ok: false, error: "invalid_source_for_medium" });
    }
  } else {
    if (!source) {
      return json(400, { ok: false, error: "source_required" });
    }
  }

  const campaign = asTrimmedString(body.utm_campaign);
  const campaignLink = asTrimmedString(body.campaign_link);
  const content = asTrimmedString(body.utm_content);
  const term = asTrimmedString(body.utm_term);

  if (!campaign || !campaignLink) {
    return json(400, { ok: false, error: "required_fields_missing" });
  }

  const utmTag = buildUtmTag({
    link: campaignLink,
    source,
    medium,
    campaign,
    content,
    term,
  });

  try {
    await context.env.UTM
      .prepare(
        `INSERT INTO utm_tags (
          created_at,
          utm_source,
          utm_medium,
          utm_campaign,
          campaign_link,
          utm_content,
          utm_term,
          utm_tag
        ) VALUES (datetime('now'), ?, ?, ?, ?, ?, ?, ?)`,
      )
      .bind(source, medium, campaign, campaignLink, content, term, utmTag)
      .run();

    const latest = await context.env.UTM
      .prepare(
        `SELECT
           created_at AS "Дата создания",
           utm_source AS "UTM Source",
           utm_medium AS "UTM Medium",
           utm_campaign AS "UTM Campaign",
           campaign_link AS "Link",
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
