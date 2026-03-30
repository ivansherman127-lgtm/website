import {
  buildYandexProjectLabelMap,
  buildYandexProjectLabelMapFromRows,
  type NoMonthRow,
} from "../lib/analytics/yandexProjectsNoMonth";

interface D1PreparedStatement {
  bind(...values: unknown[]): D1PreparedStatement;
  first<T = unknown>(): Promise<T | null>;
  all<T = unknown>(): Promise<{ results?: T[] }>;
  run(): Promise<unknown>;
}

interface D1Database {
  prepare(query: string): D1PreparedStatement;
}

interface Env {
  DB: D1Database;
}

type Cohort = "all" | "attacking_january";
type DimKey =
  | "yandex_campaign"
  | "email_campaign"
  | "event"
  | "course_code"
  | "month"
  | "funnel";

type DimSpec = {
  key: DimKey;
  label: string;
  expr: string;
};

const DIMENSIONS: Record<DimKey, DimSpec> = {
  yandex_campaign: {
    key: "yandex_campaign",
    label: "Yandex кампания",
    expr: "COALESCE(NULLIF(yandex_campaign_group, ''), '(пусто)')",
  },
  email_campaign: {
    key: "email_campaign",
    label: "Email кампания",
    expr: "COALESCE(NULLIF(email_release_group, ''), '(пусто)')",
  },
  event: {
    key: "event",
    label: "Мероприятие",
    expr: "event_group",
  },
  course_code: {
    key: "course_code",
    label: "Код курса",
    expr: "COALESCE(NULLIF(course_code_norm, ''), 'Другое')",
  },
  month: {
    key: "month",
    label: "Месяц",
    expr: "COALESCE(NULLIF(month, ''), '(без месяца)')",
  },
  funnel: {
    key: "funnel",
    label: "Воронка",
    expr: "COALESCE(NULLIF(funnel_group, ''), 'Другое')",
  },
};

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "public, max-age=60",
    },
  });
}

function parseDims(raw: string | null): DimKey[] {
  const src = (raw || "yandex_campaign")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
  const uniq = [...new Set(src)] as string[];
  if (uniq.length === 0 || uniq.length > 2) return [];
  if (!uniq.every((k) => k in DIMENSIONS)) return [];
  return uniq as DimKey[];
}

function parseCohort(raw: string | null): Cohort {
  return raw === "attacking_january" ? "attacking_january" : "all";
}

function isIsoMonth(raw: string | null): boolean {
  if (!raw) return false;
  return /^\d{4}-\d{2}$/.test(raw);
}

type CacheRow = {
  cache_key: string;
  source_watermark: string;
  generated_at: string;
  response_json: string;
};

type EmailSendRow = {
  utm_campaign: string | null;
  release_name: string | null;
};

function safeJsonParseArray(raw: string): unknown[] | null {
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function sqlQuote(value: string): string {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function sqlNormalizeLookupExpr(expr: string): string {
  const replacements: Array<[string, string]> = [
    ["ё", "е"],
    ["-", ""],
    ["_", ""],
    [" ", ""],
    [".", ""],
    ["/", ""],
    [":", ""],
    [",", ""],
    ["'", ""],
    ['"', ""],
    ["«", ""],
    ["»", ""],
    ["(", ""],
    [")", ""],
  ];

  let out = `LOWER(TRIM(COALESCE(${expr}, '')))`;
  for (const [from, to] of replacements) {
    out = `REPLACE(${out}, ${sqlQuote(from)}, ${sqlQuote(to)})`;
  }
  return out;
}

function normalizeLookupKey(value: unknown): string {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/ё/g, "е")
    .replace(/[^a-zа-я0-9]+/giu, "");
}

function unwrapQuotedValue(value: string): string {
  const match = value.match(/[«"]([^«»"]+)[»"]/u);
  return match?.[1]?.trim() || value.trim();
}

function stripDateLikeSuffix(value: string): string {
  let current = String(value ?? "").trim();
  let prev = "";
  while (current && current !== prev) {
    prev = current;
    current = current.replace(/(?:[_\-\s]+)?\d{1,2}:\d{2}$/u, "").trim();
    current = current.replace(/(?:[_\-\s]+)?\d{8}$/u, "").trim();
    current = current.replace(/(?:[_\-\s]+)?\d{6}$/u, "").trim();
    current = current.replace(/(?:[_\-\s]+)?\d{1,2}[_\-]\d{1,2}(?:[_\-]\d{2,4})?$/u, "").trim();
  }
  return current;
}

function cleanEmailReleaseLabel(value: string): string {
  let cleaned = String(value ?? "").replace(/\u00a0/g, " ").trim();
  cleaned = unwrapQuotedValue(cleaned);
  cleaned = cleaned.replace(/[\s,]+\d{1,2}\.\d{1,2}\.\d{4}.*$/u, "").trim();
  cleaned = cleaned.replace(/(?:[_\-\s]+)?\d{1,2}:\d{2}$/u, "").trim();
  return cleaned;
}

function cleanYandexFallbackLabel(value: string): string {
  const raw = String(value ?? "").trim();
  if (!raw) return "";
  const lower = raw.toLowerCase();
  if (
    lower === "{campaign_id}" ||
    lower === "{campaign_id}_{gbid}" ||
    lower === "%7bcampaign_id%7d" ||
    lower === "%7bcampaign_id%7d_%7bgbid%7d"
  ) {
    return "";
  }
  if (/^\|+\d+$/u.test(raw)) return "";
  const pipeIndex = raw.indexOf("|");
  if (pipeIndex > 0) {
    return raw.slice(0, pipeIndex).trim();
  }
  return raw;
}

function buildEmailReleaseAliasMap(rows: EmailSendRow[]): Map<string, string> {
  const aliasMap = new Map<string, { label: string; priority: number }>();

  const remember = (alias: string, label: string, priority: number) => {
    if (!alias || !label) return;
    const prev = aliasMap.get(alias);
    if (!prev || priority > prev.priority || (priority === prev.priority && label.length < prev.label.length)) {
      aliasMap.set(alias, { label, priority });
    }
  };

  for (const row of rows) {
    const releaseLabel = cleanEmailReleaseLabel(String(row.release_name ?? ""));
    if (!releaseLabel) continue;

    const strippedRelease = stripDateLikeSuffix(releaseLabel);
    const utmCampaign = String(row.utm_campaign ?? "").trim();

    remember(normalizeLookupKey(releaseLabel), releaseLabel, 2);
    remember(normalizeLookupKey(strippedRelease), releaseLabel, 1);
    if (utmCampaign) {
      remember(normalizeLookupKey(utmCampaign), releaseLabel, 3);
      remember(normalizeLookupKey(stripDateLikeSuffix(utmCampaign)), releaseLabel, 2);
    }
  }

  return new Map([...aliasMap.entries()].map(([alias, entry]) => [alias, entry.label]));
}

function resolveEmailReleaseLabel(rawCampaign: string, aliasMap: Map<string, string>): string | null {
  const exactKey = normalizeLookupKey(rawCampaign);
  if (!exactKey) return null;

  const exact = aliasMap.get(exactKey);
  if (exact) return exact;

  const strippedKey = normalizeLookupKey(stripDateLikeSuffix(rawCampaign));
  if (strippedKey) {
    const stripped = aliasMap.get(strippedKey);
    if (stripped) return stripped;

    const prefixMatches = [...aliasMap.entries()].filter(([alias]) => {
      if (alias.length < 5) return false;
      return alias.startsWith(strippedKey) || strippedKey.startsWith(alias);
    });
    if (prefixMatches.length === 1) return prefixMatches[0]![1];
  }

  return null;
}

async function ensureCacheTable(db: D1Database): Promise<void> {
  await db
    .prepare(
      `
      CREATE TABLE IF NOT EXISTS assoc_revenue_reports_cache (
        cache_key TEXT PRIMARY KEY,
        source_watermark TEXT NOT NULL,
        generated_at TEXT NOT NULL DEFAULT (datetime('now')),
        response_json TEXT NOT NULL
      )
    `,
    )
    .run();
}

async function tableExists(db: D1Database, tableName: string): Promise<boolean> {
  const row = await db
    .prepare(
      `
      SELECT name
      FROM sqlite_master
      WHERE type = 'table' AND name = ?
      LIMIT 1
    `,
    )
    .bind(tableName)
    .first<{ name: string }>();
  return !!row?.name;
}

async function tableWatermark(db: D1Database, tableName: string): Promise<string> {
  const exists = await tableExists(db, tableName);
  if (!exists) return `${tableName}:missing`;

  const countRow = await db
    .prepare(`SELECT COUNT(*) AS cnt FROM ${tableName}`)
    .first<{ cnt: number }>();
  const count = Number(countRow?.cnt ?? 0);

  const pragma = await db.prepare(`PRAGMA table_info(${tableName})`).all<{ name: string }>();
  const columns = (pragma.results ?? []).map((x: { name: string }) => x.name);

  const tsCandidates = [
    "updated_at",
    "created_at",
    "date_modify",
    "date_create",
    "Loaded At",
    "Month",
    "month",
  ];
  const maxColumn = tsCandidates.find((c) => columns.includes(c));

  if (!maxColumn) {
    return `${tableName}:${count}:no_ts`;
  }

  const maxRow = await db
    .prepare(`SELECT MAX("${maxColumn.replace(/"/g, "")}") AS max_value FROM ${tableName}`)
    .first<{ max_value: string | null }>();

  return `${tableName}:${count}:${maxColumn}:${maxRow?.max_value ?? "null"}`;
}

async function computeSourceWatermark(db: D1Database, cohort: Cohort): Promise<string> {
  const watchedTables = [
    "raw_bitrix",
    "stg_bitrix_deals",
    "mart_deals_enriched",
    "stg_email_sends",
    "stg_yandex_stats",
    "mart_yandex_revenue_projects_raw",
    cohort === "attacking_january" ? "mart_attacking_january_cohort_deals" : "mart_deals_enriched",
  ];
  const uniq = [...new Set(watchedTables)];
  const parts = await Promise.all(uniq.map((t) => tableWatermark(db, t)));
  return parts.join("|");
}

function makeCacheKey(params: {
  dims: DimKey[];
  cohort: Cohort;
  fromMonth: string | null;
  toMonth: string | null;
}): string {
  const { dims, cohort, fromMonth, toMonth } = params;
  return `v7|dims=${dims.join(",")}|cohort=${cohort}|from=${fromMonth ?? ""}|to=${toMonth ?? ""}`;
}

export async function onRequestGet(context: {
  request: Request;
  env: Env;
}): Promise<Response> {
  const url = new URL(context.request.url);
  const dims = parseDims(url.searchParams.get("dims"));
  if (!dims.length) {
    return json(400, {
      ok: false,
      error: "invalid_dims",
      message: "Use 1-2 allowed dims: yandex_campaign,email_campaign,event,course_code,month,funnel",
    });
  }

  const cohort = parseCohort(url.searchParams.get("cohort"));
  const fromMonth = url.searchParams.get("from");
  const toMonth = url.searchParams.get("to");
  const forceRecalc = /^(1|true|yes)$/i.test((url.searchParams.get("recalc") || "").trim());
  if (fromMonth && !isIsoMonth(fromMonth)) {
    return json(400, { ok: false, error: "invalid_from", message: "from must be YYYY-MM" });
  }
  if (toMonth && !isIsoMonth(toMonth)) {
    return json(400, { ok: false, error: "invalid_to", message: "to must be YYYY-MM" });
  }

  const table = cohort === "attacking_january" ? "mart_attacking_january_cohort_deals" : "mart_deals_enriched";
  const specs = dims.map((d) => DIMENSIONS[d]);

  const selectParts = specs.map((d, i) => `${d.expr} AS d${i + 1}`);
  const dimColumns = specs.map((_, i) => `d${i + 1}`).join(", ");
  const dimColumnsQualified = specs.map((_, i) => `s.d${i + 1}`).join(", ");
  const emailSourceExpr = "LOWER(TRIM(COALESCE(\"UTM Source\", ''))) = 'sendsay'";

  const wheres: string[] = [];
  const binds: unknown[] = [];

  if (fromMonth) {
    wheres.push("month >= ?");
    binds.push(fromMonth);
  }
  if (toMonth) {
    wheres.push("month <= ?");
    binds.push(toMonth);
  }

  if (dims.includes("yandex_campaign")) {
    wheres.push("LOWER(COALESCE(\"UTM Source\", '')) LIKE 'y%'");
    wheres.push("LOWER(COALESCE(\"UTM Source\", '')) <> 'yah'");
  }

  if (dims.includes("email_campaign")) {
    // Keep parity with legacy email_notebook logic: email attribution comes from Sendsay source.
    wheres.push(emailSourceExpr);
  }

  const whereSql = wheres.length ? `WHERE ${wheres.join(" AND ")}` : "";

  try {
    await ensureCacheTable(context.env.DB);

    const sourceWatermark = await computeSourceWatermark(context.env.DB, cohort);
    const cacheKey = makeCacheKey({ dims, cohort, fromMonth, toMonth });

    if (!forceRecalc) {
      const cached = await context.env.DB
        .prepare(
          `
          SELECT cache_key, source_watermark, generated_at, response_json
          FROM assoc_revenue_reports_cache
          WHERE cache_key = ?
          LIMIT 1
        `,
        )
        .bind(cacheKey)
        .first<CacheRow>();

      if (cached && cached.source_watermark === sourceWatermark) {
        const parsed = safeJsonParseArray(cached.response_json);
        if (parsed) {
          return json(200, parsed);
        }
      }
    }

    const hasYandexStats = await tableExists(context.env.DB, "stg_yandex_stats");
    const hasEmailSends = await tableExists(context.env.DB, "stg_email_sends");
    let yandexCampaignExpr = "'(без маппинга в Yandex raw)'";
    let emailCampaignExpr = "'(без маппинга в email raw)'";
    let yandexCampaignPairs: Array<[string, string]> = [];
    let yandexSpendAvailable = false;

    if (hasYandexStats && dims.includes("yandex_campaign")) {
      const rawRowsExists = await tableExists(context.env.DB, "mart_yandex_revenue_projects_raw");
      let labelMap = new Map<string, string>();

      if (rawRowsExists) {
        const rowsRes = await context.env.DB
          .prepare(
            `
            SELECT
              project_name,
              SUM(leads_raw) AS leads_raw,
              SUM(paid_deals_raw) AS payments_count,
              SUM(paid_deals_raw) AS paid_deals_raw,
              SUM(revenue_raw) AS revenue_raw,
              SUM(spend) AS spend
            FROM mart_yandex_revenue_projects_raw
            GROUP BY project_name
            ORDER BY revenue_raw DESC
          `,
          )
          .all<NoMonthRow>();

        labelMap = buildYandexProjectLabelMapFromRows(rowsRes.results ?? [], 0.6);
      }

      if (labelMap.size === 0) {
        const namesRes = await context.env.DB
          .prepare(
            `
            SELECT DISTINCT NULLIF(TRIM(COALESCE("Название кампании", '')), '') AS project_name
            FROM stg_yandex_stats
            WHERE NULLIF(TRIM(COALESCE("Название кампании", '')), '') IS NOT NULL
          `,
          )
          .all<{ project_name: string }>();
        const names = (namesRes.results ?? []).map((r) => r.project_name).filter(Boolean);
        labelMap = buildYandexProjectLabelMap(names, 0.6);
      }

      const pairs = [...labelMap.entries()];
      yandexCampaignPairs = pairs;
      yandexSpendAvailable = rawRowsExists;

      if (pairs.length > 0) {
        yandexCampaignExpr = `COALESCE(CASE ym.project_name ${pairs
          .map(([original, label]) => `WHEN ${sqlQuote(original)} THEN ${sqlQuote(label)}`)
          .join(" ")} ELSE ym.project_name END, '(без маппинга в Yandex raw)')`;
      } else {
        yandexCampaignExpr = `COALESCE(ym.project_name, '(без маппинга в Yandex raw)')`;
      }
    }

    if (hasEmailSends && dims.includes("email_campaign")) {
      const emailRows = await context.env.DB
        .prepare(
          `
          SELECT
            NULLIF(TRIM(COALESCE(utm_campaign, '')), '') AS utm_campaign,
            NULLIF(TRIM(COALESCE("Название выпуска", '')), '') AS release_name
          FROM stg_email_sends
          WHERE NULLIF(TRIM(COALESCE("Название выпуска", '')), '') IS NOT NULL
        `,
        )
        .all<EmailSendRow>();
      const aliasMap = buildEmailReleaseAliasMap(emailRows.results ?? []);

      if (aliasMap.size > 0) {
        const sourceCampaigns = await context.env.DB
          .prepare(
            `
            SELECT DISTINCT NULLIF(TRIM(COALESCE("UTM Campaign", '')), '') AS utm_campaign
            FROM ${table}
            WHERE ${emailSourceExpr}
              AND NULLIF(TRIM(COALESCE("UTM Campaign", '')), '') IS NOT NULL
          `,
          )
          .all<{ utm_campaign: string }>();

        const normalizedPairsMap = new Map<string, string>();
        for (const row of sourceCampaigns.results ?? []) {
          const label = resolveEmailReleaseLabel(row.utm_campaign, aliasMap);
          if (!label) continue;
          const normalizedKey = normalizeLookupKey(row.utm_campaign);
          if (!normalizedKey || normalizedPairsMap.has(normalizedKey)) continue;
          normalizedPairsMap.set(normalizedKey, label);
        }

        const pairs = [...normalizedPairsMap.entries()];

        if (pairs.length > 0) {
          const emailNormExpr = sqlNormalizeLookupExpr(`NULLIF(TRIM(COALESCE(src."UTM Campaign", '')), '')`);
          emailCampaignExpr = `COALESCE(CASE ${emailNormExpr} ${pairs
            .map(([key, label]) => `WHEN ${sqlQuote(key)} THEN ${sqlQuote(label)}`)
            .join(" ")} ELSE NULL END, '(без маппинга в email raw)')`;
        }
      }
    }

    let yandexSpendCteSql = "";
    let yandexSpendJoinSql = "";
    let includeYandexSpend = false;

    if (yandexSpendAvailable && dims.includes("yandex_campaign")) {
      const yandexDimIdx = dims.indexOf("yandex_campaign") + 1; // 1-based, matches d1/d2 column naming
      const spendMappingExpr = yandexCampaignPairs.length > 0
        ? `COALESCE(CASE spend_src.project_name ${yandexCampaignPairs
            .map(([orig, lbl]) => `WHEN ${sqlQuote(orig)} THEN ${sqlQuote(lbl)}`)
            .join(" ")} ELSE spend_src.project_name END, '(без маппинга в Yandex raw)')`
        : `COALESCE(spend_src.project_name, '(без маппинга в Yandex raw)')`;

      yandexSpendCteSql = `,
      yandex_spend_by_label AS (
        SELECT
          ${spendMappingExpr} AS campaign_label,
          COALESCE(SUM(spend), 0) AS total_spend
        FROM mart_yandex_revenue_projects_raw spend_src
        GROUP BY campaign_label
      )`;

      yandexSpendJoinSql = `LEFT JOIN yandex_spend_by_label ysl ON ysl.campaign_label = s.d${yandexDimIdx}`;
      includeYandexSpend = true;
    }

    const sourceDealsCte = `
      source_deals AS (
        ${hasYandexStats ? `WITH yandex_map AS (
          SELECT
            REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') AS ad_id,
            MIN(NULLIF(TRIM(COALESCE("Название кампании", '')), '')) AS project_name
          FROM stg_yandex_stats
          WHERE REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') <> ''
          GROUP BY 1
        )` : ""}
        SELECT
          src.*,
          ${yandexCampaignExpr} AS yandex_campaign_group,
          ${emailCampaignExpr} AS email_release_group,
          CASE
            WHEN COALESCE(src.is_attacking_january, 0) = 1 THEN 'Attacking January'
            ELSE COALESCE(NULLIF(src.event_class, ''), 'Другое')
          END AS event_group
        FROM ${table} src
        ${hasYandexStats ? `LEFT JOIN yandex_map ym
          ON ym.ad_id = REPLACE(TRIM(COALESCE(src."UTM Content", '')), '.0', '')
        ` : ""}
      )`;

    const sql = `
      WITH ${sourceDealsCte},
      source_scoped AS (
        SELECT
          ${selectParts.join(",\n          ")},
          COALESCE(NULLIF("Контакт: ID", ''), '') AS contact_id,
          COALESCE(NULLIF(ID, ''), '') AS source_deal_id
        FROM source_deals
        ${whereSql}
      ),
      source_group_stats AS (
        SELECT
          ${dimColumns},
          COUNT(*) AS deals_total,
          COUNT(DISTINCT CASE WHEN contact_id <> '' THEN contact_id ELSE NULL END) AS contacts_in_pool
        FROM source_scoped
        GROUP BY ${dimColumns}
      ),
      contact_pool AS (
        SELECT DISTINCT
          ${dimColumns},
          contact_id
        FROM source_scoped
        WHERE contact_id <> ''
      ),
      paid_by_contact AS (
        SELECT
          COALESCE(NULLIF("Контакт: ID", ''), '') AS contact_id,
          COUNT(*) AS paid_deals,
          COALESCE(SUM(revenue_amount), 0) AS revenue
        FROM mart_deals_enriched
        WHERE is_revenue_variant3 = 1
          AND COALESCE(NULLIF("Контакт: ID", ''), '') <> ''
        GROUP BY COALESCE(NULLIF("Контакт: ID", ''), '')
      )${yandexSpendCteSql}
      SELECT
        ${specs.map((_, i) => `s.d${i + 1}`).join(",\n        ")},
        s.deals_total AS deals_total,
        s.contacts_in_pool AS contacts_in_pool,
        COALESCE(SUM(p.paid_deals), 0) AS paid_deals,
        COUNT(DISTINCT CASE WHEN p.paid_deals > 0 THEN cp.contact_id ELSE NULL END) AS contacts_with_revenue,
        COALESCE(SUM(p.revenue), 0) AS revenue,
        CASE
          WHEN COALESCE(SUM(p.paid_deals), 0) = 0 THEN 0
          ELSE COALESCE(SUM(p.revenue), 0) * 1.0 / COALESCE(SUM(p.paid_deals), 0)
        END AS avg_check${includeYandexSpend ? `,
        -- yandex_spend_by_label is pre-aggregated by campaign_label (1:1 with the campaign dim); MAX() satisfies the aggregate context
        COALESCE(MAX(ysl.total_spend), 0) AS yandex_spend` : ""}
      FROM source_group_stats s
      LEFT JOIN contact_pool cp
        ON ${specs.map((_, i) => `cp.d${i + 1} = s.d${i + 1}`).join(" AND ")}
      LEFT JOIN paid_by_contact p
        ON p.contact_id = cp.contact_id
      ${yandexSpendJoinSql}
      GROUP BY ${dimColumnsQualified}, s.deals_total, s.contacts_in_pool
      ORDER BY revenue DESC, ${dimColumnsQualified}
    `;

    const query = context.env.DB.prepare(sql).bind(...binds);
    const result = await query.all<Record<string, unknown>>();
    let rows = (result.results ?? []).map((r: Record<string, unknown>) => {
      const out: Record<string, unknown> = {};
      specs.forEach((d, i) => {
        out[d.label] = r[`d${i + 1}`];
      });
      out["Выручка"] = r.revenue;
      out["Средний_чек"] = r.avg_check;
      out["Сделок_с_выручкой"] = r.paid_deals;
      out["Контактов_с_выручкой"] = r.contacts_with_revenue;
      out["Контактов_в_пуле"] = r.contacts_in_pool;
      out["Сделок_всего"] = r.deals_total;
      if (includeYandexSpend) {
        const spend = Number(r.yandex_spend ?? 0);
        out["Расход"] = spend;
        out["Прибыль"] = Number(r.revenue) - spend;
      }
      return out;
    });

    if (dims.length === 1 && dims[0] === "event") {
      const breakdownSql = `
        WITH ${sourceDealsCte},
        source_scoped AS (
          SELECT
            event_group AS parent_event,
            COALESCE(NULLIF("Контакт: ID", ''), '') AS contact_id
          FROM source_deals
          ${whereSql}
        ),
        contact_pool AS (
          SELECT DISTINCT parent_event, contact_id
          FROM source_scoped
          WHERE contact_id <> ''
        ),
        paid_deals AS (
          SELECT
            COALESCE(NULLIF("Контакт: ID", ''), '') AS contact_id,
            COALESCE(revenue_amount, 0) AS revenue,
            CASE
              WHEN COALESCE(is_attacking_january, 0) = 1 THEN 'Attacking January'
              WHEN COALESCE(NULLIF(event_class, ''), '') <> '' AND COALESCE(event_class, '') <> 'Другое' THEN event_class
              WHEN LOWER(TRIM(COALESCE("UTM Source", ''))) LIKE 'y%' AND LOWER(TRIM(COALESCE("UTM Source", ''))) <> 'yah' THEN 'Yandex'
              WHEN LOWER(TRIM(COALESCE("UTM Source", ''))) = 'sendsay' THEN 'Email'
              ELSE 'Другое'
            END AS source_label
          FROM mart_deals_enriched
          WHERE is_revenue_variant3 = 1
            AND COALESCE(NULLIF("Контакт: ID", ''), '') <> ''
        )
        SELECT
          cp.parent_event AS parent_event,
          pd.source_label AS source_label,
          COUNT(*) AS paid_deals,
          COUNT(DISTINCT cp.contact_id) AS contacts_with_revenue,
          COALESCE(SUM(pd.revenue), 0) AS revenue
        FROM contact_pool cp
        INNER JOIN paid_deals pd ON pd.contact_id = cp.contact_id
        GROUP BY cp.parent_event, pd.source_label
        ORDER BY cp.parent_event, revenue DESC, pd.source_label
      `;

      const breakdownResult = await context.env.DB.prepare(breakdownSql).bind(...binds).all<Record<string, unknown>>();
      const breakdownByParent = new Map<string, Record<string, unknown>[]>();
      for (const r of breakdownResult.results ?? []) {
        const parentEvent = String(r.parent_event ?? "").trim();
        const sourceLabel = String(r.source_label ?? "").trim() || "Другое";
        if (!parentEvent) continue;
        const paidDeals = Number(r.paid_deals ?? 0);
        const revenue = Number(r.revenue ?? 0);
        const child: Record<string, unknown> = {
          [specs[0].label]: sourceLabel === parentEvent ? `> Прямо: ${sourceLabel}` : `> ${sourceLabel}`,
          "Сделок_всего": paidDeals,
          "Контактов_в_пуле": Number(r.contacts_with_revenue ?? 0),
          "Сделок_с_выручкой": paidDeals,
          "Контактов_с_выручкой": Number(r.contacts_with_revenue ?? 0),
          "Выручка": revenue,
          "Средний_чек": paidDeals > 0 ? revenue / paidDeals : 0,
          "__assoc_event_detail": 1,
          "__assoc_event_ctx": parentEvent,
        };
        const bucket = breakdownByParent.get(parentEvent);
        if (bucket) bucket.push(child);
        else breakdownByParent.set(parentEvent, [child]);
      }

      for (const [parentEvent, details] of breakdownByParent.entries()) {
        details.sort((a, b) => {
          const aLabel = String(a[specs[0].label] ?? "");
          const bLabel = String(b[specs[0].label] ?? "");
          const rank = (label: string): number => {
            if (label.includes("> Прямо:")) return 0;
            if (label === "> Yandex" || label === "> Email") return 2;
            if (label === "> Другое") return 3;
            return 1;
          };
          const rankDiff = rank(aLabel) - rank(bLabel);
          if (rankDiff !== 0) return rankDiff;
          return Number(b["Выручка"] ?? 0) - Number(a["Выручка"] ?? 0);
        });
        breakdownByParent.set(parentEvent, details);
      }

      rows = rows.flatMap((row) => {
        const parentEvent = String(row[specs[0].label] ?? "").trim();
        const details = breakdownByParent.get(parentEvent) ?? [];
        if (details.length > 0) row["__assoc_event_has_details"] = 1;
        return [row, ...details];
      });
    }

    const aliases: Record<string, string> = {
      "Сделок_всего": "Сделок всего",
      "Контактов_в_пуле": "Контактов в пуле",
      "Сделок_с_выручкой": "Сделок с выручкой",
      "Контактов_с_выручкой": "Контактов с выручкой",
      "Выручка": "Выручка",
      "Средний_чек": "Средний чек",
    };

    if (includeYandexSpend) {
      aliases["Расход"] = "Расход";
      aliases["Прибыль"] = "Прибыль";
    }

    const payload = [{ __type: "column_aliases", ...aliases }, ...rows];

    await context.env.DB
      .prepare(
        `
        INSERT INTO assoc_revenue_reports_cache (cache_key, source_watermark, generated_at, response_json)
        VALUES (?, ?, datetime('now'), ?)
        ON CONFLICT(cache_key) DO UPDATE SET
          source_watermark = excluded.source_watermark,
          generated_at = excluded.generated_at,
          response_json = excluded.response_json
      `,
      )
      .bind(cacheKey, sourceWatermark, JSON.stringify(payload))
      .run();

    return json(200, payload);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return json(500, { ok: false, error: "assoc_revenue_query_failed", message: msg });
  }
}
