/**
 * Materialize dashboard JSON blobs into dataset_json (same SQL as db/run_all_slices.export_*).
 */
import { groupYandexProjectsNoMonth } from "./yandexProjectsNoMonth";
import { sqlExtractYandexAdId } from "./yandexAdId";
import { buildYdHierarchyRows } from "./ydHierarchy";
import { buildBitrixContactsUidRows } from "./bitrixContactsUid";
import { buildLeadLogicSql } from "./leadLogicSql";
import { YANDEX_PROJECT_GROUP_ALIAS_PAIRS, YANDEX_KNOWN_GROUPS, mapYandexProjectGroup } from "../../../src/yandexProjectGroups";
import managerFirstlineNames from "../../config/manager_firstline.json";

async function tableExists(db: D1Database, tableName: string): Promise<boolean> {
  const row = await db
    .prepare(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1`)
    .bind(tableName)
    .first<{ name: string }>();
  return !!row?.name;
}

function jsonCell(v: unknown): unknown {
  if (v === null || v === undefined) return null;
  if (typeof v === "number" && (!Number.isFinite(v) || Number.isNaN(v))) return null;
  return v;
}

function rowsToJson(rows: Record<string, unknown>[]): string {
  const cleaned = rows.map((row) => {
    const o: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(row)) {
      o[k] = jsonCell(v);
    }
    return o;
  });
  return JSON.stringify(cleaned);
}

// D1 has a ~1 MB per-row limit. Split large JSON bodies into chunks of this size.
const CHUNK_SIZE = 900_000; // bytes (safe margin under 1 MB)

async function upsertDataset(db: D1Database, path: string, body: string): Promise<void> {
  await db.prepare(`DELETE FROM dataset_json WHERE path = ?`).bind(path).run();
  const chunks: string[] = [];
  for (let i = 0; i < body.length; i += CHUNK_SIZE) {
    chunks.push(body.slice(i, i + CHUNK_SIZE));
  }
  const stmt = db.prepare(
    `INSERT INTO dataset_json (path, chunk, body, updated_at) VALUES (?, ?, ?, datetime('now'))`,
  );
  // D1 batch max is 100 statements; chunks per dataset will be far fewer than that
  await db.batch(chunks.map((chunk, idx) => stmt.bind(path, idx, chunk)));
}

function sqlQuote(value: string): string {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

function buildYandexProjectGroupSqlExpr(rawExpr: string): string {
  const trimmed = `NULLIF(TRIM(COALESCE(${rawExpr}, '')), '')`;
  if (!YANDEX_PROJECT_GROUP_ALIAS_PAIRS.length) return `COALESCE(${trimmed}, 'UNMAPPED')`;
  // Use 'UNMAPPED' as the ELSE fallback so only JSON-defined group names ever appear in the output.
  return `COALESCE(CASE ${trimmed} ${YANDEX_PROJECT_GROUP_ALIAS_PAIRS
    .map(([alias, group]) => `WHEN ${sqlQuote(alias)} THEN ${sqlQuote(group)}`)
    .join(" ")} ELSE 'UNMAPPED' END, 'UNMAPPED')`;
}

/** Map a raw or already-mapped project name to a known JSON group, falling back to 'UNMAPPED'. */
function toKnownGroup(rawOrMapped: unknown): string {
  const mapped = mapYandexProjectGroup(rawOrMapped);
  if (YANDEX_KNOWN_GROUPS.has(mapped)) return mapped;
  // Already a known group name passed through (SQL pre-mapped) or a raw alias that maps to a group.
  if (YANDEX_KNOWN_GROUPS.has(String(rawOrMapped ?? "").trim())) return String(rawOrMapped ?? "").trim();
  return "UNMAPPED";
}

function isValidYandexAdId(value: unknown): boolean {
  return /^17\d{9}$/.test(String(value ?? "").trim());
}

function groupAssocQaRows(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  const agg = new Map<string, Record<string, unknown>>();
  for (const row of rows) {
    const project = toKnownGroup(row.project_name);
    const current = agg.get(project);
    if (!current) {
      agg.set(project, {
        project_name: project,
        "Лиды_Yandex": Number(row["Лиды_Yandex"] ?? 0) || 0,
        "Контактов_в_пуле": Number(row["Контактов_в_пуле"] ?? 0) || 0,
        "Сделок_Bitrix": Number(row["Сделок_Bitrix"] ?? 0) || 0,
        assoc_revenue: Number(row.assoc_revenue ?? 0) || 0,
      });
      continue;
    }
    current["Лиды_Yandex"] = (Number(current["Лиды_Yandex"] ?? 0) || 0) + (Number(row["Лиды_Yandex"] ?? 0) || 0);
    current["Контактов_в_пуле"] = (Number(current["Контактов_в_пуле"] ?? 0) || 0) + (Number(row["Контактов_в_пуле"] ?? 0) || 0);
    current["Сделок_Bitrix"] = (Number(current["Сделок_Bitrix"] ?? 0) || 0) + (Number(row["Сделок_Bitrix"] ?? 0) || 0);
    current.assoc_revenue = (Number(current.assoc_revenue ?? 0) || 0) + (Number(row.assoc_revenue ?? 0) || 0);
  }
  return [...agg.values()].sort((a, b) => (Number(b.assoc_revenue ?? 0) || 0) - (Number(a.assoc_revenue ?? 0) || 0));
}

function buildYandexNoMonthHierarchyRows(
  projectRows: Array<Record<string, unknown>>,
  adRows: Array<Record<string, unknown>>,
): Record<string, unknown>[] {
  const byProject = new Map<string, Record<string, unknown>[]>();
  for (const raw of adRows) {
    const project = toKnownGroup(raw.project_name);
    const adId = String(raw.ad_id ?? "").trim();
    if (!project || !isValidYandexAdId(adId)) continue;
    const child = {
      Level: "Ad",
      project_name: project,
      ad_id: adId,
      ad_title: String(raw.ad_title ?? "").trim(),
      first_month: String(raw.first_month ?? "").trim(),
      last_month: String(raw.last_month ?? "").trim(),
      leads_raw: Number(raw.leads_raw ?? 0) || 0,
      payments_count: Number(raw.payments_count ?? raw.paid_deals_raw ?? 0) || 0,
      paid_deals_raw: Number(raw.paid_deals_raw ?? raw.payments_count ?? 0) || 0,
      revenue_raw: Number(raw.revenue_raw ?? 0) || 0,
      clicks: Number(raw.clicks ?? 0) || 0,
      spend: Number(raw.spend ?? 0) || 0,
      assoc_revenue: Number(raw.assoc_revenue ?? 0) || 0,
      __yandex_project_ctx: project,
      __yandex_project_detail: 1,
    } satisfies Record<string, unknown>;
    const bucket = byProject.get(project);
    if (bucket) bucket.push(child);
    else byProject.set(project, [child]);
  }

  const out: Record<string, unknown>[] = [];
  for (const row of projectRows) {
    const project = String(row.project_name ?? "").trim();
    if (!project) continue;
    const details = (byProject.get(project) || []).sort((a, b) => {
      const spendDiff = (Number(b.spend ?? 0) || 0) - (Number(a.spend ?? 0) || 0);
      if (spendDiff !== 0) return spendDiff;
      return String(a.ad_id ?? "").localeCompare(String(b.ad_id ?? ""));
    });
    out.push({
      ...row,
      Level: "Project",
      __yandex_project_ctx: project,
      __yandex_project_has_details: details.length > 0 ? 1 : 0,
    });
    out.push(...details);
  }
  return out;
}

function buildYandexAssocQaHierarchyRows(
  projectRows: Array<Record<string, unknown>>,
  adRows: Array<Record<string, unknown>>,
): Record<string, unknown>[] {
  const byProject = new Map<string, Record<string, unknown>[]>();
  for (const raw of adRows) {
    const project = mapYandexProjectGroup(raw.project_name);
    const adId = String(raw.ad_id ?? "").trim();
    if (!project || !isValidYandexAdId(adId)) continue;
    const child = {
      Level: "Ad",
      "Проект": project,
      "Yandex объявление": adId,
      "Лиды_Yandex": Number(raw["Лиды_Yandex"] ?? 0) || 0,
      "Контактов_в_пуле": Number(raw["Контактов_в_пуле"] ?? 0) || 0,
      "Сделок_Bitrix": Number(raw["Сделок_Bitrix"] ?? 0) || 0,
      "Ассоц. Выручка": Number(raw.assoc_revenue ?? 0) || 0,
      __yandex_project_ctx: project,
      __yandex_project_detail: 1,
    } satisfies Record<string, unknown>;
    const bucket = byProject.get(project);
    if (bucket) bucket.push(child);
    else byProject.set(project, [child]);
  }

  const out: Record<string, unknown>[] = [];
  for (const row of projectRows) {
    const project = String(row["Проект"] ?? "").trim();
    if (!project) continue;
    const details = (byProject.get(project) || []).sort((a, b) => {
      const revenueDiff = (Number(b["Ассоц. Выручка"] ?? 0) || 0) - (Number(a["Ассоц. Выручка"] ?? 0) || 0);
      if (revenueDiff !== 0) return revenueDiff;
      return String(a["Yandex объявление"] ?? "").localeCompare(String(b["Yandex объявление"] ?? ""));
    });
    out.push({
      ...row,
      Level: "Project",
      "Yandex объявление": "-",
      __yandex_project_ctx: project,
      __yandex_project_has_details: details.length > 0 ? 1 : 0,
    });
    out.push(...details);
  }
  return out;
}

export async function materializeSliceDatasets(db: D1Database): Promise<{ paths: string[] }> {
  const paths: string[] = [];
  const bitrixLeadLogic = buildLeadLogicSql({
    funnelExpr: `"Воронка"`,
    stageExpr: `"Стадия сделки"`,
    monthExpr: "month",
  });
  // Bitrix invalid tokens are tracked in separate columns in the source data
  const BITRIX_INVALID_TOKENS = [
    "дубль",
    "тест",
    "спам",
    "чс",
    "неправильные данные",
    "партнер",
    "сотрудник",
  ];
  async function columnExists(db: D1Database, tableName: string, columnName: string): Promise<boolean> {
    try {
      const row = await db
        .prepare(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1`)
        .bind(tableName)
        .first<{ sql: string }>();
      if (!row?.sql) return false;
      const sql = String(row.sql);
      return sql.includes(columnName) || sql.includes(`"${columnName}"`) || sql.includes(`'${columnName}'`);
    } catch {
      return false;
    }
  }

  // Detect whether the invalid-token columns exist in mart_deals_enriched or raw_bitrix_deals_p01
  const hasTypyInMart = (await columnExists(db, "mart_deals_enriched", "Типы некачественного лида")) || (await columnExists(db, "mart_deals_enriched", "Типы некачественных лидов"));
  const hasRawP01 = await tableExists(db, "raw_bitrix_deals_p01");
  const hasTypyInRaw = hasRawP01 && ((await columnExists(db, "raw_bitrix_deals_p01", "Типы некачественного лида")) || (await columnExists(db, "raw_bitrix_deals_p01", "Типы некачественных лидов")));

  let bitrixInvalidCond = "0";
  if (hasTypyInMart) {
    bitrixInvalidCond = BITRIX_INVALID_TOKENS.flatMap((tok) => [
      `lower(COALESCE("Типы некачественного лида", '')) LIKE ${sqlQuote("%" + tok + "%")}`,
      `lower(COALESCE("Типы некачественных лидов", '')) LIKE ${sqlQuote("%" + tok + "%")}`,
    ]).join(" OR ");
  } else if (hasTypyInRaw) {
    // Fall back to raw table columns when mart_deals_enriched lacks them
    bitrixInvalidCond = BITRIX_INVALID_TOKENS.flatMap((tok) => [
      `lower(COALESCE(p."Типы некачественного лида", '')) LIKE ${sqlQuote("%" + tok + "%")}`,
      `lower(COALESCE(p."Типы некачественных лидов", '')) LIKE ${sqlQuote("%" + tok + "%")}`,
    ]).join(" OR ");
  }
  const bitrixInvalidExpr = `CASE WHEN (${bitrixInvalidCond}) THEN 1 ELSE 0 END`;
  const yandexLeadLogic = buildLeadLogicSql({
    funnelExpr: "funnel",
    stageExpr: "stage",
    monthExpr: "yandex_month",
  });
  const managerLeadLogic = buildLeadLogicSql({
    funnelExpr: `m."Воронка"`,
    stageExpr: `m."Стадия сделки"`,
    monthExpr: "m.month",
  });
  // Build manager invalid condition without referencing non-existent columns.
  const managerHasTypyInMart = hasTypyInMart; // mart_deals_enriched
  const managerHasTypyInRaw = hasTypyInRaw; // raw_bitrix_deals_p01
  let managerInvalidCond = "0";
  if (managerHasTypyInMart) {
    managerInvalidCond = BITRIX_INVALID_TOKENS.flatMap((tok) => [
      `lower(COALESCE(m."Типы некачественного лида", '')) LIKE ${sqlQuote("%" + tok + "%")}`,
      `lower(COALESCE(m."Типы некачественных лидов", '')) LIKE ${sqlQuote("%" + tok + "%")}`,
    ]).join(" OR ");
  } else if (managerHasTypyInRaw) {
    managerInvalidCond = BITRIX_INVALID_TOKENS.flatMap((tok) => [
      `lower(COALESCE(p."Типы некачественного лида", '')) LIKE ${sqlQuote("%" + tok + "%")}`,
      `lower(COALESCE(p."Типы некачественных лидов", '')) LIKE ${sqlQuote("%" + tok + "%")}`,
    ]).join(" OR ");
  }
  const managerInvalidExpr = `CASE WHEN (${managerInvalidCond}) THEN 1 ELSE 0 END`;
  const hasRawP01 = await tableExists(db, "raw_bitrix_deals_p01");
  const hasSendsayContacts = await tableExists(db, "stg_sendsay_contacts");
  const totalEmailContactsExpr = hasSendsayContacts
    ? `(SELECT COUNT(*) FROM stg_sendsay_contacts WHERE COALESCE(email, '') <> '')`
    : `(SELECT COUNT(DISTINCT "Контакт: ID") FROM mart_deals_enriched WHERE COALESCE("Контакт: ID", '') <> '')`;
  const goodEmailContactsExpr = hasSendsayContacts
    ? `(SELECT COUNT(*) FROM stg_sendsay_contacts WHERE COALESCE(email, '') <> '' AND COALESCE(TRIM(error_message), '') = '')`
    : totalEmailContactsExpr;

  const q1 = await db
    .prepare(
      `SELECT month,
              COALESCE("UTM Source", '') AS utm_source,
              COALESCE("UTM Medium", '') AS utm_medium,
              COUNT(DISTINCT ID) AS deals,
              SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
              SUM(revenue_amount) AS revenue
       FROM mart_deals_enriched
       GROUP BY month, COALESCE("UTM Source", ''), COALESCE("UTM Medium", '')
       ORDER BY month, revenue DESC`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "global/month_channel_bitrix.json", rowsToJson((q1.results ?? []) as Record<string, unknown>[]));
  paths.push("global/month_channel_bitrix.json");

  const q2 = await db
    .prepare(
      `SELECT month,
              COUNT(*) AS rows_count,
              SUM(COALESCE("Расход, ₽", 0)) AS yandex_spend,
              SUM(COALESCE("Клики", 0)) AS clicks,
              SUM(COALESCE("Конверсии", 0)) AS conversions
       FROM stg_yandex_stats
       GROUP BY month
       ORDER BY month`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "global/month_channel_yandex.json", rowsToJson((q2.results ?? []) as Record<string, unknown>[]));
  paths.push("global/month_channel_yandex.json");

  const q3 = await db
    .prepare(
      `SELECT month,
              COUNT(*) AS sends,
              SUM(COALESCE("Отправлено", 0)) AS sent_total,
              SUM(COALESCE("Доставлено", 0)) AS delivered_total,
              SUM(COALESCE("Уник. открытий", 0)) AS unique_opens,
              SUM(COALESCE("Уник. кликов", 0)) AS unique_clicks
       FROM stg_email_sends
       GROUP BY month
       ORDER BY month`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "global/month_channel_sendsay.json", rowsToJson((q3.results ?? []) as Record<string, unknown>[]));
  paths.push("global/month_channel_sendsay.json");

  const budgetMonthly = await db
    .prepare(
      `WITH paid_revenue_raw AS (
         SELECT
           CASE
             WHEN COALESCE("Дата оплаты", '') LIKE '____-__%' THEN SUBSTR("Дата оплаты", 1, 7)
             WHEN COALESCE("Дата оплаты", '') LIKE '__.__.____%' THEN SUBSTR("Дата оплаты", 7, 4) || '-' || SUBSTR("Дата оплаты", 4, 2)
             ELSE ''
           END AS pay_month,
           COALESCE(month, '') AS lead_month,
           revenue_amount
         FROM mart_deals_enriched
         WHERE COALESCE(is_revenue_variant3, 0) = 1
       ),
       paid_revenue AS (
         SELECT pay_month, SUM(1) AS paid_deals, SUM(COALESCE(revenue_amount, 0)) AS revenue
         FROM paid_revenue_raw
         GROUP BY pay_month
       ),
       paid_revenue_by_creation AS (
         SELECT pay_month, lead_month, COUNT(*) AS paid_deals, SUM(COALESCE(revenue_amount, 0)) AS revenue
         FROM paid_revenue_raw
         GROUP BY pay_month, lead_month
       ),
       spend_by_month AS (
         SELECT
           COALESCE(month, '') AS spend_month,
           SUM(COALESCE("Расход, ₽", 0)) AS spend
         FROM stg_yandex_stats
         GROUP BY 1
       ),
       all_months AS (
         SELECT pay_month AS month FROM paid_revenue WHERE COALESCE(pay_month, '') <> ''
         UNION
         SELECT spend_month AS month FROM spend_by_month WHERE COALESCE(spend_month, '') <> ''
       )
      SELECT "Level", "Период", "Сделок_с_выручкой", "Выручка", "Расход, ₽", "Прибыль", month, "__pay_month"
       FROM (
         -- Month header rows
         SELECT
           'Month' AS "Level",
           month AS "Период",
           COALESCE(pr.paid_deals, 0) AS "Сделок_с_выручкой",
           COALESCE(pr.revenue, 0) AS "Выручка",
           COALESCE(sb.spend, 0) AS "Расход, ₽",
           COALESCE(pr.revenue, 0) - COALESCE(sb.spend, 0) AS "Прибыль",
           month,
           month AS "__pay_month",
           0 AS _sort_level,
           '' AS _sort_lead_month
         FROM all_months am
         LEFT JOIN paid_revenue pr ON pr.pay_month = am.month
         LEFT JOIN spend_by_month sb ON sb.spend_month = am.month
         UNION ALL
         -- Detail rows (breakdown by lead creation month within a payment month)
         SELECT
           'Detail' AS "Level",
           COALESCE(pbc.lead_month, '') AS "Период",
           pbc.paid_deals AS "Сделок_с_выручкой",
           pbc.revenue AS "Выручка",
           NULL AS "Расход, ₽",
           NULL AS "Прибыль",
           pbc.pay_month AS month,
           pbc.pay_month AS "__pay_month",
           1 AS _sort_level,
           COALESCE(pbc.lead_month, '') AS _sort_lead_month
         FROM paid_revenue_by_creation pbc
         WHERE COALESCE(pbc.pay_month, '') <> ''
       ) q
       ORDER BY month DESC, _sort_level ASC, _sort_lead_month DESC`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "global/budget_monthly.json", rowsToJson((budgetMonthly.results ?? []) as Record<string, unknown>[]));
  paths.push("global/budget_monthly.json");

  const bitrixMonthTotal = await db
    .prepare(
      `WITH flags AS (
         SELECT
           month,
           COALESCE("ID", '') AS deal_id,
           COALESCE("Стадия сделки", '') AS stage,
           COALESCE("Воронка", '') AS funnel,
           COALESCE(revenue_amount, 0) AS revenue_amount,
           ${bitrixLeadLogic.qual} AS is_qual,
           ${bitrixLeadLogic.unqual} AS is_unqual,
           ${bitrixLeadLogic.refusal} AS is_refusal,
          ${bitrixInvalidExpr} AS is_invalid,
           ${bitrixLeadLogic.inWork} AS is_in_work,
           CASE WHEN COALESCE(is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END AS is_revenue
         FROM mart_deals_enriched
         WHERE COALESCE(month, '') <> ''
       )
       SELECT
         month AS "Месяц",
         COUNT(*) AS "Лиды",
         SUM(is_qual) AS "Квал",
         SUM(is_unqual) AS "Неквал",
         SUM(CASE WHEN is_qual = 0 AND is_unqual = 0 THEN 1 ELSE 0 END) AS "Неизвестно",
         SUM(is_refusal) AS "Отказы",
         SUM(is_in_work) AS "В работе",
         SUM(is_invalid) AS "Невалидные_лиды",
         SUM(is_revenue) AS "Сделок_с_выручкой",
         SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS "Выручка",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_qual) * 1.0 / COUNT(*) END AS "Конверсия в Квал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_unqual) * 1.0 / COUNT(*) END AS "Конверсия в Неквал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_refusal) * 1.0 / COUNT(*) END AS "Конверсия в Отказ",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_in_work) * 1.0 / COUNT(*) END AS "Конверсия в работе",
         CASE WHEN SUM(is_revenue) = 0 THEN 0 ELSE SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) * 1.0 / SUM(is_revenue) END AS "Средний_чек"
       FROM flags
       GROUP BY month
       ORDER BY month`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "bitrix_month_total_full.json", rowsToJson((bitrixMonthTotal.results ?? []) as Record<string, unknown>[]));
  paths.push("bitrix_month_total_full.json");

  const bitrixWeekFunnelTotal = await db
    .prepare(
      `WITH src AS (
         SELECT
           CASE
             WHEN COALESCE("Дата создания", '') LIKE '____-__-__%' THEN SUBSTR("Дата создания", 1, 10)
             WHEN COALESCE("Дата создания", '') LIKE '__.__.____%' THEN SUBSTR("Дата создания", 7, 4) || '-' || SUBSTR("Дата создания", 4, 2) || '-' || SUBSTR("Дата создания", 1, 2)
             ELSE ''
           END AS created_date,
           CASE
             WHEN COALESCE("Дата оплаты", '') LIKE '____-__-__%' THEN SUBSTR("Дата оплаты", 1, 10)
             WHEN COALESCE("Дата оплаты", '') LIKE '__.__.____%' THEN SUBSTR("Дата оплаты", 7, 4) || '-' || SUBSTR("Дата оплаты", 4, 2) || '-' || SUBSTR("Дата оплаты", 1, 2)
             ELSE ''
           END AS paid_date,
           COALESCE(NULLIF(trim(funnel_group), ''), 'Другое') AS funnel_group,
           COALESCE(revenue_amount, 0) AS revenue_amount,
           ${bitrixLeadLogic.qual} AS is_qual,
           ${bitrixLeadLogic.unqual} AS is_unqual,
           ${bitrixLeadLogic.refusal} AS is_refusal,
           ${bitrixInvalidExpr} AS is_invalid,
           ${bitrixLeadLogic.inWork} AS is_in_work,
           CASE WHEN COALESCE(is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END AS is_revenue
         FROM mart_deals_enriched
       ),
       base AS (
         SELECT
           date(created_date) AS created_dt,
           CASE WHEN paid_date <> '' THEN date(paid_date) ELSE NULL END AS paid_dt,
           date(created_date, printf('-%d days', (CAST(strftime('%w', date(created_date)) AS INTEGER) + 6) % 7)) AS week_start,
           CASE
             WHEN paid_date <> '' THEN date(paid_date, printf('-%d days', (CAST(strftime('%w', date(paid_date)) AS INTEGER) + 6) % 7))
             ELSE NULL
           END AS paid_week_start,
           funnel_group,
           revenue_amount,
           is_qual,
           is_unqual,
           is_refusal,
           is_invalid,
           is_in_work,
           is_revenue
         FROM src
         WHERE created_date <> ''
       ),
       latest AS (
         SELECT MAX(created_dt) AS latest_created_dt FROM base WHERE created_dt IS NOT NULL
       )
       SELECT
         week_start AS "Неделя",
         funnel_group AS "Воронка",
         COUNT(*) AS "Лиды",
         SUM(is_qual) AS "Квал",
         SUM(is_unqual) AS "Неквал",
         SUM(CASE WHEN is_qual = 0 AND is_unqual = 0 THEN 1 ELSE 0 END) AS "Неизвестно",
         SUM(is_refusal) AS "Отказы",
         SUM(is_in_work) AS "В работе",
         SUM(is_invalid) AS "Невалидные_лиды",
         SUM(is_revenue) AS "Сделок_с_выручкой",
         SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS "Выручка_сделки_недели",
         SUM(CASE WHEN is_revenue = 1 AND paid_week_start = week_start THEN revenue_amount ELSE 0 END) AS "Выручка_получена_на_неделе",
         MAX(created_dt) AS "Макс_дата_в_строке",
         (SELECT latest_created_dt FROM latest) AS "Дата_последней_записи_Bitrix",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_qual) * 1.0 / COUNT(*) END AS "Конверсия в Квал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_unqual) * 1.0 / COUNT(*) END AS "Конверсия в Неквал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_refusal) * 1.0 / COUNT(*) END AS "Конверсия в Отказ",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_in_work) * 1.0 / COUNT(*) END AS "Конверсия в работе"
       FROM base
       WHERE created_dt IS NOT NULL
         AND week_start IS NOT NULL
         AND created_dt >= date((SELECT latest_created_dt FROM latest), '-6 day')
       GROUP BY week_start, funnel_group
       ORDER BY week_start, funnel_group`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "bitrix_week_funnel_total.json", rowsToJson((bitrixWeekFunnelTotal.results ?? []) as Record<string, unknown>[]));
  paths.push("bitrix_week_funnel_total.json");

  const yandexWeekCampaignTotal = await db
    .prepare(
      `WITH ysrc AS (
         SELECT
           ${buildYandexProjectGroupSqlExpr("l.project_name")} AS campaign_name,
           COALESCE(NULLIF(TRIM(l.campaign_id), ''), '(пусто)') AS campaign_id,
           COALESCE(NULLIF(TRIM(l.yandex_month), ''), '') AS month,
           CASE
             WHEN COALESCE(m."Дата создания", '') LIKE '____-__-__%' THEN SUBSTR(m."Дата создания", 1, 10)
             WHEN COALESCE(m."Дата создания", '') LIKE '__.__.____%' THEN SUBSTR(m."Дата создания", 7, 4) || '-' || SUBSTR(m."Дата создания", 4, 2) || '-' || SUBSTR(m."Дата создания", 1, 2)
             ELSE ''
           END AS created_date,
           COALESCE(l.revenue_amount, 0) AS revenue_amount,
           ${yandexLeadLogic.qual} AS is_qual,
           ${yandexLeadLogic.unqual} AS is_unqual,
           ${yandexLeadLogic.refusal} AS is_refusal,
           ${yandexLeadLogic.invalid} AS is_invalid,
           CASE WHEN COALESCE(l.is_paid_deal, 0) = 1 THEN 1 ELSE 0 END AS is_revenue
         FROM mart_yandex_leads_raw l
         LEFT JOIN mart_deals_enriched m ON m."ID" = l."ID"
         WHERE COALESCE(NULLIF(TRIM(l.yandex_month), ''), '') <> ''
       ),
       base AS (
         SELECT
           campaign_name,
           campaign_id,
           month,
           date(created_date) AS created_dt,
           date(created_date, printf('-%d days', (CAST(strftime('%w', date(created_date)) AS INTEGER) + 6) % 7)) AS week_start,
           revenue_amount,
           is_qual,
           is_unqual,
           is_refusal,
           is_invalid,
           is_revenue
         FROM ysrc
         WHERE created_date <> ''
       ),
       latest AS (
         SELECT MAX(created_dt) AS latest_created_dt FROM base WHERE created_dt IS NOT NULL
       ),
       latest_spend AS (
         SELECT MAX(date(NULLIF(TRIM(COALESCE("День", '')), ''))) AS latest_spend_dt
         FROM stg_yandex_stats
         WHERE COALESCE(NULLIF(TRIM(COALESCE("День", '')), ''), '') <> ''
       ),
       latest_yandex AS (
         SELECT
           CASE
             WHEN (SELECT latest_spend_dt FROM latest_spend) IS NULL THEN (SELECT latest_created_dt FROM latest)
             WHEN (SELECT latest_created_dt FROM latest) IS NULL THEN (SELECT latest_spend_dt FROM latest_spend)
             WHEN date((SELECT latest_spend_dt FROM latest_spend)) >= date((SELECT latest_created_dt FROM latest)) THEN (SELECT latest_spend_dt FROM latest_spend)
             ELSE (SELECT latest_created_dt FROM latest)
           END AS latest_yandex_dt
       ),
       campaign_month_spend AS (
         SELECT
           COALESCE(NULLIF(TRIM("Название кампании"), ''), '(пусто)') AS campaign_name,
           COALESCE(NULLIF(REPLACE(TRIM(CAST("№ Кампании" AS TEXT)), '.0', ''), ''), '(пусто)') AS campaign_id,
           COALESCE(NULLIF(TRIM(COALESCE(month, "Месяц")), ''), '') AS month,
           SUM(COALESCE("Расход, ₽", 0)) AS spend
         FROM stg_yandex_stats
         WHERE COALESCE(NULLIF(TRIM(COALESCE(month, "Месяц")), ''), '') <> ''
         GROUP BY campaign_name, campaign_id, month
       ),
       campaign_month_leads AS (
         SELECT
           campaign_name,
           campaign_id,
           month,
           COUNT(*) AS month_leads
         FROM base
         WHERE created_dt IS NOT NULL
         GROUP BY campaign_name, campaign_id, month
       ),
       campaign_week_leads AS (
         SELECT
           week_start,
           campaign_name,
           campaign_id,
           month,
           COUNT(*) AS week_leads
         FROM base
         WHERE created_dt IS NOT NULL
         GROUP BY week_start, campaign_name, campaign_id, month
       ),
       spend_alloc AS (
         SELECT
           wl.week_start,
           wl.campaign_name,
           wl.campaign_id,
           SUM(
             CASE
               WHEN COALESCE(ml.month_leads, 0) = 0 THEN COALESCE(ms.spend, 0)
               ELSE COALESCE(ms.spend, 0) * wl.week_leads * 1.0 / ml.month_leads
             END
           ) AS spend_alloc
         FROM campaign_week_leads wl
         LEFT JOIN campaign_month_leads ml
             ON ml.campaign_id = wl.campaign_id
          AND ml.month = wl.month
         LEFT JOIN campaign_month_spend ms
             ON ms.campaign_id = wl.campaign_id
          AND ms.month = wl.month
         GROUP BY wl.week_start, wl.campaign_name, wl.campaign_id
       )
       SELECT
         b.week_start AS "Неделя",
         b.campaign_name AS "Кампания",
         b.campaign_id AS "ID кампании",
         COUNT(*) AS "Лиды",
         SUM(b.is_qual) AS "Квал",
         SUM(b.is_unqual) AS "Неквал",
         SUM(CASE WHEN b.is_qual = 0 AND b.is_unqual = 0 THEN 1 ELSE 0 END) AS "Неизвестно",
         SUM(b.is_refusal) AS "Отказы",
         SUM(b.is_invalid) AS "Невалидные_лиды",
         SUM(b.is_revenue) AS "Сделок_с_выручкой",
         SUM(CASE WHEN b.is_revenue = 1 THEN b.revenue_amount ELSE 0 END) AS "Ассоц_выручка",
         COALESCE(sa.spend_alloc, 0) AS "Расход, ₽",
         SUM(CASE WHEN b.is_revenue = 1 THEN b.revenue_amount ELSE 0 END) - COALESCE(sa.spend_alloc, 0) AS "Прибыль",
         MAX(b.created_dt) AS "Макс_дата_в_строке",
         (SELECT latest_yandex_dt FROM latest_yandex) AS "Дата_последней_записи_Yandex"
       FROM base b
       LEFT JOIN spend_alloc sa
         ON sa.week_start = b.week_start
        AND sa.campaign_name = b.campaign_name
        AND sa.campaign_id = b.campaign_id
       WHERE b.created_dt IS NOT NULL
         AND b.week_start IS NOT NULL
         AND b.created_dt >= date((SELECT latest_created_dt FROM latest), '-6 day')
       GROUP BY b.week_start, b.campaign_name, b.campaign_id, sa.spend_alloc
       ORDER BY b.week_start, b.campaign_name, b.campaign_id`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "yandex_week_campaign_total.json", rowsToJson((yandexWeekCampaignTotal.results ?? []) as Record<string, unknown>[]));
  paths.push("yandex_week_campaign_total.json");

  const emailOperationalSummary = await db
    .prepare(
      `WITH deals_by_campaign AS (
         SELECT
           lower(trim(COALESCE("UTM Campaign", ''))) AS utm_campaign_key,
           COUNT(*) AS leads,
           SUM(${bitrixLeadLogic.qual}) AS qual,
           SUM(${bitrixLeadLogic.unqual}) AS unqual,
           SUM(${bitrixLeadLogic.refusal}) AS refusal,
           SUM(CASE WHEN COALESCE(is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END) AS paid_deals,
           SUM(CASE WHEN COALESCE(is_revenue_variant3, 0) = 1 THEN COALESCE(revenue_amount, 0) ELSE 0 END) AS revenue
         FROM mart_deals_enriched
         WHERE lower(trim(COALESCE("UTM Source", ''))) = 'sendsay'
         GROUP BY lower(trim(COALESCE("UTM Campaign", '')))
       ),
       send_rows AS (
         SELECT
           month,
           COUNT(*) AS sends,
           SUM(COALESCE("Отправлено", 0)) AS sent_total,
           SUM(COALESCE("Доставлено", 0)) AS delivered_total,
           SUM(COALESCE("Ошибок", 0)) AS errors_total,
           SUM(COALESCE("Открытий", 0)) AS opens_total,
           SUM(COALESCE("Уник. открытий", 0)) AS uniq_opens,
           SUM(COALESCE("Уник. кликов", 0)) AS uniq_clicks,
           SUM(COALESCE("Отписок", 0)) AS unsub_total,
           SUM(COALESCE(dbc.leads, 0)) AS leads,
           SUM(COALESCE(dbc.paid_deals, 0)) AS paid_deals,
           SUM(COALESCE(dbc.revenue, 0)) AS revenue
         FROM stg_email_sends e
         LEFT JOIN deals_by_campaign dbc
           ON dbc.utm_campaign_key = lower(trim(COALESCE(e.utm_campaign, '')))
         WHERE COALESCE(e.month, '') <> ''
         GROUP BY e.month
       ),
       month_rows AS (
         SELECT
           CASE strftime('%m', month || '-01')
             WHEN '01' THEN 'Январь, ' || strftime('%Y', month || '-01')
             WHEN '02' THEN 'Февраль, ' || strftime('%Y', month || '-01')
             WHEN '03' THEN 'Март, ' || strftime('%Y', month || '-01')
             WHEN '04' THEN 'Апрель, ' || strftime('%Y', month || '-01')
             WHEN '05' THEN 'Май, ' || strftime('%Y', month || '-01')
             WHEN '06' THEN 'Июнь, ' || strftime('%Y', month || '-01')
             WHEN '07' THEN 'Июль, ' || strftime('%Y', month || '-01')
             WHEN '08' THEN 'Август, ' || strftime('%Y', month || '-01')
             WHEN '09' THEN 'Сентябрь, ' || strftime('%Y', month || '-01')
             WHEN '10' THEN 'Октябрь, ' || strftime('%Y', month || '-01')
             WHEN '11' THEN 'Ноябрь, ' || strftime('%Y', month || '-01')
             WHEN '12' THEN 'Декабрь, ' || strftime('%Y', month || '-01')
             ELSE month
           END AS "Период",
           sent_total,
           delivered_total,
           opens_total,
           uniq_opens,
           uniq_clicks,
           unsub_total,
           sends,
           leads,
           paid_deals,
           revenue,
           month
         FROM send_rows
       )
       SELECT
         "Период",
         ${goodEmailContactsExpr} AS "Актуальная база email",
         ${totalEmailContactsExpr} AS "Контактов email (DB)",
         sends AS "Рассылок за месяц",
         leads AS "Лиды",
         paid_deals AS "Сделок с выручкой",
         revenue AS "Выручка",
         month
       FROM month_rows
       ORDER BY month DESC`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "email_operational_summary.json", rowsToJson((emailOperationalSummary.results ?? []) as Record<string, unknown>[]));
  paths.push("email_operational_summary.json");

  const emailHierarchyBySend = await db
    .prepare(
      `WITH email_pools AS (
         SELECT DISTINCT
           lower(trim(COALESCE("UTM Campaign", ''))) AS utm_key,
           REPLACE(TRIM(COALESCE("Контакт: ID", '')), '.0', '') AS contact_id
         FROM mart_deals_enriched
         WHERE LOWER(TRIM(COALESCE("UTM Source", ''))) = 'sendsay'
           AND lower(trim(COALESCE("UTM Campaign", ''))) <> ''
           AND REPLACE(TRIM(COALESCE("Контакт: ID", '')), '.0', '') <> ''
       ),
       assoc_rev AS (
         SELECT
           ep.utm_key,
           COUNT(DISTINCT rev."ID") AS assoc_deals,
           COALESCE(SUM(COALESCE(rev.revenue_amount, 0)), 0) AS assoc_revenue
         FROM email_pools ep
         JOIN mart_deals_enriched rev
           ON REPLACE(TRIM(COALESCE(rev."Контакт: ID", '')), '.0', '') = ep.contact_id
         WHERE rev.is_revenue_variant3 = 1
         GROUP BY ep.utm_key
       ),
       deals_by_campaign AS (
         SELECT
           lower(trim(COALESCE("UTM Campaign", ''))) AS utm_campaign_key,
           COUNT(*) AS leads,
         SUM(${bitrixLeadLogic.qual}) AS qual,
         SUM(${bitrixLeadLogic.unqual}) AS unqual,
         SUM(${bitrixLeadLogic.refusal}) AS refusal,
           SUBSTR(GROUP_CONCAT(COALESCE("ID", '')), 1, 50000) AS fl_ids,
           SUM(CASE WHEN COALESCE(is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END) AS paid_deals,
           SUM(CASE WHEN COALESCE(is_revenue_variant3, 0) = 1 THEN COALESCE(revenue_amount, 0) ELSE 0 END) AS revenue
         FROM mart_deals_enriched
         WHERE lower(trim(COALESCE("UTM Source", ''))) = 'sendsay'
         GROUP BY lower(trim(COALESCE("UTM Campaign", '')))
       ),
       send_rows AS (
         SELECT
           e.month,
           e."Название выпуска" AS release_name,
           COALESCE(e.utm_campaign, 'Unmatched') AS utm_campaign,
           COALESCE(e."Тема", '-') AS subject,
           1 AS sends,
           COALESCE(e."Отправлено", 0) AS sent_total,
           COALESCE(e."Доставлено", 0) AS delivered_total,
           COALESCE(e."Ошибок", 0) AS errors_total,
           COALESCE(e."Открытий", 0) AS opens_total,
           COALESCE(e."Уник. открытий", 0) AS uniq_opens,
           COALESCE(e."Уник. кликов", 0) AS uniq_clicks,
           COALESCE(e."Отписок", 0) AS unsub_total,
           COALESCE(dbc.leads, 0) AS leads,
           COALESCE(dbc.qual, 0) AS qual,
           COALESCE(dbc.unqual, 0) AS unqual,
           COALESCE(dbc.refusal, 0) AS refusal,
           COALESCE(dbc.fl_ids, '') AS fl_ids,
           COALESCE(dbc.paid_deals, 0) AS paid_deals,
           COALESCE(dbc.revenue, 0) AS revenue,
           COALESCE(ar.assoc_revenue, 0) AS assoc_revenue,
           COALESCE(ar.assoc_deals, 0) AS assoc_deals,
           CASE WHEN COALESCE(ar.assoc_revenue, 0) < COALESCE(dbc.revenue, 0) THEN COALESCE(dbc.revenue, 0) ELSE COALESCE(ar.assoc_revenue, 0) END AS assoc_rev_eff,
           CASE WHEN COALESCE(ar.assoc_deals, 0) < COALESCE(dbc.paid_deals, 0) THEN COALESCE(dbc.paid_deals, 0) ELSE COALESCE(ar.assoc_deals, 0) END AS assoc_deals_eff
         FROM stg_email_sends e
         LEFT JOIN deals_by_campaign dbc
           ON dbc.utm_campaign_key = lower(trim(COALESCE(e.utm_campaign, '')))
         LEFT JOIN assoc_rev ar
           ON ar.utm_key = lower(trim(COALESCE(e.utm_campaign, '')))
         WHERE COALESCE(e.month, '') <> ''
       ),
       month_rows AS (
         SELECT
           CASE strftime('%m', month || '-01')
             WHEN '01' THEN 'Январь, ' || strftime('%Y', month || '-01')
             WHEN '02' THEN 'Февраль, ' || strftime('%Y', month || '-01')
             WHEN '03' THEN 'Март, ' || strftime('%Y', month || '-01')
             WHEN '04' THEN 'Апрель, ' || strftime('%Y', month || '-01')
             WHEN '05' THEN 'Май, ' || strftime('%Y', month || '-01')
             WHEN '06' THEN 'Июнь, ' || strftime('%Y', month || '-01')
             WHEN '07' THEN 'Июль, ' || strftime('%Y', month || '-01')
             WHEN '08' THEN 'Август, ' || strftime('%Y', month || '-01')
             WHEN '09' THEN 'Сентябрь, ' || strftime('%Y', month || '-01')
             WHEN '10' THEN 'Октябрь, ' || strftime('%Y', month || '-01')
             WHEN '11' THEN 'Ноябрь, ' || strftime('%Y', month || '-01')
             WHEN '12' THEN 'Декабрь, ' || strftime('%Y', month || '-01')
             ELSE month
           END AS month_label,
           month,
           SUM(sends) AS sends,
           SUM(sent_total) AS sent_total,
           SUM(delivered_total) AS delivered_total,
           SUM(errors_total) AS errors_total,
           SUM(opens_total) AS opens_total,
           SUM(uniq_opens) AS uniq_opens,
           SUM(uniq_clicks) AS uniq_clicks,
           SUM(unsub_total) AS unsub_total,
           SUM(leads) AS leads,
           SUM(qual) AS qual,
           SUM(unqual) AS unqual,
           SUM(refusal) AS refusal,
           SUBSTR(GROUP_CONCAT(fl_ids), 1, 50000) AS fl_ids,
           SUM(paid_deals) AS paid_deals,
           SUM(revenue) AS revenue,
           SUM(assoc_revenue) AS assoc_revenue,
           SUM(assoc_deals) AS assoc_deals,
           SUM(assoc_rev_eff) AS assoc_rev_eff,
           SUM(assoc_deals_eff) AS assoc_deals_eff
         FROM send_rows
         GROUP BY month
       ),
       send_rows_labeled AS (
         SELECT
           'Send' AS "Level",
           CASE strftime('%m', month || '-01')
             WHEN '01' THEN 'Январь, ' || strftime('%Y', month || '-01')
             WHEN '02' THEN 'Февраль, ' || strftime('%Y', month || '-01')
             WHEN '03' THEN 'Март, ' || strftime('%Y', month || '-01')
             WHEN '04' THEN 'Апрель, ' || strftime('%Y', month || '-01')
             WHEN '05' THEN 'Май, ' || strftime('%Y', month || '-01')
             WHEN '06' THEN 'Июнь, ' || strftime('%Y', month || '-01')
             WHEN '07' THEN 'Июль, ' || strftime('%Y', month || '-01')
             WHEN '08' THEN 'Август, ' || strftime('%Y', month || '-01')
             WHEN '09' THEN 'Сентябрь, ' || strftime('%Y', month || '-01')
             WHEN '10' THEN 'Октябрь, ' || strftime('%Y', month || '-01')
             WHEN '11' THEN 'Ноябрь, ' || strftime('%Y', month || '-01')
             WHEN '12' THEN 'Декабрь, ' || strftime('%Y', month || '-01')
             ELSE month
           END AS "Месяц",
           COALESCE(NULLIF(trim(release_name), ''), 'Unmatched') AS "Название выпуска",
           COALESCE(NULLIF(trim(utm_campaign), ''), 'Unmatched') AS utm_campaign,
           subject AS "Тема",
           leads AS "Leads",
           qual AS "Qual",
           unqual AS "Unqual",
           refusal AS "Refusal",
           fl_ids AS "fl_IDs",
           sends AS sends,
           sent_total AS "Отправлено",
           delivered_total AS "Доставлено",
           errors_total AS "Ошибок",
           opens_total AS "Открытий",
           uniq_opens AS "Уник. открытий",
           uniq_clicks AS "Уник. кликов",
           unsub_total AS "Отписок",
           leads AS "Лиды",
           qual AS "Квал Лиды",
           unqual AS "Неквал",
           refusal AS "Отказы",
           CASE WHEN uniq_opens = 0 THEN 0 ELSE ROUND(uniq_clicks * 100.0 / uniq_opens, 2) END AS "CTOR%",
           CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(uniq_opens * 100.0 / delivered_total, 2) END AS "%Уник открытий",
           CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(unsub_total * 100.0 / delivered_total, 2) END AS "Конверсия в Отписки",
           CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(leads * 100.0 / delivered_total, 2) END AS "Конверсия в Лиды",
           CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(qual * 100.0 / delivered_total, 2) END AS "Конверсия в Квал",
           CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(unqual * 100.0 / delivered_total, 2) END AS "Конверсия в Неквал",
           CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(refusal * 100.0 / delivered_total, 2) END AS "Конверсия в Отказ",
           revenue AS "Выручка",
           paid_deals AS "Сделок с выручкой",
           CASE WHEN paid_deals = 0 THEN 0 ELSE revenue * 1.0 / paid_deals END AS "Средняя за сделку",
           assoc_rev_eff AS "Ассоц. Выручка",
           assoc_deals_eff AS "Ассоц. Сделок",
           CASE WHEN assoc_deals_eff = 0 THEN 0 ELSE assoc_rev_eff * 1.0 / assoc_deals_eff END AS "Средняя ассоц. за сделку",
           0 AS "Средний остаток по сделке",
           NULL AS "Рассылок за месяц",
           NULL AS "Лидов с некорр. email",
           NULL AS "Доля некорр. email (лиды)",
           month
         FROM send_rows
       )
       SELECT
         'Month' AS "Level",
         month_label AS "Месяц",
         '-' AS "Название выпуска",
         '-' AS utm_campaign,
         '-' AS "Тема",
         leads AS "Leads",
         qual AS "Qual",
         unqual AS "Unqual",
         refusal AS "Refusal",
         fl_ids AS "fl_IDs",
         sends AS sends,
         sent_total AS "Отправлено",
         delivered_total AS "Доставлено",
         errors_total AS "Ошибок",
         opens_total AS "Открытий",
         uniq_opens AS "Уник. открытий",
         uniq_clicks AS "Уник. кликов",
         unsub_total AS "Отписок",
         leads AS "Лиды",
         qual AS "Квал Лиды",
         unqual AS "Неквал",
         refusal AS "Отказы",
         CASE WHEN uniq_opens = 0 THEN 0 ELSE ROUND(uniq_clicks * 100.0 / uniq_opens, 2) END AS "CTOR%",
         CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(uniq_opens * 100.0 / delivered_total, 2) END AS "%Уник открытий",
         CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(unsub_total * 100.0 / delivered_total, 2) END AS "Конверсия в Отписки",
         CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(leads * 100.0 / delivered_total, 2) END AS "Конверсия в Лиды",
         CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(qual * 100.0 / delivered_total, 2) END AS "Конверсия в Квал",
         CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(unqual * 100.0 / delivered_total, 2) END AS "Конверсия в Неквал",
         CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(refusal * 100.0 / delivered_total, 2) END AS "Конверсия в Отказ",
         revenue AS "Выручка",
         paid_deals AS "Сделок с выручкой",
         CASE WHEN paid_deals = 0 THEN 0 ELSE revenue * 1.0 / paid_deals END AS "Средняя за сделку",
         assoc_rev_eff AS "Ассоц. Выручка",
         assoc_deals_eff AS "Ассоц. Сделок",
         CASE WHEN assoc_deals_eff = 0 THEN 0 ELSE assoc_rev_eff * 1.0 / assoc_deals_eff END AS "Средняя ассоц. за сделку",
         0 AS "Средний остаток по сделке",
         sends AS "Рассылок за месяц",
         CASE WHEN leads > 0 THEN sends ELSE 0 END AS "Лидов с некорр. email",
         CASE WHEN leads > 0 THEN 100 ELSE 0 END AS "Доля некорр. email (лиды)",
         month
       FROM month_rows
       UNION ALL
       SELECT
         "Level",
         "Месяц",
         "Название выпуска",
         utm_campaign,
         "Тема",
         "Leads",
         "Qual",
         "Unqual",
         "Refusal",
         "fl_IDs",
         sends,
         "Отправлено",
         "Доставлено",
         "Ошибок",
         "Открытий",
         "Уник. открытий",
         "Уник. кликов",
         "Отписок",
         "Лиды",
         "Квал Лиды",
         "Неквал",
         "Отказы",
         "CTOR%",
         "%Уник открытий",
         "Конверсия в Отписки",
         "Конверсия в Лиды",
         "Конверсия в Квал",
         "Конверсия в Неквал",
         "Конверсия в Отказ",
         "Выручка",
         "Сделок с выручкой",
         "Средняя за сделку",
         "Ассоц. Выручка",
         "Ассоц. Сделок",
         "Средняя ассоц. за сделку",
         "Средний остаток по сделке",
         "Рассылок за месяц",
         "Лидов с некорр. email",
         "Доля некорр. email (лиды)",
         month
       FROM send_rows_labeled
       ORDER BY month DESC, "Level" ASC, "Название выпуска" ASC`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "email_hierarchy_by_send.json", rowsToJson((emailHierarchyBySend.results ?? []) as Record<string, unknown>[]));
  paths.push("email_hierarchy_by_send.json");

  const bitrixContactsUid = await buildBitrixContactsUidRows(db);
  await upsertDataset(db, "bitrix_contacts_uid.json", rowsToJson(bitrixContactsUid as Record<string, unknown>[]));
  paths.push("bitrix_contacts_uid.json");

  const dashboardContactsTotalRows: Record<string, unknown>[] = [{
    bitrix_contacts_actual: bitrixContactsUid.length,
    email_contacts_actual: 0,
    contacts_actual_total: bitrixContactsUid.length,
  }];
  const dashboardContactsTotal = await db
    .prepare(`SELECT ${goodEmailContactsExpr} AS email_contacts_actual`)
    .first<{ email_contacts_actual: number }>();
  const emailContactsActual = Number(dashboardContactsTotal?.email_contacts_actual ?? 0) || 0;
  dashboardContactsTotalRows[0].email_contacts_actual = emailContactsActual;
  dashboardContactsTotalRows[0].contacts_actual_total = dashboardContactsTotalRows[0].bitrix_contacts_actual + emailContactsActual;
  await upsertDataset(db, "dashboard_contacts_total.json", rowsToJson(dashboardContactsTotalRows));
  paths.push("dashboard_contacts_total.json");

  const bitrixFunnelMonthCode = await db
    .prepare(
      `WITH flags AS (
         SELECT
           COALESCE(NULLIF(trim(funnel_group), ''), 'Другое') AS funnel_group,
           month,
           COALESCE(NULLIF(trim(course_code_norm), ''), '—') AS course_code,
           COALESCE(revenue_amount, 0) AS revenue_amount,
           ${bitrixLeadLogic.qual} AS is_qual,
           ${bitrixLeadLogic.unqual} AS is_unqual,
           ${bitrixLeadLogic.refusal} AS is_refusal,
           ${bitrixInvalidExpr} AS is_invalid,
           ${bitrixLeadLogic.inWork} AS is_in_work,
           CASE WHEN COALESCE(is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END AS is_revenue
         FROM mart_deals_enriched
         WHERE COALESCE(month, '') <> ''
       )
       SELECT
         funnel_group AS "Воронка",
         month AS "Месяц",
         course_code AS "Код_курса_норм",
         COUNT(*) AS "Лиды",
         SUM(is_qual) AS "Квал",
         SUM(is_unqual) AS "Неквал",
         SUM(is_refusal) AS "Отказы",
         SUM(is_in_work) AS "В работе",
         SUM(is_invalid) AS "Невалидные_лиды",
         SUM(is_revenue) AS "Сделок_с_выручкой",
         SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS "Выручка",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_qual) * 1.0 / COUNT(*) END AS "Конверсия в Квал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_unqual) * 1.0 / COUNT(*) END AS "Конверсия в Неквал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_refusal) * 1.0 / COUNT(*) END AS "Конверсия в Отказ",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_in_work) * 1.0 / COUNT(*) END AS "Конверсия в работе",
         CASE WHEN SUM(is_revenue) = 0 THEN 0 ELSE SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) * 1.0 / SUM(is_revenue) END AS "Средний_чек"
       FROM flags
       GROUP BY funnel_group, month, course_code
       ORDER BY funnel_group, month, course_code`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "bitrix_funnel_month_code_full.json", rowsToJson((bitrixFunnelMonthCode.results ?? []) as Record<string, unknown>[]));
  paths.push("bitrix_funnel_month_code_full.json");

  const managerBaseSql = hasRawP01
    ? `WITH base AS (
         SELECT
           COALESCE(trim(p."Ответственный"), '') AS manager,
           m.month,
           CASE strftime('%m', m.month || '-01')
             WHEN '01' THEN 'Январь, ' || strftime('%Y', m.month || '-01')
             WHEN '02' THEN 'Февраль, ' || strftime('%Y', m.month || '-01')
             WHEN '03' THEN 'Март, ' || strftime('%Y', m.month || '-01')
             WHEN '04' THEN 'Апрель, ' || strftime('%Y', m.month || '-01')
             WHEN '05' THEN 'Май, ' || strftime('%Y', m.month || '-01')
             WHEN '06' THEN 'Июнь, ' || strftime('%Y', m.month || '-01')
             WHEN '07' THEN 'Июль, ' || strftime('%Y', m.month || '-01')
             WHEN '08' THEN 'Август, ' || strftime('%Y', m.month || '-01')
             WHEN '09' THEN 'Сентябрь, ' || strftime('%Y', m.month || '-01')
             WHEN '10' THEN 'Октябрь, ' || strftime('%Y', m.month || '-01')
             WHEN '11' THEN 'Ноябрь, ' || strftime('%Y', m.month || '-01')
             WHEN '12' THEN 'Декабрь, ' || strftime('%Y', m.month || '-01')
             ELSE m.month
           END AS month_label,
           COALESCE(NULLIF(trim(m.course_code_norm), ''), '—') AS course_code,
           COALESCE(m."ID", '') AS deal_id,
           COALESCE(m.revenue_amount, 0) AS revenue_amount,
           ${managerLeadLogic.qual} AS is_qual,
           ${managerLeadLogic.unqual} AS is_unqual,
           ${managerLeadLogic.refusal} AS is_refusal,
           ${managerInvalidExpr} AS is_invalid,
           ${managerLeadLogic.inWork} AS is_in_work,
           CASE WHEN COALESCE(m.is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END AS is_revenue
         FROM mart_deals_enriched m
         LEFT JOIN raw_bitrix_deals_p01 p ON p."ID" = m."ID"
         WHERE COALESCE(m.month, '') <> ''
       )`
    : `WITH base AS (
         SELECT
           'Unassigned' AS manager,
           m.month,
           CASE strftime('%m', m.month || '-01')
             WHEN '01' THEN 'Январь, ' || strftime('%Y', m.month || '-01')
             WHEN '02' THEN 'Февраль, ' || strftime('%Y', m.month || '-01')
             WHEN '03' THEN 'Март, ' || strftime('%Y', m.month || '-01')
             WHEN '04' THEN 'Апрель, ' || strftime('%Y', m.month || '-01')
             WHEN '05' THEN 'Май, ' || strftime('%Y', m.month || '-01')
             WHEN '06' THEN 'Июнь, ' || strftime('%Y', m.month || '-01')
             WHEN '07' THEN 'Июль, ' || strftime('%Y', m.month || '-01')
             WHEN '08' THEN 'Август, ' || strftime('%Y', m.month || '-01')
             WHEN '09' THEN 'Сентябрь, ' || strftime('%Y', m.month || '-01')
             WHEN '10' THEN 'Октябрь, ' || strftime('%Y', m.month || '-01')
             WHEN '11' THEN 'Ноябрь, ' || strftime('%Y', m.month || '-01')
             WHEN '12' THEN 'Декабрь, ' || strftime('%Y', m.month || '-01')
             ELSE m.month
           END AS month_label,
           COALESCE(NULLIF(trim(m.course_code_norm), ''), '—') AS course_code,
           COALESCE(m."ID", '') AS deal_id,
           COALESCE(m.revenue_amount, 0) AS revenue_amount,
           0 AS is_qual,
           0 AS is_unqual,
           0 AS is_refusal,
           0 AS is_invalid,
           0 AS is_in_work,
           CASE WHEN COALESCE(m.is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END AS is_revenue
         FROM mart_deals_enriched m
         WHERE COALESCE(m.month, '') <> ''
       )`;

  // SQLite lower()/upper() do not reliably normalize Cyrillic in D1.
  // Use exact trimmed names to keep manager datasets populated.
  const firstlineList = Array.isArray(managerFirstlineNames) ? managerFirstlineNames.map((s) => String(s ?? "").trim()).filter(Boolean) : [];
  const firstlineFilter = firstlineList.length
    ? `trim(manager) IN (${firstlineList.map((n) => sqlQuote(n)).join(", ")})`
    : `0`;
  const salesFilter = `trim(manager) IN ('Анастасия Крисанова', 'Василий Гореленков', 'Глеб Барбазанов', 'Елена Лобода')`;

  const managerFirstlineByMonth = await db
    .prepare(
      `${managerBaseSql},
       filtered AS (
         SELECT * FROM base WHERE ${firstlineFilter}
       ),
       by_month AS (
         SELECT
           manager,
           month_label,
           COUNT(*) AS leads,
           SUM(is_qual) AS qual,
           SUM(is_unqual) AS unqual,
           SUM(is_refusal) AS refusal,
           SUM(is_in_work) AS in_work,
           SUM(is_invalid) AS invalid_leads,
           SUM(is_revenue) AS paid_deals,
           SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS revenue,
           SUBSTR(GROUP_CONCAT(deal_id), 1, 50000) AS fl_ids
         FROM filtered
         GROUP BY manager, month_label
       ),
       by_manager AS (
         SELECT
           manager,
           COUNT(*) AS leads,
           SUM(is_qual) AS qual,
           SUM(is_unqual) AS unqual,
           SUM(is_refusal) AS refusal,
           SUM(is_in_work) AS in_work,
           SUM(is_invalid) AS invalid_leads,
           SUM(is_revenue) AS paid_deals,
           SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS revenue,
           SUBSTR(GROUP_CONCAT(deal_id), 1, 50000) AS fl_ids
         FROM filtered
         GROUP BY manager
       )
       SELECT
         'Manager' AS "Level",
         manager AS "Менеджер",
         '-' AS "Месяц",
         leads AS "Лиды",
         qual AS "Квал",
         unqual AS "Неквал",
         refusal AS "Отказы",
         in_work AS "В работе",
         invalid_leads AS "Невалидные_лиды",
         paid_deals AS "Сделок_с_выручкой",
         revenue AS "Выручка",
         fl_ids AS "fl_IDs",
         CASE WHEN leads = 0 THEN 0 ELSE qual * 1.0 / leads END AS "Конверсия в Квал",
         CASE WHEN leads = 0 THEN 0 ELSE unqual * 1.0 / leads END AS "Конверсия в Неквал",
         CASE WHEN leads = 0 THEN 0 ELSE refusal * 1.0 / leads END AS "Конверсия в Отказ",
         CASE WHEN leads = 0 THEN 0 ELSE in_work * 1.0 / leads END AS "Конверсия в работе",
         CASE WHEN paid_deals = 0 THEN 0 ELSE revenue * 1.0 / paid_deals END AS "Средний_чек",
         '' AS _sort_month
       FROM by_manager
       UNION ALL
       SELECT
         'Месяц' AS "Level",
         manager AS "Менеджер",
         month_label AS "Месяц",
         leads AS "Лиды",
         qual AS "Квал",
         unqual AS "Неквал",
         refusal AS "Отказы",
         in_work AS "В работе",
         invalid_leads AS "Невалидные_лиды",
         paid_deals AS "Сделок_с_выручкой",
         revenue AS "Выручка",
         fl_ids AS "fl_IDs",
         CASE WHEN leads = 0 THEN 0 ELSE qual * 1.0 / leads END AS "Конверсия в Квал",
         CASE WHEN leads = 0 THEN 0 ELSE unqual * 1.0 / leads END AS "Конверсия в Неквал",
         CASE WHEN leads = 0 THEN 0 ELSE refusal * 1.0 / leads END AS "Конверсия в Отказ",
         CASE WHEN leads = 0 THEN 0 ELSE in_work * 1.0 / leads END AS "Конверсия в работе",
         CASE WHEN paid_deals = 0 THEN 0 ELSE revenue * 1.0 / paid_deals END AS "Средний_чек",
         month_label AS _sort_month
       FROM by_month
       ORDER BY "Менеджер", "Level" DESC, _sort_month`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "manager_firstline_by_month.json", rowsToJson((managerFirstlineByMonth.results ?? []) as Record<string, unknown>[]));
  paths.push("manager_firstline_by_month.json");

  const managerFirstlineByCourse = await db
    .prepare(
      `${managerBaseSql},
       filtered AS (
         SELECT * FROM base WHERE ${firstlineFilter}
       ),
       by_course AS (
         SELECT
           manager,
           course_code,
           COUNT(*) AS leads,
           SUM(is_qual) AS qual,
           SUM(is_unqual) AS unqual,
           SUM(is_refusal) AS refusal,
           SUM(is_in_work) AS in_work,
           SUM(is_invalid) AS invalid_leads,
           SUM(is_revenue) AS paid_deals,
           SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS revenue,
           SUBSTR(GROUP_CONCAT(deal_id), 1, 50000) AS fl_ids
         FROM filtered
         GROUP BY manager, course_code
       ),
       by_manager AS (
         SELECT
           manager,
           COUNT(*) AS leads,
           SUM(is_qual) AS qual,
           SUM(is_unqual) AS unqual,
           SUM(is_refusal) AS refusal,
           SUM(is_in_work) AS in_work,
           SUM(is_invalid) AS invalid_leads,
           SUM(is_revenue) AS paid_deals,
           SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS revenue,
           SUBSTR(GROUP_CONCAT(deal_id), 1, 50000) AS fl_ids
         FROM filtered
         GROUP BY manager
       )
       SELECT
         'Manager' AS "Level",
         manager AS "Менеджер",
         '-' AS "Код курса",
         leads AS "Лиды",
         qual AS "Квал",
         unqual AS "Неквал",
         refusal AS "Отказы",
         in_work AS "В работе",
         invalid_leads AS "Невалидные_лиды",
         paid_deals AS "Сделок_с_выручкой",
         revenue AS "Выручка",
         fl_ids AS "fl_IDs",
         CASE WHEN leads = 0 THEN 0 ELSE qual * 1.0 / leads END AS "Конверсия в Квал",
         CASE WHEN leads = 0 THEN 0 ELSE unqual * 1.0 / leads END AS "Конверсия в Неквал",
         CASE WHEN leads = 0 THEN 0 ELSE refusal * 1.0 / leads END AS "Конверсия в Отказ",
         CASE WHEN leads = 0 THEN 0 ELSE in_work * 1.0 / leads END AS "Конверсия в работе",
         CASE WHEN paid_deals = 0 THEN 0 ELSE revenue * 1.0 / paid_deals END AS "Средний_чек"
       FROM by_manager
       UNION ALL
       SELECT
         'Код курса' AS "Level",
         manager AS "Менеджер",
         course_code AS "Код курса",
         leads AS "Лиды",
         qual AS "Квал",
         unqual AS "Неквал",
         refusal AS "Отказы",
         in_work AS "В работе",
         invalid_leads AS "Невалидные_лиды",
         paid_deals AS "Сделок_с_выручкой",
         revenue AS "Выручка",
         fl_ids AS "fl_IDs",
         CASE WHEN leads = 0 THEN 0 ELSE qual * 1.0 / leads END AS "Конверсия в Квал",
         CASE WHEN leads = 0 THEN 0 ELSE unqual * 1.0 / leads END AS "Конверсия в Неквал",
         CASE WHEN leads = 0 THEN 0 ELSE refusal * 1.0 / leads END AS "Конверсия в Отказ",
         CASE WHEN leads = 0 THEN 0 ELSE in_work * 1.0 / leads END AS "Конверсия в работе",
         CASE WHEN paid_deals = 0 THEN 0 ELSE revenue * 1.0 / paid_deals END AS "Средний_чек"
       FROM by_course
       ORDER BY "Менеджер", "Level" DESC, "Код курса"`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "manager_firstline_by_course.json", rowsToJson((managerFirstlineByCourse.results ?? []) as Record<string, unknown>[]));
  paths.push("manager_firstline_by_course.json");

  const managerFirstlineByCourseMonth = await db
    .prepare(
      `${managerBaseSql},
       filtered AS (
         SELECT * FROM base WHERE ${firstlineFilter}
       )
       SELECT
         'Код курса' AS "Level",
         manager AS "Менеджер",
         month_label AS "Месяц",
         course_code AS "Код курса",
         COUNT(*) AS "Лиды",
         SUM(is_qual) AS "Квал",
         SUM(is_unqual) AS "Неквал",
         SUM(is_refusal) AS "Отказы",
         SUM(is_in_work) AS "В работе",
         SUM(is_invalid) AS "Невалидные_лиды",
         SUM(is_revenue) AS "Сделок_с_выручкой",
         SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS "Выручка",
         SUBSTR(GROUP_CONCAT(deal_id), 1, 50000) AS "fl_IDs",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_qual) * 1.0 / COUNT(*) END AS "Конверсия в Квал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_unqual) * 1.0 / COUNT(*) END AS "Конверсия в Неквал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_refusal) * 1.0 / COUNT(*) END AS "Конверсия в Отказ",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_in_work) * 1.0 / COUNT(*) END AS "Конверсия в работе",
         CASE WHEN SUM(is_revenue) = 0 THEN 0 ELSE SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) * 1.0 / SUM(is_revenue) END AS "Средний_чек"
       FROM filtered
       GROUP BY manager, month_label, course_code
       ORDER BY manager, month_label DESC, course_code`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "manager_firstline_by_course_month.json", rowsToJson((managerFirstlineByCourseMonth.results ?? []) as Record<string, unknown>[]));
  paths.push("manager_firstline_by_course_month.json");

  const managerSalesByMonth = await db
    .prepare(
      `${managerBaseSql},
       filtered AS (
         SELECT * FROM base WHERE ${salesFilter}
       ),
       by_month AS (
         SELECT
           manager,
           month_label,
           COUNT(*) AS leads,
           SUM(is_qual) AS qual,
           SUM(is_unqual) AS unqual,
           SUM(is_refusal) AS refusal,
           SUM(is_in_work) AS in_work,
           SUM(is_invalid) AS invalid_leads,
           SUM(is_revenue) AS paid_deals,
           SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS revenue,
           SUBSTR(GROUP_CONCAT(deal_id), 1, 50000) AS fl_ids
         FROM filtered
         GROUP BY manager, month_label
       ),
       by_manager AS (
         SELECT
           manager,
           COUNT(*) AS leads,
           SUM(is_qual) AS qual,
           SUM(is_unqual) AS unqual,
           SUM(is_refusal) AS refusal,
           SUM(is_in_work) AS in_work,
           SUM(is_invalid) AS invalid_leads,
           SUM(is_revenue) AS paid_deals,
           SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS revenue,
           SUBSTR(GROUP_CONCAT(deal_id), 1, 50000) AS fl_ids
         FROM filtered
         GROUP BY manager
       )
       SELECT
         'Manager' AS "Level",
         manager AS "Менеджер",
         '-' AS "Месяц",
         leads AS "Лиды",
         qual AS "Квал",
         unqual AS "Неквал",
         refusal AS "Отказы",
         in_work AS "В работе",
         invalid_leads AS "Невалидные_лиды",
         paid_deals AS "Сделок_с_выручкой",
         revenue AS "Выручка",
         fl_ids AS "fl_IDs",
         CASE WHEN leads = 0 THEN 0 ELSE qual * 1.0 / leads END AS "Конверсия в Квал",
         CASE WHEN leads = 0 THEN 0 ELSE unqual * 1.0 / leads END AS "Конверсия в Неквал",
         CASE WHEN leads = 0 THEN 0 ELSE refusal * 1.0 / leads END AS "Конверсия в Отказ",
         CASE WHEN leads = 0 THEN 0 ELSE in_work * 1.0 / leads END AS "Конверсия в работе",
         CASE WHEN paid_deals = 0 THEN 0 ELSE revenue * 1.0 / paid_deals END AS "Средний_чек",
         '' AS _sort_month
       FROM by_manager
       UNION ALL
       SELECT
         'Месяц' AS "Level",
         manager AS "Менеджер",
         month_label AS "Месяц",
         leads AS "Лиды",
         qual AS "Квал",
         unqual AS "Неквал",
         refusal AS "Отказы",
         in_work AS "В работе",
         invalid_leads AS "Невалидные_лиды",
         paid_deals AS "Сделок_с_выручкой",
         revenue AS "Выручка",
         fl_ids AS "fl_IDs",
         CASE WHEN leads = 0 THEN 0 ELSE qual * 1.0 / leads END AS "Конверсия в Квал",
         CASE WHEN leads = 0 THEN 0 ELSE unqual * 1.0 / leads END AS "Конверсия в Неквал",
         CASE WHEN leads = 0 THEN 0 ELSE refusal * 1.0 / leads END AS "Конверсия в Отказ",
         CASE WHEN leads = 0 THEN 0 ELSE in_work * 1.0 / leads END AS "Конверсия в работе",
         CASE WHEN paid_deals = 0 THEN 0 ELSE revenue * 1.0 / paid_deals END AS "Средний_чек",
         month_label AS _sort_month
       FROM by_month
       ORDER BY "Менеджер", "Level" DESC, _sort_month`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "manager_sales_by_month.json", rowsToJson((managerSalesByMonth.results ?? []) as Record<string, unknown>[]));
  paths.push("manager_sales_by_month.json");

  const managerSalesByCourse = await db
    .prepare(
      `${managerBaseSql},
       filtered AS (
         SELECT * FROM base WHERE ${salesFilter}
       ),
       by_course AS (
         SELECT
           manager,
           course_code,
           COUNT(*) AS leads,
           SUM(is_qual) AS qual,
           SUM(is_unqual) AS unqual,
           SUM(is_refusal) AS refusal,
           SUM(is_in_work) AS in_work,
           SUM(is_invalid) AS invalid_leads,
           SUM(is_revenue) AS paid_deals,
           SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS revenue,
           SUBSTR(GROUP_CONCAT(deal_id), 1, 50000) AS fl_ids
         FROM filtered
         GROUP BY manager, course_code
       ),
       by_manager AS (
         SELECT
           manager,
           COUNT(*) AS leads,
           SUM(is_qual) AS qual,
           SUM(is_unqual) AS unqual,
           SUM(is_refusal) AS refusal,
           SUM(is_in_work) AS in_work,
           SUM(is_invalid) AS invalid_leads,
           SUM(is_revenue) AS paid_deals,
           SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS revenue,
           SUBSTR(GROUP_CONCAT(deal_id), 1, 50000) AS fl_ids
         FROM filtered
         GROUP BY manager
       )
       SELECT
         'Manager' AS "Level",
         manager AS "Менеджер",
         '-' AS "Код курса",
         leads AS "Лиды",
         qual AS "Квал",
         unqual AS "Неквал",
         refusal AS "Отказы",
         in_work AS "В работе",
         invalid_leads AS "Невалидные_лиды",
         paid_deals AS "Сделок_с_выручкой",
         revenue AS "Выручка",
         fl_ids AS "fl_IDs",
         CASE WHEN leads = 0 THEN 0 ELSE qual * 1.0 / leads END AS "Конверсия в Квал",
         CASE WHEN leads = 0 THEN 0 ELSE unqual * 1.0 / leads END AS "Конверсия в Неквал",
         CASE WHEN leads = 0 THEN 0 ELSE refusal * 1.0 / leads END AS "Конверсия в Отказ",
         CASE WHEN leads = 0 THEN 0 ELSE in_work * 1.0 / leads END AS "Конверсия в работе",
         CASE WHEN paid_deals = 0 THEN 0 ELSE revenue * 1.0 / paid_deals END AS "Средний_чек"
       FROM by_manager
       UNION ALL
       SELECT
         'Код курса' AS "Level",
         manager AS "Менеджер",
         course_code AS "Код курса",
         leads AS "Лиды",
         qual AS "Квал",
         unqual AS "Неквал",
         refusal AS "Отказы",
         in_work AS "В работе",
         invalid_leads AS "Невалидные_лиды",
         paid_deals AS "Сделок_с_выручкой",
         revenue AS "Выручка",
         fl_ids AS "fl_IDs",
         CASE WHEN leads = 0 THEN 0 ELSE qual * 1.0 / leads END AS "Конверсия в Квал",
         CASE WHEN leads = 0 THEN 0 ELSE unqual * 1.0 / leads END AS "Конверсия в Неквал",
         CASE WHEN leads = 0 THEN 0 ELSE refusal * 1.0 / leads END AS "Конверсия в Отказ",
         CASE WHEN leads = 0 THEN 0 ELSE in_work * 1.0 / leads END AS "Конверсия в работе",
         CASE WHEN paid_deals = 0 THEN 0 ELSE revenue * 1.0 / paid_deals END AS "Средний_чек"
       FROM by_course
       ORDER BY "Менеджер", "Level" DESC, "Код курса"`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "manager_sales_by_course.json", rowsToJson((managerSalesByCourse.results ?? []) as Record<string, unknown>[]));
  paths.push("manager_sales_by_course.json");

  const managerSalesByCourseMonth = await db
    .prepare(
      `${managerBaseSql},
       filtered AS (
         SELECT * FROM base WHERE ${salesFilter}
       )
       SELECT
         'Код курса' AS "Level",
         manager AS "Менеджер",
         month_label AS "Месяц",
         course_code AS "Код курса",
         COUNT(*) AS "Лиды",
         SUM(is_qual) AS "Квал",
         SUM(is_unqual) AS "Неквал",
         SUM(is_refusal) AS "Отказы",
         SUM(is_in_work) AS "В работе",
         SUM(is_invalid) AS "Невалидные_лиды",
         SUM(is_revenue) AS "Сделок_с_выручкой",
         SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS "Выручка",
         SUBSTR(GROUP_CONCAT(deal_id), 1, 50000) AS "fl_IDs",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_qual) * 1.0 / COUNT(*) END AS "Конверсия в Квал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_unqual) * 1.0 / COUNT(*) END AS "Конверсия в Неквал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_refusal) * 1.0 / COUNT(*) END AS "Конверсия в Отказ",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_in_work) * 1.0 / COUNT(*) END AS "Конверсия в работе",
         CASE WHEN SUM(is_revenue) = 0 THEN 0 ELSE SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) * 1.0 / SUM(is_revenue) END AS "Средний_чек"
       FROM filtered
       GROUP BY manager, month_label, course_code
       ORDER BY manager, month_label DESC, course_code`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "manager_sales_by_course_month.json", rowsToJson((managerSalesByCourseMonth.results ?? []) as Record<string, unknown>[]));
  paths.push("manager_sales_by_course_month.json");

  const yandexMonthKpis = await db
    .prepare(
      `WITH ystats AS (
         SELECT
           month,
           SUM(COALESCE("Клики", 0)) AS clicks,
           SUM(COALESCE("Расход, ₽", 0)) AS spend
         FROM stg_yandex_stats
         WHERE COALESCE(month, '') <> ''
         GROUP BY month
       ),
       mflags AS (
         SELECT
           COALESCE(yandex_month, '') AS month,
           ${yandexLeadLogic.qual} AS qual,
           ${yandexLeadLogic.unqual} AS unqual,
           ${yandexLeadLogic.refusal} AS refusal,
           1 AS leads
         FROM mart_yandex_leads_raw
         WHERE COALESCE(yandex_month, '') <> ''
       ),
       leads AS (
         SELECT month,
                SUM(leads) AS leads,
                SUM(qual) AS qual,
                SUM(unqual) AS unqual,
                SUM(refusal) AS refusal
         FROM mflags
         GROUP BY month
       ),
       months AS (
         SELECT month FROM leads
         UNION
         SELECT month FROM ystats
       )
       SELECT
         m.month AS month,
         COALESCE(l.leads, 0) AS "Leads",
         COALESCE(l.qual, 0) AS "Qual",
         COALESCE(l.unqual, 0) AS "Unqual",
         COALESCE(l.refusal, 0) AS "Refusal",
         COALESCE(y.clicks, 0) AS "Клики",
         COALESCE(y.spend, 0) AS "Расход, ₽"
       FROM months m
       LEFT JOIN leads l ON l.month = m.month
       LEFT JOIN ystats y ON y.month = m.month
       ORDER BY m.month`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "global/yandex_month_kpis.json", rowsToJson((yandexMonthKpis.results ?? []) as Record<string, unknown>[]));
  paths.push("global/yandex_month_kpis.json");

  const yandexCampaignKpis = await db
    .prepare(
      `WITH ystats AS (
         SELECT
           COALESCE("Название кампании", '') AS campaign_name,
           COALESCE("№ Кампании", '') AS campaign_id,
           month,
           SUM(COALESCE("Клики", 0)) AS clicks,
           SUM(COALESCE("Расход, ₽", 0)) AS spend
         FROM stg_yandex_stats
         WHERE COALESCE(month, '') <> ''
         GROUP BY campaign_name, campaign_id, month
       ),
       lflags AS (
         SELECT
           COALESCE(project_name, '') AS campaign_name,
           COALESCE(campaign_id, '') AS campaign_id,
           COALESCE(yandex_month, '') AS month,
           ${yandexLeadLogic.qual} AS qual,
           ${yandexLeadLogic.unqual} AS unqual,
           ${yandexLeadLogic.refusal} AS refusal,
           1 AS leads
         FROM mart_yandex_leads_raw
         WHERE COALESCE(yandex_month, '') <> ''
       ),
       leads AS (
         SELECT campaign_name, campaign_id, month,
                SUM(leads) AS leads,
                SUM(qual) AS qual,
                SUM(unqual) AS unqual,
                SUM(refusal) AS refusal
         FROM lflags
         GROUP BY campaign_name, campaign_id, month
       ),
       dims AS (
         SELECT campaign_name, campaign_id, month FROM ystats
         UNION
         SELECT campaign_name, campaign_id, month FROM leads
       )
       SELECT
         d.campaign_name AS "Название кампании",
         d.campaign_id AS "№ Кампании",
         d.month AS "Месяц",
         COALESCE(l.leads, 0) AS "Leads",
         COALESCE(l.qual, 0) AS "Qual",
         COALESCE(l.unqual, 0) AS "Unqual",
         COALESCE(l.refusal, 0) AS "Refusal",
         COALESCE(y.clicks, 0) AS "Клики",
         COALESCE(y.spend, 0) AS "Расход, ₽"
       FROM dims d
       LEFT JOIN leads l
         ON l.campaign_name = d.campaign_name AND l.campaign_id = d.campaign_id AND l.month = d.month
       LEFT JOIN ystats y
         ON y.campaign_name = d.campaign_name AND y.campaign_id = d.campaign_id AND y.month = d.month
       ORDER BY d.month, d.campaign_name`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "global/yandex_campaign_kpis.json", rowsToJson((yandexCampaignKpis.results ?? []) as Record<string, unknown>[]));
  paths.push("global/yandex_campaign_kpis.json");

  const ydHierarchyRows = await buildYdHierarchyRows(db);

  await upsertDataset(db, "yd_hierarchy.json", rowsToJson(ydHierarchyRows as Record<string, unknown>[]));
  paths.push("yd_hierarchy.json");

  const q4 = await db
    .prepare(
      `SELECT COALESCE(funnel_group, '') AS funnel,
              COALESCE("Стадия сделки", '') AS stage,
              COUNT(DISTINCT ID) AS deals,
              SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
              SUM(revenue_amount) AS revenue
       FROM mart_deals_enriched
       GROUP BY COALESCE(funnel_group, ''), COALESCE("Стадия сделки", '')
       ORDER BY revenue DESC`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "global/funnel_stage.json", rowsToJson((q4.results ?? []) as Record<string, unknown>[]));
  paths.push("global/funnel_stage.json");

  const q5 = await db
    .prepare(
      `SELECT COALESCE(event_class, 'Другое') AS event_class,
              COALESCE(NULLIF(course_code_norm, ''), 'Другое') AS course_code_norm,
              COUNT(DISTINCT ID) AS deals,
              SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
              SUM(revenue_amount) AS revenue
       FROM mart_deals_enriched
       GROUP BY COALESCE(event_class, 'Другое'), COALESCE(NULLIF(course_code_norm, ''), 'Другое')
       ORDER BY revenue DESC`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "global/event_course.json", rowsToJson((q5.results ?? []) as Record<string, unknown>[]));
  paths.push("global/event_course.json");

  const q6 = await db
    .prepare(
      `SELECT "Контакт: ID" AS contact_id,
              COUNT(DISTINCT ID) AS deals_total,
              SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
              SUM(revenue_amount) AS revenue,
              CASE WHEN SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) = 0 THEN 0
                   ELSE SUM(revenue_amount) * 1.0 / SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END)
              END AS avg_check
       FROM mart_attacking_january_cohort_deals
       WHERE COALESCE("Контакт: ID", '') <> ''
       GROUP BY "Контакт: ID"
       ORDER BY revenue DESC`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(
    db,
    "cohorts/attacking_january/cohort_assoc_contacts.json",
    rowsToJson((q6.results ?? []) as Record<string, unknown>[]),
  );
  paths.push("cohorts/attacking_january/cohort_assoc_contacts.json");

  const q7 = await db
    .prepare(
      `SELECT COALESCE(event_class, 'Другое') AS event_class,
              COALESCE(NULLIF(course_code_norm, ''), 'Другое') AS course_code_norm,
              COUNT(DISTINCT ID) AS deals,
              SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
              SUM(revenue_amount) AS revenue
       FROM mart_attacking_january_cohort_deals
       GROUP BY COALESCE(event_class, 'Другое'), COALESCE(NULLIF(course_code_norm, ''), 'Другое')
       ORDER BY revenue DESC`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(
    db,
    "cohorts/attacking_january/cohort_assoc_event_course.json",
    rowsToJson((q7.results ?? []) as Record<string, unknown>[]),
  );
  paths.push("cohorts/attacking_january/cohort_assoc_event_course.json");

  const q8 = await db
    .prepare(
      `SELECT (SELECT COUNT(*) FROM mart_yandex_leads_raw) AS leads_raw,
              (SELECT COUNT(*) FROM mart_yandex_leads_dedup) AS leads_dedup,
              (SELECT COALESCE(SUM(is_paid_deal),0) FROM mart_yandex_leads_raw) AS paid_deals_raw,
              (SELECT COALESCE(SUM(paid_deals),0) FROM mart_yandex_leads_dedup) AS paid_deals_dedup,
              (SELECT COALESCE(SUM(revenue_amount),0) FROM mart_yandex_leads_raw) AS revenue_raw,
              (SELECT COALESCE(SUM(revenue),0) FROM mart_yandex_leads_dedup) AS revenue_dedup`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "global/yandex_dedup_summary.json", rowsToJson((q8.results ?? []) as Record<string, unknown>[]));
  paths.push("global/yandex_dedup_summary.json");

  const q9 = await db
    .prepare(
      `SELECT r.project_name,
              r.yandex_month AS month,
              r.leads_raw,
              d.leads_dedup,
              r.paid_deals_raw,
              d.paid_deals_dedup,
              r.revenue_raw,
              d.revenue_dedup,
              r.spend
       FROM mart_yandex_revenue_projects_raw r
       LEFT JOIN mart_yandex_revenue_projects_dedup d
         ON r.project_name = d.project_name AND r.yandex_month = d.yandex_month
       ORDER BY r.revenue_raw DESC`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(
    db,
    "global/yandex_projects_revenue_raw_vs_dedup.json",
    rowsToJson((q9.results ?? []) as Record<string, unknown>[]),
  );
  paths.push("global/yandex_projects_revenue_raw_vs_dedup.json");

  const q10 = await db
    .prepare(
      `WITH matched AS (
         SELECT yandex_month AS month,
                SUM(leads_raw) AS leads_raw,
                SUM(paid_deals_raw) AS paid_deals_raw,
                SUM(revenue_raw) AS revenue_raw
         FROM mart_yandex_revenue_projects_raw
         GROUP BY yandex_month
       ),
       ystats AS (
         SELECT month,
                SUM(COALESCE("Клики", 0)) AS clicks,
                SUM(COALESCE("Расход, ₽", 0)) AS spend
         FROM stg_yandex_stats
         WHERE COALESCE(month, '') <> ''
         GROUP BY month
       ),
       all_months AS (
         SELECT month FROM matched
         UNION
         SELECT month FROM ystats
       )
       SELECT m.month AS month,
              COALESCE(ma.leads_raw, 0) AS leads_raw,
              COALESCE(ma.paid_deals_raw, 0) AS paid_deals_raw,
              COALESCE(ma.revenue_raw, 0) AS revenue_raw,
              COALESCE(ys.clicks, 0) AS clicks,
              COALESCE(ys.spend, 0) AS spend
       FROM all_months m
       LEFT JOIN matched ma ON ma.month = m.month
       LEFT JOIN ystats ys ON ys.month = m.month
       ORDER BY m.month`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(
    db,
    "global/yandex_projects_revenue_by_month.json",
    rowsToJson((q10.results ?? []) as Record<string, unknown>[]),
  );
  paths.push("global/yandex_projects_revenue_by_month.json");

  const q11 = await db
    .prepare(
      `SELECT project_name,
              SUM(leads_raw) AS leads_raw,
              SUM(paid_deals_raw) AS payments_count,
              SUM(paid_deals_raw) AS paid_deals_raw,
              SUM(revenue_raw) AS revenue_raw,
              SUM(spend) AS spend
       FROM mart_yandex_revenue_projects_raw
       GROUP BY project_name
       ORDER BY revenue_raw DESC`,
    )
    .all<Record<string, unknown>>();

  // Build per-project associated revenue QA stats using the same logic as /api/assoc-revenue:
  // derive the contact pool directly from mart_deals_enriched (Yandex-sourced deals) joined with
  // stg_yandex_stats for project names, then find all variant3 paid deals for those contacts.
  // This avoids the contact_id normalization mismatch that occurred when going through
  // mart_yandex_leads_raw (where IDs were stored via idNorm, stripping ".0" suffixes, so the
  // join back to mart_deals_enriched."Контакт: ID" returned zero rows).
  //
  // If stg_contacts_uid exists (maps contact_id → contact_uid one row per ID, built by matching
  // emails/phones across Bitrix contacts before being pushed to D1), use it for proper dedup so
  // that the same real person with multiple Bitrix contact_ids is counted once and all their
  // deals are found.
  const hasContactsUid = await tableExists(db, "stg_contacts_uid");
  const sourceYandexAdExpr = sqlExtractYandexAdId(`src."UTM Content"`);
  const validSourceYandexAdExpr = `LENGTH(${sourceYandexAdExpr}) = 11 AND SUBSTR(${sourceYandexAdExpr}, 1, 2) = '17' AND ${sourceYandexAdExpr} NOT GLOB '*[^0-9]*'`;
  const groupedStatsProjectExpr = buildYandexProjectGroupSqlExpr(`"Название кампании"`);
  // ym.project_name is already a mapped group name (produced by groupedStatsProjectExpr in yandex_map).
  // Re-applying the alias CASE would fail to match group names and return UNMAPPED everywhere.
  // Just pass through the already-mapped value, falling back to UNMAPPED for NULL/empty only.
  const groupedMappedProjectExpr = `COALESCE(NULLIF(TRIM(COALESCE(ym.project_name, '')), ''), 'UNMAPPED')`;

  const assocQaSql = hasContactsUid
    ? `WITH
       -- Build ad_id → project_name lookup from Yandex stats (same as assoc-revenue.ts).
       yandex_map AS (
         SELECT
           REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') AS ad_id,
           MIN(${groupedStatsProjectExpr}) AS project_name
         FROM stg_yandex_stats
         WHERE REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') <> ''
         GROUP BY 1
       ),
       -- All Yandex-sourced deals with their mapped project name.
       -- contact_id is normalized (strip trailing ".0") so that IDs stored as floats
       -- (e.g. "12345.0" from Excel/CSV import) match integer-formatted IDs in paid deals.
       yandex_source_deals AS (
         SELECT
           REPLACE(TRIM(COALESCE(src."Контакт: ID", '')), '.0', '') AS contact_id,
           COALESCE(${groupedMappedProjectExpr}, 'UNMAPPED') AS project_name
         FROM mart_deals_enriched src
         LEFT JOIN yandex_map ym
           ON ym.ad_id = ${sourceYandexAdExpr}
         WHERE LOWER(TRIM(COALESCE(src."UTM Source", ''))) LIKE 'y%'
           AND LOWER(TRIM(COALESCE(src."UTM Source", ''))) <> 'yah'
       ),
       yandex_leads AS (
         SELECT project_name, COUNT(*) AS yandex_leads_count
         FROM yandex_source_deals
         GROUP BY project_name
       ),
       -- Step 1: For each Yandex deal's contact_id, find the canonical contact_uid.
       --   Uses a LEFT JOIN so that contacts not in stg_contacts_uid still appear
       --   (they fall back to using their contact_id directly as the uid).
       campaign_uid_pool AS (
         SELECT DISTINCT
           ysd.project_name,
           COALESCE(cu.contact_uid, ysd.contact_id) AS contact_uid
         FROM yandex_source_deals ysd
         LEFT JOIN stg_contacts_uid cu ON cu.contact_id = ysd.contact_id
         WHERE ysd.contact_id <> ''
       ),
       -- Step 2: Expand each contact_uid to ALL of its associated Bitrix contact_ids.
       --   For UIDs found in stg_contacts_uid: cu2 returns every linked contact_id.
       --   For IDs used as fallback UIDs: cu2 returns NULL, so we keep the original
       --   contact_id (stored in contact_uid column) via COALESCE.
       --   Normalize the resulting contact_id to handle float-formatted IDs.
       all_pool_contact_ids AS (
         SELECT DISTINCT
           cup.project_name,
           REPLACE(TRIM(COALESCE(cu2.contact_id, cup.contact_uid)), '.0', '') AS contact_id
         FROM campaign_uid_pool cup
         LEFT JOIN stg_contacts_uid cu2 ON cu2.contact_uid = cup.contact_uid
         WHERE REPLACE(TRIM(COALESCE(cu2.contact_id, cup.contact_uid)), '.0', '') <> ''
       ),
       -- Step 3: All variant3 Bitrix deals for the full contact pool.
       -- Normalize mart_deals_enriched contact IDs on the join to handle float formats.
       contact_deals AS (
         SELECT apc.project_name, d.ID AS deal_id, d.revenue_amount
         FROM all_pool_contact_ids apc
         JOIN mart_deals_enriched d
           ON REPLACE(TRIM(COALESCE(d."Контакт: ID", '')), '.0', '') = apc.contact_id
         WHERE d.is_revenue_variant3 = 1
       ),
       -- Pre-aggregate revenue per project to avoid Cartesian-product overcounting
       -- when joining campaign_uid_pool (many contacts) with contact_deals (many deals).
       project_assoc_revenue AS (
         SELECT
           project_name,
           COUNT(DISTINCT deal_id) AS deal_count,
           COALESCE(SUM(revenue_amount), 0) AS assoc_revenue
         FROM contact_deals
         GROUP BY project_name
       )
       SELECT
         yl.project_name,
         MAX(yl.yandex_leads_count) AS "Лиды_Yandex",
         COUNT(DISTINCT cup.contact_uid) AS "Контактов_в_пуле",
         COALESCE(MAX(par.deal_count), 0) AS "Сделок_Bitrix",
         COALESCE(MAX(par.assoc_revenue), 0) AS assoc_revenue
       FROM yandex_leads yl
       LEFT JOIN campaign_uid_pool cup ON cup.project_name = yl.project_name
       LEFT JOIN project_assoc_revenue par ON par.project_name = yl.project_name
       GROUP BY yl.project_name
       ORDER BY assoc_revenue DESC`
    : `WITH
       -- Build ad_id → project_name lookup from Yandex stats (same as assoc-revenue.ts).
       yandex_map AS (
         SELECT
           REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') AS ad_id,
           MIN(${groupedStatsProjectExpr}) AS project_name
         FROM stg_yandex_stats
         WHERE REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') <> ''
         GROUP BY 1
       ),
       -- All Yandex-sourced deals with their mapped project name.
       -- contact_id is normalized (strip trailing ".0") so that IDs stored as floats
       -- (e.g. "12345.0" from Excel/CSV import) match integer-formatted IDs in paid deals.
       yandex_source_deals AS (
         SELECT
           REPLACE(TRIM(COALESCE(src."Контакт: ID", '')), '.0', '') AS contact_id,
           COALESCE(${groupedMappedProjectExpr}, 'UNMAPPED') AS project_name
         FROM mart_deals_enriched src
         LEFT JOIN yandex_map ym
           ON ym.ad_id = ${sourceYandexAdExpr}
         WHERE LOWER(TRIM(COALESCE(src."UTM Source", ''))) LIKE 'y%'
           AND LOWER(TRIM(COALESCE(src."UTM Source", ''))) <> 'yah'
       ),
       yandex_leads AS (
         SELECT project_name, COUNT(*) AS yandex_leads_count
         FROM yandex_source_deals
         GROUP BY project_name
       ),
       -- Step 1: Unique contact_ids per project from Yandex-sourced deals.
       campaign_contacts AS (
         SELECT DISTINCT project_name, contact_id
         FROM yandex_source_deals
         WHERE contact_id <> ''
       ),
       -- Step 2: All variant3 Bitrix deals for those contacts (entire Bitrix history).
       -- Normalize mart_deals_enriched contact IDs on the join to handle float formats.
       contact_deals AS (
         SELECT cc.project_name, d.ID AS deal_id, d.revenue_amount
         FROM campaign_contacts cc
         JOIN mart_deals_enriched d
           ON REPLACE(TRIM(COALESCE(d."Контакт: ID", '')), '.0', '') = cc.contact_id
         WHERE d.is_revenue_variant3 = 1
       ),
       -- Pre-aggregate revenue per project to avoid Cartesian-product overcounting
       -- when joining campaign_contacts (many contacts) with contact_deals (many deals).
       project_assoc_revenue AS (
         SELECT
           project_name,
           COUNT(DISTINCT deal_id) AS deal_count,
           COALESCE(SUM(revenue_amount), 0) AS assoc_revenue
         FROM contact_deals
         GROUP BY project_name
       )
       SELECT
         yl.project_name,
         MAX(yl.yandex_leads_count) AS "Лиды_Yandex",
         COUNT(DISTINCT cc.contact_id) AS "Контактов_в_пуле",
         COALESCE(MAX(par.deal_count), 0) AS "Сделок_Bitrix",
         COALESCE(MAX(par.assoc_revenue), 0) AS assoc_revenue
       FROM yandex_leads yl
       LEFT JOIN campaign_contacts cc ON cc.project_name = yl.project_name
       LEFT JOIN project_assoc_revenue par ON par.project_name = yl.project_name
       GROUP BY yl.project_name
       ORDER BY assoc_revenue DESC`;

  const assocQaByAdSql = hasContactsUid
    ? `WITH
       yandex_map AS (
         SELECT
           REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') AS ad_id,
           MIN(${groupedStatsProjectExpr}) AS project_name
         FROM stg_yandex_stats
         WHERE REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') <> ''
         GROUP BY 1
       ),
       yandex_source_deals AS (
         SELECT
           REPLACE(TRIM(COALESCE(src."Контакт: ID", '')), '.0', '') AS contact_id,
           ${sourceYandexAdExpr} AS ad_id,
           COALESCE(${groupedMappedProjectExpr}, 'UNMAPPED') AS project_name
         FROM mart_deals_enriched src
         LEFT JOIN yandex_map ym
           ON ym.ad_id = ${sourceYandexAdExpr}
         WHERE LOWER(TRIM(COALESCE(src."UTM Source", ''))) LIKE 'y%'
           AND LOWER(TRIM(COALESCE(src."UTM Source", ''))) <> 'yah'
           AND ${validSourceYandexAdExpr}
       ),
       yandex_leads AS (
         SELECT project_name, ad_id, COUNT(*) AS yandex_leads_count
         FROM yandex_source_deals
         GROUP BY project_name, ad_id
       ),
       campaign_uid_pool AS (
         SELECT DISTINCT
           ysd.project_name,
           ysd.ad_id,
           COALESCE(cu.contact_uid, ysd.contact_id) AS contact_uid
         FROM yandex_source_deals ysd
         LEFT JOIN stg_contacts_uid cu ON cu.contact_id = ysd.contact_id
         WHERE ysd.contact_id <> ''
       ),
       all_pool_contact_ids AS (
         SELECT DISTINCT
           cup.project_name,
           cup.ad_id,
           REPLACE(TRIM(COALESCE(cu2.contact_id, cup.contact_uid)), '.0', '') AS contact_id
         FROM campaign_uid_pool cup
         LEFT JOIN stg_contacts_uid cu2 ON cu2.contact_uid = cup.contact_uid
         WHERE REPLACE(TRIM(COALESCE(cu2.contact_id, cup.contact_uid)), '.0', '') <> ''
       ),
       contact_deals AS (
         SELECT apc.project_name, apc.ad_id, d.ID AS deal_id, d.revenue_amount
         FROM all_pool_contact_ids apc
         JOIN mart_deals_enriched d
           ON REPLACE(TRIM(COALESCE(d."Контакт: ID", '')), '.0', '') = apc.contact_id
         WHERE d.is_revenue_variant3 = 1
       ),
       ad_assoc_revenue AS (
         SELECT
           project_name,
           ad_id,
           COUNT(DISTINCT deal_id) AS deal_count,
           COALESCE(SUM(revenue_amount), 0) AS assoc_revenue
         FROM contact_deals
         GROUP BY project_name, ad_id
       )
       SELECT
         yl.project_name,
         yl.ad_id,
         MAX(yl.yandex_leads_count) AS "Лиды_Yandex",
         COUNT(DISTINCT cup.contact_uid) AS "Контактов_в_пуле",
         COALESCE(MAX(aar.deal_count), 0) AS "Сделок_Bitrix",
         COALESCE(MAX(aar.assoc_revenue), 0) AS assoc_revenue
       FROM yandex_leads yl
       LEFT JOIN campaign_uid_pool cup
         ON cup.project_name = yl.project_name AND cup.ad_id = yl.ad_id
       LEFT JOIN ad_assoc_revenue aar
         ON aar.project_name = yl.project_name AND aar.ad_id = yl.ad_id
       GROUP BY yl.project_name, yl.ad_id
       ORDER BY assoc_revenue DESC`
    : `WITH
       yandex_map AS (
         SELECT
           REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') AS ad_id,
           MIN(${groupedStatsProjectExpr}) AS project_name
         FROM stg_yandex_stats
         WHERE REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') <> ''
         GROUP BY 1
       ),
       yandex_source_deals AS (
         SELECT
           REPLACE(TRIM(COALESCE(src."Контакт: ID", '')), '.0', '') AS contact_id,
           ${sourceYandexAdExpr} AS ad_id,
           COALESCE(${groupedMappedProjectExpr}, 'UNMAPPED') AS project_name
         FROM mart_deals_enriched src
         LEFT JOIN yandex_map ym
           ON ym.ad_id = ${sourceYandexAdExpr}
         WHERE LOWER(TRIM(COALESCE(src."UTM Source", ''))) LIKE 'y%'
           AND LOWER(TRIM(COALESCE(src."UTM Source", ''))) <> 'yah'
           AND ${validSourceYandexAdExpr}
       ),
       yandex_leads AS (
         SELECT project_name, ad_id, COUNT(*) AS yandex_leads_count
         FROM yandex_source_deals
         GROUP BY project_name, ad_id
       ),
       campaign_contacts AS (
         SELECT DISTINCT project_name, ad_id, contact_id
         FROM yandex_source_deals
         WHERE contact_id <> ''
       ),
       contact_deals AS (
         SELECT cc.project_name, cc.ad_id, d.ID AS deal_id, d.revenue_amount
         FROM campaign_contacts cc
         JOIN mart_deals_enriched d
           ON REPLACE(TRIM(COALESCE(d."Контакт: ID", '')), '.0', '') = cc.contact_id
         WHERE d.is_revenue_variant3 = 1
       ),
       ad_assoc_revenue AS (
         SELECT
           project_name,
           ad_id,
           COUNT(DISTINCT deal_id) AS deal_count,
           COALESCE(SUM(revenue_amount), 0) AS assoc_revenue
         FROM contact_deals
         GROUP BY project_name, ad_id
       )
       SELECT
         yl.project_name,
         yl.ad_id,
         MAX(yl.yandex_leads_count) AS "Лиды_Yandex",
         COUNT(DISTINCT cc.contact_id) AS "Контактов_в_пуле",
         COALESCE(MAX(aar.deal_count), 0) AS "Сделок_Bitrix",
         COALESCE(MAX(aar.assoc_revenue), 0) AS assoc_revenue
       FROM yandex_leads yl
       LEFT JOIN campaign_contacts cc
         ON cc.project_name = yl.project_name AND cc.ad_id = yl.ad_id
       LEFT JOIN ad_assoc_revenue aar
         ON aar.project_name = yl.project_name AND aar.ad_id = yl.ad_id
       GROUP BY yl.project_name, yl.ad_id
       ORDER BY assoc_revenue DESC`;

  const adPerfSql = `WITH
       yandex_map AS (
         SELECT
           REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') AS ad_id,
           MIN(${groupedStatsProjectExpr}) AS project_name
         FROM stg_yandex_stats
         WHERE REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') <> ''
         GROUP BY 1
       ),
       ystats AS (
         SELECT
           ${groupedStatsProjectExpr} AS project_name,
           REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') AS ad_id,
           MIN(NULLIF(TRIM(COALESCE("Заголовок", '')), '')) AS ad_title,
           MIN(COALESCE(NULLIF(TRIM(COALESCE(month, "Месяц")), ''), '')) AS first_spend_month,
           MAX(COALESCE(NULLIF(TRIM(COALESCE(month, "Месяц")), ''), '')) AS last_spend_month,
           SUM(COALESCE("Клики", 0)) AS clicks,
           SUM(COALESCE("Расход, ₽", 0)) AS spend
         FROM stg_yandex_stats
         WHERE REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') <> ''
         GROUP BY 1, 2
       ),
       ydeal AS (
         SELECT
           COALESCE(${groupedMappedProjectExpr}, 'UNMAPPED') AS project_name,
           ${sourceYandexAdExpr} AS ad_id,
           MIN(COALESCE(NULLIF(TRIM(COALESCE(src.month, '')), ''), '')) AS first_lead_month,
           MAX(COALESCE(NULLIF(TRIM(COALESCE(src.month, '')), ''), '')) AS last_lead_month,
           COUNT(*) AS leads_raw,
           SUM(CASE WHEN COALESCE(src.is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END) AS payments_count,
           SUM(CASE WHEN COALESCE(src.is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END) AS paid_deals_raw,
           SUM(COALESCE(src.revenue_amount, 0)) AS revenue_raw
         FROM mart_deals_enriched src
         LEFT JOIN yandex_map ym
           ON ym.ad_id = ${sourceYandexAdExpr}
         WHERE LOWER(TRIM(COALESCE(src."UTM Source", ''))) LIKE 'y%'
           AND LOWER(TRIM(COALESCE(src."UTM Source", ''))) <> 'yah'
           AND ${validSourceYandexAdExpr}
         GROUP BY 1, 2
       ),
       dims AS (
         SELECT project_name, ad_id FROM ystats
         UNION
         SELECT project_name, ad_id FROM ydeal
       )
       SELECT
         d.project_name,
         d.ad_id,
         COALESCE(ys.ad_title, '') AS ad_title,
         COALESCE(NULLIF(ys.first_spend_month, ''), NULLIF(yd.first_lead_month, ''), '') AS first_month,
         COALESCE(NULLIF(ys.last_spend_month, ''), NULLIF(yd.last_lead_month, ''), '') AS last_month,
         COALESCE(yd.leads_raw, 0) AS leads_raw,
         COALESCE(yd.payments_count, 0) AS payments_count,
         COALESCE(yd.paid_deals_raw, 0) AS paid_deals_raw,
         COALESCE(yd.revenue_raw, 0) AS revenue_raw,
         COALESCE(ys.clicks, 0) AS clicks,
         COALESCE(ys.spend, 0) AS spend
       FROM dims d
       LEFT JOIN ystats ys ON ys.project_name = d.project_name AND ys.ad_id = d.ad_id
       LEFT JOIN ydeal yd ON yd.project_name = d.project_name AND yd.ad_id = d.ad_id
       ORDER BY spend DESC, d.ad_id`;

  const q11assocQa = await db.prepare(assocQaSql).all<Record<string, unknown>>();
  const assocQaRows = groupAssocQaRows(q11assocQa.results ?? []);
  const q11assocQaByAd = await db.prepare(assocQaByAdSql).all<Record<string, unknown>>();
  const q11adPerf = await db.prepare(adPerfSql).all<Record<string, unknown>>();

  // Build the assoc revenue lookup map (feeds into yandex_projects_revenue_no_month.json).
  const assocRevenueByProject = new Map<string, number>();
  for (const r of assocQaRows) {
    const pn = String(r.project_name ?? "").trim();
    if (pn) assocRevenueByProject.set(pn, Number(r.assoc_revenue ?? 0) || 0);
  }

  // Materialize the per-project QA dataset.
  const qaProjectRows = assocQaRows.map((r) => ({
    "Проект": String(r.project_name ?? "").trim(),
    "Лиды_Yandex": Number(r["Лиды_Yandex"] ?? 0) || 0,
    "Контактов_в_пуле": Number(r["Контактов_в_пуле"] ?? 0) || 0,
    "Сделок_Bitrix": Number(r["Сделок_Bitrix"] ?? 0) || 0,
    "Ассоц. Выручка": Number(r.assoc_revenue ?? 0) || 0,
  }));
  const qaRows = buildYandexAssocQaHierarchyRows(qaProjectRows, q11assocQaByAd.results ?? []);
  await upsertDataset(db, "qa/yandex_assoc_revenue_qa.json", rowsToJson(qaRows));
  paths.push("qa/yandex_assoc_revenue_qa.json");

  // Note: assoc_revenue is NOT included here. groupYandexProjectsNoMonth sums numeric fields,
  // so if we passed assoc_revenue (which is already a per-group total) it would be multiplied
  // by the number of raw campaign rows that map to the same group. Instead we apply it once
  // per group after grouping, directly from assocRevenueByProject.
  const q11rows = (q11.results ?? []).map((r) => ({
    project_name: toKnownGroup(r.project_name),
    leads_raw: Number(r.leads_raw ?? 0) || 0,
    payments_count: Number(r.payments_count ?? 0) || 0,
    paid_deals_raw: Number(r.paid_deals_raw ?? 0) || 0,
    revenue_raw: Number(r.revenue_raw ?? 0) || 0,
    spend: Number(r.spend ?? 0) || 0,
    assoc_revenue: 0,
  }));

  const grouped = groupYandexProjectsNoMonth(q11rows).map((r) => ({
    ...r,
    assoc_revenue: Math.max(assocRevenueByProject.get(r.project_name) ?? 0, r.revenue_raw),
  }));

  // Enrich ad-perf rows with assoc_revenue from QA data; enforce assoc >= direct.
  const assocByAd = new Map<string, number>();
  for (const r of q11assocQaByAd.results ?? []) {
    const key = `${toKnownGroup(r.project_name)}||${String(r.ad_id ?? "").trim()}`;
    assocByAd.set(key, Number(r.assoc_revenue ?? 0) || 0);
  }
  const adPerfEnriched = (q11adPerf.results ?? []).map((r) => {
    const proj = toKnownGroup(r.project_name);
    const adId = String(r.ad_id ?? "").trim();
    const assocRev = assocByAd.get(`${proj}||${adId}`) ?? 0;
    const directRev = Number(r.revenue_raw ?? 0) || 0;
    return { ...r, assoc_revenue: Math.max(assocRev, directRev) };
  });
  const groupedWithDetails = buildYandexNoMonthHierarchyRows(grouped as Record<string, unknown>[], adPerfEnriched);
  await upsertDataset(db, "global/yandex_projects_revenue_no_month.json", rowsToJson(groupedWithDetails));
  paths.push("global/yandex_projects_revenue_no_month.json");


  const qa1 = await db
    .prepare(
      `SELECT COUNT(*) AS revenue_deals,
              SUM(CASE WHEN COALESCE(event_class, 'Другое') = 'Другое' THEN 1 ELSE 0 END) AS other_deals,
              CASE WHEN COUNT(*) = 0 THEN 0
                   ELSE SUM(CASE WHEN COALESCE(event_class, 'Другое') = 'Другое' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
              END AS other_share
       FROM mart_deals_enriched
       WHERE is_revenue_variant3 = 1`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "qa/other_share_global.json", rowsToJson((qa1.results ?? []) as Record<string, unknown>[]));
  paths.push("qa/other_share_global.json");

  const qa2 = await db
    .prepare(
      `SELECT ID,
              "Контакт: ID" AS contact_id,
              "Название сделки" AS deal_name,
              "Код_курса_сайт" AS course_code_site,
              "Код курса" AS course_code,
              "UTM Campaign" AS utm_campaign,
              "Источник (подробно)" AS source_detail,
              "Источник обращения" AS source_ref,
              revenue_amount,
              classification_source
       FROM mart_attacking_january_cohort_deals
       WHERE is_revenue_variant3 = 1 AND COALESCE(event_class, 'Другое') = 'Другое'
       ORDER BY revenue_amount DESC
       LIMIT 50`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "qa/other_top50_cohort.json", rowsToJson((qa2.results ?? []) as Record<string, unknown>[]));
  paths.push("qa/other_top50_cohort.json");

  const qa3 = await db
    .prepare(
      `SELECT COUNT(*) AS rows_in_mart,
              COUNT(DISTINCT ID) AS distinct_ids,
              COUNT(*) - COUNT(DISTINCT ID) AS duplicate_rows
       FROM mart_deals_enriched`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "qa/dedup_check.json", rowsToJson((qa3.results ?? []) as Record<string, unknown>[]));
  paths.push("qa/dedup_check.json");

  const qa4 = await db
    .prepare(
      `SELECT lead_key,
              COUNT(*) AS rows_count,
              COUNT(DISTINCT project_name) AS projects_count,
              SUM(revenue_amount) AS revenue_raw
       FROM mart_yandex_leads_raw
       GROUP BY lead_key
       HAVING COUNT(*) > 1
       ORDER BY rows_count DESC, revenue_raw DESC
       LIMIT 100`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(
    db,
    "qa/yandex_dedup_keys_top_collisions.json",
    rowsToJson((qa4.results ?? []) as Record<string, unknown>[]),
  );
  paths.push("qa/yandex_dedup_keys_top_collisions.json");

  const qa5 = await db
    .prepare(
      `SELECT s.leads_raw,
              s.leads_dedup,
              (s.leads_raw - s.leads_dedup) AS leads_delta,
              s.paid_deals_raw,
              s.paid_deals_dedup,
              (s.paid_deals_raw - s.paid_deals_dedup) AS paid_deals_delta,
              s.revenue_raw,
              s.revenue_dedup,
              (s.revenue_raw - s.revenue_dedup) AS revenue_delta
       FROM (
         SELECT
           (SELECT COUNT(*) FROM mart_yandex_leads_raw) AS leads_raw,
           (SELECT COUNT(*) FROM mart_yandex_leads_dedup) AS leads_dedup,
           (SELECT COALESCE(SUM(is_paid_deal),0) FROM mart_yandex_leads_raw) AS paid_deals_raw,
           (SELECT COALESCE(SUM(paid_deals),0) FROM mart_yandex_leads_dedup) AS paid_deals_dedup,
           (SELECT COALESCE(SUM(revenue_amount),0) FROM mart_yandex_leads_raw) AS revenue_raw,
           (SELECT COALESCE(SUM(revenue),0) FROM mart_yandex_leads_dedup) AS revenue_dedup
       ) s`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "qa/yandex_raw_vs_dedup_delta.json", rowsToJson((qa5.results ?? []) as Record<string, unknown>[]));
  paths.push("qa/yandex_raw_vs_dedup_delta.json");

  const qa6 = await db
    .prepare(
      `SELECT COALESCE(y."Название кампании", '') AS project_name,
              COALESCE(y."№ Кампании", '') AS campaign_id,
              COALESCE(y.month, '') AS month,
              COUNT(*) AS yandex_rows
       FROM stg_yandex_stats y
       WHERE NOT EXISTS (
         SELECT 1 FROM mart_yandex_leads_raw r
         WHERE COALESCE(r.campaign_id, '') = COALESCE(y."№ Кампании", '')
           AND COALESCE(r.yandex_month, '') = COALESCE(y.month, '')
       )
       GROUP BY COALESCE(y."Название кампании", ''), COALESCE(y."№ Кампании", ''), COALESCE(y.month, '')
       ORDER BY yandex_rows DESC`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(
    db,
    "qa/yandex_unmatched_to_bitrix.json",
    rowsToJson((qa6.results ?? []) as Record<string, unknown>[]),
  );
  paths.push("qa/yandex_unmatched_to_bitrix.json");

  const qa7 = await db
    .prepare(
      `WITH grouped AS (
         SELECT COALESCE(project_name, '') AS project_name,
                COALESCE(campaign_id, '') AS campaign_id,
                COALESCE(yandex_month, '') AS month,
                COALESCE(deal_name, '') AS deal_name,
                COUNT(*) AS candidate_rows
         FROM mart_yandex_leads_raw
         GROUP BY COALESCE(project_name, ''), COALESCE(campaign_id, ''), COALESCE(yandex_month, ''), COALESCE(deal_name, '')
       ),
       ranked AS (
         SELECT project_name, campaign_id, month, deal_name, candidate_rows,
                SUM(candidate_rows) OVER (PARTITION BY project_name, campaign_id, month) AS yandex_rows,
                ROW_NUMBER() OVER (PARTITION BY project_name, campaign_id, month ORDER BY candidate_rows DESC, deal_name) AS rn
         FROM grouped
       )
       SELECT project_name, campaign_id, month, yandex_rows,
              '' AS map_to_utm_campaign,
              deal_name AS map_to_project,
              CASE WHEN yandex_rows = 0 THEN 0
                   ELSE ROUND(candidate_rows * 1.0 / yandex_rows, 4)
              END AS map_confidence,
              CASE WHEN COALESCE(deal_name, '') = '' THEN 'no_deal_name_candidate'
                   WHEN candidate_rows = yandex_rows THEN 'single_candidate'
                   ELSE 'top_candidate_from_deal_name'
              END AS comment
       FROM ranked
       WHERE rn = 1
       ORDER BY yandex_rows DESC, map_confidence DESC`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(
    db,
    "qa/yandex_campaign_mapping_seed.json",
    rowsToJson((qa7.results ?? []) as Record<string, unknown>[]),
  );
  paths.push("qa/yandex_campaign_mapping_seed.json");

  return { paths };
}
