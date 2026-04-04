/**
 * Materialize dashboard JSON blobs into dataset_json (same SQL as db/run_all_slices.export_*).
 */
import { groupYandexProjectsNoMonth } from "./yandexProjectsNoMonth";
import { sqlExtractYandexAdId } from "./yandexAdId";
import { buildYdHierarchyRows } from "./ydHierarchy";
import { sqlQuote, sqlMonthFromDateExpr, isValidYandexAdId, buildInvalidTokenCond } from "./sqlHelpers";
import { buildBitrixContactsUidRows } from "./bitrixContactsUid";
import { buildLeadLogicSql, buildPotentialCond } from "./leadLogicSql";
import { YANDEX_PROJECT_GROUP_ALIAS_PAIRS, YANDEX_KNOWN_GROUPS, mapYandexProjectGroup } from "../../../src/yandexProjectGroups";
import managerFirstlineNames from "../../config/manager_firstline.json";
import {
  sqlMonthLabel,
  buildManagerBaseSql,
  buildManagerPnlBaseSql,
  buildManagerByMonthSql,
  buildManagerByCourseSql,
  buildManagerByCourseMonthSql,
  buildFirstlineFilter,
  type ManagerBaseExprs,
} from "./managerSql";
import { buildAssocQaSql, buildAssocQaByAdSql, buildAdPerfSql, type AssocRevenueExprs } from "./assocRevenueSql";

async function tableExists(db: D1Database, tableName: string): Promise<boolean> {
  const row = await db
    .prepare(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1`)
    .bind(tableName)
    .first<{ name: string }>();
  return !!row?.name;
}

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
  const chunks: string[] = [];
  for (let i = 0; i < body.length; i += CHUNK_SIZE) {
    chunks.push(body.slice(i, i + CHUNK_SIZE));
  }
  const stmt = db.prepare(
    `INSERT OR REPLACE INTO dataset_json (path, chunk, body, updated_at) VALUES (?, ?, ?, datetime('now'))`,
  );
  // Upsert chunks in-place. Using INSERT OR REPLACE avoids a DELETE-then-INSERT window where
  // old data would be gone but new data not yet written (catastrophic if D1 hits CPU limit mid-write).
  // If D1 resets mid-batch, the worst case is some chunks have new data and some have old data —
  // the path remains partially readable rather than completely missing.
  await db.batch(chunks.map((chunk, idx) => stmt.bind(path, idx, chunk)));
  // Remove any stale extra chunks from a previous version that needed more chunks than the new one.
  // Best-effort: if this fails the extra chunks are harmless (they'll be overwritten next run).
  await db
    .prepare(`DELETE FROM dataset_json WHERE path = ? AND chunk >= ?`)
    .bind(path, chunks.length)
    .run()
    .catch(() => {});
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
    const project = toKnownGroup(String(row.project_name ?? "").trim());
    if (!project || project === "UNMAPPED") continue;
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

// Bitrix invalid-token list (used for column-based quality checks).
const BITRIX_INVALID_TOKENS = [
  "спам",
  "дубль",
  "тест",
  "некорректные данные",
  "чс",
  "неправильные данные",
  "партнер или сотрудник cybered",
  "партнеры, не нужно связываться",
];

// buildInvalidTokenCond is imported from sqlHelpers.ts

export async function materializeSliceDatasets(db: D1Database): Promise<{ paths: string[] }> {
  const paths: string[] = [];
  type RR = Record<string, unknown>;

  // ── Stage 0: Schema detection (parallel within each stage) ──────────────────
  const [hasTypyInMart1, hasTypyInMart2, hasRawP01, hasSendsayContacts, hasContactsUid] =
    await Promise.all([
      columnExists(db, "mart_deals_enriched", "Типы некачественного лида"),
      columnExists(db, "mart_deals_enriched", "Типы некачественных лидов"),
      tableExists(db, "raw_bitrix_deals_p01"),
      tableExists(db, "stg_sendsay_contacts"),
      tableExists(db, "stg_contacts_uid"),
    ]);
  const hasTypyInMart = hasTypyInMart1 || hasTypyInMart2;

  const [hasTypyInRaw1, hasTypyInRaw2, hasPassedByFirstLine, hasModifyDateRu, hasModifyDateShortRu, hasModifyDateEn] = hasRawP01
    ? await Promise.all([
        columnExists(db, "raw_bitrix_deals_p01", "Типы некачественного лида"),
        columnExists(db, "raw_bitrix_deals_p01", "Типы некачественных лидов"),
        columnExists(db, "raw_bitrix_deals_p01", "Передан первой линией"),
        columnExists(db, "raw_bitrix_deals_p01", "Дата изменения сделки"),
        columnExists(db, "raw_bitrix_deals_p01", "Дата изменения"),
        columnExists(db, "raw_bitrix_deals_p01", "date_modify"),
      ])
    : ([false, false, false, false, false, false] as const);
  const hasTypyInRaw = hasTypyInRaw1 || hasTypyInRaw2;

  // ── Build SQL expression fragments ──────────────────────────────────────────
  const bitrixInvalidCond = hasTypyInMart
    ? buildInvalidTokenCond(BITRIX_INVALID_TOKENS, "")
    : "0";
  // bitrixInvalidExpr is used in queries that SELECT directly from mart_deals_enriched
  // (no p. alias). When the column is only in raw p01, the stage-token detection in
  // buildLeadLogicSql (via extraInvalidCond) handles it; we still expose a direct expr
  // so that explicit SUM(is_invalid) lines in Batch 1 queries are consistent.
  const bitrixInvalidExpr = hasTypyInMart
    ? `CASE WHEN (${bitrixInvalidCond}) THEN 1 ELSE 0 END`
    : `CASE WHEN 0 THEN 1 ELSE 0 END`;

  const managerInvalidCond = hasTypyInMart
    ? buildInvalidTokenCond(BITRIX_INVALID_TOKENS, "m.")
    : hasTypyInRaw
    ? buildInvalidTokenCond(BITRIX_INVALID_TOKENS, "p.")
    : "0";
  const managerInvalidExpr = `CASE WHEN (${managerInvalidCond}) THEN 1 ELSE 0 END`;

  const yandexExtraInvalidCond = hasTypyInMart
    ? buildInvalidTokenCond(BITRIX_INVALID_TOKENS, "m.", "like")
    : "";

  const bitrixLeadLogic = buildLeadLogicSql({
    funnelExpr: `"Воронка"`,
    stageExpr: `"Стадия сделки"`,
    monthExpr: "month",
    extraInvalidCond: hasTypyInMart ? bitrixInvalidCond : "",
  });
  const yandexLeadLogic = buildLeadLogicSql({
    funnelExpr: "funnel",
    stageExpr: "stage",
    monthExpr: "yandex_month",
    extraInvalidCond: yandexExtraInvalidCond,
  });
  // Used in queries that SELECT directly from mart_yandex_leads_raw without a JOIN to
  // mart_deals_enriched — yandexLeadLogic.unqual contains m."Типы..." which would fail.
  const yandexLeadLogicSimple = buildLeadLogicSql({
    funnelExpr: "funnel",
    stageExpr: "stage",
    monthExpr: "yandex_month",
  });
  const managerLeadLogic = buildLeadLogicSql({
    funnelExpr: `m."Воронка"`,
    stageExpr: `m."Стадия сделки"`,
    monthExpr: "m.month",
    extraInvalidCond: managerInvalidCond,
  });

  const totalEmailContactsExpr = hasSendsayContacts
    ? `(SELECT COUNT(*) FROM stg_sendsay_contacts WHERE COALESCE(email, '') <> '')`
    : `(SELECT COUNT(DISTINCT "Контакт: ID") FROM mart_deals_enriched WHERE COALESCE("Контакт: ID", '') <> '')`;
  const goodEmailContactsExpr = hasSendsayContacts
    ? `(SELECT COUNT(*) FROM stg_sendsay_contacts WHERE COALESCE(email, '') <> '' AND COALESCE(TRIM(error_message), '') = '')`
    : totalEmailContactsExpr;

  // Yandex SQL expression helpers
  const sourceYandexAdExpr = sqlExtractYandexAdId(`src."UTM Content"`);
  const validSourceYandexAdExpr = `LENGTH(${sourceYandexAdExpr}) = 11 AND SUBSTR(${sourceYandexAdExpr}, 1, 2) = '17' AND ${sourceYandexAdExpr} NOT GLOB '*[^0-9]*'`;
  const groupedStatsProjectExpr = buildYandexProjectGroupSqlExpr(`"Название кампании"`);
  // ym.project_name is already a mapped group name (produced by groupedStatsProjectExpr in yandex_map).
  // Re-applying the alias CASE would fail to match group names and return UNMAPPED everywhere.
  // Just pass through the already-mapped value, falling back to UNMAPPED for NULL/empty only.
  const groupedMappedProjectExpr = `COALESCE(NULLIF(TRIM(COALESCE(ym.project_name, '')), ''), 'UNMAPPED')`;
  const assocExprs: AssocRevenueExprs = {
    sourceYandexAdExpr,
    validSourceYandexAdExpr,
    groupedStatsProjectExpr,
    groupedMappedProjectExpr,
  };

  // Manager SQL
  // SQLite lower()/upper() do not reliably normalize Cyrillic in D1.
  // Use exact trimmed names to keep manager datasets populated.
  const firstlineList = Array.isArray(managerFirstlineNames)
    ? managerFirstlineNames.map((s) => String(s ?? "").trim()).filter(Boolean)
    : [];
  const firstlineFilter = buildFirstlineFilter(firstlineList, hasPassedByFirstLine);
  const salesFilter = `trim(manager) IN ('Анастасия Крисанова', 'Василий Гореленков', 'Глеб Барбазанов', 'Елена Лобода')`;

  const managerPotentialExpr = buildPotentialCond(`m."Стадия сделки"`);
  const managerBaseExprs: ManagerBaseExprs = {
    qual: managerLeadLogic.qual,
    unqual: managerLeadLogic.unqual,
    refusal: managerLeadLogic.refusal,
    invalidExpr: managerInvalidExpr,
    inWork: managerLeadLogic.inWork,
    potential: managerPotentialExpr,
    firstlineFilter,
    firstlineHybridMode: hasPassedByFirstLine,
    hasPassedByFirstLine,
  };
  const managerBaseSql = buildManagerBaseSql(hasRawP01, managerBaseExprs);

  // ── Batch 1: Global channel + budget + Bitrix month totals (parallel reads) ─
  const [r_q1, r_q2, r_q3, r_budget, r_bitrixMonthTotal] = await Promise.all([
    db.prepare(
      `SELECT month,
              COALESCE("UTM Source", '') AS utm_source,
              COALESCE("UTM Medium", '') AS utm_medium,
              COUNT(DISTINCT ID) AS deals,
              SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
              SUM(revenue_amount) AS revenue
       FROM mart_deals_enriched
       GROUP BY month, COALESCE("UTM Source", ''), COALESCE("UTM Medium", '')
       ORDER BY month, revenue DESC`,
    ).all<RR>(),
    db.prepare(
      `SELECT month,
              COUNT(*) AS rows_count,
              SUM(COALESCE("Расход, ₽", 0)) AS yandex_spend,
              SUM(COALESCE("Клики", 0)) AS clicks,
              SUM(COALESCE("Конверсии", 0)) AS conversions
       FROM stg_yandex_stats
       GROUP BY month
       ORDER BY month`,
    ).all<RR>(),
    db.prepare(
      `SELECT month,
              COUNT(*) AS sends,
              SUM(COALESCE("Отправлено", 0)) AS sent_total,
              SUM(COALESCE("Доставлено", 0)) AS delivered_total,
              SUM(COALESCE("Уник. открытий", 0)) AS unique_opens,
              SUM(COALESCE("Уник. кликов", 0)) AS unique_clicks
       FROM stg_email_sends
       GROUP BY month
       ORDER BY month`,
    ).all<RR>(),
    db.prepare(
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
    ).all<RR>(),
    db.prepare(
      `WITH flags AS (
         SELECT
           month,
           CASE
             WHEN COALESCE("Дата оплаты", '') LIKE '____-__%' THEN SUBSTR("Дата оплаты", 1, 7)
             WHEN COALESCE("Дата оплаты", '') LIKE '__.__.____%' THEN SUBSTR("Дата оплаты", 7, 4) || '-' || SUBSTR("Дата оплаты", 4, 2)
             ELSE ''
           END AS pay_month,
           COALESCE("ID", '') AS deal_id,
           COALESCE("Стадия сделки", '') AS stage,
           COALESCE("Воронка", '') AS funnel,
           COALESCE(revenue_amount, 0) AS revenue_amount,
           ${bitrixLeadLogic.qual} AS is_qual,
           ${bitrixLeadLogic.unqual} AS is_unqual,
           ${bitrixLeadLogic.refusal} AS is_refusal,
           ${bitrixLeadLogic.invalid} AS is_invalid,
           ${bitrixLeadLogic.inWork} AS is_in_work,
           ${buildPotentialCond(`"Стадия сделки"`)} AS is_potential,
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
         SUM(is_potential) AS "В потенциале",
         SUM(is_revenue) AS "Сделок_с_выручкой",
         SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS "Выручка",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_qual) * 1.0 / COUNT(*) END AS "Конверсия в Квал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_unqual) * 1.0 / COUNT(*) END AS "Конверсия в Неквал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_refusal) * 1.0 / COUNT(*) END AS "Конверсия в Отказ",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_in_work) * 1.0 / COUNT(*) END AS "Конверсия в работе",
         CASE WHEN SUM(is_qual) = 0 THEN 0 ELSE SUM(is_revenue) * 1.0 / SUM(is_qual) END AS "Конверсия Квал→Оплата",
         CASE WHEN SUM(is_revenue) = 0 THEN 0 ELSE SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) * 1.0 / SUM(is_revenue) END AS "Средний_чек"
       FROM flags
       GROUP BY month
       ORDER BY month`,
    ).all<RR>(),
  ]);
  await Promise.all([
    upsertDataset(db, "global/month_channel_bitrix.json", rowsToJson(r_q1.results ?? [])),
    upsertDataset(db, "global/month_channel_yandex.json", rowsToJson(r_q2.results ?? [])),
    upsertDataset(db, "global/month_channel_sendsay.json", rowsToJson(r_q3.results ?? [])),
    upsertDataset(db, "global/budget_monthly.json", rowsToJson(r_budget.results ?? [])),
    upsertDataset(db, "bitrix_month_total_full.json", rowsToJson(r_bitrixMonthTotal.results ?? [])),
  ]);
  paths.push(
    "global/month_channel_bitrix.json",
    "global/month_channel_yandex.json",
    "global/month_channel_sendsay.json",
    "global/budget_monthly.json",
    "bitrix_month_total_full.json",
  );

  // ── Batch 2: Weekly + email + contacts + funnel/code (parallel reads) ────────
  const [
    r_bitrixWeek,
    r_yandexWeek,
    r_emailOps,
    r_emailHier,
    r_bitrixContactsUid,
    r_dashContactsCount,
    r_bitrixFunnelCode,
    r_newEventContacts,
  ] = await Promise.all([
    db.prepare(
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
    ).all<RR>(),
    db.prepare(
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
    ).all<RR>(),
    db.prepare(
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
           ${sqlMonthLabel("month")} AS "Период",
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
    ).all<RR>(),
    db.prepare(
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
           ${sqlMonthLabel("month")} AS month_label,
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
           ${sqlMonthLabel("month")} AS "Месяц",
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
    ).all<RR>(),
    buildBitrixContactsUidRows(db),
    db.prepare(`SELECT ${goodEmailContactsExpr} AS email_contacts_actual`)
      .first<{ email_contacts_actual: number }>(),
    db.prepare(
      `WITH flags AS (
         SELECT
           COALESCE(NULLIF(trim(funnel_group), ''), 'Другое') AS funnel_group,
           month,
           COALESCE(NULLIF(trim(course_code_norm), ''), '—') AS course_code,
           COALESCE(revenue_amount, 0) AS revenue_amount,
           ${bitrixLeadLogic.qual} AS is_qual,
           ${bitrixLeadLogic.unqual} AS is_unqual,
           ${bitrixLeadLogic.refusal} AS is_refusal,
           ${bitrixLeadLogic.invalid} AS is_invalid,
           ${bitrixLeadLogic.inWork} AS is_in_work,
           ${buildPotentialCond(`"Стадия сделки"`)} AS is_potential,
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
         SUM(CASE WHEN is_qual = 0 AND is_unqual = 0 THEN 1 ELSE 0 END) AS "Неизвестно",
         SUM(is_refusal) AS "Отказы",
         SUM(is_in_work) AS "В работе",
         SUM(is_invalid) AS "Невалидные_лиды",
         SUM(is_potential) AS "В потенциале",
         SUM(is_revenue) AS "Сделок_с_выручкой",
         SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS "Выручка",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_qual) * 1.0 / COUNT(*) END AS "Конверсия в Квал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_unqual) * 1.0 / COUNT(*) END AS "Конверсия в Неквал",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_refusal) * 1.0 / COUNT(*) END AS "Конверсия в Отказ",
         CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(is_in_work) * 1.0 / COUNT(*) END AS "Конверсия в работе",
         CASE WHEN SUM(is_qual) = 0 THEN 0 ELSE SUM(is_revenue) * 1.0 / SUM(is_qual) END AS "Конверсия Квал→Оплата",
         CASE WHEN SUM(is_revenue) = 0 THEN 0 ELSE SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) * 1.0 / SUM(is_revenue) END AS "Средний_чек"
       FROM flags
       GROUP BY funnel_group, month, course_code
       ORDER BY funnel_group, month, course_code`,
    ).all<RR>(),
    db.prepare(
      `WITH first_contact_months AS (
         SELECT
           "Контакт: ID" AS cid,
           MIN(month) AS first_month
         FROM mart_deals_enriched
         WHERE COALESCE("Контакт: ID", '') <> ''
           AND COALESCE(month, '') <> ''
         GROUP BY "Контакт: ID"
       ),
       first_event_classes AS (
         SELECT
           fcm.cid,
           fcm.first_month,
           MAX(CASE WHEN lower(COALESCE(m.event_class, '')) IN ('webinar', 'demo', 'event') THEN 1 ELSE 0 END) AS has_event
         FROM first_contact_months fcm
         JOIN mart_deals_enriched m
           ON m."Контакт: ID" = fcm.cid
           AND m.month = fcm.first_month
         GROUP BY fcm.cid, fcm.first_month
       )
       SELECT
         first_month AS "Месяц",
         COUNT(*) AS "Контактов всего",
         SUM(has_event) AS "Новых с мероприятия"
       FROM first_event_classes
       GROUP BY first_month
       ORDER BY first_month`,
    ).all<RR>(),
  ]);

  const bitrixContactsUidRows = r_bitrixContactsUid as RR[];
  const emailContactsActual = Number(r_dashContactsCount?.email_contacts_actual ?? 0) || 0;
  const dashboardContactsTotalRows: RR[] = [{
    bitrix_contacts_actual: bitrixContactsUidRows.length,
    email_contacts_actual: emailContactsActual,
    contacts_actual_total: bitrixContactsUidRows.length + emailContactsActual,
  }];

  await Promise.all([
    upsertDataset(db, "bitrix_week_funnel_total.json", rowsToJson(r_bitrixWeek.results ?? [])),
    upsertDataset(db, "yandex_week_campaign_total.json", rowsToJson(r_yandexWeek.results ?? [])),
    upsertDataset(db, "email_operational_summary.json", rowsToJson(r_emailOps.results ?? [])),
    upsertDataset(db, "email_hierarchy_by_send.json", rowsToJson(r_emailHier.results ?? [])),
    upsertDataset(db, "bitrix_contacts_uid.json", rowsToJson(bitrixContactsUidRows)),
    upsertDataset(db, "dashboard_contacts_total.json", rowsToJson(dashboardContactsTotalRows)),
    upsertDataset(db, "bitrix_funnel_month_code_full.json", rowsToJson(r_bitrixFunnelCode.results ?? [])),
    upsertDataset(db, "bitrix_new_event_contacts_by_event.json", rowsToJson(r_newEventContacts.results ?? [])),
  ]);
  paths.push(
    "bitrix_week_funnel_total.json",
    "yandex_week_campaign_total.json",
    "email_operational_summary.json",
    "email_hierarchy_by_send.json",
    "bitrix_contacts_uid.json",
    "dashboard_contacts_total.json",
    "bitrix_funnel_month_code_full.json",
    "bitrix_new_event_contacts_by_event.json",
  );

  // ── Batch 3: Manager reports — 6 queries in parallel ─────────────────────────
  const [
    r_mgr_fl_month,
    r_mgr_fl_course,
    r_mgr_fl_course_month,
    r_mgr_sales_month,
    r_mgr_sales_course,
    r_mgr_sales_course_month,
  ] = await Promise.all([
    db.prepare(buildManagerByMonthSql(managerBaseSql, firstlineFilter)).all<RR>(),
    db.prepare(buildManagerByCourseSql(managerBaseSql, firstlineFilter)).all<RR>(),
    db.prepare(buildManagerByCourseMonthSql(managerBaseSql, firstlineFilter)).all<RR>(),
    db.prepare(buildManagerByMonthSql(managerBaseSql, salesFilter)).all<RR>(),
    db.prepare(buildManagerByCourseSql(managerBaseSql, salesFilter)).all<RR>(),
    db.prepare(buildManagerByCourseMonthSql(managerBaseSql, salesFilter)).all<RR>(),
  ]);
  await Promise.all([
    upsertDataset(db, "manager_firstline_by_month.json", rowsToJson(r_mgr_fl_month.results ?? [])),
    upsertDataset(db, "manager_firstline_by_course.json", rowsToJson(r_mgr_fl_course.results ?? [])),
    upsertDataset(db, "manager_firstline_by_course_month.json", rowsToJson(r_mgr_fl_course_month.results ?? [])),
    upsertDataset(db, "manager_sales_by_month.json", rowsToJson(r_mgr_sales_month.results ?? [])),
    upsertDataset(db, "manager_sales_by_course.json", rowsToJson(r_mgr_sales_course.results ?? [])),
    upsertDataset(db, "manager_sales_by_course_month.json", rowsToJson(r_mgr_sales_course_month.results ?? [])),
  ]);
  paths.push(
    "manager_firstline_by_month.json",
    "manager_firstline_by_course.json",
    "manager_firstline_by_course_month.json",
    "manager_sales_by_month.json",
    "manager_sales_by_course.json",
    "manager_sales_by_course_month.json",
  );

  // ── Batch 3b: PNL variants — mixed clocks by report month:
  // leads by created month, qual-state by modified month, revenue by pay month.
  const managerPnlBaseSql = buildManagerPnlBaseSql(hasRawP01, managerBaseExprs);
  const PAY_MONTH_EXPR = sqlMonthFromDateExpr(`m."Дата оплаты"`);
  const FUNNEL_MODIFY_MONTH_EXPR = hasRawP01
    ? sqlMonthFromDateExpr(`p.modify_raw`)
    : "COALESCE(m.month, '')";
  const FUNNEL_RAW_WITH_SQL = hasRawP01
    ? `p01 AS (
         SELECT
           COALESCE("ID", '') AS deal_id,
           MAX(COALESCE("Дата изменения сделки", "Дата изменения", "date_modify", '')) AS modify_raw
         FROM raw_bitrix_deals_p01
         GROUP BY COALESCE("ID", '')
       ),`
    : "";
  const FUNNEL_RAW_JOIN_SQL = hasRawP01 ? `LEFT JOIN p01 p ON p.deal_id = m."ID"` : "";
  const [
    r_mgr_fl_month_pnl,
    r_mgr_fl_course_month_pnl,
    r_mgr_sales_month_pnl,
    r_mgr_sales_course_month_pnl,
    r_bitrixFunnelCodePnl,
  ] = await Promise.all([
    db.prepare(buildManagerByMonthSql(managerPnlBaseSql, firstlineFilter)).all<RR>(),
    db.prepare(buildManagerByCourseMonthSql(managerPnlBaseSql, firstlineFilter)).all<RR>(),
    db.prepare(buildManagerByMonthSql(managerPnlBaseSql, salesFilter)).all<RR>(),
    db.prepare(buildManagerByCourseMonthSql(managerPnlBaseSql, salesFilter)).all<RR>(),
    db.prepare(
      `WITH ${FUNNEL_RAW_WITH_SQL}
       source AS (
         SELECT
           COALESCE(NULLIF(trim(funnel_group), ''), 'Другое') AS funnel_group,
           COALESCE(month, '') AS create_month,
           ${FUNNEL_MODIFY_MONTH_EXPR} AS modify_month,
           ${PAY_MONTH_EXPR} AS pay_month,
           COALESCE(NULLIF(trim(course_code_norm), ''), '—') AS course_code,
           COALESCE(revenue_amount, 0) AS revenue_amount,
           ${bitrixLeadLogic.qual} AS is_qual,
           ${bitrixLeadLogic.unqual} AS is_unqual,
           ${bitrixLeadLogic.refusal} AS is_refusal,
           ${bitrixLeadLogic.invalid} AS is_invalid,
           ${bitrixLeadLogic.inWork} AS is_in_work,
           ${buildPotentialCond(`"Стадия сделки"`)} AS is_potential,
           CASE WHEN COALESCE(is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END AS is_revenue
         FROM mart_deals_enriched m
         ${FUNNEL_RAW_JOIN_SQL}
       ),
       events AS (
         SELECT
           funnel_group,
           create_month AS month,
           course_code,
           1 AS is_lead,
           0 AS is_qual,
           0 AS is_unqual,
           0 AS is_refusal,
           0 AS is_invalid,
           0 AS is_in_work,
           0 AS is_potential,
           0 AS is_revenue,
           0 AS revenue_amount
         FROM source
         WHERE COALESCE(create_month, '') <> ''
         UNION ALL
         SELECT
           funnel_group,
           modify_month AS month,
           course_code,
           0 AS is_lead,
           is_qual,
           is_unqual,
           is_refusal,
           is_invalid,
           is_in_work,
           is_potential,
           0 AS is_revenue,
           0 AS revenue_amount
         FROM source
         WHERE COALESCE(modify_month, '') <> ''
         UNION ALL
         SELECT
           funnel_group,
           pay_month AS month,
           course_code,
           0 AS is_lead,
           0 AS is_qual,
           0 AS is_unqual,
           0 AS is_refusal,
           0 AS is_invalid,
           0 AS is_in_work,
           0 AS is_potential,
           is_revenue,
           CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END AS revenue_amount
         FROM source
         WHERE COALESCE(pay_month, '') <> ''
           AND is_revenue = 1
       )
       SELECT
         funnel_group AS "Воронка",
         month AS "Месяц",
         course_code AS "Код_курса_норм",
         SUM(is_lead) AS "Лиды",
         SUM(is_qual) AS "Квал",
         SUM(is_unqual) AS "Неквал",
         SUM(CASE WHEN is_qual = 0 AND is_unqual = 0 THEN 1 ELSE 0 END) AS "Неизвестно",
         SUM(is_refusal) AS "Отказы",
         SUM(is_in_work) AS "В работе",
         SUM(is_invalid) AS "Невалидные_лиды",
         SUM(is_potential) AS "В потенциале",
         SUM(is_revenue) AS "Сделок_с_выручкой",
         SUM(revenue_amount) AS "Выручка",
         CASE WHEN SUM(is_lead) = 0 THEN 0 ELSE SUM(is_qual) * 1.0 / SUM(is_lead) END AS "Конверсия в Квал",
         CASE WHEN SUM(is_lead) = 0 THEN 0 ELSE SUM(is_unqual) * 1.0 / SUM(is_lead) END AS "Конверсия в Неквал",
         CASE WHEN SUM(is_lead) = 0 THEN 0 ELSE SUM(is_refusal) * 1.0 / SUM(is_lead) END AS "Конверсия в Отказ",
         CASE WHEN SUM(is_lead) = 0 THEN 0 ELSE SUM(is_in_work) * 1.0 / SUM(is_lead) END AS "Конверсия в работе",
         CASE WHEN SUM(is_qual) = 0 THEN 0 ELSE SUM(is_revenue) * 1.0 / SUM(is_qual) END AS "Конверсия Квал→Оплата",
         CASE WHEN SUM(is_revenue) = 0 THEN 0 ELSE SUM(revenue_amount) * 1.0 / SUM(is_revenue) END AS "Средний_чек"
       FROM events
       WHERE month <> ''
       GROUP BY funnel_group, month, course_code
       ORDER BY funnel_group, month, course_code`,
    ).all<RR>(),
  ]);
  await Promise.all([
    upsertDataset(db, "manager_firstline_by_month_pnl.json", rowsToJson(r_mgr_fl_month_pnl.results ?? [])),
    upsertDataset(db, "manager_firstline_by_course_month_pnl.json", rowsToJson(r_mgr_fl_course_month_pnl.results ?? [])),
    upsertDataset(db, "manager_sales_by_month_pnl.json", rowsToJson(r_mgr_sales_month_pnl.results ?? [])),
    upsertDataset(db, "manager_sales_by_course_month_pnl.json", rowsToJson(r_mgr_sales_course_month_pnl.results ?? [])),
    upsertDataset(db, "bitrix_funnel_month_code_full_pnl.json", rowsToJson(r_bitrixFunnelCodePnl.results ?? [])),
  ]);
  paths.push(
    "manager_firstline_by_month_pnl.json",
    "manager_firstline_by_course_month_pnl.json",
    "manager_sales_by_month_pnl.json",
    "manager_sales_by_course_month_pnl.json",
    "bitrix_funnel_month_code_full_pnl.json",
  );

  // ── Batch 4: Yandex KPIs + hierarchy + project revenue + cohort + QA (parallel) ─
  const [
    r_yandexMonthKpis,
    r_yandexCampaignKpis,
    r_ydHierarchy,
    r_q4,
    r_q5,
    r_q6,
    r_q7,
    r_q8,
    r_q9,
    r_q10,
    r_q11,
    r_assocQa,
    r_assocQaByAd,
    r_adPerf,
    r_qa1,
    r_qa2,
    r_qa3,
    r_qa4,
    r_qa5,
    r_qa6,
    r_qa7,
  ] = await Promise.all([
    db.prepare(
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
           ${yandexLeadLogicSimple.qual} AS qual,
           ${yandexLeadLogicSimple.unqual} AS unqual,
           ${yandexLeadLogicSimple.refusal} AS refusal,
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
    ).all<RR>(),
    db.prepare(
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
           ${yandexLeadLogicSimple.qual} AS qual,
           ${yandexLeadLogicSimple.unqual} AS unqual,
           ${yandexLeadLogicSimple.refusal} AS refusal,
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
    ).all<RR>(),
    buildYdHierarchyRows(db),
    db.prepare(
      `SELECT COALESCE(funnel_group, '') AS funnel,
              COALESCE("Стадия сделки", '') AS stage,
              COUNT(DISTINCT ID) AS deals,
              SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
              SUM(revenue_amount) AS revenue
       FROM mart_deals_enriched
       GROUP BY COALESCE(funnel_group, ''), COALESCE("Стадия сделки", '')
       ORDER BY revenue DESC`,
    ).all<RR>(),
    db.prepare(
      `SELECT COALESCE(event_class, 'Другое') AS event_class,
              COALESCE(NULLIF(course_code_norm, ''), 'Другое') AS course_code_norm,
              COUNT(DISTINCT ID) AS deals,
              SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
              SUM(revenue_amount) AS revenue
       FROM mart_deals_enriched
       GROUP BY COALESCE(event_class, 'Другое'), COALESCE(NULLIF(course_code_norm, ''), 'Другое')
       ORDER BY revenue DESC`,
    ).all<RR>(),
    db.prepare(
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
    ).all<RR>(),
    db.prepare(
      `SELECT COALESCE(event_class, 'Другое') AS event_class,
              COALESCE(NULLIF(course_code_norm, ''), 'Другое') AS course_code_norm,
              COUNT(DISTINCT ID) AS deals,
              SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
              SUM(revenue_amount) AS revenue
       FROM mart_attacking_january_cohort_deals
       GROUP BY COALESCE(event_class, 'Другое'), COALESCE(NULLIF(course_code_norm, ''), 'Другое')
       ORDER BY revenue DESC`,
    ).all<RR>(),
    db.prepare(
      `SELECT (SELECT COUNT(*) FROM mart_yandex_leads_raw) AS leads_raw,
              (SELECT COUNT(*) FROM mart_yandex_leads_dedup) AS leads_dedup,
              (SELECT COALESCE(SUM(is_paid_deal),0) FROM mart_yandex_leads_raw) AS paid_deals_raw,
              (SELECT COALESCE(SUM(paid_deals),0) FROM mart_yandex_leads_dedup) AS paid_deals_dedup,
              (SELECT COALESCE(SUM(revenue_amount),0) FROM mart_yandex_leads_raw) AS revenue_raw,
              (SELECT COALESCE(SUM(revenue),0) FROM mart_yandex_leads_dedup) AS revenue_dedup`,
    ).all<RR>(),
    db.prepare(
      `SELECT project_name,
              yandex_month AS month,
              leads_raw,
              leads_dedup,
              paid_deals_raw,
              paid_deals_dedup,
              revenue_raw,
              revenue_dedup,
              spend
       FROM mart_yandex_revenue_projects
       ORDER BY revenue_raw DESC`,
    ).all<RR>(),
    db.prepare(
      `WITH matched AS (
         SELECT yandex_month AS month,
                SUM(leads_raw) AS leads_raw,
                SUM(paid_deals_raw) AS paid_deals_raw,
                SUM(revenue_raw) AS revenue_raw
         FROM mart_yandex_revenue_projects
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
    ).all<RR>(),
    db.prepare(
      `SELECT project_name,
              SUM(leads_raw) AS leads_raw,
              SUM(paid_deals_raw) AS payments_count,
              SUM(paid_deals_raw) AS paid_deals_raw,
              SUM(revenue_raw) AS revenue_raw,
              SUM(spend) AS spend
       FROM mart_yandex_revenue_projects
       GROUP BY project_name
       ORDER BY revenue_raw DESC`,
    ).all<RR>(),
    db.prepare(buildAssocQaSql(hasContactsUid, assocExprs)).all<RR>(),
    db.prepare(buildAssocQaByAdSql(hasContactsUid, assocExprs)).all<RR>(),
    db.prepare(buildAdPerfSql(assocExprs)).all<RR>(),
    db.prepare(
      `SELECT COUNT(*) AS revenue_deals,
              SUM(CASE WHEN COALESCE(event_class, 'Другое') = 'Другое' THEN 1 ELSE 0 END) AS other_deals,
              CASE WHEN COUNT(*) = 0 THEN 0
                   ELSE SUM(CASE WHEN COALESCE(event_class, 'Другое') = 'Другое' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
              END AS other_share
       FROM mart_deals_enriched
       WHERE is_revenue_variant3 = 1`,
    ).all<RR>(),
    db.prepare(
      `SELECT ID,
              "Контакт: ID" AS contact_id,
              "Название сделки" AS deal_name,
              "Код_курса_сайт" AS course_code_site,
              "Код курса" AS course_code,
              "UTM Campaign" AS utm_campaign,
              NULL AS source_detail,
              NULL AS source_ref,
              revenue_amount,
              classification_source
       FROM mart_attacking_january_cohort_deals
       WHERE is_revenue_variant3 = 1 AND COALESCE(event_class, 'Другое') = 'Другое'
       ORDER BY revenue_amount DESC
       LIMIT 50`,
    ).all<RR>(),
    db.prepare(
      `SELECT COUNT(*) AS rows_in_mart,
              COUNT(DISTINCT ID) AS distinct_ids,
              COUNT(*) - COUNT(DISTINCT ID) AS duplicate_rows
       FROM mart_deals_enriched`,
    ).all<RR>(),
    db.prepare(
      `SELECT lead_key,
              COUNT(*) AS rows_count,
              COUNT(DISTINCT project_name) AS projects_count,
              SUM(revenue_amount) AS revenue_raw
       FROM mart_yandex_leads_raw
       GROUP BY lead_key
       HAVING COUNT(*) > 1
       ORDER BY rows_count DESC, revenue_raw DESC
       LIMIT 100`,
    ).all<RR>(),
    db.prepare(
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
    ).all<RR>(),
    db.prepare(
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
    ).all<RR>(),
    db.prepare(
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
    ).all<RR>(),
  ]);

  // ── Post-process assoc-revenue results ──────────────────────────────────────
  const assocQaRows = groupAssocQaRows(r_assocQa.results ?? []);

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
  const qaRows = buildYandexAssocQaHierarchyRows(qaProjectRows, r_assocQaByAd.results ?? []);

  // Note: assoc_revenue is NOT included here. groupYandexProjectsNoMonth sums numeric fields,
  // so if we passed assoc_revenue (which is already a per-group total) it would be multiplied
  // by the number of raw campaign rows that map to the same group. Instead we apply it once
  // per group after grouping, directly from assocRevenueByProject.
  const q11rows = (r_q11.results ?? []).map((r) => ({
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
  for (const r of r_assocQaByAd.results ?? []) {
    const key = `${toKnownGroup(r.project_name)}||${String(r.ad_id ?? "").trim()}`;
    assocByAd.set(key, Number(r.assoc_revenue ?? 0) || 0);
  }
  const adPerfEnriched = (r_adPerf.results ?? []).map((r) => {
    const proj = toKnownGroup(r.project_name);
    const adId = String(r.ad_id ?? "").trim();
    const assocRev = assocByAd.get(`${proj}||${adId}`) ?? 0;
    const directRev = Number(r.revenue_raw ?? 0) || 0;
    return { ...r, assoc_revenue: Math.max(assocRev, directRev) };
  });
  const groupedWithDetails = buildYandexNoMonthHierarchyRows(grouped as RR[], adPerfEnriched);

  // ── All remaining upserts in parallel ────────────────────────────────────────
  await Promise.all([
    upsertDataset(db, "global/yandex_month_kpis.json", rowsToJson(r_yandexMonthKpis.results ?? [])),
    upsertDataset(db, "global/yandex_campaign_kpis.json", rowsToJson(r_yandexCampaignKpis.results ?? [])),
    upsertDataset(db, "yd_hierarchy.json", rowsToJson(r_ydHierarchy as RR[])),
    upsertDataset(db, "global/funnel_stage.json", rowsToJson(r_q4.results ?? [])),
    upsertDataset(db, "global/event_course.json", rowsToJson(r_q5.results ?? [])),
    upsertDataset(db, "cohorts/attacking_january/cohort_assoc_contacts.json", rowsToJson(r_q6.results ?? [])),
    upsertDataset(db, "cohorts/attacking_january/cohort_assoc_event_course.json", rowsToJson(r_q7.results ?? [])),
    upsertDataset(db, "global/yandex_dedup_summary.json", rowsToJson(r_q8.results ?? [])),
    upsertDataset(db, "global/yandex_projects_revenue_raw_vs_dedup.json", rowsToJson(r_q9.results ?? [])),
    upsertDataset(db, "global/yandex_projects_revenue_by_month.json", rowsToJson(r_q10.results ?? [])),
    upsertDataset(db, "qa/yandex_assoc_revenue_qa.json", rowsToJson(qaRows)),
    upsertDataset(db, "global/yandex_projects_revenue_no_month.json", rowsToJson(groupedWithDetails)),
    upsertDataset(db, "qa/other_share_global.json", rowsToJson(r_qa1.results ?? [])),
    upsertDataset(db, "qa/other_top50_cohort.json", rowsToJson(r_qa2.results ?? [])),
    upsertDataset(db, "qa/dedup_check.json", rowsToJson(r_qa3.results ?? [])),
    upsertDataset(db, "qa/yandex_dedup_keys_top_collisions.json", rowsToJson(r_qa4.results ?? [])),
    upsertDataset(db, "qa/yandex_raw_vs_dedup_delta.json", rowsToJson(r_qa5.results ?? [])),
    upsertDataset(db, "qa/yandex_unmatched_to_bitrix.json", rowsToJson(r_qa6.results ?? [])),
    upsertDataset(db, "qa/yandex_campaign_mapping_seed.json", rowsToJson(r_qa7.results ?? [])),
  ]);
  paths.push(
    "global/yandex_month_kpis.json",
    "global/yandex_campaign_kpis.json",
    "yd_hierarchy.json",
    "global/funnel_stage.json",
    "global/event_course.json",
    "cohorts/attacking_january/cohort_assoc_contacts.json",
    "cohorts/attacking_january/cohort_assoc_event_course.json",
    "global/yandex_dedup_summary.json",
    "global/yandex_projects_revenue_raw_vs_dedup.json",
    "global/yandex_projects_revenue_by_month.json",
    "qa/yandex_assoc_revenue_qa.json",
    "global/yandex_projects_revenue_no_month.json",
    "qa/other_share_global.json",
    "qa/other_top50_cohort.json",
    "qa/dedup_check.json",
    "qa/yandex_dedup_keys_top_collisions.json",
    "qa/yandex_raw_vs_dedup_delta.json",
    "qa/yandex_unmatched_to_bitrix.json",
    "qa/yandex_campaign_mapping_seed.json",
  );

  return { paths };
}
