import { buildLeadLogicSql } from "../lib/analytics/leadLogicSql";
import { buildInvalidTokenCond, sqlQuote } from "../lib/analytics/sqlHelpers";

interface Env {
  DB: D1Database;
}

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

type DimKey = "course" | "project" | "medium";

function isValidDim(v: string | null): v is DimKey {
  return v === "course" || v === "project" || v === "medium";
}

function isIsoMonth(s: string | null): boolean {
  return !!s && /^\d{4}-\d{2}$/.test(s);
}

async function columnExists(db: D1Database, tableName: string, columnName: string): Promise<boolean> {
  const row = await db
    .prepare(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1`)
    .bind(tableName)
    .first<{ sql: string }>();
  if (!row?.sql) return false;
  const sql = String(row.sql);
  return sql.includes(columnName) || sql.includes(`"${columnName}"`) || sql.includes(`'${columnName}'`);
}

function sqlIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

export async function onRequestGet(context: { request: Request; env: Env }): Promise<Response> {
  const url = new URL(context.request.url);
  const dimRaw = url.searchParams.get("dim");
  const fromRaw = url.searchParams.get("from");
  const toRaw = url.searchParams.get("to");

  if (!isValidDim(dimRaw)) {
    return new Response(JSON.stringify({ error: "invalid dim, expected course|project|medium" }), {
      status: 400,
      headers: { "content-type": "application/json; charset=utf-8" },
    });
  }

  const dimExprMap: Record<DimKey, { expr: string; label: string }> = {
    course: {
      expr: `COALESCE(NULLIF(TRIM(course_code_norm), ''), '(без кода)')`,
      label: "Код курса",
    },
    project: {
      expr: `COALESCE(NULLIF(TRIM(event_class), ''), 'Другое')`,
      label: "Проект",
    },
    medium: {
      expr: `COALESCE(NULLIF(TRIM("UTM Medium"), ''), '(без utm_medium)')`,
      label: "UTM Medium",
    },
  };

  const dim = dimRaw;
  const dimSpec = dimExprMap[dim];
  const fromMonth = isIsoMonth(fromRaw) ? fromRaw! : "2000-01";
  const toMonth = isIsoMonth(toRaw) ? toRaw! : "2099-12";

  const db = context.env.DB;

  const hasTypySingular = await columnExists(db, "mart_deals_enriched", "Типы некачественного лида");
  const hasTypyPlural = await columnExists(db, "mart_deals_enriched", "Типы некачественных лидов");
  const extraInvalidCond =
    hasTypySingular || hasTypyPlural ? buildInvalidTokenCond(BITRIX_INVALID_TOKENS, "", "like") : "";

  const ll = buildLeadLogicSql({
    funnelExpr: `"Воронка"`,
    stageExpr: `"Стадия сделки"`,
    monthExpr: "month",
    extraInvalidCond,
  });

  // Fetch distinct funnels for pivot columns, ordered by volume within the date range
  const funnelRes = await db
    .prepare(
      `SELECT COALESCE(NULLIF(TRIM("Воронка"), ''), '(без воронки)') AS funnel, COUNT(*) AS cnt
       FROM mart_deals_enriched
       WHERE COALESCE(month, '') <> '' AND month >= ? AND month <= ?
       GROUP BY funnel
       ORDER BY cnt DESC`,
    )
    .bind(fromMonth, toMonth)
    .all<{ funnel: string; cnt: number }>();

  const ALLOWED_FUNNELS = ["Горячая", "Холодная", "B2C", "B2B", "Реактивация"];
  const allFunnels = (funnelRes.results ?? []).map((r) => r.funnel);
  const funnels = ALLOWED_FUNNELS.filter((f) => allFunnels.includes(f));

  const funnelPivotSql = funnels
    .map((f) => {
      const matchExpr = `funnel_name = ${sqlQuote(f)}`;
      return `SUM(CASE WHEN ${matchExpr} THEN 1 ELSE 0 END) AS ${sqlIdent(f)}`;
    })
    .join(",\n       ");

  const sql = `
    WITH base AS (
      SELECT
        ${dimSpec.expr} AS dim_key,
        ${ll.qual} AS is_qual,
        ${ll.refusal} AS is_refusal,
        ${ll.invalid} AS is_invalid,
        CASE WHEN COALESCE(is_revenue_variant3, 0) = 1 THEN COALESCE(revenue_amount, 0) ELSE 0 END AS rev,
        COALESCE(NULLIF(TRIM("Воронка"), ''), '(без воронки)') AS funnel_name
      FROM mart_deals_enriched
      WHERE COALESCE(month, '') <> ''
        AND month >= ? AND month <= ?
    )
    SELECT
      dim_key AS ${sqlIdent(dimSpec.label)},
      COUNT(*) AS "Лиды",
      SUM(is_qual) AS "Квал",
      SUM(CASE WHEN is_qual = 0 AND is_invalid = 0 AND is_refusal = 0 THEN 1 ELSE 0 END) AS "Неквал",
      SUM(is_invalid) AS "Невалидные",
      SUM(is_refusal) AS "Отказы",
      CASE WHEN COUNT(*) = 0 THEN 0.0 ELSE ROUND(100.0 * SUM(is_qual) / COUNT(*), 1) END AS "Конв. лиды→квал %",
      CASE WHEN COUNT(*) = 0 THEN 0.0 ELSE ROUND(100.0 * SUM(CASE WHEN is_qual = 0 AND is_invalid = 0 AND is_refusal = 0 THEN 1 ELSE 0 END) / COUNT(*), 1) END AS "Конв. лиды→неквал %",
      CASE WHEN SUM(is_qual) = 0 THEN 0.0 ELSE ROUND(100.0 * SUM(is_refusal) / SUM(is_qual), 1) END AS "Конв. квал→отказ %",
      ${funnelPivotSql},
      SUM(rev) AS "Выручка"
    FROM base
    GROUP BY dim_key
    ORDER BY "Лиды" DESC
  `;

  const result = await db
    .prepare(sql)
    .bind(fromMonth, toMonth)
    .all<Record<string, unknown>>();

  return new Response(JSON.stringify(result.results ?? []), {
    status: 200,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store, no-cache",
    },
  });
}
