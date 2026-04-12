/**
 * GET /api/data?path=<relative path under public/data>
 */
import { buildYdHierarchyRows } from "../lib/analytics/ydHierarchy";
import { buildBitrixContactsUidRows } from "../lib/analytics/bitrixContactsUid";
import { buildLeadLogicSql, buildPotentialCond } from "../lib/analytics/leadLogicSql";
import { sqlQuote, buildInvalidTokenCond } from "../lib/analytics/sqlHelpers";
import { enrichDashboardSummaryRows } from "../lib/analytics/dashboardSummaryKpiHtml";

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

async function columnExists(db: D1Database, tableName: string, columnName: string): Promise<boolean> {
  const row = await db
    .prepare(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1`)
    .bind(tableName)
    .first<{ sql: string }>();
  if (!row?.sql) return false;
  const sql = String(row.sql);
  return sql.includes(columnName) || sql.includes(`"${columnName}"`) || sql.includes(`'${columnName}'`);
}

async function buildBitrixMonthTotalRows(db: D1Database): Promise<Record<string, unknown>[]> {
  const hasTypySingular = await columnExists(db, "mart_deals_enriched", "Типы некачественного лида");
  const hasTypyPlural = await columnExists(db, "mart_deals_enriched", "Типы некачественных лидов");
  const extraInvalidCond = hasTypySingular || hasTypyPlural ? buildInvalidTokenCond(BITRIX_INVALID_TOKENS, "", "like") : "";
  const bitrixLeadLogic = buildLeadLogicSql({
    funnelExpr: `"Воронка"`,
    stageExpr: `"Стадия сделки"`,
    monthExpr: "month",
    extraInvalidCond,
  });

  const result = await db.prepare(
    `WITH flags AS (
       SELECT
         month,
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
     ORDER BY month`
  ).all<Record<string, unknown>>();

  return result.results ?? [];
}

async function buildDashboardSummaryRows(
  db: D1Database,
  from: string,
  to: string,
): Promise<Record<string, unknown>[]> {
  const hasTypySingular = await columnExists(db, "mart_deals_enriched", "Типы некачественного лида");
  const hasTypyPlural = await columnExists(db, "mart_deals_enriched", "Типы некачественных лидов");
  const extraInvalidCond = hasTypySingular || hasTypyPlural ? buildInvalidTokenCond(BITRIX_INVALID_TOKENS, "", "like") : "";
  const bitrixLeadLogic = buildLeadLogicSql({
    funnelExpr: `"Воронка"`,
    stageExpr: `"Стадия сделки"`,
    monthExpr: "month",
    extraInvalidCond,
  });
  const dFrom = sqlQuote(from);
  const dTo = sqlQuote(to);
  const row = await db.prepare(
    `WITH range AS (
       SELECT ${dFrom} AS d_from, ${dTo} AS d_to
     ),
     bitrix_src AS (
       SELECT
         CASE
           WHEN COALESCE("Дата создания", '') LIKE '____-__-__%' THEN SUBSTR("Дата создания", 1, 10)
           WHEN COALESCE("Дата создания", '') LIKE '__.__.____%' THEN SUBSTR("Дата создания", 7, 4) || '-' || SUBSTR("Дата создания", 4, 2) || '-' || SUBSTR("Дата создания", 1, 2)
           ELSE ''
         END AS created_date,
         ${bitrixLeadLogic.qual} AS is_qual,
         COALESCE(is_revenue_variant3, 0) AS is_revenue,
         COALESCE(revenue_amount, 0) AS revenue_amount,
         LOWER(COALESCE("UTM Source", '')) AS utm_source,
         LOWER(COALESCE("UTM Medium", '')) AS utm_medium,
         LOWER(COALESCE(event_class, '')) AS event_class_lc,
         LOWER(COALESCE("Название сделки", '')) AS deal_name_lower
       FROM mart_deals_enriched
     ),
     bitrix_agg AS (
       SELECT
         COUNT(*) AS total_leads,
         SUM(is_qual) AS qual_leads,
         SUM(is_revenue) AS payments,
         SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS revenue,
         SUM(CASE WHEN utm_source IN ('email', 'sendsay') OR utm_medium = 'email' THEN 1 ELSE 0 END) AS email_leads,
         SUM(CASE WHEN event_class_lc = 'пбх' THEN 1 ELSE 0 END) AS pbh_regs,
         SUM(CASE
           WHEN event_class_lc = 'старт карьеры в иб'
             OR event_class_lc LIKE 'старт карьеры в иб (%'
           THEN 1 ELSE 0
         END) AS start_ib_regs
       FROM bitrix_src
       WHERE created_date BETWEEN (SELECT d_from FROM range) AND (SELECT d_to FROM range)
     ),
     yandex_spend AS (
       SELECT COALESCE(SUM("Расход, ₽"), 0) AS budget
       FROM stg_yandex_stats
       WHERE COALESCE(NULLIF(TRIM(COALESCE("День", '')), ''), '') <> ''
         AND date(NULLIF(TRIM(COALESCE("День", '')), '')) BETWEEN (SELECT d_from FROM range) AND (SELECT d_to FROM range)
     ),
     yandex_clicks AS (
       SELECT COALESCE(SUM(COALESCE("Клики", 0)), 0) AS clicks
       FROM stg_yandex_stats
       WHERE COALESCE(NULLIF(TRIM(COALESCE("День", '')), ''), '') <> ''
         AND date(NULLIF(TRIM(COALESCE("День", '')), '')) BETWEEN (SELECT d_from FROM range) AND (SELECT d_to FROM range)
     ),
     yandex_leads AS (
       SELECT COUNT(*) AS leads
       FROM mart_yandex_leads_raw l
       LEFT JOIN mart_deals_enriched m ON m."ID" = l."ID"
       WHERE (
         CASE
           WHEN COALESCE(m."Дата создания", '') LIKE '____-__-__%' THEN SUBSTR(m."Дата создания", 1, 10)
           WHEN COALESCE(m."Дата создания", '') LIKE '__.__.____%' THEN SUBSTR(m."Дата создания", 7, 4) || '-' || SUBSTR(m."Дата создания", 4, 2) || '-' || SUBSTR(m."Дата создания", 1, 2)
           ELSE ''
         END
       ) BETWEEN (SELECT d_from FROM range) AND (SELECT d_to FROM range)
     ),
     email_agg AS (
       SELECT
         COUNT(*) AS campaigns,
         COALESCE(SUM("Открытий"), 0) AS opens
       FROM stg_email_sends
       WHERE date("Дата отправки") BETWEEN (SELECT d_from FROM range) AND (SELECT d_to FROM range)
     )
     SELECT
       (SELECT d_from FROM range) AS date_from,
       (SELECT d_to FROM range) AS date_to,
       ba.total_leads AS "Всего заявок",
       ba.qual_leads AS "Квал лидов",
       CASE WHEN ba.total_leads > 0 THEN ROUND(ba.qual_leads * 100.0 / ba.total_leads, 1) ELSE 0 END AS "Конверсия в квал %",
       ba.payments AS "Оплат",
       CASE WHEN ba.qual_leads > 0 THEN ROUND(ba.payments * 100.0 / ba.qual_leads, 1) ELSE 0 END AS "Конверсия в оплату из квал %",
       ba.revenue AS "Выручка",
       CASE WHEN ba.payments > 0 THEN ROUND(ba.revenue * 1.0 / ba.payments, 0) ELSE 0 END AS "Средний чек",
       ys.budget AS "Бюджет на рекламу",
       yc.clicks AS "Кликов из Яндекса",
       yl.leads AS "Лидов с рекламы",
       CASE WHEN yl.leads > 0 THEN ROUND(ys.budget * 1.0 / yl.leads, 0) ELSE 0 END AS "Стоимость лида",
       ea.campaigns AS "Рассылок",
       ea.opens AS "Открытий email",
       ba.email_leads AS "Заявок email",
       ba.pbh_regs AS "Рег на ПБХ",
       ba.start_ib_regs AS "Рег на Старт в ИБ",
       ba.start_ib_regs AS "Рег на ИБ"
     FROM bitrix_agg ba, yandex_spend ys, yandex_clicks yc, yandex_leads yl, email_agg ea`
  ).first<Record<string, unknown>>();
  return row ? [row] : [];
}

/** Same Mon–Sun bounds as `weekly_summary` / materialize `last_week` (SQLite `date('now', …)`). */
async function getLastCompletedWeekBounds(db: D1Database): Promise<{ from: string; to: string } | null> {
  const row = await db
    .prepare(
      `SELECT
         date('now', printf('-%d days', (CAST(strftime('%w', 'now') AS INTEGER) + 6) % 7 + 7)) AS mon,
         date('now', printf('-%d days', (CAST(strftime('%w', 'now') AS INTEGER) + 6) % 7 + 1)) AS sun`,
    )
    .first<{ mon: string; sun: string }>();
  const from = row?.mon?.trim() ?? "";
  const to = row?.sun?.trim() ?? "";
  if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) return null;
  return { from, to };
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

  // Always build unique contacts from the canonical UID mapping table.
  // This avoids stale dataset_json snapshots when contact mapping was updated.
  if (path === "bitrix_contacts_uid.json") {
    try {
      const rows = await buildBitrixContactsUidRows(context.env.DB);
      return new Response(JSON.stringify(rows), {
        status: 200,
        headers: {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store, no-cache",
        },
      });
    } catch (err) {
      console.error("buildBitrixContactsUidRows failed", err);
      return new Response(JSON.stringify([]), {
        status: 200,
        headers: {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store, no-cache",
        },
      });
    }
  }
  if (path === "bitrix_month_total_full.json") {
    try {
      const rows = await buildBitrixMonthTotalRows(context.env.DB);
      return new Response(JSON.stringify(rows), {
        status: 200,
        headers: {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store, no-cache",
        },
      });
    } catch (err) {
      console.error("buildBitrixMonthTotalRows failed", err);
      return new Response(JSON.stringify([]), {
        status: 200,
        headers: {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store, no-cache",
        },
      });
    }
  }
  if (path === "dashboard_summary_dynamic.json") {
    const preset = String(url.searchParams.get("preset") ?? "").trim();
    let from = String(url.searchParams.get("from") ?? "").trim();
    let to = String(url.searchParams.get("to") ?? "").trim();
    if (preset === "last_week") {
      const bounds = await getLastCompletedWeekBounds(context.env.DB);
      if (!bounds) {
        return new Response(JSON.stringify([]), {
          status: 200,
          headers: {
            "content-type": "application/json; charset=utf-8",
            "cache-control": "no-store, no-cache",
          },
        });
      }
      from = bounds.from;
      to = bounds.to;
    }
    if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to) || from > to) {
      return new Response(JSON.stringify({ error: "invalid_date_range" }), {
        status: 400,
        headers: { "content-type": "application/json; charset=utf-8" },
      });
    }
    try {
      const rows = await buildDashboardSummaryRows(context.env.DB, from, to);
      const h2Param = String(url.searchParams.get("h2_style") ?? "").trim();
      const titleStyle: "week" | "period" =
        h2Param === "week" ? "week" : h2Param === "period" ? "period" : preset === "last_week" ? "week" : "period";
      let payload: Record<string, unknown>[];
      try {
        payload = enrichDashboardSummaryRows(rows, from, to, titleStyle);
      } catch (enrichErr) {
        console.error("enrichDashboardSummaryRows failed", enrichErr);
        payload = rows;
      }
      return new Response(JSON.stringify(payload), {
        status: 200,
        headers: {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store, no-cache",
          "x-dashboard-summary": "kpi-v2",
        },
      });
    } catch (err) {
      console.error("buildDashboardSummaryRows failed", err);
      return new Response(JSON.stringify([]), {
        status: 200,
        headers: {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store, no-cache",
        },
      });
    }
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
      try {
        const rows = await buildYdHierarchyRows(context.env.DB);
        return new Response(JSON.stringify(rows), {
          status: 200,
          headers: {
            "content-type": "application/json; charset=utf-8",
            "cache-control": "no-store, no-cache",
          },
        });
      } catch (err) {
        console.error("buildYdHierarchyRows failed", err);
        return new Response(JSON.stringify([]), {
          status: 200,
          headers: {
            "content-type": "application/json; charset=utf-8",
            "cache-control": "no-store, no-cache",
          },
        });
      }
    }
    if (path === "bitrix_contacts_uid.json") {
      const rows = await buildBitrixContactsUidRows(context.env.DB);
      return new Response(JSON.stringify(rows), {
        status: 200,
        headers: {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store, no-cache",
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
      "cache-control": "no-store, no-cache",
    },
  });
}
