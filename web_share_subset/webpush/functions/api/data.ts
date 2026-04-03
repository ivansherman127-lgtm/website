/**
 * GET /api/data?path=<relative path under public/data>
 */
import { buildYdHierarchyRows } from "../lib/analytics/ydHierarchy";
import { buildBitrixContactsUidRows } from "../lib/analytics/bitrixContactsUid";
import { buildLeadLogicSql, buildPotentialCond } from "../lib/analytics/leadLogicSql";

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

function sqlQuote(value: string): string {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

function buildInvalidTokenCond(tokens: string[]): string {
  return tokens
    .flatMap((token) => [
      `lower(COALESCE("Типы некачественного лида", '')) LIKE ${sqlQuote("%" + token + "%")}`,
      `lower(COALESCE("Типы некачественных лидов", '')) LIKE ${sqlQuote("%" + token + "%")}`,
    ])
    .join(" OR ");
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

async function buildBitrixMonthTotalRows(db: D1Database): Promise<Record<string, unknown>[]> {
  const hasTypySingular = await columnExists(db, "mart_deals_enriched", "Типы некачественного лида");
  const hasTypyPlural = await columnExists(db, "mart_deals_enriched", "Типы некачественных лидов");
  const extraInvalidCond = hasTypySingular || hasTypyPlural ? buildInvalidTokenCond(BITRIX_INVALID_TOKENS) : "";
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
