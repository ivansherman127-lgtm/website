/**
 * SQL builder functions for manager breakdown reports.
 *
 * Extracted from materializeDatasets.ts to reduce file size.
 * Each builder takes the shared base CTE string and a filter expression,
 * returning the full SQL to pass to db.prepare().
 */

/**
 * Returns a SQLite CASE expression that converts a YYYY-MM month expression
 * to a Russian month-year label like "Январь, 2025".
 *
 * @param monthExpr - SQL expression yielding a YYYY-MM string (e.g. `month`, `m.month`)
 */
export function sqlMonthLabel(monthExpr: string): string {
  const d = `${monthExpr} || '-01'`;
  return `CASE strftime('%m', ${d})
             WHEN '01' THEN 'Январь, ' || strftime('%Y', ${d})
             WHEN '02' THEN 'Февраль, ' || strftime('%Y', ${d})
             WHEN '03' THEN 'Март, ' || strftime('%Y', ${d})
             WHEN '04' THEN 'Апрель, ' || strftime('%Y', ${d})
             WHEN '05' THEN 'Май, ' || strftime('%Y', ${d})
             WHEN '06' THEN 'Июнь, ' || strftime('%Y', ${d})
             WHEN '07' THEN 'Июль, ' || strftime('%Y', ${d})
             WHEN '08' THEN 'Август, ' || strftime('%Y', ${d})
             WHEN '09' THEN 'Сентябрь, ' || strftime('%Y', ${d})
             WHEN '10' THEN 'Октябрь, ' || strftime('%Y', ${d})
             WHEN '11' THEN 'Ноябрь, ' || strftime('%Y', ${d})
             WHEN '12' THEN 'Декабрь, ' || strftime('%Y', ${d})
             ELSE ${monthExpr}
           END`;
}

export type ManagerBaseExprs = {
  qual: string;
  unqual: string;
  refusal: string;
  invalidExpr: string;
  inWork: string;
};

/**
 * Builds the shared `WITH base AS (...)` CTE used by all manager breakdown queries.
 */
export function buildManagerBaseSql(hasRawP01: boolean, exprs: ManagerBaseExprs): string {
  const monthLabel = sqlMonthLabel("m.month");
  if (hasRawP01) {
    return `WITH base AS (
         SELECT
           COALESCE(trim(p."Ответственный"), '') AS manager,
           m.month,
           ${monthLabel} AS month_label,
           COALESCE(NULLIF(trim(m.course_code_norm), ''), '—') AS course_code,
           COALESCE(m."ID", '') AS deal_id,
           COALESCE(m.revenue_amount, 0) AS revenue_amount,
           ${exprs.qual} AS is_qual,
           ${exprs.unqual} AS is_unqual,
           ${exprs.refusal} AS is_refusal,
           ${exprs.invalidExpr} AS is_invalid,
           ${exprs.inWork} AS is_in_work,
           CASE WHEN COALESCE(m.is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END AS is_revenue
         FROM mart_deals_enriched m
         LEFT JOIN raw_bitrix_deals_p01 p ON p."ID" = m."ID"
         WHERE COALESCE(m.month, '') <> ''
       )`;
  }
  return `WITH base AS (
         SELECT
           'Unassigned' AS manager,
           m.month,
           ${monthLabel} AS month_label,
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
}

// Shared aggregate columns for manager CTEs
const AGGR = `
           COUNT(*) AS leads,
           SUM(is_qual) AS qual,
           SUM(is_unqual) AS unqual,
           SUM(is_refusal) AS refusal,
           SUM(is_in_work) AS in_work,
           SUM(is_invalid) AS invalid_leads,
           SUM(is_revenue) AS paid_deals,
           SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS revenue,
           SUBSTR(GROUP_CONCAT(deal_id), 1, 50000) AS fl_ids`;

// Shared KPI output column list (used in both Manager header and detail rows)
const KPI = `
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
         CASE WHEN paid_deals = 0 THEN 0 ELSE revenue * 1.0 / paid_deals END AS "Средний_чек"`;

/**
 * Manager header + Month detail hierarchy.
 * Used for manager_firstline_by_month and manager_sales_by_month.
 */
export function buildManagerByMonthSql(baseSql: string, filter: string): string {
  return `${baseSql},
       filtered AS (
         SELECT * FROM base WHERE ${filter}
       ),
       by_month AS (
         SELECT
           manager,
           month_label,${AGGR}
         FROM filtered
         GROUP BY manager, month_label
       ),
       by_manager AS (
         SELECT
           manager,${AGGR}
         FROM filtered
         GROUP BY manager
       )
       SELECT
         'Manager' AS "Level",
         manager AS "Менеджер",
         '-' AS "Месяц",${KPI},
         '' AS _sort_month
       FROM by_manager
       UNION ALL
       SELECT
         'Месяц' AS "Level",
         manager AS "Менеджер",
         month_label AS "Месяц",${KPI},
         month_label AS _sort_month
       FROM by_month
       ORDER BY "Менеджер", "Level" DESC, _sort_month`;
}

/**
 * Manager header + Course detail hierarchy.
 * Used for manager_firstline_by_course and manager_sales_by_course.
 */
export function buildManagerByCourseSql(baseSql: string, filter: string): string {
  return `${baseSql},
       filtered AS (
         SELECT * FROM base WHERE ${filter}
       ),
       by_course AS (
         SELECT
           manager,
           course_code,${AGGR}
         FROM filtered
         GROUP BY manager, course_code
       ),
       by_manager AS (
         SELECT
           manager,${AGGR}
         FROM filtered
         GROUP BY manager
       )
       SELECT
         'Manager' AS "Level",
         manager AS "Менеджер",
         '-' AS "Код курса",${KPI}
       FROM by_manager
       UNION ALL
       SELECT
         'Код курса' AS "Level",
         manager AS "Менеджер",
         course_code AS "Код курса",${KPI}
       FROM by_course
       ORDER BY "Менеджер", "Level" DESC, "Код курса"`;
}

/**
 * Flat course × month breakdown (no hierarchy rows).
 * Used for manager_firstline_by_course_month and manager_sales_by_course_month.
 */
export function buildManagerByCourseMonthSql(baseSql: string, filter: string): string {
  return `${baseSql},
       filtered AS (
         SELECT * FROM base WHERE ${filter}
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
       ORDER BY manager, month_label DESC, course_code`;
}
