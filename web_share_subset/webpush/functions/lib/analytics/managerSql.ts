/**
 * SQL builder functions for manager breakdown reports.
 *
 * Extracted from materializeDatasets.ts to reduce file size.
 * Each builder takes the shared base CTE string and a filter expression,
 * returning the full SQL to pass to db.prepare().
 */

function sqlQuote(value: string): string {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

/**
 * Builds the WHERE filter expression used for the firstline manager segment.
 *
 * Before 2026-02 the "Передан первой линией" column was unreliable, so we always
 * fall back to matching the "Ответственный" column against the firstline name list.
 * From 2026-02 onwards, if the "Передан первой линией" column is present, use it.
 */
export function buildFirstlineFilter(
  nameList: string[],
  hasPassedByFirstLineCol: boolean,
): string {
  if (!nameList.length) return "0";
  const quoted = nameList.map(sqlQuote).join(", ");
  if (!hasPassedByFirstLineCol) {
    return `trim(manager) IN (${quoted})`;
  }
  // Hybrid: for months before 2026-02 use Ответственный; from 2026-02 use Передан первой линией
  return `(
    (month < '2026-02' AND trim(manager) IN (${quoted}))
    OR
    (month >= '2026-02' AND trim(COALESCE(passed_by_firstline, '')) IN (${quoted}))
  )`;
}

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
  potential: string;
  firstlineFilter: string;
  firstlineHybridMode: boolean; // true = use "Передан первой линией" column for month >= 2026-02
  hasPassedByFirstLine: boolean; // true = column "Передан первой линией" exists in raw_bitrix_deals_p01
};

/**
 * Builds the shared `WITH base AS (...)` CTE used by all manager breakdown queries.
 */
export function buildManagerBaseSql(hasRawP01: boolean, exprs: ManagerBaseExprs): string {
  const monthLabel = sqlMonthLabel("m.month");
  if (hasRawP01) {
    return `WITH p01 AS (
         SELECT
           COALESCE("ID", '') AS deal_id,
           MAX(COALESCE(trim("Ответственный"), '')) AS manager,
           MAX(COALESCE(trim("Передан первой линией"), '')) AS passed_by_firstline,
           MAX(COALESCE("Типы некачественного лида", '')) AS "Типы некачественного лида",
           MAX(COALESCE("Типы некачественных лидов", '')) AS "Типы некачественных лидов"
         FROM raw_bitrix_deals_p01
         GROUP BY COALESCE("ID", '')
       ),
       base AS (
         SELECT
           COALESCE(p.manager, '') AS manager,
           m.month,
           ${monthLabel} AS month_label,
           COALESCE(NULLIF(trim(m.course_code_norm), ''), '—') AS course_code,
           COALESCE(p.passed_by_firstline, '') AS passed_by_firstline,
           COALESCE(m."ID", '') AS deal_id,
           COALESCE(m.revenue_amount, 0) AS revenue_amount,
           1 AS is_lead,
           ${exprs.qual} AS is_qual,
           ${exprs.unqual} AS is_unqual,
           ${exprs.refusal} AS is_refusal,
           ${exprs.invalidExpr} AS is_invalid,
           ${exprs.inWork} AS is_in_work,
           ${exprs.potential} AS is_potential,
           CASE WHEN COALESCE(m.is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END AS is_revenue
         FROM mart_deals_enriched m
         LEFT JOIN p01 p ON p.deal_id = m."ID"
       )`;
  }
  return `WITH base AS (
         SELECT
           'Unassigned' AS manager,
           m.month,
           ${monthLabel} AS month_label,
           COALESCE(NULLIF(trim(m.course_code_norm), ''), '—') AS course_code,
           '' AS passed_by_firstline,
           COALESCE(m."ID", '') AS deal_id,
           COALESCE(m.revenue_amount, 0) AS revenue_amount,
           1 AS is_lead,
           0 AS is_qual,
           0 AS is_unqual,
           0 AS is_refusal,
           0 AS is_invalid,
           0 AS is_in_work,
           0 AS is_potential,
           CASE WHEN COALESCE(m.is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END AS is_revenue
         FROM mart_deals_enriched m
       )`;
}

// Shared aggregate columns for manager CTEs
const AGGR = `
           SUM(is_lead) AS leads,
           SUM(is_qual) AS qual,
           SUM(is_unqual) AS unqual,
           SUM(is_refusal) AS refusal,
           SUM(is_in_work) AS in_work,
           SUM(is_invalid) AS invalid_leads,
           SUM(is_revenue) AS paid_deals,
           SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS revenue,
           SUM(is_potential) AS potential,
           SUBSTR(GROUP_CONCAT(DISTINCT deal_id), 1, 50000) AS fl_ids`;

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
         potential AS "В потенциале",
         fl_ids AS "fl_IDs",
         CASE WHEN leads = 0 THEN 0 ELSE qual * 1.0 / leads END AS "Конверсия в Квал",
         CASE WHEN leads = 0 THEN 0 ELSE unqual * 1.0 / leads END AS "Конверсия в Неквал",
         CASE WHEN leads = 0 THEN 0 ELSE refusal * 1.0 / leads END AS "Конверсия в Отказ",
         CASE WHEN leads = 0 THEN 0 ELSE in_work * 1.0 / leads END AS "Конверсия в работе",
         CASE WHEN qual = 0 THEN 0 ELSE paid_deals * 1.0 / qual END AS "Конверсия Квал→Оплата",
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
           month,
           month_label,${AGGR}
         FROM filtered
         GROUP BY manager, month, month_label
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
         '-' AS "Месяц",
         '' AS "month",${KPI},
         '' AS _sort_month
       FROM by_manager
       UNION ALL
       SELECT
         'Месяц' AS "Level",
         manager AS "Менеджер",
         month_label AS "Месяц",
         month AS "month",${KPI},
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
         month AS "month",
         course_code AS "Код курса",
         SUM(is_lead) AS "Лиды",
         SUM(is_qual) AS "Квал",
         SUM(is_unqual) AS "Неквал",
         SUM(is_refusal) AS "Отказы",
         SUM(is_in_work) AS "В работе",
         SUM(is_invalid) AS "Невалидные_лиды",
         SUM(is_revenue) AS "Сделок_с_выручкой",
         SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) AS "Выручка",
         SUM(is_potential) AS "В потенциале",
         SUBSTR(GROUP_CONCAT(DISTINCT deal_id), 1, 50000) AS "fl_IDs",
         CASE WHEN SUM(is_lead) = 0 THEN 0 ELSE SUM(is_qual) * 1.0 / SUM(is_lead) END AS "Конверсия в Квал",
         CASE WHEN SUM(is_lead) = 0 THEN 0 ELSE SUM(is_unqual) * 1.0 / SUM(is_lead) END AS "Конверсия в Неквал",
         CASE WHEN SUM(is_lead) = 0 THEN 0 ELSE SUM(is_refusal) * 1.0 / SUM(is_lead) END AS "Конверсия в Отказ",
         CASE WHEN SUM(is_lead) = 0 THEN 0 ELSE SUM(is_in_work) * 1.0 / SUM(is_lead) END AS "Конверсия в работе",
         CASE WHEN SUM(is_qual) = 0 THEN 0 ELSE SUM(is_revenue) * 1.0 / SUM(is_qual) END AS "Конверсия Квал→Оплата",
         CASE WHEN SUM(is_revenue) = 0 THEN 0 ELSE SUM(CASE WHEN is_revenue = 1 THEN revenue_amount ELSE 0 END) * 1.0 / SUM(is_revenue) END AS "Средний_чек"
       FROM filtered
       GROUP BY manager, month, month_label, course_code
       ORDER BY manager, month_label DESC, course_code`;
}

/**
 * SQL expression that derives an ISO YYYY-MM month from the "Дата оплаты" column
 * of mart_deals_enriched (table alias m).
 */
const PAY_MONTH_OF_M = `CASE
  WHEN COALESCE(m."Дата оплаты", '') LIKE '____-__%' THEN SUBSTR(m."Дата оплаты", 1, 7)
  WHEN COALESCE(m."Дата оплаты", '') LIKE '__.__.____%' THEN SUBSTR(m."Дата оплаты", 7, 4) || '-' || SUBSTR(m."Дата оплаты", 4, 2)
  ELSE ''
END`;

/**
 * Like buildManagerBaseSql but groups by pay_month (payment date) instead of
 * deal-creation month.  Only includes is_revenue_variant3 = 1 deals.
 * Used to produce the PNL manager report variants (_pnl.json).
 */
export function buildManagerPnlBaseSql(hasRawP01: boolean, exprs: ManagerBaseExprs): string {
  const createMonthLabel = sqlMonthLabel("create_month");
  const modifyMonthLabel = sqlMonthLabel("modify_month");
  const payMonthLabel = sqlMonthLabel("pay_month");
  if (hasRawP01) {
    return `WITH p01 AS (
         SELECT
           COALESCE("ID", '') AS deal_id,
           MAX(COALESCE(trim("Ответственный"), '')) AS manager,
           MAX(COALESCE(trim("Передан первой линией"), '')) AS passed_by_firstline,
           MAX(COALESCE("Дата изменения сделки", "Дата изменения", "date_modify", '')) AS modify_raw,
           MAX(COALESCE("Типы некачественного лида", '')) AS "Типы некачественного лида",
           MAX(COALESCE("Типы некачественных лидов", '')) AS "Типы некачественных лидов"
         FROM raw_bitrix_deals_p01
         GROUP BY COALESCE("ID", '')
       ),
       source AS (
         SELECT
           COALESCE(p.manager, '') AS manager,
           COALESCE(m.month, '') AS create_month,
           CASE
             WHEN COALESCE(p.modify_raw, '') LIKE '____-__%' THEN SUBSTR(p.modify_raw, 1, 7)
             WHEN COALESCE(p.modify_raw, '') LIKE '__.__.____%' THEN SUBSTR(p.modify_raw, 7, 4) || '-' || SUBSTR(p.modify_raw, 4, 2)
             ELSE COALESCE(m.month, '')
           END AS modify_month,
           ${PAY_MONTH_OF_M} AS pay_month,
           COALESCE(NULLIF(trim(m.course_code_norm), ''), '—') AS course_code,
           COALESCE(p.passed_by_firstline, '') AS passed_by_firstline,
           COALESCE(m."ID", '') AS deal_id,
           COALESCE(m.revenue_amount, 0) AS revenue_amount,
           ${exprs.qual} AS is_qual,
           ${exprs.unqual} AS is_unqual,
           ${exprs.refusal} AS is_refusal,
           ${exprs.invalidExpr} AS is_invalid,
           ${exprs.inWork} AS is_in_work,
           ${exprs.potential} AS is_potential,
           CASE WHEN COALESCE(m.is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END AS is_revenue
         FROM mart_deals_enriched m
         LEFT JOIN p01 p ON p.deal_id = m."ID"
         WHERE COALESCE(m.month, '') <> ''
       ),
       base AS (
         SELECT
           manager,
           create_month AS month,
           ${createMonthLabel} AS month_label,
           course_code,
           passed_by_firstline,
           deal_id,
           0 AS revenue_amount,
           1 AS is_lead,
           0 AS is_qual,
           0 AS is_unqual,
           0 AS is_refusal,
           0 AS is_invalid,
           0 AS is_in_work,
           0 AS is_potential,
           0 AS is_revenue
         FROM source
         WHERE COALESCE(create_month, '') <> ''
         UNION ALL
         SELECT
           manager,
           modify_month AS month,
           ${modifyMonthLabel} AS month_label,
           course_code,
           passed_by_firstline,
           deal_id,
           0 AS revenue_amount,
           0 AS is_lead,
           is_qual,
           is_unqual,
           is_refusal,
           is_invalid,
           is_in_work,
           is_potential,
           0 AS is_revenue
         FROM source
         WHERE COALESCE(modify_month, '') <> ''
         UNION ALL
         SELECT
           manager,
           pay_month AS month,
           ${payMonthLabel} AS month_label,
           course_code,
           passed_by_firstline,
           deal_id,
           revenue_amount,
           0 AS is_lead,
           0 AS is_qual,
           0 AS is_unqual,
           0 AS is_refusal,
           0 AS is_invalid,
           0 AS is_in_work,
           0 AS is_potential,
           is_revenue
         FROM source
         WHERE COALESCE(pay_month, '') <> ''
           AND is_revenue = 1
       )`;
  }
  return `WITH source AS (
         SELECT
           'Unassigned' AS manager,
           COALESCE(m.month, '') AS create_month,
           COALESCE(m.month, '') AS modify_month,
           ${PAY_MONTH_OF_M} AS pay_month,
           COALESCE(NULLIF(trim(m.course_code_norm), ''), '—') AS course_code,
           '' AS passed_by_firstline,
           COALESCE(m."ID", '') AS deal_id,
           COALESCE(m.revenue_amount, 0) AS revenue_amount,
           ${exprs.qual} AS is_qual,
           ${exprs.unqual} AS is_unqual,
           ${exprs.refusal} AS is_refusal,
           ${exprs.invalidExpr} AS is_invalid,
           ${exprs.inWork} AS is_in_work,
           ${exprs.potential} AS is_potential,
           CASE WHEN COALESCE(m.is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END AS is_revenue
         FROM mart_deals_enriched m
         WHERE COALESCE(m.month, '') <> ''
       ),
       base AS (
         SELECT
           manager,
           create_month AS month,
           ${createMonthLabel} AS month_label,
           course_code,
           passed_by_firstline,
           deal_id,
           0 AS revenue_amount,
           1 AS is_lead,
           0 AS is_qual,
           0 AS is_unqual,
           0 AS is_refusal,
           0 AS is_invalid,
           0 AS is_in_work,
           0 AS is_potential,
           0 AS is_revenue
         FROM source
         WHERE COALESCE(create_month, '') <> ''
         UNION ALL
         SELECT
           manager,
           modify_month AS month,
           ${modifyMonthLabel} AS month_label,
           course_code,
           passed_by_firstline,
           deal_id,
           0 AS revenue_amount,
           0 AS is_lead,
           is_qual,
           is_unqual,
           is_refusal,
           is_invalid,
           is_in_work,
           is_potential,
           0 AS is_revenue
         FROM source
         WHERE COALESCE(modify_month, '') <> ''
         UNION ALL
         SELECT
           manager,
           pay_month AS month,
           ${payMonthLabel} AS month_label,
           course_code,
           passed_by_firstline,
           deal_id,
           revenue_amount,
           0 AS is_lead,
           0 AS is_qual,
           0 AS is_unqual,
           0 AS is_refusal,
           0 AS is_invalid,
           0 AS is_in_work,
           0 AS is_potential,
           is_revenue
         FROM source
         WHERE COALESCE(pay_month, '') <> ''
           AND is_revenue = 1
       )`;
}
