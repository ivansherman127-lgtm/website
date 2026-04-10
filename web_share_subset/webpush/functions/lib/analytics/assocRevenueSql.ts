/**
 * SQL builder functions for Yandex associated-revenue QA and ad-performance reports.
 *
 * Extracted from materializeDatasets.ts to reduce file size.
 * Each builder accepts pre-computed SQL expression strings via AssocRevenueExprs.
 *
 * Associated-revenue definition:
 *   A paid deal (is_revenue_variant3=1) is counted for a project if:
 *   1. The contact appears in a Yandex-sourced deal for that project.
 *   2. The deal's payment date (Дата оплаты) is strictly AFTER the contact's
 *      earliest registration date for that project (creation date of the Yandex deal).
 */

export type AssocRevenueExprs = {
  /** SQL expression for project group from stg_yandex_stats.  Applied to "Название кампании". */
  groupedStatsProjectExpr: string;
  /** SQL expression to pass-through the already-mapped ym.project_name with UNMAPPED fallback. */
  groupedMappedProjectExpr: string;
  /** SQL expression to extract a Yandex ad ID from src."UTM Content". */
  sourceYandexAdExpr: string;
  /** SQL boolean condition validating a Yandex ad ID (11-digit, starts with "17"). */
  validSourceYandexAdExpr: string;
};

// ---------------------------------------------------------------------------
// Shared date-normalization SQL snippets
// ---------------------------------------------------------------------------

/** Normalise src."Дата создания" → ISO YYYY-MM-DD (or '' if unrecognised). */
const SRC_REG_DATE = `CASE
           WHEN COALESCE(src."Дата создания", '') LIKE '____-__-__%' THEN SUBSTR(src."Дата создания", 1, 10)
           WHEN COALESCE(src."Дата создания", '') LIKE '__.__.____%' THEN SUBSTR(src."Дата создания", 7, 4) || '-' || SUBSTR(src."Дата создания", 4, 2) || '-' || SUBSTR(src."Дата создания", 1, 2)
           ELSE ''
         END`;

/** Normalise d."Дата оплаты" → ISO YYYY-MM-DD (or '' if unrecognised). */
const DEAL_PAID_DATE = `CASE
           WHEN COALESCE(d."Дата оплаты", '') LIKE '____-__-__%' THEN SUBSTR(d."Дата оплаты", 1, 10)
           WHEN COALESCE(d."Дата оплаты", '') LIKE '__.__.____%' THEN SUBSTR(d."Дата оплаты", 7, 4) || '-' || SUBSTR(d."Дата оплаты", 4, 2) || '-' || SUBSTR(d."Дата оплаты", 1, 2)
           ELSE ''
         END`;

/**
 * Builds the per-project associated-revenue QA SQL.
 * Returns the contacts-uid-aware variant when hasContactsUid is true.
 */
export function buildAssocQaSql(hasContactsUid: boolean, exprs: AssocRevenueExprs): string {
  const { groupedStatsProjectExpr, groupedMappedProjectExpr, sourceYandexAdExpr } = exprs;

  if (hasContactsUid) {
    return `WITH
       -- Build ad_id → project_name lookup from Yandex stats (same as assoc-revenue.ts).
       yandex_map AS (
         SELECT
           REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') AS ad_id,
           MIN(${groupedStatsProjectExpr}) AS project_name
         FROM stg_yandex_stats
         WHERE REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') <> ''
         GROUP BY 1
       ),
       -- All Yandex-sourced deals with their mapped project name and registration date.
       -- reg_date = creation date of this Yandex-sourced deal (ISO YYYY-MM-DD).
       yandex_source_deals AS (
         SELECT
           REPLACE(TRIM(COALESCE(src."Контакт: ID", '')), '.0', '') AS contact_id,
           COALESCE(${groupedMappedProjectExpr}, 'UNMAPPED') AS project_name,
           ${SRC_REG_DATE} AS reg_date
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
       --   Track earliest reg_date per project+uid (first touch across all linked contact_ids).
       campaign_uid_pool AS (
         SELECT
           ysd.project_name,
           COALESCE(cu.contact_uid, ysd.contact_id) AS contact_uid,
           MIN(CASE WHEN ysd.reg_date <> '' THEN ysd.reg_date ELSE NULL END) AS reg_date
         FROM yandex_source_deals ysd
         LEFT JOIN stg_contacts_uid cu ON cu.contact_id = ysd.contact_id
         WHERE ysd.contact_id <> ''
         GROUP BY ysd.project_name, COALESCE(cu.contact_uid, ysd.contact_id)
       ),
       -- Step 2: Expand each contact_uid to ALL of its associated Bitrix contact_ids.
       all_pool_contact_ids AS (
         SELECT DISTINCT
           cup.project_name,
           REPLACE(TRIM(COALESCE(cu2.contact_id, cup.contact_uid)), '.0', '') AS contact_id,
           cup.reg_date
         FROM campaign_uid_pool cup
         LEFT JOIN stg_contacts_uid cu2 ON cu2.contact_uid = cup.contact_uid
         WHERE REPLACE(TRIM(COALESCE(cu2.contact_id, cup.contact_uid)), '.0', '') <> ''
       ),
       -- Step 3: Paid deals where payment date is AFTER the contact's registration date.
       contact_deals AS (
         SELECT apc.project_name, d."ID" AS deal_id, d.revenue_amount
         FROM all_pool_contact_ids apc
         JOIN mart_deals_enriched d
           ON REPLACE(TRIM(COALESCE(d."Контакт: ID", '')), '.0', '') = apc.contact_id
         WHERE d.is_revenue_variant3 = 1
           AND apc.reg_date IS NOT NULL
           AND (${DEAL_PAID_DATE}) > apc.reg_date
       ),
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
       ORDER BY assoc_revenue DESC`;
  }

  return `WITH
       -- Build ad_id → project_name lookup from Yandex stats (same as assoc-revenue.ts).
       yandex_map AS (
         SELECT
           REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') AS ad_id,
           MIN(${groupedStatsProjectExpr}) AS project_name
         FROM stg_yandex_stats
         WHERE REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') <> ''
         GROUP BY 1
       ),
       -- All Yandex-sourced deals with their mapped project name and registration date.
       yandex_source_deals AS (
         SELECT
           REPLACE(TRIM(COALESCE(src."Контакт: ID", '')), '.0', '') AS contact_id,
           COALESCE(${groupedMappedProjectExpr}, 'UNMAPPED') AS project_name,
           ${SRC_REG_DATE} AS reg_date
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
       -- Step 1: Earliest registration date per project+contact.
       campaign_contacts AS (
         SELECT
           project_name,
           contact_id,
           MIN(CASE WHEN reg_date <> '' THEN reg_date ELSE NULL END) AS reg_date
         FROM yandex_source_deals
         WHERE contact_id <> ''
         GROUP BY project_name, contact_id
       ),
       -- Step 2: Paid deals where payment date is AFTER the contact's registration date.
       contact_deals AS (
         SELECT cc.project_name, d."ID" AS deal_id, d.revenue_amount
         FROM campaign_contacts cc
         JOIN mart_deals_enriched d
           ON REPLACE(TRIM(COALESCE(d."Контакт: ID", '')), '.0', '') = cc.contact_id
         WHERE d.is_revenue_variant3 = 1
           AND cc.reg_date IS NOT NULL
           AND (${DEAL_PAID_DATE}) > cc.reg_date
       ),
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
}

/**
 * Builds the per-ad associated-revenue QA SQL (ad-level breakdown of assocQaSql).
 */
export function buildAssocQaByAdSql(hasContactsUid: boolean, exprs: AssocRevenueExprs): string {
  const { groupedStatsProjectExpr, groupedMappedProjectExpr, sourceYandexAdExpr, validSourceYandexAdExpr } = exprs;

  if (hasContactsUid) {
    return `WITH
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
           COALESCE(${groupedMappedProjectExpr}, 'UNMAPPED') AS project_name,
           ${SRC_REG_DATE} AS reg_date
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
         SELECT
           ysd.project_name,
           ysd.ad_id,
           COALESCE(cu.contact_uid, ysd.contact_id) AS contact_uid,
           MIN(CASE WHEN ysd.reg_date <> '' THEN ysd.reg_date ELSE NULL END) AS reg_date
         FROM yandex_source_deals ysd
         LEFT JOIN stg_contacts_uid cu ON cu.contact_id = ysd.contact_id
         WHERE ysd.contact_id <> ''
         GROUP BY ysd.project_name, ysd.ad_id, COALESCE(cu.contact_uid, ysd.contact_id)
       ),
       all_pool_contact_ids AS (
         SELECT DISTINCT
           cup.project_name,
           cup.ad_id,
           REPLACE(TRIM(COALESCE(cu2.contact_id, cup.contact_uid)), '.0', '') AS contact_id,
           cup.reg_date
         FROM campaign_uid_pool cup
         LEFT JOIN stg_contacts_uid cu2 ON cu2.contact_uid = cup.contact_uid
         WHERE REPLACE(TRIM(COALESCE(cu2.contact_id, cup.contact_uid)), '.0', '') <> ''
       ),
       contact_deals AS (
         SELECT apc.project_name, apc.ad_id, d."ID" AS deal_id, d.revenue_amount
         FROM all_pool_contact_ids apc
         JOIN mart_deals_enriched d
           ON REPLACE(TRIM(COALESCE(d."Контакт: ID", '')), '.0', '') = apc.contact_id
         WHERE d.is_revenue_variant3 = 1
           AND apc.reg_date IS NOT NULL
           AND (${DEAL_PAID_DATE}) > apc.reg_date
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
       ORDER BY assoc_revenue DESC`;
  }

  return `WITH
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
           COALESCE(${groupedMappedProjectExpr}, 'UNMAPPED') AS project_name,
           ${SRC_REG_DATE} AS reg_date
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
         SELECT
           project_name,
           ad_id,
           contact_id,
           MIN(CASE WHEN reg_date <> '' THEN reg_date ELSE NULL END) AS reg_date
         FROM yandex_source_deals
         WHERE contact_id <> ''
         GROUP BY project_name, ad_id, contact_id
       ),
       contact_deals AS (
         SELECT cc.project_name, cc.ad_id, d."ID" AS deal_id, d.revenue_amount
         FROM campaign_contacts cc
         JOIN mart_deals_enriched d
           ON REPLACE(TRIM(COALESCE(d."Контакт: ID", '')), '.0', '') = cc.contact_id
         WHERE d.is_revenue_variant3 = 1
           AND cc.reg_date IS NOT NULL
           AND (${DEAL_PAID_DATE}) > cc.reg_date
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
}

/**
 * Builds the ad-performance SQL (spend + leads + revenue per ad, all time, no month grouping).
 */
export function buildAdPerfSql(exprs: AssocRevenueExprs): string {
  const { groupedStatsProjectExpr, groupedMappedProjectExpr, sourceYandexAdExpr, validSourceYandexAdExpr } = exprs;
  return `WITH
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
}
