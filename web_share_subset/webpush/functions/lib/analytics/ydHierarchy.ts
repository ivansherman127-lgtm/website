async function tableExists(db: D1Database, tableName: string): Promise<boolean> {
  const row = await db
    .prepare(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1`)
    .bind(tableName)
    .first<{ name: string }>();
  return !!row?.name;
}

function toMonthLabel(monthIso: string): string {
  const month = String(monthIso || "").trim();
  if (!/^\d{4}-\d{2}$/.test(month)) return month;
  const mon = month.slice(5, 7);
  const year = month.slice(0, 4);
  const names: Record<string, string> = {
    "01": "Январь",
    "02": "Февраль",
    "03": "Март",
    "04": "Апрель",
    "05": "Май",
    "06": "Июнь",
    "07": "Июль",
    "08": "Август",
    "09": "Сентябрь",
    "10": "Октябрь",
    "11": "Ноябрь",
    "12": "Декабрь",
  };
  return `${names[mon] || month}, ${year}`;
}

export async function buildYdHierarchyRows(db: D1Database): Promise<Record<string, unknown>[]> {
  const hasStats = await tableExists(db, "stg_yandex_stats");
  const hasLeads = await tableExists(db, "mart_yandex_leads_raw");
  if (!hasStats || !hasLeads) return [];

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
           CASE
             WHEN COALESCE(funnel, '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                  AND COALESCE(stage, '') = 'Получившие демо-доступ' THEN 1
             WHEN COALESCE(funnel, '') IN ('B2B', 'B2C') AND COALESCE(stage, '') <> 'Сделка не заключена' THEN 1
             ELSE 0
           END AS qual,
           CASE
             WHEN COALESCE(funnel, '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                  AND COALESCE(stage, '') <> 'Некачественный лид'
                  AND COALESCE(stage, '') <> 'Получившие демо-доступ' THEN 1
             WHEN COALESCE(funnel, '') = 'Реактивация' AND COALESCE(stage, '') <> 'Некачественный лид' THEN 1
             WHEN lower(COALESCE(stage, '')) LIKE '%неквал%'
                  OR lower(COALESCE(stage, '')) LIKE '%некачеств%'
                  OR lower(COALESCE(stage, '')) LIKE '%дубл%'
                  OR lower(COALESCE(stage, '')) LIKE '%спам%'
                  OR lower(COALESCE(stage, '')) LIKE '%чс%'
                  OR lower(COALESCE(stage, '')) LIKE '%тест%'
                  OR lower(COALESCE(stage, '')) LIKE '%неправильн%данн%'
             THEN 1
             ELSE 0
           END AS unqual,
           CASE
             WHEN COALESCE(funnel, '') IN ('B2B', 'B2C') AND COALESCE(stage, '') = 'Сделка не заключена' THEN 1
             WHEN lower(COALESCE(stage, '')) LIKE '%отказ%' THEN 1
             ELSE 0
           END AS refusal,
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
           CASE
             WHEN COALESCE(funnel, '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                  AND COALESCE(stage, '') = 'Получившие демо-доступ' THEN 1
             WHEN COALESCE(funnel, '') IN ('B2B', 'B2C') AND COALESCE(stage, '') <> 'Сделка не заключена' THEN 1
             ELSE 0
           END AS qual,
           CASE
             WHEN COALESCE(funnel, '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                  AND COALESCE(stage, '') <> 'Некачественный лид'
                  AND COALESCE(stage, '') <> 'Получившие демо-доступ' THEN 1
             WHEN COALESCE(funnel, '') = 'Реактивация' AND COALESCE(stage, '') <> 'Некачественный лид' THEN 1
             WHEN lower(COALESCE(stage, '')) LIKE '%неквал%'
                  OR lower(COALESCE(stage, '')) LIKE '%некачеств%'
                  OR lower(COALESCE(stage, '')) LIKE '%дубл%'
                  OR lower(COALESCE(stage, '')) LIKE '%спам%'
                  OR lower(COALESCE(stage, '')) LIKE '%чс%'
                  OR lower(COALESCE(stage, '')) LIKE '%тест%'
                  OR lower(COALESCE(stage, '')) LIKE '%неправильн%данн%'
             THEN 1
             ELSE 0
           END AS unqual,
           CASE
             WHEN COALESCE(funnel, '') IN ('B2B', 'B2C') AND COALESCE(stage, '') = 'Сделка не заключена' THEN 1
             WHEN lower(COALESCE(stage, '')) LIKE '%отказ%' THEN 1
             ELSE 0
           END AS refusal,
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

  const monthRows = (yandexMonthKpis.results ?? []).map((r) => {
    const leads = Number(r["Leads"] ?? 0) || 0;
    const qual = Number(r["Qual"] ?? 0) || 0;
    const unqual = Number(r["Unqual"] ?? 0) || 0;
    const refusal = Number(r["Refusal"] ?? 0) || 0;
    const spend = Number(r["Расход, ₽"] ?? 0) || 0;
    const clicks = Number(r["Клики"] ?? 0) || 0;
    const month = String(r.month ?? "").trim();
    return {
      "Level": "Month",
      "month": month,
      "Месяц": toMonthLabel(month),
      "№ Кампании": "-",
      "№ Объявления": "-",
      "Название кампании": "-",
      "Заголовок": "-",
      "Название группы": "-",
      "Leads": leads,
      "Qual": qual,
      "Unqual": unqual,
      "Refusal": refusal,
      "Расход, ₽": spend,
      "Клики": clicks,
      "%Qual": leads === 0 ? 0 : (qual * 100.0) / leads,
      "%Unqual": leads === 0 ? 0 : (unqual * 100.0) / leads,
      "%Refusal": qual === 0 ? 0 : (refusal * 100.0) / qual,
      "QPA": qual === 0 ? "-" : String(spend / qual),
      "fl_IDs": null,
      __month: month,
      __ord: 0,
      __campaign: "-",
    } as Record<string, unknown>;
  });

  const campaignRows = (yandexCampaignKpis.results ?? []).map((r) => {
    const leads = Number(r["Leads"] ?? 0) || 0;
    const qual = Number(r["Qual"] ?? 0) || 0;
    const unqual = Number(r["Unqual"] ?? 0) || 0;
    const refusal = Number(r["Refusal"] ?? 0) || 0;
    const spend = Number(r["Расход, ₽"] ?? 0) || 0;
    const clicks = Number(r["Клики"] ?? 0) || 0;
    const month = String(r["Месяц"] ?? "").trim();
    const campaignName = String(r["Название кампании"] ?? "").trim() || "UNMAPPED";
    const campaignId = String(r["№ Кампании"] ?? "").trim() || "-";
    return {
      "Level": "Campaign",
      "month": month,
      "Месяц": toMonthLabel(month),
      "№ Кампании": campaignId,
      "№ Объявления": "-",
      "Название кампании": campaignName,
      "Заголовок": "-",
      "Название группы": "-",
      "Leads": leads,
      "Qual": qual,
      "Unqual": unqual,
      "Refusal": refusal,
      "Расход, ₽": spend,
      "Клики": clicks,
      "%Qual": leads === 0 ? 0 : (qual * 100.0) / leads,
      "%Unqual": leads === 0 ? 0 : (unqual * 100.0) / leads,
      "%Refusal": qual === 0 ? 0 : (refusal * 100.0) / qual,
      "QPA": qual === 0 ? "-" : String(spend / qual),
      "fl_IDs": null,
      __month: month,
      __ord: 1,
      __campaign: campaignName,
    } as Record<string, unknown>;
  });

  return [...monthRows, ...campaignRows]
    .sort((a, b) => {
      const am = String(a.__month ?? "");
      const bm = String(b.__month ?? "");
      if (am !== bm) return bm.localeCompare(am);
      const ao = Number(a.__ord ?? 0);
      const bo = Number(b.__ord ?? 0);
      if (ao !== bo) return ao - bo;
      return String(a.__campaign ?? "").localeCompare(String(b.__campaign ?? ""), "ru");
    })
    .map((row, idx) => {
      const out = { ...row, "Unnamed: 0": idx };
      delete out.__month;
      delete out.__ord;
      delete out.__campaign;
      return out;
    });
}