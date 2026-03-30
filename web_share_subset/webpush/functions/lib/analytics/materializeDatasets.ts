/**
 * Materialize dashboard JSON blobs into dataset_json (same SQL as db/run_all_slices.export_*).
 */
import { groupYandexProjectsNoMonth } from "./yandexProjectsNoMonth";

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

async function upsertDataset(db: D1Database, path: string, body: string): Promise<void> {
  await db.prepare(`DELETE FROM dataset_json WHERE path = ?`).bind(path).run();
  await db
    .prepare(
      `INSERT INTO dataset_json (path, chunk, body, updated_at) VALUES (?, 0, ?, datetime('now'))`,
    )
    .bind(path, body)
    .run();
}

export async function materializeSliceDatasets(db: D1Database): Promise<{ paths: number }> {
  const paths: string[] = [];
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
      `WITH paid_revenue AS (
         SELECT
           CASE
             WHEN COALESCE("Дата оплаты", '') LIKE '____-__%' THEN SUBSTR("Дата оплаты", 1, 7)
             WHEN COALESCE("Дата оплаты", '') LIKE '__.__.____%' THEN SUBSTR("Дата оплаты", 7, 4) || '-' || SUBSTR("Дата оплаты", 4, 2)
             ELSE ''
           END AS pay_month,
           COUNT(*) AS paid_deals,
           SUM(COALESCE(revenue_amount, 0)) AS revenue
         FROM mart_deals_enriched
         WHERE COALESCE(is_revenue_variant3, 0) = 1
         GROUP BY 1
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
       SELECT
         "Период",
         "Сделок_с_выручкой",
         "Выручка",
         "Расход, ₽",
         "Прибыль",
         month
       FROM (
         SELECT
           month AS "Период",
           COALESCE(pr.paid_deals, 0) AS "Сделок_с_выручкой",
           COALESCE(pr.revenue, 0) AS "Выручка",
           COALESCE(sb.spend, 0) AS "Расход, ₽",
           COALESCE(pr.revenue, 0) - COALESCE(sb.spend, 0) AS "Прибыль",
           month,
           0 AS _sort_total
         FROM all_months am
         LEFT JOIN paid_revenue pr ON pr.pay_month = am.month
         LEFT JOIN spend_by_month sb ON sb.spend_month = am.month
         UNION ALL
         SELECT
           'Итого' AS "Период",
           (SELECT COALESCE(SUM(paid_deals), 0) FROM paid_revenue) AS "Сделок_с_выручкой",
           (SELECT COALESCE(SUM(revenue), 0) FROM paid_revenue) AS "Выручка",
           (SELECT COALESCE(SUM(spend), 0) FROM spend_by_month) AS "Расход, ₽",
           (SELECT COALESCE(SUM(revenue), 0) FROM paid_revenue) - (SELECT COALESCE(SUM(spend), 0) FROM spend_by_month) AS "Прибыль",
           '' AS month,
           1 AS _sort_total
       ) q
       ORDER BY month DESC, _sort_total ASC`,
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
           CASE
             WHEN COALESCE("Воронка", '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                  AND COALESCE("Стадия сделки", '') = 'Получившие демо-доступ' THEN 1
             WHEN COALESCE("Воронка", '') IN ('B2B', 'B2C') AND COALESCE("Стадия сделки", '') <> 'Сделка не заключена' THEN 1
             ELSE 0
           END AS is_qual,
           CASE
             WHEN COALESCE("Воронка", '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                  AND COALESCE("Стадия сделки", '') <> 'Некачественный лид'
                  AND COALESCE("Стадия сделки", '') <> 'Получившие демо-доступ' THEN 1
             WHEN COALESCE("Воронка", '') = 'Реактивация' AND COALESCE("Стадия сделки", '') <> 'Некачественный лид' THEN 1
             WHEN lower(COALESCE("Стадия сделки", '')) LIKE '%неквал%'
                  OR lower(COALESCE("Стадия сделки", '')) LIKE '%некачеств%'
                  OR lower(COALESCE("Стадия сделки", '')) LIKE '%дубл%'
                  OR lower(COALESCE("Стадия сделки", '')) LIKE '%спам%'
                  OR lower(COALESCE("Стадия сделки", '')) LIKE '%чс%'
                  OR lower(COALESCE("Стадия сделки", '')) LIKE '%тест%'
                  OR lower(COALESCE("Стадия сделки", '')) LIKE '%неправильн%данн%'
             THEN 1
             ELSE 0
           END AS is_unqual,
           CASE
             WHEN COALESCE("Воронка", '') IN ('B2B', 'B2C') AND COALESCE("Стадия сделки", '') = 'Сделка не заключена' THEN 1
             WHEN lower(COALESCE("Стадия сделки", '')) LIKE '%отказ%' THEN 1
             ELSE 0
           END AS is_refusal,
           CASE
             WHEN lower(COALESCE("Стадия сделки", '')) LIKE '%неквал%'
               OR lower(COALESCE("Стадия сделки", '')) LIKE '%некачеств%'
               OR lower(COALESCE("Стадия сделки", '')) LIKE '%дубл%'
               OR lower(COALESCE("Стадия сделки", '')) LIKE '%спам%'
               OR lower(COALESCE("Стадия сделки", '')) LIKE '%чс%'
               OR lower(COALESCE("Стадия сделки", '')) LIKE '%тест%'
               OR lower(COALESCE("Стадия сделки", '')) LIKE '%неправильн%данн%'
             THEN 1
             ELSE 0
           END AS is_invalid,
           CASE
             WHEN COALESCE("Стадия сделки", '') <> ''
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%сделка заключена%'
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%сделка закрыта%'
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%отказ%'
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%неквал%'
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%спам%'
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%дубл%'
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%чс%'
             THEN 1
             ELSE 0
           END AS is_in_work,
           CASE WHEN COALESCE(is_revenue_variant3, 0) = 1 THEN 1 ELSE 0 END AS is_revenue
         FROM mart_deals_enriched
         WHERE COALESCE(month, '') <> ''
       )
       SELECT
         month AS "Месяц",
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
       GROUP BY month
       ORDER BY month`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "bitrix_month_total_full.json", rowsToJson((bitrixMonthTotal.results ?? []) as Record<string, unknown>[]));
  paths.push("bitrix_month_total_full.json");

  const emailOperationalSummary = await db
    .prepare(
      `WITH deals_by_campaign AS (
         SELECT
           lower(trim(COALESCE("UTM Campaign", ''))) AS utm_campaign_key,
           COUNT(*) AS leads,
           SUM(CASE
                 WHEN COALESCE("Воронка", '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                      AND COALESCE("Стадия сделки", '') = 'Получившие демо-доступ' THEN 1
                 WHEN COALESCE("Воронка", '') IN ('B2B', 'B2C') AND COALESCE("Стадия сделки", '') <> 'Сделка не заключена' THEN 1
                 ELSE 0
               END) AS qual,
           SUM(CASE
                 WHEN COALESCE("Воронка", '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                      AND COALESCE("Стадия сделки", '') <> 'Некачественный лид'
                      AND COALESCE("Стадия сделки", '') <> 'Получившие демо-доступ' THEN 1
                 WHEN COALESCE("Воронка", '') = 'Реактивация' AND COALESCE("Стадия сделки", '') <> 'Некачественный лид' THEN 1
                 WHEN lower(COALESCE("Стадия сделки", '')) LIKE '%неквал%'
                      OR lower(COALESCE("Стадия сделки", '')) LIKE '%некачеств%'
                      OR lower(COALESCE("Стадия сделки", '')) LIKE '%дубл%'
                      OR lower(COALESCE("Стадия сделки", '')) LIKE '%спам%'
                      OR lower(COALESCE("Стадия сделки", '')) LIKE '%чс%'
                      OR lower(COALESCE("Стадия сделки", '')) LIKE '%тест%'
                      OR lower(COALESCE("Стадия сделки", '')) LIKE '%неправильн%данн%'
                 THEN 1
                 ELSE 0
               END) AS unqual,
           SUM(CASE
                 WHEN COALESCE("Воронка", '') IN ('B2B', 'B2C') AND COALESCE("Стадия сделки", '') = 'Сделка не заключена' THEN 1
                 WHEN lower(COALESCE("Стадия сделки", '')) LIKE '%отказ%' THEN 1
                 ELSE 0
               END) AS refusal,
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
         "Актуальная база email",
         "Контактов email (DB)",
         "Рассылок за месяц",
         "Лиды",
         "Сделок с выручкой",
         "Выручка",
         month
       FROM (
         SELECT
           "Период",
           ${goodEmailContactsExpr} AS "Актуальная база email",
           ${totalEmailContactsExpr} AS "Контактов email (DB)",
           sends AS "Рассылок за месяц",
           leads AS "Лиды",
           paid_deals AS "Сделок с выручкой",
           revenue AS "Выручка",
           month,
           0 AS _sort_total
         FROM month_rows
         UNION ALL
         SELECT
           'Итого' AS "Период",
           ${goodEmailContactsExpr} AS "Актуальная база email",
           ${totalEmailContactsExpr} AS "Контактов email (DB)",
           SUM(sends) AS "Рассылок за месяц",
           SUM(leads) AS "Лиды",
           SUM(paid_deals) AS "Сделок с выручкой",
           SUM(revenue) AS "Выручка",
           '' AS month,
           1 AS _sort_total
         FROM month_rows
       ) q
       ORDER BY month DESC, _sort_total ASC`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "email_operational_summary.json", rowsToJson((emailOperationalSummary.results ?? []) as Record<string, unknown>[]));
  paths.push("email_operational_summary.json");

  const emailHierarchyBySend = await db
    .prepare(
      `WITH deals_by_campaign AS (
         SELECT
           lower(trim(COALESCE("UTM Campaign", ''))) AS utm_campaign_key,
           COUNT(*) AS leads,
           SUM(CASE
                 WHEN COALESCE("Воронка", '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                      AND COALESCE("Стадия сделки", '') = 'Получившие демо-доступ' THEN 1
                 WHEN COALESCE("Воронка", '') IN ('B2B', 'B2C') AND COALESCE("Стадия сделки", '') <> 'Сделка не заключена' THEN 1
                 ELSE 0
               END) AS qual,
           SUM(CASE
                 WHEN COALESCE("Воронка", '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                      AND COALESCE("Стадия сделки", '') <> 'Некачественный лид'
                      AND COALESCE("Стадия сделки", '') <> 'Получившие демо-доступ' THEN 1
                 WHEN COALESCE("Воронка", '') = 'Реактивация' AND COALESCE("Стадия сделки", '') <> 'Некачественный лид' THEN 1
                 WHEN lower(COALESCE("Стадия сделки", '')) LIKE '%неквал%'
                      OR lower(COALESCE("Стадия сделки", '')) LIKE '%некачеств%'
                      OR lower(COALESCE("Стадия сделки", '')) LIKE '%дубл%'
                      OR lower(COALESCE("Стадия сделки", '')) LIKE '%спам%'
                      OR lower(COALESCE("Стадия сделки", '')) LIKE '%чс%'
                      OR lower(COALESCE("Стадия сделки", '')) LIKE '%тест%'
                      OR lower(COALESCE("Стадия сделки", '')) LIKE '%неправильн%данн%'
                 THEN 1
                 ELSE 0
               END) AS unqual,
           SUM(CASE
                 WHEN COALESCE("Воронка", '') IN ('B2B', 'B2C') AND COALESCE("Стадия сделки", '') = 'Сделка не заключена' THEN 1
                 WHEN lower(COALESCE("Стадия сделки", '')) LIKE '%отказ%' THEN 1
                 ELSE 0
               END) AS refusal,
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
           COALESCE(dbc.revenue, 0) AS revenue
         FROM stg_email_sends e
         LEFT JOIN deals_by_campaign dbc
           ON dbc.utm_campaign_key = lower(trim(COALESCE(e.utm_campaign, '')))
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
           SUM(revenue) AS revenue
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
           CASE WHEN qual <= leads THEN 'OK' ELSE 'ERR' END AS "QualCheck",
           CASE WHEN uniq_opens = 0 THEN 0 ELSE ROUND(uniq_clicks * 100.0 / uniq_opens, 2) END AS "CTOR%",
           CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(uniq_opens * 100.0 / delivered_total, 2) END AS "%Уник открытий",
           CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(unsub_total * 100.0 / delivered_total, 2) END AS "Конверсия в Отписки",
           CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(leads * 100.0 / delivered_total, 2) END AS "Конверсия в Лиды",
           CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(qual * 100.0 / delivered_total, 2) END AS "Конверсия в Квал",
           CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(unqual * 100.0 / delivered_total, 2) END AS "Конверсия в Неквал",
           CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(refusal * 100.0 / delivered_total, 2) END AS "Конверсия в Отказ",
           revenue AS "Выручка",
           paid_deals AS "Сделок с выручкой",
           CASE WHEN paid_deals = 0 THEN 0 ELSE revenue * 1.0 / paid_deals END AS "Средняя выручка на сделку",
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
         NULL AS "QualCheck",
         CASE WHEN uniq_opens = 0 THEN 0 ELSE ROUND(uniq_clicks * 100.0 / uniq_opens, 2) END AS "CTOR%",
         CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(uniq_opens * 100.0 / delivered_total, 2) END AS "%Уник открытий",
         CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(unsub_total * 100.0 / delivered_total, 2) END AS "Конверсия в Отписки",
         CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(leads * 100.0 / delivered_total, 2) END AS "Конверсия в Лиды",
         CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(qual * 100.0 / delivered_total, 2) END AS "Конверсия в Квал",
         CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(unqual * 100.0 / delivered_total, 2) END AS "Конверсия в Неквал",
         CASE WHEN delivered_total = 0 THEN 0 ELSE ROUND(refusal * 100.0 / delivered_total, 2) END AS "Конверсия в Отказ",
         revenue AS "Выручка",
         paid_deals AS "Сделок с выручкой",
         CASE WHEN paid_deals = 0 THEN 0 ELSE revenue * 1.0 / paid_deals END AS "Средняя выручка на сделку",
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
         "QualCheck",
         "CTOR%",
         "%Уник открытий",
         "Конверсия в Отписки",
         "Конверсия в Лиды",
         "Конверсия в Квал",
         "Конверсия в Неквал",
         "Конверсия в Отказ",
         "Выручка",
         "Сделок с выручкой",
         "Средняя выручка на сделку",
         "Средний остаток по сделке",
         "Рассылок за месяц",
         "Лидов с некорр. email",
         "Доля некорр. email (лиды)",
         month
       FROM send_rows_labeled
       ORDER BY month DESC, "Level" DESC, "Название выпуска" ASC`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "email_hierarchy_by_send.json", rowsToJson((emailHierarchyBySend.results ?? []) as Record<string, unknown>[]));
  paths.push("email_hierarchy_by_send.json");

  const bitrixContactsUid = await db
    .prepare(
      `SELECT DISTINCT COALESCE("Контакт: ID", '') AS uid
       FROM mart_deals_enriched
       WHERE COALESCE("Контакт: ID", '') <> ''
       ORDER BY uid`,
    )
    .all<Record<string, unknown>>();
  await upsertDataset(db, "bitrix_contacts_uid.json", rowsToJson((bitrixContactsUid.results ?? []) as Record<string, unknown>[]));
  paths.push("bitrix_contacts_uid.json");

  const bitrixFunnelMonthCode = await db
    .prepare(
      `WITH flags AS (
         SELECT
           COALESCE(NULLIF(trim(funnel_group), ''), 'Другое') AS funnel_group,
           month,
           COALESCE(NULLIF(trim(course_code_norm), ''), '—') AS course_code,
           COALESCE(revenue_amount, 0) AS revenue_amount,
           CASE
             WHEN COALESCE("Воронка", '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                  AND COALESCE("Стадия сделки", '') = 'Получившие демо-доступ' THEN 1
             WHEN COALESCE("Воронка", '') IN ('B2B', 'B2C') AND COALESCE("Стадия сделки", '') <> 'Сделка не заключена' THEN 1
             ELSE 0
           END AS is_qual,
           CASE
             WHEN COALESCE("Воронка", '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                  AND COALESCE("Стадия сделки", '') <> 'Некачественный лид'
                  AND COALESCE("Стадия сделки", '') <> 'Получившие демо-доступ' THEN 1
             WHEN COALESCE("Воронка", '') = 'Реактивация' AND COALESCE("Стадия сделки", '') <> 'Некачественный лид' THEN 1
             WHEN lower(COALESCE("Стадия сделки", '')) LIKE '%неквал%'
                  OR lower(COALESCE("Стадия сделки", '')) LIKE '%некачеств%'
                  OR lower(COALESCE("Стадия сделки", '')) LIKE '%дубл%'
                  OR lower(COALESCE("Стадия сделки", '')) LIKE '%спам%'
                  OR lower(COALESCE("Стадия сделки", '')) LIKE '%чс%'
                  OR lower(COALESCE("Стадия сделки", '')) LIKE '%тест%'
                  OR lower(COALESCE("Стадия сделки", '')) LIKE '%неправильн%данн%'
             THEN 1
             ELSE 0
           END AS is_unqual,
           CASE
             WHEN COALESCE("Воронка", '') IN ('B2B', 'B2C') AND COALESCE("Стадия сделки", '') = 'Сделка не заключена' THEN 1
             WHEN lower(COALESCE("Стадия сделки", '')) LIKE '%отказ%' THEN 1
             ELSE 0
           END AS is_refusal,
           CASE
             WHEN lower(COALESCE("Стадия сделки", '')) LIKE '%неквал%'
               OR lower(COALESCE("Стадия сделки", '')) LIKE '%некачеств%'
               OR lower(COALESCE("Стадия сделки", '')) LIKE '%дубл%'
               OR lower(COALESCE("Стадия сделки", '')) LIKE '%спам%'
               OR lower(COALESCE("Стадия сделки", '')) LIKE '%чс%'
               OR lower(COALESCE("Стадия сделки", '')) LIKE '%тест%'
               OR lower(COALESCE("Стадия сделки", '')) LIKE '%неправильн%данн%'
             THEN 1
             ELSE 0
           END AS is_invalid,
           CASE
             WHEN COALESCE("Стадия сделки", '') <> ''
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%сделка заключена%'
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%сделка закрыта%'
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%отказ%'
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%неквал%'
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%спам%'
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%дубл%'
              AND lower(COALESCE("Стадия сделки", '')) NOT LIKE '%чс%'
             THEN 1
             ELSE 0
           END AS is_in_work,
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
           CASE
             WHEN COALESCE(m."Воронка", '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                  AND COALESCE(m."Стадия сделки", '') = 'Получившие демо-доступ' THEN 1
             WHEN COALESCE(m."Воронка", '') IN ('B2B', 'B2C') AND COALESCE(m."Стадия сделки", '') <> 'Сделка не заключена' THEN 1
             ELSE 0
           END AS is_qual,
           CASE
             WHEN COALESCE(m."Воронка", '') IN ('Входящие лиды', 'Горячая воронка', 'Холодная воронка', 'Карьерная консультация')
                  AND COALESCE(m."Стадия сделки", '') <> 'Некачественный лид'
                  AND COALESCE(m."Стадия сделки", '') <> 'Получившие демо-доступ' THEN 1
             WHEN COALESCE(m."Воронка", '') = 'Реактивация' AND COALESCE(m."Стадия сделки", '') <> 'Некачественный лид' THEN 1
             WHEN lower(COALESCE(m."Стадия сделки", '')) LIKE '%неквал%'
                  OR lower(COALESCE(m."Стадия сделки", '')) LIKE '%некачеств%'
                  OR lower(COALESCE(m."Стадия сделки", '')) LIKE '%дубл%'
                  OR lower(COALESCE(m."Стадия сделки", '')) LIKE '%спам%'
                  OR lower(COALESCE(m."Стадия сделки", '')) LIKE '%чс%'
                  OR lower(COALESCE(m."Стадия сделки", '')) LIKE '%тест%'
                  OR lower(COALESCE(m."Стадия сделки", '')) LIKE '%неправильн%данн%'
             THEN 1
             ELSE 0
           END AS is_unqual,
           CASE
             WHEN COALESCE(m."Воронка", '') IN ('B2B', 'B2C') AND COALESCE(m."Стадия сделки", '') = 'Сделка не заключена' THEN 1
             WHEN lower(COALESCE(m."Стадия сделки", '')) LIKE '%отказ%' THEN 1
             ELSE 0
           END AS is_refusal,
           CASE
             WHEN lower(COALESCE(m."Стадия сделки", '')) LIKE '%неквал%'
               OR lower(COALESCE(m."Стадия сделки", '')) LIKE '%некачеств%'
               OR lower(COALESCE(m."Стадия сделки", '')) LIKE '%дубл%'
               OR lower(COALESCE(m."Стадия сделки", '')) LIKE '%спам%'
               OR lower(COALESCE(m."Стадия сделки", '')) LIKE '%чс%'
               OR lower(COALESCE(m."Стадия сделки", '')) LIKE '%тест%'
               OR lower(COALESCE(m."Стадия сделки", '')) LIKE '%неправильн%данн%'
             THEN 1
             ELSE 0
           END AS is_invalid,
           CASE
             WHEN COALESCE(m."Стадия сделки", '') <> ''
              AND lower(COALESCE(m."Стадия сделки", '')) NOT LIKE '%сделка заключена%'
              AND lower(COALESCE(m."Стадия сделки", '')) NOT LIKE '%сделка закрыта%'
              AND lower(COALESCE(m."Стадия сделки", '')) NOT LIKE '%отказ%'
              AND lower(COALESCE(m."Стадия сделки", '')) NOT LIKE '%неквал%'
              AND lower(COALESCE(m."Стадия сделки", '')) NOT LIKE '%спам%'
              AND lower(COALESCE(m."Стадия сделки", '')) NOT LIKE '%дубл%'
              AND lower(COALESCE(m."Стадия сделки", '')) NOT LIKE '%чс%'
             THEN 1
             ELSE 0
           END AS is_in_work,
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
  const firstlineFilter = `trim(manager) IN ('Алена Тиханова', 'Георгий Воеводин')`;
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
  await upsertDataset(db, "global/yandex_campaign_kpis.json", rowsToJson((yandexCampaignKpis.results ?? []) as Record<string, unknown>[]));
  paths.push("global/yandex_campaign_kpis.json");

  const toMonthLabel = (monthIso: string): string => {
    const m = String(monthIso || "").trim();
    if (!/^\d{4}-\d{2}$/.test(m)) return m;
    const mon = m.slice(5, 7);
    const year = m.slice(0, 4);
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
    return `${names[mon] || m}, ${year}`;
  };

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

  const ydHierarchyRows = [...monthRows, ...campaignRows]
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

  const assocQaSql = hasContactsUid
    ? `WITH
       -- Build ad_id → project_name lookup from Yandex stats (same as assoc-revenue.ts).
       yandex_map AS (
         SELECT
           REPLACE(TRIM(COALESCE("№ Объявления", '')), '.0', '') AS ad_id,
           MIN(NULLIF(TRIM(COALESCE("Название кампании", '')), '')) AS project_name
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
           COALESCE(ym.project_name, 'UNMAPPED') AS project_name
         FROM mart_deals_enriched src
         LEFT JOIN yandex_map ym
           ON ym.ad_id = REPLACE(TRIM(COALESCE(src."UTM Content", '')), '.0', '')
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
           MIN(NULLIF(TRIM(COALESCE("Название кампании", '')), '')) AS project_name
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
           COALESCE(ym.project_name, 'UNMAPPED') AS project_name
         FROM mart_deals_enriched src
         LEFT JOIN yandex_map ym
           ON ym.ad_id = REPLACE(TRIM(COALESCE(src."UTM Content", '')), '.0', '')
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

  const q11assocQa = await db.prepare(assocQaSql).all<Record<string, unknown>>();
  const assocQaRows = q11assocQa.results ?? [];

  // Build the assoc revenue lookup map (feeds into yandex_projects_revenue_no_month.json).
  const assocRevenueByProject = new Map<string, number>();
  for (const r of assocQaRows) {
    const pn = String(r.project_name ?? "").trim();
    if (pn) assocRevenueByProject.set(pn, Number(r.assoc_revenue ?? 0) || 0);
  }

  // Materialize the per-project QA dataset.
  const qaRows = assocQaRows.map((r) => ({
    "Проект": String(r.project_name ?? "").trim(),
    "Лиды_Yandex": Number(r["Лиды_Yandex"] ?? 0) || 0,
    "Контактов_в_пуле": Number(r["Контактов_в_пуле"] ?? 0) || 0,
    "Сделок_Bitrix": Number(r["Сделок_Bitrix"] ?? 0) || 0,
    "Ассоц. Выручка": Number(r.assoc_revenue ?? 0) || 0,
  }));
  await upsertDataset(db, "qa/yandex_assoc_revenue_qa.json", rowsToJson(qaRows));
  paths.push("qa/yandex_assoc_revenue_qa.json");

  const q11rows = (q11.results ?? []).map((r) => ({
    project_name: String(r.project_name ?? "").trim(),
    leads_raw: Number(r.leads_raw ?? 0) || 0,
    payments_count: Number(r.payments_count ?? 0) || 0,
    paid_deals_raw: Number(r.paid_deals_raw ?? 0) || 0,
    revenue_raw: Number(r.revenue_raw ?? 0) || 0,
    spend: Number(r.spend ?? 0) || 0,
    assoc_revenue: assocRevenueByProject.get(String(r.project_name ?? "").trim()) ?? 0,
  }));

  const grouped = groupYandexProjectsNoMonth(q11rows);
  await upsertDataset(db, "global/yandex_projects_revenue_no_month.json", JSON.stringify(grouped));
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

  return { paths: paths.length };
}
