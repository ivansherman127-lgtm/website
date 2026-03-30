/**
 * GET /api/cohort-deals
 */
interface Env {
  DB: D1Database;
}

const COHORT_DEALS_SQL = `
SELECT
  "ID",
  "Контакт: ID",
  "Название сделки",
  "Дата создания",
  month,
  "Воронка",
  funnel_group,
  "Стадия сделки",
  "Сделка закрыта",
  "Дата оплаты",
  "Сумма",
  revenue_amount AS "Выручка",
  CASE WHEN is_revenue_variant3 = 1 THEN 'true' ELSE 'false' END AS "Выручка_учитывается",
  "UTM Source",
  "UTM Medium",
  "UTM Campaign",
  "Код_курса_сайт",
  "Код курса",
  course_code_norm AS "Нормализованный_код_курса",
  course_code_norm AS "Код_курса_норм",
  event_class AS "Мероприятие",
  classification_source,
  classification_pattern,
  classification_confidence,
  is_attacking_january
FROM mart_attacking_january_cohort_deals
`;

export async function onRequestGet(context: { env: Env }): Promise<Response> {
  const res = await context.env.DB.prepare(COHORT_DEALS_SQL).all();
  const rows = (res as { results?: unknown[] }).results ?? [];
  return Response.json(rows, {
    headers: { "cache-control": "public, max-age=60" },
  });
}
