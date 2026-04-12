/**
 * Single source of truth: Weekly Summary KPI rows (dashboard + API HTML/TSV).
 * Imported by src/app.ts and functions/lib/analytics/dashboardSummaryKpiHtml.ts
 */

function fmtNum(v: unknown): string {
  return v != null && v !== "" ? Number(v).toLocaleString("ru-RU") : "—";
}

function fmtMoney(v: unknown): string {
  return v != null && v !== "" ? "р." + Math.round(Number(v)).toLocaleString("ru-RU") : "—";
}

function fmtPct(v: unknown): string {
  return v != null && v !== "" ? Number(v).toLocaleString("ru-RU") + "%" : "—";
}

/** Label + formatted value for each KPI line (same order everywhere). */
export function getDashboardSummaryKpiPairs(row: Record<string, unknown>): [string, string][] {
  return [
    ["Всего заявок", fmtNum(row["Всего заявок"])],
    ["Конверсия сайта", "—"],
    ["Кол-во квал лидов", fmtNum(row["Квал лидов"])],
    ["Конверсия в квал. лиды", fmtPct(row["Конверсия в квал %"])],
    ["Кол-во оплат", fmtNum(row["Оплат"])],
    ["Конверсия в оплату из квал", fmtPct(row["Конверсия в оплату из квал %"])],
    ["Выручка", fmtMoney(row["Выручка"])],
    ["Средний чек", fmtMoney(row["Средний чек"])],
    ["Бюджет на рекламу", fmtMoney(row["Бюджет на рекламу"])],
    ["Кликов из Яндекса", fmtNum(row["Кликов из Яндекса"])],
    ["Лидов с рекламы", fmtNum(row["Лидов с рекламы"])],
    ["Стоимость лида", fmtMoney(row["Стоимость лида"])],
    ["Рассылок", fmtNum(row["Рассылок"])],
    ["Открытий email", fmtNum(row["Открытий email"])],
    ["Заявок email", fmtNum(row["Заявок email"])],
    ["Рег на Старт в ИБ", fmtNum(row["Рег на Старт в ИБ"])],
  ];
}
