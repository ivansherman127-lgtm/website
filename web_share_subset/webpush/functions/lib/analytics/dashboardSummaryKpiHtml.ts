/**
 * Server-built Weekly Summary table (same KPI order as dashboard).
 * Lets the Worker add new metrics without relying on an updated static JS bundle.
 */
import { getDashboardSummaryKpiPairs } from "../../../shared/dashboardSummaryKpi";

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

export function formatRuDate(iso: string): string {
  const t = iso.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(t)) return t;
  return t.split("-").reverse().join(".");
}

export function buildWeeklySummaryTitle(
  from: string,
  to: string,
  style: "week" | "period",
): string {
  const a = formatRuDate(from);
  const b = formatRuDate(to);
  return style === "week" ? `Неделя ${a} — ${b}` : `Период ${a} — ${b}`;
}

export function buildWeeklySummaryHtml(row: Record<string, unknown>, title: string): string {
  const pairs = getDashboardSummaryKpiPairs(row);
  const body = pairs
    .map(
      ([k, v]) =>
        `<tr><td class="ws-label">${escapeHtml(k)}</td><td class="ws-value">${escapeHtml(v)}</td></tr>`,
    )
    .join("");
  return `<section class="weekly-summary"><h2>${escapeHtml(title)}</h2><table class="ws-table"><tbody>${body}</tbody></table></section>`;
}

export function buildWeeklySummaryTsv(row: Record<string, unknown>, title: string): string {
  const pairs = getDashboardSummaryKpiPairs(row);
  return [title, ...pairs.map(([k, v]) => `${k}\t${v}`)].join("\n");
}

export function enrichDashboardSummaryRows(
  rows: Record<string, unknown>[],
  from: string,
  to: string,
  titleStyle: "week" | "period",
): Record<string, unknown>[] {
  const title = buildWeeklySummaryTitle(from, to, titleStyle);
  return rows.map((r) => ({
    ...r,
    __weekly_summary_html: buildWeeklySummaryHtml(r, title),
    __weekly_summary_tsv: buildWeeklySummaryTsv(r, title),
  }));
}
