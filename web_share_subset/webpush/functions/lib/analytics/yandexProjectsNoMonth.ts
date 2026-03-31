import { buildExplicitYandexProjectLabelMap, mapYandexProjectGroup } from "../../../src/yandexProjectGroups";

export function mapYandexProjectNameToLabel(
  projectName: string,
  clusterLabels: string[],
): string | null {
  const mapped = mapYandexProjectGroup(projectName);
  return clusterLabels.includes(mapped) ? mapped : clusterLabels.includes(String(projectName ?? "").trim()) ? String(projectName ?? "").trim() : null;
}

export interface NoMonthRow {
  project_name: string;
  leads_raw: number;
  payments_count: number;
  paid_deals_raw: number;
  revenue_raw: number;
  spend: number;
  assoc_revenue?: number;
}

export function buildYandexProjectLabelMap(
  projectNames: string[],
): Map<string, string> {
  return buildExplicitYandexProjectLabelMap(projectNames);
}

export function buildYandexProjectLabelMapFromRows(
  rows: NoMonthRow[],
): Map<string, string> {
  return buildExplicitYandexProjectLabelMap(rows.map((row) => String(row.project_name ?? "").trim()));
}

export function groupYandexProjectsNoMonth(
  rows: NoMonthRow[],
): NoMonthRow[] {
  if (!rows.length) return [];
  const work = [...rows].sort((a, b) => {
    const r = (b.revenue_raw ?? 0) - (a.revenue_raw ?? 0);
    if (r !== 0) return r;
    return (b.leads_raw ?? 0) - (a.leads_raw ?? 0);
  });
  const agg = new Map<string, NoMonthRow>();
  for (const r of work) {
    const label = mapYandexProjectGroup(r.project_name);
    const ex = agg.get(label);
    if (!ex) {
      agg.set(label, {
        project_name: label,
        leads_raw: r.leads_raw,
        payments_count: r.payments_count,
        paid_deals_raw: r.paid_deals_raw,
        revenue_raw: r.revenue_raw,
        spend: r.spend,
        assoc_revenue: r.assoc_revenue ?? 0,
      });
    } else {
      ex.leads_raw += r.leads_raw;
      ex.payments_count += r.payments_count;
      ex.paid_deals_raw += r.paid_deals_raw;
      ex.revenue_raw += r.revenue_raw;
      ex.spend += r.spend;
      ex.assoc_revenue = (ex.assoc_revenue ?? 0) + (r.assoc_revenue ?? 0);
    }
  }
  return [...agg.values()].sort((a, b) => (b.revenue_raw ?? 0) - (a.revenue_raw ?? 0));
}
