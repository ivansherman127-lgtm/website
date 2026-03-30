/**
 * Approximates db/run_all_slices._group_yandex_projects_no_month (SequenceMatcher ratio).
 * Uses Sørensen–Dice on character bigrams over normalized names; threshold default 0.6.
 */
function normName(s: string): string {
  return s
    .toLowerCase()
    .replace(/[^a-zа-я0-9]+/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function bigramDice(a: string, b: string): number {
  if (a === b) return 1;
  if (a.length < 2 || b.length < 2) return 0;
  const am = new Map<string, number>();
  const bm = new Map<string, number>();
  for (let i = 0; i < a.length - 1; i++) {
    const bg = a.slice(i, i + 2);
    am.set(bg, (am.get(bg) ?? 0) + 1);
  }
  for (let i = 0; i < b.length - 1; i++) {
    const bg = b.slice(i, i + 2);
    bm.set(bg, (bm.get(bg) ?? 0) + 1);
  }
  let inter = 0;
  for (const [k, va] of am) {
    const vb = bm.get(k);
    if (vb) inter += Math.min(va, vb);
  }
  const total = a.length - 1 + b.length - 1;
  return total ? (2 * inter) / total : 0;
}

function sim(a: string, b: string): number {
  const na = normName(a);
  const nb = normName(b);
  if (!na || !nb) return 0;
  return bigramDice(na, nb);
}

export function mapYandexProjectNameToLabel(
  projectName: string,
  clusterLabels: string[],
  threshold = 0.6,
): string | null {
  const target = String(projectName ?? "").trim();
  if (!target) return null;

  let bestLabel: string | null = null;
  let bestScore = 0;

  for (const label of clusterLabels) {
    const score = sim(target, label);
    if (score > bestScore) {
      bestScore = score;
      bestLabel = label;
    }
  }

  return bestScore >= threshold ? bestLabel : null;
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
  threshold = 0.6,
): Map<string, string> {
  const uniq = [...new Set(projectNames.map((x) => String(x ?? "").trim()).filter(Boolean))];
  const clusterNorms: string[] = [];
  const clusterLabels: string[] = [];
  const mapped = new Map<string, string>();

  for (const pn of uniq) {
    const n = normName(pn);
    if (!n) {
      mapped.set(pn, "UNMAPPED");
      continue;
    }
    let bestI = -1;
    let bestS = 0;
    for (let i = 0; i < clusterNorms.length; i++) {
      const s = sim(n, clusterNorms[i]!);
      if (s > bestS) {
        bestS = s;
        bestI = i;
      }
    }
    if (bestI >= 0 && bestS >= threshold) {
      mapped.set(pn, clusterLabels[bestI]!);
    } else {
      clusterNorms.push(n);
      clusterLabels.push(pn);
      mapped.set(pn, pn);
    }
  }

  return mapped;
}

export function buildYandexProjectLabelMapFromRows(
  rows: NoMonthRow[],
  threshold = 0.6,
): Map<string, string> {
  if (!rows.length) return new Map<string, string>();

  const work = [...rows].sort((a, b) => {
    const r = (b.revenue_raw ?? 0) - (a.revenue_raw ?? 0);
    if (r !== 0) return r;
    return (b.leads_raw ?? 0) - (a.leads_raw ?? 0);
  });

  const clusterNorms: string[] = [];
  const clusterLabels: string[] = [];
  const mapped = new Map<string, string>();

  for (const row of work) {
    const pn = String(row.project_name ?? "").trim();
    const n = normName(pn);
    if (!n) {
      mapped.set(pn, "UNMAPPED");
      continue;
    }

    let bestI = -1;
    let bestS = 0;
    for (let i = 0; i < clusterNorms.length; i++) {
      const s = sim(n, clusterNorms[i]!);
      if (s > bestS) {
        bestS = s;
        bestI = i;
      }
    }

    if (bestI >= 0 && bestS >= threshold) {
      mapped.set(pn, clusterLabels[bestI]!);
    } else {
      clusterNorms.push(n);
      clusterLabels.push(pn);
      mapped.set(pn, pn);
    }
  }

  return mapped;
}

export function groupYandexProjectsNoMonth(
  rows: NoMonthRow[],
  threshold = 0.6,
): NoMonthRow[] {
  if (!rows.length) return [];
  const work = [...rows].sort((a, b) => {
    const r = (b.revenue_raw ?? 0) - (a.revenue_raw ?? 0);
    if (r !== 0) return r;
    return (b.leads_raw ?? 0) - (a.leads_raw ?? 0);
  });
  const clusterNorms: string[] = [];
  const clusterLabels: string[] = [];
  const mapped: string[] = [];
  for (const row of work) {
    const pn = String(row.project_name ?? "").trim();
    const n = normName(pn);
    if (!n) {
      mapped.push("UNMAPPED");
      continue;
    }
    let bestI = -1;
    let bestS = 0;
    for (let i = 0; i < clusterNorms.length; i++) {
      const s = sim(n, clusterNorms[i]!);
      if (s > bestS) {
        bestS = s;
        bestI = i;
      }
    }
    if (bestI >= 0 && bestS >= threshold) {
      mapped.push(clusterLabels[bestI]!);
    } else {
      clusterNorms.push(n);
      clusterLabels.push(pn);
      mapped.push(pn);
    }
  }
  const agg = new Map<string, NoMonthRow>();
  for (let i = 0; i < work.length; i++) {
    const label = mapped[i]!;
    const r = work[i]!;
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
