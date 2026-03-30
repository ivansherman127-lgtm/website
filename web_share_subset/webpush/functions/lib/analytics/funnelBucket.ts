/**
 * Mirrors db/bitrix_lead_quality.funnel_report_bucket / funnel_report_bucket_series.
 */
const EXCLUDED = new Set(["Спец. проекты", "Учебный центр", "Спецпроекты"]);

const CANONICAL = [
  "B2B",
  "B2C",
  "Горячая воронка",
  "Карьерная консультация",
  "Реактивация",
  "Холодная воронка",
] as const;

const CANONICAL_SET = new Set<string>(CANONICAL);
export const FUNNEL_REPORT_OTHER = "Другое";

export function funnelReportBucket(raw: unknown): string {
  let t = "";
  if (raw === null || raw === undefined) t = "";
  else t = String(raw).trim();
  if (!t || t.toLowerCase() === "nan") return FUNNEL_REPORT_OTHER;
  if (EXCLUDED.has(t)) return FUNNEL_REPORT_OTHER;
  if (CANONICAL_SET.has(t)) return t;
  return FUNNEL_REPORT_OTHER;
}
