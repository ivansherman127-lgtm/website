/** Month label YYYY-MM from Bitrix date string (dayfirst, like pandas). */
export function monthFromCreated(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = String(v).trim();
  if (!s) return "";
  const m2 = /^(\d{1,2})[./](\d{1,2})[./](\d{2,4})/.exec(s);
  if (m2) {
    const day = Number(m2[1]);
    const mon = Number(m2[2]);
    let yr = Number(m2[3]);
    if (yr < 100) yr += yr >= 70 ? 1900 : 2000;
    if (day >= 1 && day <= 31 && mon >= 1 && mon <= 12) {
      return `${yr}-${String(mon).padStart(2, "0")}`;
    }
  }
  const d = new Date(s);
  if (!Number.isNaN(d.getTime())) {
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    return `${y}-${m}`;
  }
  return "";
}
