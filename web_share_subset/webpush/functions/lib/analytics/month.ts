/** Month label YYYY-MM from Bitrix date string (dayfirst, like pandas). */
export function monthFromCreated(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = String(v).trim();
  if (!s) return "";

  // Prefer explicit parsers first to avoid JS Date interpreting DD.MM.YYYY
  // as locale-specific MM.DD.YYYY and shifting months into the future.
  const iso = /^(\d{4})-(\d{2})/.exec(s);
  if (iso) return `${iso[1]}-${iso[2]}`;

  const m2 = /^(\d{1,2})[./](\d{1,2})[./](\d{2,4})/.exec(s);
  if (m2) {
    let day = Number(m2[1]);
    let mon = Number(m2[2]);
    let yr = Number(m2[3]);
    if (yr < 100) yr += yr >= 70 ? 1900 : 2000;
    if (mon > 12 && day <= 12) [day, mon] = [mon, day];
    const dt = new Date(Date.UTC(yr, mon - 1, day));
    if (!Number.isNaN(dt.getTime())) {
      return `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, "0")}`;
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
