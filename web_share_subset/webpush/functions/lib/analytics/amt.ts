/** Parse deal amount like Python _amt / dashboard num(). */
export function parseAmount(v: unknown): number {
  if (v === null || v === undefined) return 0;
  const s = String(v)
    .replace(/\s+/g, "")
    .replace(/\u00a0/g, "")
    .replace(",", ".")
    .trim();
  if (!s || s.toLowerCase() === "nan") return 0;
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}
