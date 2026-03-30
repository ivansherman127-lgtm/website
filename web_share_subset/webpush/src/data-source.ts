/**
 * Static files vs Cloudflare D1-backed API (see db/d1/README.md).
 * Build with VITE_DATA_SOURCE=d1 for production Pages + D1.
 */
export function dataUrl(path: string): string {
  const base = import.meta.env.BASE_URL || "/";
  if (path.startsWith("/api/")) {
    return path;
  }
  const p = path.startsWith("/") ? path.slice(1) : path;
  const mode = import.meta.env.VITE_DATA_SOURCE ?? "static";
  if (mode === "d1") {
    if (p.startsWith("data/")) {
      const rel = p.slice("data/".length);
      return `${base}api/data?path=${encodeURIComponent(rel)}`;
    }
    return `${base}${p}`;
  }
  return `${base}${p}`;
}

/** Always fetches from static assets, bypassing D1 API routing. Use for mapping/config files. */
export function staticUrl(path: string): string {
  const base = import.meta.env.BASE_URL || "/";
  const p = path.startsWith("/") ? path.slice(1) : path;
  return `${base}${p}`;
}

export function cohortDealsUrl(): string {
  const base = import.meta.env.BASE_URL || "/";
  const mode = import.meta.env.VITE_DATA_SOURCE ?? "static";
  if (mode === "d1") {
    return `${base}api/cohort-deals`;
  }
  return `${base}data/attacking_january_associative_deals_base.json`;
}
