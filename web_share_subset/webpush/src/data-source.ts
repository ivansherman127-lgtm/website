/**
 * All data is served from the Cloudflare D1-backed API. Static assets are only used for
 * mapping/config files (see staticUrl). Use "npm run worker:dev" for local development.
 */

// VITE_API_BASE is injected at build time for sub-path branch deployments (e.g. "/branch").
// Empty string in normal builds — all /api/ calls go to the domain root.
const API_BASE: string = (import.meta.env.VITE_API_BASE as string | undefined) || "";

export function dataUrl(path: string): string {
  if (path.startsWith("/api/")) {
    return `${API_BASE}${path}`;
  }
  const p = path.startsWith("/") ? path.slice(1) : path;
  if (p.startsWith("data/")) {
    const rel = p.slice("data/".length);
    // Always use an absolute path so the API calls reach the backend regardless
    // of the SPA's base URL (/utm/ or /analytics/).
    return `${API_BASE}/api/data?path=${encodeURIComponent(rel)}`;
  }
  const base = import.meta.env.BASE_URL || "/";
  return `${base}${p}`;
}

/** Always fetches from static assets, bypassing D1 API routing. Use for mapping/config files. */
export function staticUrl(path: string): string {
  const base = import.meta.env.BASE_URL || "/";
  const p = path.startsWith("/") ? path.slice(1) : path;
  return `${base}${p}`;
}

export function cohortDealsUrl(): string {
  return `${API_BASE}/api/cohort-deals`;
}
