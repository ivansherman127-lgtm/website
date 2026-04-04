import { defineConfig } from "vite";

/**
 * Analytics-specific build: same SPA as the default build but served at /analytics/.
 * Output goes to dist-analytics/ so it doesn't collide with dist/ (utm build).
 * API calls use absolute paths (/api/data etc.) which nginx proxies to port 3000.
 */
export default defineConfig({
  base: "/analytics/",
  publicDir: "public",
  build: {
    outDir: "dist-analytics",
    emptyOutDir: true,
  },
});
