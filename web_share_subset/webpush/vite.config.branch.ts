import { defineConfig } from "vite";

/**
 * Branch-preview build: analytics SPA served at /branch/analytics/.
 * Output goes to dist-branch-analytics/.
 *
 * VITE_API_BASE is set to "/branch" so all /api/... calls from the SPA are
 * prefixed with /branch and nginx routes them to the branch Node.js process
 * (port 3001) instead of the production process (port 3000).
 */
export default defineConfig({
  base: "/branch/analytics/",
  publicDir: "public",
  build: {
    outDir: "dist-branch-analytics",
    emptyOutDir: true,
  },
  define: {
    "import.meta.env.VITE_API_BASE": JSON.stringify("/branch"),
  },
});
