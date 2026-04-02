import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  base: "./",
  publicDir: false,
  build: {
    outDir: "dist-utm",
    emptyOutDir: true,
    rollupOptions: {
      input: {
        utm: resolve(__dirname, "utm.html"),
      },
    },
  },
});
