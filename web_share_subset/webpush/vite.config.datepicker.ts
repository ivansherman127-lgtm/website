import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { resolve } from "path";
import { fileURLToPath } from "url";
import { defineConfig } from "vite";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": resolve(__dirname, "./src"),
    },
  },
  root: __dirname,
  build: {
    outDir: "dist-datepicker",
    emptyOutDir: true,
    rollupOptions: {
      input: {
        datepicker: resolve(__dirname, "datepicker.html"),
      },
    },
  },
  server: {
    port: 5174,
  },
});
