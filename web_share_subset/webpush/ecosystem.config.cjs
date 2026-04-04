// PM2 ecosystem config for the UTM server.
// CommonJS (.cjs) so PM2 can load it even when package.json has "type":"module".
//
// Usage:
//   PORT=3000 UTM_DB_PATH=/opt/utm-app/utm.db pm2 start ecosystem.config.cjs
//   pm2 save

const path = require("path");

// Resolve tsx binary relative to this file so it works regardless of cwd.
const tsxBin = path.join(__dirname, "node_modules", ".bin", "tsx");

module.exports = {
  apps: [
    {
      name: "utm-server",
      script: "server/index.ts",
      interpreter: tsxBin,
      cwd: __dirname,
      watch: false,
      env: {
        NODE_ENV: "production",
        PORT: process.env.PORT || "3000",
        UTM_DB_PATH: process.env.UTM_DB_PATH || "/opt/utm-app/utm.db",
        DIST_DIR: process.env.DIST_DIR || require("path").join(__dirname, "dist-utm"),
        // Set this on the server: UTM_PASSWORD=yourpassword pm2 start ...
        // Or export UTM_PASSWORD=yourpassword before running pm2 reload
        UTM_PASSWORD: process.env.UTM_PASSWORD || "",
      },
    },
  ],
};
