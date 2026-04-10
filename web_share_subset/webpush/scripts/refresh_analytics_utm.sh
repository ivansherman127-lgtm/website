#!/usr/bin/env bash
set -euo pipefail

WEB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WEB_DIR"

echo "==> Refreshing frontend bundles (utm + analytics)"
echo "    repo: $WEB_DIR"
echo "    node: $(node -v)"
echo "    npm:  $(npm -v)"

# Keep this lightweight for server refreshes:
# - install deps only when node_modules is missing
if [[ ! -d node_modules ]]; then
  echo "==> node_modules missing; running npm ci"
  npm ci --prefer-offline
fi

echo "==> Building UTM bundle (dist)"
npm run build

echo "==> Building analytics bundle (dist-analytics)"
npm run build:analytics

if command -v pm2 >/dev/null 2>&1; then
  echo "==> Restarting pm2 app: utm-server"
  pm2 restart utm-server --update-env
  pm2 save >/dev/null 2>&1 || true
else
  echo "WARN: pm2 not found; build completed but process was not restarted."
fi

echo ""
echo "Done. Refreshed:"
echo "  - UTM bundle:        $WEB_DIR/dist"
echo "  - Analytics bundle:  $WEB_DIR/dist-analytics"
