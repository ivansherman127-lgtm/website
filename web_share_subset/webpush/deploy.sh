#!/usr/bin/env bash
# deploy.sh — Bootstrap and update the UTM server on Ubuntu 24.04
#
# First-time install (run as root or with sudo):
#   curl -fsSL https://raw.githubusercontent.com/ivansherman127-lgtm/website/server-deploy/web_share_subset/webpush/deploy.sh \
#     | sudo DOMAIN=new.cyber-ed.ru CERTBOT_EMAIL=you@example.com bash
#
# Subsequent updates (as root or with sudo):
#   sudo bash /opt/utm-app/web_share_subset/webpush/deploy.sh --update
#
# Environment variables:
#   DOMAIN          Fully-qualified domain name  (default: new.cyber-ed.ru)
#   CERTBOT_EMAIL   Email for Let's Encrypt registration
#   APP_DIR         Clone path                   (default: /opt/utm-app)
#   PORT            Node.js listen port          (default: 3000)

set -euo pipefail

DOMAIN="${DOMAIN:-new.cyber-ed.ru}"
CERTBOT_EMAIL="${CERTBOT_EMAIL:-}"
APP_DIR="${APP_DIR:-/opt/utm-app}"
APP_SUBDIR="web_share_subset/webpush"
REPO="https://github.com/ivansherman127-lgtm/website.git"
BRANCH="server-deploy"
PORT="${PORT:-3000}"
UPDATE_ONLY="${1:-}"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "ERROR: run as root or with sudo" >&2
  exit 1
fi

# ── 1. Node.js 20 ────────────────────────────────────────────────────────────
if ! command -v node &>/dev/null || [[ "$(node -e 'process.stdout.write(process.versions.node.split(".")[0])')" -lt 20 ]]; then
  echo "==> Installing Node.js 20..."
  curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
  apt-get install -y nodejs
fi
echo "    node $(node --version)  npm $(npm --version)"

# ── 2. PM2 ───────────────────────────────────────────────────────────────────
if ! command -v pm2 &>/dev/null; then
  echo "==> Installing PM2..."
  npm install -g pm2
  pm2 startup systemd --no-daemon -u root --hp /root
fi

# ── 3. Nginx + Certbot ───────────────────────────────────────────────────────
if ! command -v nginx &>/dev/null; then
  echo "==> Installing Nginx and Certbot..."
  apt-get update -y
  apt-get install -y nginx certbot python3-certbot-nginx
fi

# ── 4. Clone or pull repo ────────────────────────────────────────────────────
if [[ -d "$APP_DIR/.git" ]]; then
  echo "==> Pulling latest $BRANCH..."
  git -C "$APP_DIR" fetch origin
  git -C "$APP_DIR" checkout "$BRANCH"
  git -C "$APP_DIR" reset --hard "origin/$BRANCH"
else
  echo "==> Cloning repo to $APP_DIR..."
  git clone --branch "$BRANCH" --depth 1 "$REPO" "$APP_DIR"
fi

WEB_DIR="$APP_DIR/$APP_SUBDIR"

# ── 5. Install Node dependencies ─────────────────────────────────────────────
echo "==> npm ci..."
cd "$WEB_DIR"
npm ci --prefer-offline

# ── 6. Build frontend ────────────────────────────────────────────────────────
echo "==> Building dist-utm..."
npm run build:utm

# ── 7. Nginx config (only written once; skipped on --update) ─────────────────
NGINX_CONF="/etc/nginx/sites-available/${DOMAIN}.conf"
if [[ "$UPDATE_ONLY" != "--update" ]] || [[ ! -f "$NGINX_CONF" ]]; then
  echo "==> Writing Nginx config for $DOMAIN..."
  cat >"$NGINX_CONF" <<NGINXEOF
server {
  listen 80;
  server_name ${DOMAIN};
  return 301 https://\$host\$request_uri;
}

server {
  listen 443 ssl http2;
  server_name ${DOMAIN};

  ssl_certificate     /etc/letsencrypt/live/${DOMAIN}/fullchain.pem;
  ssl_certificate_key /etc/letsencrypt/live/${DOMAIN}/privkey.pem;
  ssl_protocols TLSv1.2 TLSv1.3;
  ssl_prefer_server_ciphers on;

  location / {
    proxy_pass         http://127.0.0.1:${PORT};
    proxy_set_header   Host              \$host;
    proxy_set_header   X-Real-IP         \$remote_addr;
    proxy_set_header   X-Forwarded-For   \$proxy_add_x_forwarded_for;
    proxy_set_header   X-Forwarded-Proto \$scheme;
    proxy_buffering    off;
  }
}
NGINXEOF

  ln -sf "$NGINX_CONF" "/etc/nginx/sites-enabled/${DOMAIN}.conf"
  rm -f /etc/nginx/sites-enabled/default

  # Temporary HTTP-only config so Certbot can complete the ACME challenge
  # before the TLS config references the cert paths.
  cat >"/tmp/${DOMAIN}-http.conf" <<TMPEOF
server {
  listen 80;
  server_name ${DOMAIN};
  location / {
    proxy_pass http://127.0.0.1:${PORT};
  }
}
TMPEOF

  cp "/tmp/${DOMAIN}-http.conf" "$NGINX_CONF"
  ln -sf "$NGINX_CONF" "/etc/nginx/sites-enabled/${DOMAIN}.conf"
  nginx -t && systemctl reload nginx

  echo "==> Obtaining TLS certificate..."
  if [[ -n "$CERTBOT_EMAIL" ]]; then
    certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos --email "$CERTBOT_EMAIL"
  else
    certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos --register-unsafely-without-email
  fi

  # Write the final HTTPS config (Certbot may have already done this; write it explicitly)
  cat >"$NGINX_CONF" <<NGINXEOF
server {
  listen 80;
  server_name ${DOMAIN};
  return 301 https://\$host\$request_uri;
}

server {
  listen 443 ssl http2;
  server_name ${DOMAIN};

  ssl_certificate     /etc/letsencrypt/live/${DOMAIN}/fullchain.pem;
  ssl_certificate_key /etc/letsencrypt/live/${DOMAIN}/privkey.pem;
  ssl_protocols TLSv1.2 TLSv1.3;
  ssl_prefer_server_ciphers on;

  location / {
    proxy_pass         http://127.0.0.1:${PORT};
    proxy_set_header   Host              \$host;
    proxy_set_header   X-Real-IP         \$remote_addr;
    proxy_set_header   X-Forwarded-For   \$proxy_add_x_forwarded_for;
    proxy_set_header   X-Forwarded-Proto \$scheme;
    proxy_buffering    off;
  }
}
NGINXEOF
  nginx -t && systemctl reload nginx
fi

# ── 8. Start / restart PM2 process ───────────────────────────────────────────
echo "==> Starting utm-server with PM2..."
cd "$WEB_DIR"

pm2 delete utm-server 2>/dev/null || true

PORT="$PORT" UTM_DB_PATH="$APP_DIR/utm.db" \
  pm2 start ecosystem.config.cjs

pm2 save

echo ""
echo "==> Deploy complete!"
echo "    https://${DOMAIN}/"
echo "    https://${DOMAIN}/api/utm"
echo ""
echo "    Update command:"
echo "    sudo bash $WEB_DIR/deploy.sh --update"
