#!/usr/bin/env bash
set -euo pipefail

# setup-proxy-new.cyber-ed.ru.sh
# Non-interactive script to install Nginx, configure a reverse proxy for
# new.cyber-ed.ru, and obtain a Let's Encrypt certificate via Certbot.
#
# Usage
#   sudo SUBDOMAIN=new.cyber-ed.ru CERTBOT_EMAIL=you@example.com bash setup-proxy-new.cyber-ed.ru.sh
#   or
#   sudo bash setup-proxy-new.cyber-ed.ru.sh new.cyber-ed.ru you@example.com

DOMAIN="${1:-${SUBDOMAIN:-new.cyber-ed.ru}}"
EMAIL="${2:-${CERTBOT_EMAIL:-}}"

if [ "$(id -u)" -ne 0 ]; then
  echo "This script must be run as root. Use: sudo $0" >&2
  exit 1
fi

echo "Configuring reverse proxy for: $DOMAIN"

echo "* Installing packages..."
apt-get update -y
apt-get install -y nginx certbot python3-certbot-nginx

NGINX_CONF="/etc/nginx/sites-available/${DOMAIN}.conf"
cat >"$NGINX_CONF" <<'EOF'
server {
  listen 80;
  server_name DOMAIN_REPLACE;
  return 301 https://$host$request_uri;
}

server {
  listen 443 ssl http2;
  server_name DOMAIN_REPLACE;

  # TLS certs (Certbot will write these paths)
  ssl_certificate /etc/letsencrypt/live/DOMAIN_REPLACE/fullchain.pem;
  ssl_certificate_key /etc/letsencrypt/live/DOMAIN_REPLACE/privkey.pem;
  ssl_protocols TLSv1.2 TLSv1.3;

  location / {
    proxy_pass https://website-utm.ivansherman127.workers.dev;
    proxy_set_header Host website-utm.ivansherman127.workers.dev;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_ssl_server_name on;
    proxy_buffering off;
  }
}
EOF

# Substitute the domain placeholder
sed -i "s/DOMAIN_REPLACE/${DOMAIN//\//\/}/g" "$NGINX_CONF"

ln -sf "$NGINX_CONF" "/etc/nginx/sites-enabled/${DOMAIN}.conf"

echo "* Testing Nginx config and reloading..."
nginx -t
systemctl reload nginx || true

echo "* Requesting TLS certificate from Let's Encrypt (Certbot)..."
if [ -n "$EMAIL" ]; then
  certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos --email "$EMAIL"
else
  echo "  WARNING: CERTBOT_EMAIL not provided; registering without email (not recommended)"
  certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos --register-unsafely-without-email
fi

echo "* Reloading Nginx with TLS certs..."
systemctl reload nginx

echo
echo "Done. Verify with:"
echo "  curl -I https://$DOMAIN/"
echo "  curl -I https://$DOMAIN/api/utm"

exit 0
