Nginx reverse-proxy for new.cyber-ed.ru -> website-utm
=====================================================

This file contains a ready-to-run Nginx configuration and exact commands to expose the UTM worker
at `https://new.cyber-ed.ru/` by reverse-proxying to the public worker domain `website-utm.ivansherman127.workers.dev`.

Prerequisites
- DNS A record: `new.cyber-ed.ru` -> `130.49.149.212` (make sure this is set before obtaining a cert)
- A host accessible at `130.49.149.212` with ports 80 and 443 open
- sudo/root access on that host

Quick steps (Ubuntu/Debian):

```bash
# 1) Install Nginx + Certbot
sudo apt update
sudo apt install -y nginx certbot python3-certbot-nginx

# 2) Write the Nginx config (creates /etc/nginx/sites-available/new.cyber-ed.ru.conf)
sudo tee /etc/nginx/sites-available/new.cyber-ed.ru.conf > /dev/null <<'EOF'
server {
  listen 80;
  server_name new.cyber-ed.ru;
  return 301 https://$host$request_uri;
}

server {
  listen 443 ssl http2;
  server_name new.cyber-ed.ru;

  # TLS certs (Certbot will install these paths)
  ssl_certificate /etc/letsencrypt/live/new.cyber-ed.ru/fullchain.pem;
  ssl_certificate_key /etc/letsencrypt/live/new.cyber-ed.ru/privkey.pem;
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

sudo ln -sf /etc/nginx/sites-available/new.cyber-ed.ru.conf /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx

# 3) Obtain a Let's Encrypt certificate (interactive by default; use --non-interactive in scripts)
sudo certbot --nginx -d new.cyber-ed.ru

# 4) Verify
curl -I https://new.cyber-ed.ru/
curl -I https://new.cyber-ed.ru/api/utm
```

Notes & troubleshooting
- If `certbot` fails due to DNS not pointing to the host yet, wait for DNS propagation and re-run step 3.
- Ensure your firewall allows inbound TCP 80 and 443.
- This proxy forwards the `Host` header as `website-utm.ivansherman127.workers.dev`; the Worker sees the request as if it came to its default domain. That preserves asset paths and API routing.
- If you instead want the Worker mounted directly on `new.cyber-ed.ru` via Cloudflare (recommended if the zone is in your Cloudflare account), let me know and I will add a `routes` entry to `web_share_subset/webpush/wrangler.utm.jsonc` and attempt to deploy. That requires the zone to be managed in the same Cloudflare account used by `wrangler`.

Security
- This reverse proxy is minimal; you can harden it with rate-limiting, access controls, or IP allowlists as needed.
- If the origin worker requires specific headers or auth, add `proxy_set_header` directives accordingly.

If you want, I can produce a one-liner script to run on `130.49.149.212` that performs all steps non-interactively (including certbot with `--non-interactive` and an email). Reply if you'd like that script and whether you want Certbot to register with a specific email address.
