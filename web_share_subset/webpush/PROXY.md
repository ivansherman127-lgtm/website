Reverse proxy setup for custom subdomain
======================================

If your DNS points a subdomain to the server at `130.49.149.212` (or you control that host and want to expose a custom FQDN), you can proxy requests to the Cloudflare Worker `website-utm.ivansherman127.workers.dev` so the UTM UI appears under your domain.

Prerequisites
- A DNS A record for the desired subdomain (e.g. `utm.example.com`) pointing to `130.49.149.212`.
- A host with root/ sudo access at `130.49.149.212` and open ports 80/443.

Notes
- This approach does not require the zone to be managed in Cloudflare. It simply forwards incoming requests to the public worker domain.
- If you add the zone to Cloudflare and want the worker mounted directly to the zone, it's a different flow (add a route in `wrangler.utm.jsonc` and deploy from the Cloudflare account that controls the zone).

Minimal Nginx reverse-proxy (recommended for Ubuntu/Debian)

1. Install Nginx + Certbot:

```bash
sudo apt update
sudo apt install -y nginx certbot python3-certbot-nginx
```

2. Create an Nginx server config for your subdomain (replace `SUBDOMAIN`):

```nginx
server {
  listen 80;
  server_name SUBDOMAIN;
  return 301 https://$host$request_uri;
}

server {
  listen 443 ssl http2;
  server_name SUBDOMAIN;

  # TLS certs (Certbot will write these paths)
  ssl_certificate /etc/letsencrypt/live/SUBDOMAIN/fullchain.pem;
  ssl_certificate_key /etc/letsencrypt/live/SUBDOMAIN/privkey.pem;
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
```

3. Enable the site and reload Nginx:

```bash
sudo tee /etc/nginx/sites-available/SUBDOMAIN.conf > /dev/null <<'EOF'
# (paste the config above)
EOF
sudo ln -s /etc/nginx/sites-available/SUBDOMAIN.conf /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

4. Obtain a Let's Encrypt TLS certificate (Certbot will modify your Nginx config automatically):

```bash
sudo certbot --nginx -d SUBDOMAIN
```

5. Verify

```bash
curl -I https://SUBDOMAIN/
curl -I https://SUBDOMAIN/api/utm
```

Troubleshooting and tips
- Make sure ports 80 and 443 are reachable from the public internet (firewall/NAT rules).
- If you prefer automatic TLS with fewer steps, consider using Caddy instead of Nginx — it provisions certificates automatically.
- If you control the DNS in Cloudflare and want the worker mounted directly, instead add a `routes` entry to `wrangler.utm.jsonc` and deploy the worker from the Cloudflare account that owns the zone.

Security
- This proxy simply forwards requests to the public worker domain. Do not expose admin credentials or other sensitive endpoints on the proxy host unless you secure them.

Questions
- Tell me the exact FQDN you want (e.g. `utm.example.com`) and whether you prefer Nginx or Caddy; I can produce a ready-to-run config and the exact commands to run on `130.49.149.212`.
