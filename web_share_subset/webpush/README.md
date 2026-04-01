# Web Share Subset

This folder is a lightweight repo subset for deploying the webpage.

## Run locally

```bash
npm install
npm run worker:dev
```

This runs as a Cloudflare Worker with static assets from `dist`. Assoc report generation is served by `/api/assoc-revenue` and uses D1 binding `DB`.

Wrangler config source of truth: `wrangler.jsonc` (Wrangler v4 prefers it when both config files exist).

## UTM-only deployment

This repository now supports a separate UTM-only deployment that does not expose analytics routes.

- Frontend entry: `utm.html` + `src/utm-only.ts`
- Worker entry: `worker-utm.ts`
- Wrangler config: `wrangler.utm.jsonc`
- Build output: `dist-utm`

Run locally:

```bash
npm run dev:utm
```

Run as Worker locally:

```bash
npm run worker:dev:utm
```

Deploy UTM-only:

```bash
npm run worker:deploy:utm
```

Cloudflare Git deploy settings (repo root = `website`):

- Build/deploy command for UTM-only service:

```bash
npm run cf:deploy:utm
```

- Build/deploy command for analytics service:

```bash
npm run cf:deploy:analytics
```

Why this matters: if Cloudflare runs `wrangler deploy` with the default/root config, it expects `web_share_subset/webpush/dist`. UTM-only builds output `web_share_subset/webpush/dist-utm`, so you must deploy with `--config web_share_subset/webpush/wrangler.utm.jsonc`.

The UTM worker serves only `/api/utm` and static UTM page assets. Other `/api/*` paths return `404`.

## D1 sync from local SQLite

Run from this repo root (`web_share_subset/webpush`):

```bash
./scripts/sync_d1_from_sqlite.sh check
./scripts/sync_d1_from_sqlite.sh full
```

Modes:

- `check`: validate paths/tools/env.
- `migrate`: apply D1 migrations remotely.
- `push`: push local SQLite + JSON to remote D1.
- `push-local`: push to local D1 (no `--remote`).
- `rebuild`: call cloud rebuild endpoint.
- `full`: `migrate + upsert raw + run slices + push + rebuild`.

For `rebuild`/`full`, set:

```bash
export D1_ANALYTICS_REBUILD_URL="https://<your-pages-host>/api/analytics/rebuild"
export ANALYTICS_REBUILD_SECRET="<secret>"
```

## Update workflow (JSON-only)

1. In the main project, regenerate needed JSON files.
2. Copy updated files into `public/data/` here.
3. Commit and push this subset repo.

Do **not** copy `attacking_january_associative_deals_base.json` into this tree: it is not used by the webpush UI and exceeds Cloudflare’s per-asset size limit (`prebuild` strips it if present).

## Required data files

- `public/data/attacking_january_associative_revenue_by_month.json`
- `public/data/attacking_january_associative_revenue_by_events.json`
- `public/data/attacking_january_associative_revenue_by_course_codes.json`
- `public/data/email_hierarchy_by_send.json`
- `public/data/yd_hierarchy.json`
- `public/data/bitrix_month_total_full.json`
- `public/data/manager_sales_by_course.json`
- `public/data/manager_sales_by_month.json`
- `public/data/manager_firstline_by_course.json`
- `public/data/manager_firstline_by_month.json`
- `public/data/bitrix_funnel_month_code_full.json`
- `public/data/bitrix_contacts_uid.json`
- `public/data/member_list_03.26_statuses_categories.json`
