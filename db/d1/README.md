# Cloudflare D1 sync for `website.db`

## You need SQL tables in D1, not only `dataset_json`

The dashboard can read **pre-baked JSON** from `dataset_json` (`/api/data?path=...`), but **deal-level metrics** come from relational tables such as `mart_deals_enriched`, `stg_deals_analytics`, Yandex marts, etc. Those are copied from your **local** `website.db` by `push_from_sqlite.py`.

1. **Populate SQLite locally** (creates `mart_*`, `stg_*`, etc.):

   ```bash
   python db/run_all_slices.py
   ```

   If you refresh Bitrix exports and want incremental raw updates first:

   ```bash
   ./.venv/bin/python3 db/upsert_raw_bitrix_from_union.py
   ```

   This refreshes the relational `raw_bitrix_deals` table from the current Bitrix union export.

   Optional full refresh: `python db/refresh_analytics_pipeline.py` (includes static JSON under `web/public/data`).

2. **Verify counts before push** (optional):

   ```bash
   python db/d1/push_from_sqlite.py --preflight
   ```

3. **Push to D1** (SQL tables first, then JSON blobs unless `--no-json`):

   ```bash
   python db/d1/push_from_sqlite.py --remote --wrangler-config web/wrangler.toml --strict
   ```

   `--strict` fails if `raw_bitrix_deals`, `mart_deals_enriched`, or `stg_deals_analytics` are missing or empty — use this so you never silently sync “JSON only”.

4. **Cloud rebuild** (recomputes marts from `stg_deals_analytics` + materializes slice JSON): `POST /api/analytics/rebuild` (see below).

If you skip step 1, `push_from_sqlite` **skips** missing tables and you may only see rows in `dataset_json` from local `web/public/data/*.json`.

## Inventory (local `website.db`)

| Table | Rows (typical) | Sync to D1 |
|-------|----------------|------------|
| `stg_bitrix_deals_wide` | ~11k | **No** — ~481 columns, ~140MB+; keep local only. |
| `raw_bitrix_deals` | ~11k | **Yes** — canonical raw Bitrix relational table (one deal per row + batch metadata). |
| `raw_source_batches` | small | **Yes** — lineage for raw imports/backfills. |
| `mart_deals_enriched` | ~11k | **Yes** |
| `mart_attacking_january_contacts` | ~1k | **Yes** |
| `mart_attacking_january_cohort_deals` | ~2–3k | **Yes** (powers `/api/cohort-deals` instead of 56MB JSON) |
| `mart_yandex_leads_raw` / `dedup` | varies | **Yes** |
| `mart_yandex_revenue_projects_raw` / `dedup` | varies | **Yes** |
| `stg_yandex_stats` | ~4k | **Yes** |
| `stg_matched_yandex` | ~5k | **Yes** — full `matched_yd.csv` rows as `row_json` (Worker merge). |
| `stg_deals_analytics` | ~11k | **Yes** — trimmed deal fields for `POST /api/analytics/rebuild`. |
| `stg_email_sends` | varies | **Yes** |
| `analytics_build_meta` | small | **Yes** — last rebuild time / version (optional). |
| `deals`, `email_sends`, `yandex_stats` | legacy ETL | **Yes** (if present) |
| `sheets_sync_log` | small | Optional |
| `dataset_json` | N/A | **Yes** — stores `public/data/**/*.json` (chunked if large); `GET /api/data` concatenates chunks. |

**Large static JSON:** `attacking_january_associative_deals_base.json` (~56MB) is **not** stored in `dataset_json` by default. The dashboard uses **`GET /api/cohort-deals`** (SQL on `mart_attacking_january_cohort_deals`) when `VITE_DATA_SOURCE=d1`.

## One-time: create D1 and bind

1. Install Wrangler and log in: `npx wrangler login`
2. List or create a database: `npx wrangler d1 list` / `npx wrangler d1 create <name>`  
   Copy **`database_id`** and **`name`** into both:
   - [`web/wrangler.toml`](../../web/wrangler.toml) (`database_name`, `database_id`)
   - [`web_share_subset/webpush/wrangler.toml`](../../web_share_subset/webpush/wrangler.toml)  
   This repo is currently wired to **`cybered`**; use that name in `migrations apply` and `push_from_sqlite --database-name`.
3. Apply migrations (from repo root; replace `cybered` if your D1 has another name):

```bash
npx wrangler d1 migrations apply cybered --remote --config web/wrangler.toml
```

[`web/wrangler.toml`](../../web/wrangler.toml) sets `migrations_dir = "migrations"` ([`web/migrations`](../../web/migrations) is a symlink to this folder).

## Cloud rebuild (Workers + D1)

After migrations and push, materialize marts and `dataset_json` slice blobs in D1 (no local Python):

1. Set a secret: `npx wrangler pages secret put ANALYTICS_REBUILD_SECRET --project-name website-web` (or use `[vars]` in dev only).
2. `curl -X POST "https://<pages-host>/api/analytics/rebuild" -H "Authorization: Bearer <ANALYTICS_REBUILD_SECRET>"`

This runs [`web/functions/lib/analytics/analyticsRebuild.ts`](../../web/functions/lib/analytics/analyticsRebuild.ts): refill `mart_deals_enriched` from `stg_deals_analytics`, cohort tables, Yandex marts, then SQL-backed JSON for global/qa/cohort paths (same queries as `db/run_all_slices.export_slices`).

Local workflow: `python3 db/refresh_analytics_pipeline.py --d1-only` (still runs `run_all_slices` for SQLite staging/marts) → `push_from_sqlite.py --remote` → `POST /api/analytics/rebuild`.

For explicit raw refresh before slices: `./.venv/bin/python3 db/upsert_raw_bitrix_from_union.py && ./.venv/bin/python3 db/run_all_slices.py`.

Single-command option: `python3 db/refresh_analytics_pipeline.py --upsert-raw-bitrix --d1-only --d1-sync`.

## Security

[`web/functions/api/data.ts`](../../web/functions/api/data.ts) and [`cohort-deals.ts`](../../web/functions/api/cohort-deals.ts) are **GET-only**; they do not execute client-provided SQL. [`api/analytics/rebuild.ts`](../../web/functions/api/analytics/rebuild.ts) is **POST**, requires `Authorization: Bearer` matching `ANALYTICS_REBUILD_SECRET`.

## One-command pipeline (local DB → D1 → cloud rebuild)

From repo root, after `run_all_slices` has filled `website.db`:

```bash
export D1_ANALYTICS_REBUILD_URL="https://<your-pages-host>/api/analytics/rebuild"
export ANALYTICS_REBUILD_SECRET="<secret>"
python3 db/refresh_analytics_pipeline.py --d1-only --d1-sync
```

Or only push (`--push-d1`) or only rebuild (`--d1-rebuild`). Use `--push-d1-local` to push without `--remote` (local Miniflare).

## Push local data → D1

From [`push_from_sqlite.py`](push_from_sqlite.py):

```bash
./.venv/bin/python3 db/d1/push_from_sqlite.py --remote --wrangler-config web/wrangler.toml --strict
```

Options:

- `--strict` — exit with error unless `raw_bitrix_deals`, `mart_deals_enriched`, and `stg_deals_analytics` exist and have rows (recommended for remote sync).
- `--preflight` — print per-table row counts in local `website.db` and exit (no push).
- `--no-json` — push **only** SQL tables; skip loading `web/public/data` into `dataset_json`.
- `--json-dir web/public/data` — upsert `dataset_json` (default; skips files over `--max-json-mb` except with `--include-large-json`).
- `--tables` — override which SQL tables to refresh (default: raw + marts + staging, no `stg_bitrix_deals_wide`).

Requires `CLOUDFLARE_API_TOKEN` with D1 edit permissions for `--remote`, or omit `--remote` to write a local `.sql` file for manual `wrangler d1 execute --file`.

If push fails with **`SQLITE_TOOBIG` / statement too long**, D1 rejected a single SQL **statement**. Common causes:

- **Relational tables:** multi-row `INSERT`s with long text (e.g. **`yandex_stats`** ad copy). The push script caps **each INSERT** to ~48 KB by packing rows greedily; **`stg_matched_yandex`** uses **one row per INSERT**. Override: `D1_MAX_INSERT_STATEMENT_BYTES`.
- **`dataset_json`:** large files are split into **multiple** `INSERT`s (`chunk` = 0,1,…) so each statement stays under the cap. **`GET /api/data`** concatenates chunks by `path`. **Apply migration `0003_dataset_json_chunks.sql`** before pushing. Cap: **`D1_MAX_DATASET_INSERT_BYTES`** (default same as relational `INSERT`s). **Relational** tables: oversized rows use **INSERT + `UPDATE` … `WHERE rowid = last_insert_rowid()`** in chunks; segments are never split across wrangler files.

### `Authentication error [code: 10000]` on `wrangler d1 execute --remote`

That comes from the **Cloudflare API** (D1 import), not from SQL. Typical fixes:

1. **Refresh OAuth:** `npx wrangler login` (browser) — OAuth sessions expire.
2. **API token:** create one in the Cloudflare dashboard with **Account → D1 → Edit** (or “D1 Edit”), export `CLOUDFLARE_API_TOKEN`, and ensure the token’s account matches the D1 database (`database_id` in `web/wrangler.toml`).
3. **Stale env token:** if `CLOUDFLARE_API_TOKEN` is set to an old/revoked value, **unset** it so Wrangler uses `wrangler login`, or replace it with a new token.
4. **Wrong account:** `wrangler whoami` / dashboard — the DB must live under the same account as the token.

## Frontend

Set `VITE_DATA_SOURCE=d1` at build time so the SPA loads from `/api/data?path=...` and `/api/cohort-deals` instead of static `public/data` files.

