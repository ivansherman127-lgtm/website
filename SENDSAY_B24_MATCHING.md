# Sendsay → Bitrix24 Leads Matching

**Handoff document** for matching email marketing campaigns (Sendsay) to CRM leads (Bitrix24).

**Last updated**: 2026-04-12  
**Owner**: Analytics pipeline  
**Database**: SQLite (`website.db`)

---

## Overview

This system matches email campaign sends (Sendsay) to inbound leads/deals (Bitrix24) using UTM parameters and contact email addresses. Two matching strategies are used:

1. **Direct attribution** — Leads with `UTM Source = 'sendsay'` matched to campaigns via `UTM Campaign`
2. **Associated revenue** — Contacts acquired via email who later convert through other channels

---

## Data Sources

### 1. Raw Bitrix24 Deals (`raw_bitrix_deals`)

**Source**: Bitrix24 CRM export (CSV union or API)  
**Key columns**:
- `ID` — Deal ID (text, primary key)
- `Контакт: ID` — Contact ID (may have multiple for one deal)
- `Контакт: E-mail рабочий` — Primary email
- `Контакт: Телефон рабочий` — Primary phone
- `Дата создания` — Deal creation date (DD.MM.YYYY or YYYY-MM-DD)
- `Дата оплаты` — Payment date (DD.MM.YYYY or YYYY-MM-DD)
- `Сумма` — Deal amount
- `UTM Source` — Traffic source (`'sendsay'`, `'yandex'`, etc.)
- `UTM Medium` — Marketing medium (`'email'`, `'cpc'`, etc.)
- `UTM Campaign` — Campaign identifier (lowercase normalized for matching)
- `Воронка` — Funnel name
- `Стадия сделки` — Deal stage
- `Название сделки` — Deal name

**Ingestion**: `db/upsert_raw_bitrix_from_union.py` (CSV) or `db/b24_ingest.py` (API)

---

### 2. Sendsay Email Campaigns (`stg_email_sends`)

**Source**: Sendsay API (`stat.uni` endpoint)  
**Key columns**:
- `Дата отправки` — Send date (YYYY-MM-DD HH:MM:SS)
- `Название выпуска` — Release/send name
- `Тема` — Email subject line
- `Отправлено` — Total sent count
- `Доставлено` — Delivered count
- `Ошибок` — Delivery errors
- `Открытий` — Total opens
- `Уник. открытий` — Unique opens
- `Кликов` — Total clicks
- `Уник. кликов` — Unique clicks
- `CTOR, %` — Click-to-open rate
- `Отписок` — Unsubscribes
- `UTOR, %` — Unsubscribe-to-open rate
- `ID` — Sendsay issue ID
- `Номер задания` — Sendsay track ID
- `utm_campaign` — Campaign slug (extracted from email links)
- `utm_content` — UTM content parameter
- `utm_medium` — UTM medium (usually `'email'`)
- `utm_source` — UTM source (usually `'sendsay'`)
- `utm_term` — UTM term parameter
- `month` — YYYY-MM (derived from `Дата отправки`)

**Ingestion**: `db/fetch_sendsay_emails.py`

```bash
# Fetch all campaigns (replace entire table)
python db/fetch_sendsay_emails.py

# Incremental update
python db/fetch_sendsay_emails.py --if-exists append --from 2026-01-01

# Test run (no DB writes)
python db/fetch_sendsay_emails.py --dry-run
```

**Credentials** (from `keys.json`):
- Login: `info@cyber-ed.ru`
- Password: `X$T3V4{!ko`

---

### 3. Contact Deduplication (`stg_contacts_uid`)

**Purpose**: Merge duplicate contacts by email address across all deals  
**Key columns**:
- `contact_uid` — Unique contact identifier (UUID)
- `contact_id` — Original Bitrix24 contact ID
- `all_emails` — Pipe-separated normalized emails
- `all_phones` — Pipe-separated normalized phones
- `all_names` — Pipe-separated names
- `first_deal_date` — ISO date of earliest deal (YYYY-MM-DD)
- `first_touch_event` — `event_class` of first deal (attribution)

**Generation**: `db/build_contacts_uid.py`

**Merge rule**:
- Contacts are merged **only if they share at least one normalized email address**
- Phone numbers are collected but do not trigger merges
- Union-Find algorithm groups contacts into `contact_uid` clusters

**Email normalization**:
```python
def normalize_email(value: str) -> str:
    v = (value or "").strip().lower()
    m = EMAIL_RE.search(v)
    return m.group(0) if m else ""
```

**Phone normalization**:
- Strip non-digits
- Normalize Russian phones: `8XXXXXXXXXX` → `7XXXXXXXXXX`
- Reject placeholder patterns: `123456`, `999999`, `1212121212`, etc.

---

## Matching Logic

### Strategy 1: Direct Attribution (UTM-based)

**Query** (from `deals_by_campaign` CTE in TypeScript materializer):

```sql
SELECT
  LOWER(TRIM(COALESCE("UTM Campaign", ''))) AS utm_campaign_key,
  COUNT(*) AS leads,
  SUM(CASE WHEN is_revenue_variant3 = 1 THEN 1 ELSE 0 END) AS paid_deals,
  SUM(CASE WHEN is_revenue_variant3 = 1 THEN revenue_amount ELSE 0 END) AS revenue,
  GROUP_CONCAT(COALESCE("ID", '')) AS fl_ids  -- FirstLine deal IDs
FROM mart_deals_enriched
WHERE LOWER(TRIM(COALESCE("UTM Source", ''))) = 'sendsay'
GROUP BY utm_campaign_key;
```

**Join to email sends**:

```sql
FROM stg_email_sends e
LEFT JOIN deals_by_campaign dbc
  ON dbc.utm_campaign_key = LOWER(TRIM(COALESCE(e.utm_campaign, '')))
```

**Match key**: Lowercase, trimmed `UTM Campaign` value

**Example**:
- Email send: `utm_campaign = 'feb-webinar-2026'`
- B24 deal: `UTM Source = 'sendsay'`, `UTM Campaign = 'Feb-Webinar-2026'`
- Normalized key: `'feb-webinar-2026'` → **MATCHED**

---

### Strategy 2: Associated Revenue (Contact-based)

**Purpose**: Attribute revenue from contacts who:
1. Initially registered via an email campaign (`UTM Source = 'sendsay'`)
2. Later converted through a *different* channel (cross-channel attribution)

**Query** (from `assoc_rev` CTE):

```sql
WITH email_pools AS (
  -- Earliest registration per contact × email campaign
  SELECT
    LOWER(TRIM(COALESCE("UTM Campaign", ''))) AS utm_key,
    REPLACE(TRIM(COALESCE("Контакт: ID", '')), '.0', '') AS contact_id,
    MIN(deal_creation_date) AS reg_date
  FROM mart_deals_enriched
  WHERE LOWER(TRIM(COALESCE("UTM Source", ''))) = 'sendsay'
    AND LOWER(TRIM(COALESCE("UTM Campaign", ''))) <> ''
    AND contact_id <> ''
  GROUP BY utm_key, contact_id
),
assoc_rev AS (
  -- Count paid deals whose payment date > registration date
  SELECT
    ep.utm_key,
    COUNT(DISTINCT rev."ID") AS assoc_deals,
    SUM(COALESCE(rev.revenue_amount, 0)) AS assoc_revenue
  FROM email_pools ep
  JOIN mart_deals_enriched rev
    ON rev."Контакт: ID" = ep.contact_id
  WHERE rev.is_revenue_variant3 = 1
    AND ep.reg_date IS NOT NULL
    AND rev.payment_date > ep.reg_date
  GROUP BY ep.utm_key
)
```

**Example**:
1. User registers via email campaign `'march-course'` on **2026-03-15**
2. User purchases course via Yandex ad on **2026-03-25** (revenue = ₽50,000)
3. **Associated revenue** for `'march-course'` campaign = ₽50,000

**Effective revenue** (used in reports):
```sql
CASE
  WHEN assoc_revenue < direct_revenue THEN direct_revenue
  ELSE assoc_revenue
END AS revenue_eff
```

This ensures campaigns get credit for **both** direct conversions and long-term influence.

---

## Enrichment Tables

### `mart_deal_enrichments`

**Purpose**: Calculated columns derived from `raw_bitrix_deals`  
**Key columns**:
- `deal_id` — FK to `raw_bitrix_deals."ID"`
- `month` — YYYY-MM (from `Дата создания`)
- `pay_month` — YYYY-MM (from `Дата оплаты`)
- `revenue_amount` — Calculated revenue (float)
- `is_revenue_variant3` — 1 if paid deal (according to variant-3 revenue logic)
- `event_class` — Event type: `'вебинар'`, `'демо'`, `'курс'`, `'Attacking January'`, etc.
- `funnel_group` — Funnel grouping (e.g., `'ПБХ'`, `'Demo'`, `'Attacking January'`)
- `course_code_norm` — Normalized course code
- `lead_quality_types` — JSON array of quality issues (e.g., `["Дубль контакта"]`)

**Writers**:
- `db/revenue_variant3.py` — Revenue calculation
- `db/event_classifier.py` — Event classification
- `db/bitrix_lead_quality.py` — Quality flags

---

### `mart_deals_enriched`

**Purpose**: Flat query mart for API performance (pre-computed JOIN)

```sql
CREATE TABLE mart_deals_enriched AS
SELECT
  r.*,
  e.month,
  e.pay_month,
  e.revenue_amount,
  e.is_revenue_variant3,
  e.event_class,
  e.funnel_group,
  e.course_code_norm,
  e.lead_quality_types
FROM raw_bitrix_deals r
LEFT JOIN mart_deal_enrichments e ON e.deal_id = r."ID";
```

**Regenerated by**:
- `db/refresh_analytics_pipeline.py` (local SQLite)
- `web_share_subset/webpush/functions/lib/analytics/rebuildMartDeals.ts` (Cloudflare D1)

---

## Email Hierarchy Reports

### Python Implementation

**Script**: `db/email_hierarchy_revenue.py`

**Functions**:
- `enrich_hierarchy_with_revenue(hierarchy, deals_source)` → Adds revenue columns
- `deal_id_to_revenue(deals)` → Map of `{deal_id: revenue_amount}`
- `_fl_ids_revenue_stats(fl_ids, id_rev, by_id)` → Stats per send (revenue, deal count, avg)

**Usage** (from Jupyter notebook):
```python
from email_hierarchy_revenue import enrich_hierarchy_with_revenue

hierarchy = pd.read_json("email_hierarchy_by_send.json")
deals = pd.read_csv("bitrix_union.csv", sep=";")

enriched = enrich_hierarchy_with_revenue(
    hierarchy,
    deals,
    ids_col="fl_IDs",
    revenue_col="Выручка"
)
```

**Output columns**:
- `Выручка` — Total revenue from `fl_IDs` (FirstLine deal IDs)
- `Сделок с выручкой` — Count of paid deals
- `Средняя выручка на сделку` — Average revenue per paid deal
- `Средний остаток по сделке` — Average unpaid balance (`Сумма - Оплачено`)

---

### TypeScript Implementation (Cloudflare)

**Module**: `web_share_subset/webpush/functions/lib/analytics/materializeDatasets.ts`

**Datasets**:
1. **Send-level** (`email_sends.json`):
   - Rows: One per email send
   - Columns: Send metrics + matched leads + revenue + associated revenue
   - Levels: `'Send'`, `'Month'`, `'Spacer'` (month totals)

2. **Month-level** (`email_summary.json`):
   - Rows: One per month
   - Aggregated metrics: sends, leads, revenue, email base size

**Key CTEs**:
```typescript
WITH email_pools AS (
  -- Earliest registration per contact × campaign
  ...
),
assoc_rev AS (
  -- Associated revenue (cross-channel attribution)
  ...
),
deals_by_campaign AS (
  -- Direct UTM-matched leads
  ...
),
send_rows AS (
  -- Join email sends to leads and assoc revenue
  SELECT e.*, dbc.leads, ar.assoc_revenue
  FROM stg_email_sends e
  LEFT JOIN deals_by_campaign dbc ON dbc.utm_campaign_key = e.utm_campaign
  LEFT JOIN assoc_rev ar ON ar.utm_key = e.utm_campaign
  ...
)
```

---

## Data Pipeline Workflow

### 1. Fetch Raw Data

```bash
# Fetch Sendsay campaigns
cd db/
python fetch_sendsay_emails.py

# Update Bitrix deals from API
python b24_ingest.py

# Or: Load from CSV export
python upsert_raw_bitrix_from_union.py bitrix_union.csv
```

---

### 2. Build Contact UID Mapping

```bash
# Generate stg_contacts_uid from all deals
python build_contacts_uid.py

# Output:
#   - bitrix_contacts_uid.csv
#   - bitrix_contacts_uid_report.json
#   - Updates stg_contacts_uid table
```

---

### 3. Enrich Deals

```bash
# Classify events + compute revenue + quality flags
python event_classifier.py
python revenue_variant3.py
python bitrix_lead_quality.py

# Rebuild mart_deals_enriched
python refresh_analytics_pipeline.py
```

---

### 4. Build Email Hierarchy

**Option A: Python (for local analysis)**
```python
from email_hierarchy_revenue import enrich_hierarchy_with_revenue
import pandas as pd

deals = pd.read_sql("SELECT * FROM mart_deals_enriched", engine)
hierarchy = build_email_hierarchy_by_send(deals)  # custom function
enriched = enrich_hierarchy_with_revenue(hierarchy, deals)

enriched.to_json("email_hierarchy_enriched.json", orient="records", force_ascii=False)
```

**Option B: TypeScript (Cloudflare production)**
```bash
cd web_share_subset/webpush/
npm run deploy

# Materializer runs on schedule:
# cron: '0 */6 * * *'  # every 6 hours
```

API endpoint: `https://website-webpush.ivansherman127.workers.dev/api/data?path=email_sends.json`

---

### 5. Push to Production Server (Optional)

```bash
# Upload SQLite database to server
python db/push_stg_to_server.py

# Deploy Cloudflare Workers
cd web_share_subset/webpush/
npm run deploy
```

**Server credentials** (from `keys.json`):
- Host: `130.49.149.212`
- User: `deploy`
- Password: `cybered-lending9463!`

---

## Key Normalizations

### Date Parsing

Bitrix24 exports use **two date formats**:

1. **DD.MM.YYYY** (Russian Excel export)
2. **YYYY-MM-DD** (API export)

**SQL parsing**:
```sql
CASE
  WHEN "Дата создания" LIKE '____-__-__%'
    THEN SUBSTR("Дата создания", 1, 10)
  WHEN "Дата создания" LIKE '__.__.____%'
    THEN SUBSTR("Дата создания", 7, 4) || '-' ||
         SUBSTR("Дата создания", 4, 2) || '-' ||
         SUBSTR("Дата создания", 1, 2)
  ELSE ''
END AS deal_date_iso
```

---

### UTM Normalization

**Rule**: Lowercase + trim for all matching

```sql
LOWER(TRIM(COALESCE("UTM Campaign", '')))
```

**Common suffixes** (retained as-is):
- `_email`, `_sendsay`, `_2026`, etc.

**Case sensitivity**: Sendsay API returns lowercase UTM values; Bitrix24 export may have mixed case → normalization ensures matches.

---

### Contact ID Cleanup

Bitrix24 CSV exports sometimes render IDs as floats: `109067.0`

**Cleanup**:
```sql
REPLACE(TRIM(COALESCE("Контакт: ID", '')), '.0', '')
```

**Result**: `'109067'` (text)

---

## Revenue Logic (Variant 3)

**Script**: `db/revenue_variant3.py`

**Rules** (simplified):
1. Deal must have `Дата оплаты` (payment date)
2. Must be in a valid funnel (exclude test/archive funnels)
3. `Сумма > 0` (amount > 0)
4. Not marked as invalid lead type

**Implementation**:
```python
def variant3_revenue_mask(df: pd.DataFrame) -> pd.Series:
    has_payment_date = df["Дата оплаты"].notna() & df["Дата оплаты"].ne("")
    valid_funnel = ~df["Воронка"].isin(EXCLUDED_FUNNELS)
    amount_positive = pd.to_numeric(df["Сумма"], errors="coerce") > 0
    return has_payment_date & valid_funnel & amount_positive
```

**Excluded funnels**:
- Archive, Test, Demo (non-revenue), etc.

---

## Quality Checks

### Data Validation

**Check 1**: Email sends without leads
```sql
SELECT e."Название выпуска", e.utm_campaign
FROM stg_email_sends e
LEFT JOIN (
  SELECT LOWER(TRIM("UTM Campaign")) AS utm_key, COUNT(*) AS cnt
  FROM mart_deals_enriched
  WHERE LOWER(TRIM("UTM Source")) = 'sendsay'
  GROUP BY utm_key
) b ON b.utm_key = LOWER(TRIM(e.utm_campaign))
WHERE b.cnt IS NULL OR b.cnt = 0
ORDER BY e."Дата отправки" DESC;
```

**Expected**: Campaign sends that didn't generate leads (normal for some newsletters)

---

**Check 2**: Leads without email send
```sql
SELECT "UTM Campaign", COUNT(*) AS lead_count
FROM mart_deals_enriched
WHERE LOWER(TRIM("UTM Source")) = 'sendsay'
  AND LOWER(TRIM("UTM Campaign")) NOT IN (
    SELECT LOWER(TRIM(utm_campaign))
    FROM stg_email_sends
    WHERE utm_campaign IS NOT NULL AND utm_campaign <> ''
  )
GROUP BY "UTM Campaign"
ORDER BY lead_count DESC;
```

**Expected**: Leads from old campaigns (before Sendsay API integration) or manual UTM tagging errors

---

**Check 3**: Email send metrics vs. B24 leads
```sql
SELECT
  e."Название выпуска",
  e."Уник. кликов" AS sendsay_clicks,
  COUNT(DISTINCT b."ID") AS bitrix_leads,
  ROUND(100.0 * COUNT(DISTINCT b."ID") / NULLIF(e."Уник. кликов", 0), 2) AS conversion_pct
FROM stg_email_sends e
LEFT JOIN mart_deals_enriched b
  ON LOWER(TRIM(b."UTM Campaign")) = LOWER(TRIM(e.utm_campaign))
  AND LOWER(TRIM(b."UTM Source")) = 'sendsay'
WHERE e."Уник. кликов" > 0
GROUP BY e."ID", e."Название выпуска", e."Уник. кликов"
ORDER BY conversion_pct DESC;
```

**Expected**: Conversion rate varies by campaign type (webinar invites ~5-15%, newsletters ~1-3%)

---

## Troubleshooting

### Issue: No leads matched to email send

**Symptoms**:
- Email send in `stg_email_sends`
- `utm_campaign` populated
- Zero leads in `mart_deals_enriched`

**Diagnosis**:
1. Check UTM parameter in email links:
   ```sql
   SELECT utm_campaign, utm_source
   FROM stg_email_sends
   WHERE "Название выпуска" LIKE '%<campaign name>%';
   ```

2. Check B24 deals with similar UTM:
   ```sql
   SELECT "UTM Campaign", "UTM Source", COUNT(*)
   FROM mart_deals_enriched
   WHERE "UTM Campaign" LIKE '%<partial slug>%'
   GROUP BY "UTM Campaign", "UTM Source";
   ```

3. **Common causes**:
   - UTM parameter typo in email links
   - Bitrix24 web form not capturing UTM parameters
   - UTM Source set to something other than `'sendsay'` (e.g., `'email'`)

**Fix**:
- Update Sendsay email template to use correct UTM parameters
- Or: Manually update `raw_bitrix_deals` UTM values in SQLite

---

### Issue: Associated revenue is zero despite paid deals

**Symptoms**:
- Direct revenue (`revenue`) > 0
- Associated revenue (`assoc_revenue`) = 0

**Diagnosis**:
```sql
SELECT
  "Контакт: ID",
  MIN("Дата создания") AS first_deal,
  MAX("Дата оплаты") AS last_payment,
  SUM(revenue_amount) AS total_revenue
FROM mart_deals_enriched
WHERE LOWER(TRIM("UTM Source")) = 'sendsay'
  AND LOWER(TRIM("UTM Campaign")) = '<campaign slug>'
GROUP BY "Контакт: ID";
```

**Common causes**:
1. Payment date (`Дата оплаты`) is **before** registration date → Not counted in assoc revenue
2. Contact ID is blank/null → No cross-deal tracking
3. Revenue deals are from *same* UTM campaign → Counted as direct, not associated

**Expected behavior**: Associated revenue only counts deals where:
- Contact registered via email campaign A
- Later paid via *different* source (e.g., Yandex, organic)
- Payment date > registration date

---

### Issue: Contact merge not working

**Symptoms**:
- Same person appears as multiple `contact_uid` values
- Email addresses match visually but not in `stg_contacts_uid`

**Diagnosis**:
```python
# Check raw email values
import pandas as pd
from conn import get_engine

engine = get_engine()
df = pd.read_sql_query("""
  SELECT "Контакт: ID", "Контакт: E-mail рабочий"
  FROM raw_bitrix_deals
  WHERE "Контакт: E-mail рабочий" LIKE '%<domain>%'
""", engine)

print(df["Контакт: E-mail рабочий"].unique())
```

**Common causes**:
1. Extra whitespace: `'user@example.com'` vs. `'user@example.com '`
2. Case mismatch: `'User@Example.com'` vs. `'user@example.com'`
3. Invalid email format in CRM (missing `@`, typos)

**Fix**:
- Re-run `build_contacts_uid.py` (applies normalization)
- Or: Manually clean data in Bitrix24 CRM

---

## Schema Diagram

```
┌─────────────────────────┐
│ raw_bitrix_deals        │  ← CSV export / API
│ ─────────────────────── │
│ ID (PK)                 │
│ Контакт: ID             │
│ Контакт: E-mail         │
│ Дата создания           │
│ Дата оплаты             │
│ UTM Source              │
│ UTM Campaign            │←──┐
│ ...                     │   │
└─────────────────────────┘   │
           │                  │
           ↓                  │
┌─────────────────────────┐   │  Matching key:
│ stg_contacts_uid        │   │  LOWER(TRIM(utm_campaign))
│ ─────────────────────── │   │
│ contact_uid (PK)        │   │
│ contact_id (PK)         │   │
│ all_emails              │   │
│ first_deal_date         │   │
│ first_touch_event       │   │
└─────────────────────────┘   │
                              │
┌─────────────────────────┐   │
│ stg_email_sends         │←──┘  ← Sendsay API
│ ─────────────────────── │
│ ID                      │
│ Название выпуска        │
│ Дата отправки           │
│ utm_campaign            │
│ utm_source = 'sendsay'  │
│ Отправлено              │
│ Уник. открытий          │
│ Уник. кликов            │
│ ...                     │
└─────────────────────────┘
           │
           ↓
┌─────────────────────────┐
│ Email Hierarchy         │  ← Python / TypeScript
│ ─────────────────────── │
│ Месяц                   │
│ Название выпуска        │
│ Лиды                    │
│ Выручка (direct)        │
│ Выручка (assoc)         │
└─────────────────────────┘
```

---

## API Endpoints (Cloudflare Workers)

Base URL: `https://website-webpush.ivansherman127.workers.dev`

### GET `/api/data?path=email_sends.json`

**Returns**: Send-level email hierarchy

**Sample response**:
```json
[
  {
    "Level": "Send",
    "Месяц": "2026-03",
    "Название выпуска": "Webinar: Security Automation",
    "utm_campaign": "march-webinar-2026",
    "Тема": "Приглашение на вебинар...",
    "Leads": 47,
    "Qual": 42,
    "Unqual": 5,
    "Refusal": 0,
    "fl_IDs": "123456,123457,123458...",
    "Отправлено": 5000,
    "Доставлено": 4950,
    "Уник. открытий": 1200,
    "Уник. кликов": 320,
    "Сделок с выручкой": 8,
    "Выручка": 450000.0
  },
  ...
]
```

---

### GET `/api/data?path=email_summary.json`

**Returns**: Month-level email summary

**Sample response**:
```json
[
  {
    "Период": "Март 2026",
    "Актуальная база email": 18500,
    "Контактов email (DB)": 19200,
    "Рассылок за месяц": 12,
    "Лиды": 234,
    "Сделок с выручкой": 45,
    "Выручка": 2350000.0,
    "month": "2026-03"
  },
  ...
]
```

---

## File Locations

### Database
- **SQLite**: `/Users/ivan/Documents/website/website.db`
- **Cloudflare D1**: `website-analytics` (Workers KV binding)

### Scripts
- `db/fetch_sendsay_emails.py` — Sendsay API ingest
- `db/build_contacts_uid.py` — Email-based contact dedup
- `db/email_hierarchy_revenue.py` — Revenue enrichment (Python)
- `db/refresh_analytics_pipeline.py` — Full pipeline rebuild

### TypeScript (Cloudflare)
- `web_share_subset/webpush/functions/lib/analytics/materializeDatasets.ts` — Email hierarchy materializer
- `web_share_subset/webpush/functions/api/data.ts` — JSON API endpoint

### Credentials
- `keys.json` — Sendsay login, server SSH, Bitrix24 webhook, etc.

---

## Maintenance Checklist

### Weekly
- [ ] Fetch new Sendsay campaigns: `python db/fetch_sendsay_emails.py --if-exists append`
- [ ] Update Bitrix24 deals: `python db/b24_ingest.py`
- [ ] Rebuild contact UID mapping: `python db/build_contacts_uid.py`
- [ ] Regenerate email hierarchy: `python db/refresh_analytics_pipeline.py`

### Monthly
- [ ] Audit unmatched email sends (see Quality Checks above)
- [ ] Review associated revenue logic for new edge cases
- [ ] Archive old email sends (>1 year) to separate table

### On Schema Changes
- [ ] Update TypeScript types in `web_share_subset/webpush/functions/lib/analytics/types.ts`
- [ ] Migrate D1 Database: `npx wrangler d1 migrations apply website-analytics`
- [ ] Update this handoff document

---

## Contact

**Owner**: Ivan Sherman  
**Email**: ivansherman127@yandex.ru  
**Repository**: `/Users/ivan/Documents/website/`

For questions about:
- **Sendsay API**: Refer to `db/fetch_sendsay_emails.py` docstring
- **Contact deduplication**: Refer to `db/build_contacts_uid.py` docstring
- **Revenue logic**: Refer to `db/revenue_variant3.py`
- **TypeScript pipeline**: Refer to `web_share_subset/webpush/functions/lib/analytics/README.md`

---

**End of handoff document**
