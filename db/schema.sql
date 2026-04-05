-- ============================================================
-- Schema layers:
--   Layer 1  Raw source          raw_bitrix_deals  (Bitrix CSV, script-updated)
--                                stg_yandex_stats  (Yandex Ads, CSV/API import)
--                                stg_email_sends   (Sendsay API import)
--                                raw_source_batches (import lineage)
--   Layer 2  Staging reference   stg_contacts_uid  (contact deduplication mapping)
--   Layer 3  Deal enrichments    mart_deal_enrichments  (calculated cols, keyed to raw)
--   Layer 4  Flat query mart     mart_deals_enriched    (pre-computed JOIN for API)
--   Layer 5  Aggregate facts     mart_event_contacts, mart_yandex_revenue_projects
-- ============================================================

-- Legacy source tables kept for backward compatibility with older scripts.
-- New analytics code reads from raw_bitrix_deals / mart_deal_enrichments.

-- Firstline (Bitrix24) deals: ID as string PK, UTM + funnel columns.
CREATE TABLE IF NOT EXISTS deals (
  "ID" TEXT PRIMARY KEY,
  "Воронка" TEXT,
  "Стадия сделки" TEXT,
  "Дата создания" TEXT,
  "UTM Source" TEXT,
  "UTM Medium" TEXT,
  "UTM Campaign" TEXT,
  "UTM Content" TEXT,
  "UTM Term" TEXT
);

-- Yandex ad stats: row id + campaign/ad hierarchy and metrics.
CREATE TABLE IF NOT EXISTS yandex_stats (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  "Месяц" TEXT,
  "№ Кампании" TEXT,
  "Название кампании" TEXT,
  "№ Группы" TEXT,
  "Название группы" TEXT,
  "№ Объявления" TEXT,
  "Статус объявления" TEXT,
  "Тип объявления" TEXT,
  "Заголовок" TEXT,
  "Текст" TEXT,
  "Ссылка" TEXT,
  "Расход, ₽" REAL,
  "Клики" INTEGER,
  "Конверсии" INTEGER,
  "CR, %" REAL,
  "CPA, ₽" REAL
);

-- Mass email sends: one row per send with UTM and metrics.
CREATE TABLE IF NOT EXISTS email_sends (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  "Дата отправки" TEXT,
  "Название выпуска" TEXT,
  "Получатели" TEXT,
  "Тема" TEXT,
  "Отправлено" INTEGER,
  "Доставлено" INTEGER,
  "Ошибок" INTEGER,
  "Открытий" INTEGER,
  "Уник. открытий" INTEGER,
  "Кликов" INTEGER,
  "Уник. кликов" INTEGER,
  "CTOR, %" REAL,
  "Отписок" INTEGER,
  "UTOR, %" REAL,
  "ID" TEXT,
  "Номер задания" TEXT,
  utm_campaign TEXT,
  utm_content TEXT,
  utm_medium TEXT,
  utm_source TEXT,
  utm_term TEXT
);

-- Optional: audit log of Google Sheets syncs.
CREATE TABLE IF NOT EXISTS sheets_sync_log (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  sheet_id TEXT NOT NULL,
  worksheet_name TEXT NOT NULL,
  synced_at TEXT NOT NULL,
  row_count INTEGER
);

-- ============================================================
-- Layer 1 — Raw source
-- ============================================================

-- Raw Bitrix deals: ALL columns from the union CSV export, written by
-- db/upsert_raw_bitrix_from_union.py via pandas.to_sql(..., if_exists='replace').
-- Never add calculated columns here.
CREATE TABLE IF NOT EXISTS raw_bitrix_deals (
  "ID" TEXT PRIMARY KEY,
  source_batch TEXT NOT NULL,
  ingested_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_bitrix_deals_ingested_at
  ON raw_bitrix_deals (ingested_at);

-- Batch import lineage.
CREATE TABLE IF NOT EXISTS raw_source_batches (
  source_batch TEXT PRIMARY KEY,
  source_type TEXT NOT NULL,
  source_ref TEXT,
  row_count INTEGER,
  created_at TEXT NOT NULL
);

-- Yandex Ads stats: one row per ad per month, written by upsert_yandex_from_csv.py.
-- Schema includes all columns from the Yandex CSV export.  "День" added in migration 0007.
-- Never add calculated columns here.
CREATE TABLE IF NOT EXISTS stg_yandex_stats (
  "Месяц" TEXT,
  "День" TEXT,
  "№ Кампании" REAL,
  "Название кампании" TEXT,
  "№ Группы" REAL,
  "Название группы" TEXT,
  "№ Объявления" REAL,
  "Статус объявления" TEXT,
  "Тип объявления" TEXT,
  "Заголовок" TEXT,
  "Текст" TEXT,
  "Ссылка" TEXT,
  "Путь до изображения" TEXT,
  "Название файла изображения" TEXT,
  "Идентификатор видео" TEXT,
  "Путь до превью видео" TEXT,
  "Место клика" TEXT,
  "Формат" TEXT,
  "Источник текста" TEXT,
  "Расход, ₽" REAL,
  "Клики" INTEGER,
  "Конверсии" INTEGER,
  "CR, %" TEXT,
  "CPA, ₽" TEXT,
  month TEXT
);

-- Sendsay email campaigns: one row per send, written by fetch_sendsay_emails.py.
-- Matches the legacy CSV export column schema.  Never add calculated columns here.
CREATE TABLE IF NOT EXISTS stg_email_sends (
  "Дата отправки" TEXT,
  "Название выпуска" TEXT,
  "Получатели" TEXT,
  "Тема" TEXT,
  "Отправлено" INTEGER,
  "Доставлено" INTEGER,
  "Ошибок" INTEGER,
  "Открытий" INTEGER,
  "Уник. открытий" INTEGER,
  "Кликов" INTEGER,
  "Уник. кликов" INTEGER,
  "CTOR, %" REAL,
  "Отписок" INTEGER,
  "UTOR, %" REAL,
  "ID" TEXT,
  "Номер задания" TEXT,
  utm_campaign TEXT,
  utm_content TEXT,
  utm_medium TEXT,
  utm_source TEXT,
  utm_term TEXT,
  month TEXT
);

-- ============================================================
-- Layer 2 — Staging / reference tables
-- ============================================================

-- Canonical contact deduplication mapping: N contact_ids → 1 contact_uid.
-- Also carries first-touch attribution computed at build time.
CREATE TABLE IF NOT EXISTS stg_contacts_uid (
  contact_uid TEXT NOT NULL,
  contact_id TEXT NOT NULL,
  all_names TEXT,
  all_phones TEXT,
  all_emails TEXT,
  contact_ids_count INTEGER,
  names_count INTEGER,
  phones_count INTEGER,
  emails_count INTEGER,
  first_deal_date TEXT,        -- ISO YYYY-MM-DD of earliest deal for this uid
  first_touch_event TEXT,      -- event_class of that earliest deal
  all_events TEXT,             -- pipe-separated distinct event_class values
  PRIMARY KEY (contact_uid, contact_id)
);

CREATE INDEX IF NOT EXISTS idx_stg_contacts_uid_contact_id
  ON stg_contacts_uid (contact_id);

CREATE INDEX IF NOT EXISTS idx_stg_contacts_uid_contact_uid
  ON stg_contacts_uid (contact_uid);

-- ============================================================
-- Layer 3 — Deal enrichments (calculated columns only)
-- ============================================================

-- One row per deal. Contains only values derived by scripts from raw_bitrix_deals.
-- Written by: event_classifier.py, bitrix_lead_quality.py, revenue_variant3.py.
-- mart_deals_enriched (Layer 4) is built as:
--   SELECT r.*, e.*
--   FROM raw_bitrix_deals r
--   LEFT JOIN mart_deal_enrichments e ON e.deal_id = r."ID"
CREATE TABLE IF NOT EXISTS mart_deal_enrichments (
  deal_id                   TEXT    PRIMARY KEY,  -- FK → raw_bitrix_deals."ID"
  month                     TEXT,                 -- YYYY-MM from "Дата создания"
  pay_month                 TEXT,                 -- YYYY-MM from "Дата оплаты"
  revenue_amount            REAL,
  is_revenue_variant3       INTEGER,              -- 1 = paid deal in variant-3 logic
  funnel_group              TEXT,
  event_class               TEXT,                 -- webinar/demo/event/course/Другое
  course_code_norm          TEXT,
  classification_source     TEXT,
  classification_pattern    TEXT,
  classification_confidence TEXT,
  lead_quality_types        TEXT,                 -- JSON array of quality-issue labels
  responsible               TEXT
);

CREATE INDEX IF NOT EXISTS idx_deal_enrichments_month
  ON mart_deal_enrichments (month);

CREATE INDEX IF NOT EXISTS idx_deal_enrichments_is_revenue
  ON mart_deal_enrichments (is_revenue_variant3);

CREATE INDEX IF NOT EXISTS idx_deal_enrichments_event_class
  ON mart_deal_enrichments (event_class);

-- ============================================================
-- Layer 4 — Flat query mart (pre-computed for API performance)
-- ============================================================

-- mart_deals_enriched is the main table queried by the API (assoc-revenue.ts etc.).
-- It is regenerated from raw_bitrix_deals JOIN mart_deal_enrichments before each
-- D1 push; do not edit it directly.
-- Schema defined by D1 migrations (0001_initial.sql + ALTER migrations).

-- mart_attacking_january_cohort_deals has been DROPPED (migration 0016).
-- Use: SELECT … FROM mart_deals_enriched WHERE event_class = 'Attacking January'

-- ============================================================
-- Layer 5 — Aggregate fact tables
-- ============================================================

-- Per-(event_class, month) contact counts.
-- 'all' in the month column = all-time aggregate.
-- Fixes "Новых с мероприятия = 0" by providing a table keyed on event_class.
CREATE TABLE IF NOT EXISTS mart_event_contacts (
  event_class    TEXT    NOT NULL,
  month          TEXT    NOT NULL,  -- 'YYYY-MM' or 'all'
  contacts_total INTEGER NOT NULL DEFAULT 0,
  contacts_new   INTEGER NOT NULL DEFAULT 0,

  PRIMARY KEY (event_class, month)
);

CREATE INDEX IF NOT EXISTS idx_event_contacts_month
  ON mart_event_contacts (month);

