-- Core source tables (subset of columns used in matching/reporting).
-- Column names match CSV/notebook expectations where possible.

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

-- Raw Bitrix deals as a relational table (columns come from export headers).
-- Maintained by db/upsert_raw_bitrix_from_union.py via pandas.to_sql(..., if_exists='replace').
CREATE TABLE IF NOT EXISTS raw_bitrix_deals (
  "ID" TEXT PRIMARY KEY,
  source_batch TEXT NOT NULL,
  ingested_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_bitrix_deals_ingested_at
  ON raw_bitrix_deals (ingested_at);

-- Optional lineage per batch import.
CREATE TABLE IF NOT EXISTS raw_source_batches (
  source_batch TEXT PRIMARY KEY,
  source_type TEXT NOT NULL,
  source_ref TEXT,
  row_count INTEGER,
  created_at TEXT NOT NULL
);
