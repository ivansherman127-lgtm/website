-- Raw Bitrix baseline in D1 (table may be recreated from SQLite schema during push).

CREATE TABLE IF NOT EXISTS raw_bitrix_deals (
  "ID" TEXT PRIMARY KEY NOT NULL,
  source_batch TEXT NOT NULL,
  ingested_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_bitrix_deals_ingested_at
  ON raw_bitrix_deals (ingested_at);

CREATE TABLE IF NOT EXISTS raw_source_batches (
  source_batch TEXT PRIMARY KEY NOT NULL,
  source_type TEXT NOT NULL,
  source_ref TEXT,
  row_count INTEGER,
  created_at TEXT NOT NULL
);
