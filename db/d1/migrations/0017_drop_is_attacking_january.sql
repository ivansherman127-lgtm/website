-- Migration 0017: drop is_attacking_january column from mart_deal_enrichments
-- and mart_deals_enriched.
--
-- The column was a legacy override that duplicated event_class logic.
-- Classification now sets event_class = 'Attacking January' directly.
-- Equivalent filter: WHERE event_class = 'Attacking January'

-- SQLite does not support DROP COLUMN directly on older versions;
-- recreate the tables without the column.

-- mart_deal_enrichments
CREATE TABLE mart_deal_enrichments_new (
  deal_id                   TEXT    PRIMARY KEY,
  month                     TEXT,
  pay_month                 TEXT,
  revenue_amount            REAL,
  is_revenue_variant3       INTEGER,
  funnel_group              TEXT,
  event_class               TEXT,
  course_code_norm          TEXT,
  classification_source     TEXT,
  classification_pattern    TEXT,
  classification_confidence TEXT,
  lead_quality_types        TEXT,
  responsible               TEXT
);
INSERT INTO mart_deal_enrichments_new
  SELECT deal_id, month, pay_month, revenue_amount, is_revenue_variant3,
         funnel_group, event_class, course_code_norm, classification_source,
         classification_pattern, classification_confidence,
         lead_quality_types, responsible
  FROM mart_deal_enrichments;
DROP TABLE mart_deal_enrichments;
ALTER TABLE mart_deal_enrichments_new RENAME TO mart_deal_enrichments;

-- Restore indices
CREATE INDEX IF NOT EXISTS idx_deal_enrichments_month
  ON mart_deal_enrichments (month);
CREATE INDEX IF NOT EXISTS idx_deal_enrichments_is_revenue
  ON mart_deal_enrichments (is_revenue_variant3);
CREATE INDEX IF NOT EXISTS idx_deal_enrichments_event_class
  ON mart_deal_enrichments (event_class);
