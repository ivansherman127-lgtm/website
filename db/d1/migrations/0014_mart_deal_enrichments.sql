-- Separate calculated/enrichment columns from raw Bitrix deal data.
--
-- mart_deal_enrichments: one row per deal, contains only derived values that
-- scripts compute from the raw export (event classification, revenue flags,
-- lead quality). FK deal_id → raw_bitrix_deals."ID".
--
-- mart_deals_enriched will continue to exist as the flat pre-computed table
-- that the API queries. Going forward it is built as:
--   SELECT r.*, e.*
--   FROM raw_bitrix_deals r
--   LEFT JOIN mart_deal_enrichments e ON e.deal_id = r."ID"
-- and pushed to D1 before each deploy.

CREATE TABLE mart_deal_enrichments (
  deal_id                 TEXT    PRIMARY KEY,   -- raw_bitrix_deals."ID"
  month                   TEXT,                  -- YYYY-MM extracted from "Дата создания"
  pay_month               TEXT,                  -- YYYY-MM extracted from "Дата оплаты"
  revenue_amount          REAL,                  -- numeric parse of "Сумма"
  is_revenue_variant3     INTEGER,               -- 1 = counts as paid deal in variant-3 logic
  funnel_group            TEXT,                  -- normalised funnel bucket
  event_class             TEXT,                  -- smart classification: webinar/demo/event/course/Другое
  course_code_norm        TEXT,                  -- normalised course code
  classification_source   TEXT,
  classification_pattern  TEXT,
  classification_confidence TEXT,
  is_attacking_january    INTEGER,               -- 1 = belongs to Attacking January cohort
  lead_quality_types      TEXT,                  -- JSON array of quality-issue labels
  responsible             TEXT                   -- "Ответственный" manager name
);

CREATE INDEX IF NOT EXISTS idx_deal_enrichments_month
  ON mart_deal_enrichments (month);

CREATE INDEX IF NOT EXISTS idx_deal_enrichments_is_revenue
  ON mart_deal_enrichments (is_revenue_variant3);

CREATE INDEX IF NOT EXISTS idx_deal_enrichments_event_class
  ON mart_deal_enrichments (event_class);
