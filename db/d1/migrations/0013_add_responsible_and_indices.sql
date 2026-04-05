-- Add "Ответственный" (responsible manager) column to mart tables
-- so manager reports work without raw_bitrix_deals_p01.
ALTER TABLE mart_deals_enriched ADD COLUMN "Ответственный" TEXT;
ALTER TABLE mart_attacking_january_cohort_deals ADD COLUMN "Ответственный" TEXT;

-- Add indices to mart_deals_enriched to speed up assoc-revenue queries,
-- watermark computation (MAX on month), and manager reports.
CREATE INDEX IF NOT EXISTS idx_mart_deals_month          ON mart_deals_enriched (month);
CREATE INDEX IF NOT EXISTS idx_mart_deals_is_revenue     ON mart_deals_enriched (is_revenue_variant3);
CREATE INDEX IF NOT EXISTS idx_mart_deals_contact_id     ON mart_deals_enriched ("Контакт: ID");
CREATE INDEX IF NOT EXISTS idx_mart_deals_event_class    ON mart_deals_enriched (event_class);
CREATE INDEX IF NOT EXISTS idx_mart_deals_responsible    ON mart_deals_enriched ("Ответственный");
