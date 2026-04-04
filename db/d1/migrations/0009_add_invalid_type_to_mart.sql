-- Add invalid-type column to mart_deals_enriched so the quality-check flag
-- is preserved after mart rebuild, independent of which raw p0x chunk holds it.
ALTER TABLE mart_deals_enriched ADD COLUMN "Типы некачественного лида" TEXT;
