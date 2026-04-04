-- Sync mart_attacking_january_cohort_deals schema with mart_deals_enriched:
-- add the invalid-type column that was missed when 0009 was added to mart_deals_enriched only.
ALTER TABLE mart_attacking_january_cohort_deals ADD COLUMN "Типы некачественного лида" TEXT;
