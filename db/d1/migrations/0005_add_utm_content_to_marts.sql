-- Add missing UTM Content columns used by runtime Yandex attribution queries.
ALTER TABLE mart_deals_enriched ADD COLUMN "UTM Content" TEXT;
ALTER TABLE mart_attacking_january_cohort_deals ADD COLUMN "UTM Content" TEXT;
