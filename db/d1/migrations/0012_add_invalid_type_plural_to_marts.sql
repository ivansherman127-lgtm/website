-- Add plural variant of invalid-lead-type column to both mart tables so that
-- buildInvalidTokenCond() can safely reference both column names.
ALTER TABLE mart_deals_enriched ADD COLUMN "Типы некачественных лидов" TEXT;
ALTER TABLE mart_attacking_january_cohort_deals ADD COLUMN "Типы некачественных лидов" TEXT;
