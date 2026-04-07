-- Drop mart_attacking_january_cohort_deals.
--
-- This table was a full schema-copy of mart_deals_enriched filtered to
-- is_attacking_january = 1. Maintaining it meant:
--   - 2× push time for every mart rebuild
--   - guaranteed schema drift (every ALTER to mart_deals_enriched also
--     needed a matching ALTER to this table, e.g. migrations 0011/0012/0013)
--
-- Replacement: assoc-revenue.ts applies AND is_attacking_january = 1
-- directly on mart_deals_enriched when cohort=attacking_january is requested.
-- No data is lost; the flag column remains in mart_deals_enriched.

DROP TABLE IF EXISTS mart_attacking_january_cohort_deals;
