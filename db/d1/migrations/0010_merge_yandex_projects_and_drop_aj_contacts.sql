-- Merge mart_yandex_revenue_projects_raw + mart_yandex_revenue_projects_dedup
-- into a single mart_yandex_revenue_projects table, and drop the now-redundant
-- mart_attacking_january_contacts staging table.

CREATE TABLE mart_yandex_revenue_projects (
  project_name      TEXT,
  yandex_month      TEXT,
  leads_raw         INTEGER,
  deals_raw         INTEGER,
  paid_deals_raw    INTEGER,
  revenue_raw       REAL,
  spend             INTEGER,
  leads_dedup       INTEGER,
  paid_deals_dedup  INTEGER,
  revenue_dedup     REAL
);

DROP TABLE mart_yandex_revenue_projects_raw;
DROP TABLE mart_yandex_revenue_projects_dedup;
DROP TABLE mart_attacking_january_contacts;
