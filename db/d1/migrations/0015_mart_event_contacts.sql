-- Per-(event_class, month) contact counts, used by the assoc_dynamic view
-- to show "Новых с мероприятия" (new contacts whose first deal was via this
-- event class in this month).
--
-- Populated by the materialize step that currently generates
-- bitrix_new_event_contacts_by_event.json via an ad-hoc SQL query.
-- Having it as a proper table:
--   1. Makes the data queryable and inspectable.
--   2. Provides a stable, correctly-named column (event_class) for app.ts
--      to use as the ecMap key — fixing the "= 0 everywhere" bug.
--
-- The all-time aggregate (month = 'all') gives a quick per-project total
-- without a date filter.

CREATE TABLE mart_event_contacts (
  event_class      TEXT    NOT NULL,
  month            TEXT    NOT NULL,  -- 'YYYY-MM' or 'all' for aggregate row
  contacts_total   INTEGER NOT NULL DEFAULT 0,
  contacts_new     INTEGER NOT NULL DEFAULT 0,  -- first-ever deal in this event_class × month

  PRIMARY KEY (event_class, month)
);

CREATE INDEX IF NOT EXISTS idx_event_contacts_month
  ON mart_event_contacts (month);
