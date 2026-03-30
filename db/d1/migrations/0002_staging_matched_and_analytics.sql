-- Staging for D1-first rebuild: matched Yandex rows (JSON) + trimmed deal facts for Workers TS/SQL.
-- Populated locally by run_all_slices → push_from_sqlite.

CREATE TABLE stg_matched_yandex (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  row_json TEXT NOT NULL
);

CREATE TABLE stg_deals_analytics (
  deal_id TEXT PRIMARY KEY NOT NULL,
  contact_id TEXT,
  created_at TEXT,
  funnel_raw TEXT,
  stage_raw TEXT,
  closed_yes TEXT,
  pay_date TEXT,
  installment_schedule TEXT,
  sum_text TEXT,
  utm_source TEXT,
  utm_medium TEXT,
  utm_campaign TEXT,
  deal_name TEXT,
  code_site TEXT,
  code_course TEXT,
  source_detail TEXT,
  source_inquiry TEXT
);

CREATE TABLE analytics_build_meta (
  k TEXT PRIMARY KEY NOT NULL,
  v TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
