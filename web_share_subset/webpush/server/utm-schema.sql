-- UTM tags table — applied automatically on server startup if missing.
CREATE TABLE IF NOT EXISTS utm_tags (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at   TEXT NOT NULL DEFAULT (datetime('now')),
  utm_source   TEXT NOT NULL,
  utm_medium   TEXT NOT NULL,
  utm_campaign TEXT NOT NULL,
  campaign_link TEXT NOT NULL,
  utm_content  TEXT NOT NULL,
  utm_term     TEXT NOT NULL,
  utm_tag      TEXT NOT NULL,
  created_by   TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_utm_tags_created_at ON utm_tags (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_utm_tags_medium_source ON utm_tags (utm_medium, utm_source);
