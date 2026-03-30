-- Allow large JSON blobs: split body across multiple rows keyed by (path, chunk).
CREATE TABLE dataset_json_new (
  path TEXT NOT NULL,
  chunk INTEGER NOT NULL,
  body TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (path, chunk)
);

INSERT INTO dataset_json_new (path, chunk, body, updated_at)
  SELECT path, 0, body, updated_at FROM dataset_json;

DROP TABLE dataset_json;
ALTER TABLE dataset_json_new RENAME TO dataset_json;
