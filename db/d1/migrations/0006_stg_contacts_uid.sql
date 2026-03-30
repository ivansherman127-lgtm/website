CREATE TABLE IF NOT EXISTS stg_contacts_uid (
  contact_uid TEXT NOT NULL,
  contact_id TEXT NOT NULL,
  all_names TEXT,
  all_phones TEXT,
  all_emails TEXT,
  contact_ids_count INTEGER,
  names_count INTEGER,
  phones_count INTEGER,
  emails_count INTEGER,
  PRIMARY KEY (contact_uid, contact_id)
);

CREATE INDEX IF NOT EXISTS idx_stg_contacts_uid_contact_id
  ON stg_contacts_uid (contact_id);

CREATE INDEX IF NOT EXISTS idx_stg_contacts_uid_contact_uid
  ON stg_contacts_uid (contact_uid);