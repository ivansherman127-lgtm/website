-- Migration 0018: add first-touch attribution columns to stg_contacts_uid
--
-- first_deal_date   – ISO YYYY-MM-DD of the earliest deal for this contact uid
-- first_touch_event – event_class of that earliest deal (from event_classifier)
-- all_events        – pipe-separated distinct event_class values across all deals

ALTER TABLE stg_contacts_uid ADD COLUMN first_deal_date TEXT;
ALTER TABLE stg_contacts_uid ADD COLUMN first_touch_event TEXT;
ALTER TABLE stg_contacts_uid ADD COLUMN all_events TEXT;
