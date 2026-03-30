-- D1 schema mirroring local SQLite marts (no stg_bitrix_deals_wide).
-- dataset_json: static dashboard JSON keyed by path under public/data/.

CREATE TABLE dataset_json (
  path TEXT PRIMARY KEY NOT NULL,
  body TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE mart_deals_enriched (
  "ID" TEXT,
  "Контакт: ID" TEXT,
  "Дата создания" TEXT,
  month TEXT,
  "Воронка" TEXT,
  funnel_group TEXT,
  "Стадия сделки" TEXT,
  "Сделка закрыта" TEXT,
  "Дата оплаты" TEXT,
  "Сумма" TEXT,
  revenue_amount REAL,
  is_revenue_variant3 INTEGER,
  "UTM Source" TEXT,
  "UTM Medium" TEXT,
  "UTM Campaign" TEXT,
  "Название сделки" TEXT,
  "Код_курса_сайт" TEXT,
  "Код курса" TEXT,
  course_code_norm TEXT,
  event_class TEXT,
  classification_source TEXT,
  classification_pattern TEXT,
  classification_confidence TEXT,
  is_attacking_january INTEGER
);

CREATE TABLE mart_yandex_leads_raw (
  "ID" TEXT,
  contact_id TEXT,
  lead_key TEXT,
  deal_month TEXT,
  utm_campaign TEXT,
  project_name TEXT,
  campaign_id TEXT,
  yandex_month TEXT,
  yandex_spend INTEGER,
  deal_name TEXT,
  is_paid_deal INTEGER,
  revenue_amount REAL,
  funnel TEXT,
  stage TEXT
);

CREATE TABLE mart_yandex_leads_dedup (
  lead_key TEXT,
  project_name TEXT,
  deals_count INTEGER,
  paid_deals INTEGER,
  revenue REAL,
  contact_id TEXT,
  campaign_id TEXT,
  yandex_month TEXT
);

CREATE TABLE mart_yandex_revenue_projects_raw (
  project_name TEXT,
  yandex_month TEXT,
  leads_raw INTEGER,
  deals_raw INTEGER,
  paid_deals_raw INTEGER,
  revenue_raw REAL,
  spend INTEGER
);

CREATE TABLE mart_yandex_revenue_projects_dedup (
  project_name TEXT,
  yandex_month TEXT,
  leads_dedup INTEGER,
  paid_deals_dedup INTEGER,
  revenue_dedup REAL
);

CREATE TABLE mart_attacking_january_contacts (
  contact_id TEXT
);

CREATE TABLE mart_attacking_january_cohort_deals (
  "ID" TEXT,
  "Контакт: ID" TEXT,
  "Дата создания" TEXT,
  month TEXT,
  "Воронка" TEXT,
  funnel_group TEXT,
  "Стадия сделки" TEXT,
  "Сделка закрыта" TEXT,
  "Дата оплаты" TEXT,
  "Сумма" TEXT,
  revenue_amount REAL,
  is_revenue_variant3 INTEGER,
  "UTM Source" TEXT,
  "UTM Medium" TEXT,
  "UTM Campaign" TEXT,
  "Название сделки" TEXT,
  "Код_курса_сайт" TEXT,
  "Код курса" TEXT,
  course_code_norm TEXT,
  event_class TEXT,
  classification_source TEXT,
  classification_pattern TEXT,
  classification_confidence TEXT,
  is_attacking_january INTEGER
);

CREATE TABLE stg_yandex_stats (
  "Месяц" TEXT,
  "№ Кампании" REAL,
  "Название кампании" TEXT,
  "№ Группы" REAL,
  "Название группы" TEXT,
  "№ Объявления" REAL,
  "Статус объявления" TEXT,
  "Тип объявления" TEXT,
  "Заголовок" TEXT,
  "Текст" TEXT,
  "Ссылка" TEXT,
  "Путь до изображения" TEXT,
  "Название файла изображения" TEXT,
  "Идентификатор видео" TEXT,
  "Путь до превью видео" TEXT,
  "Место клика" TEXT,
  "Формат" TEXT,
  "Источник текста" TEXT,
  "Расход, ₽" REAL,
  "Клики" INTEGER,
  "Конверсии" INTEGER,
  "CR, %" TEXT,
  "CPA, ₽" TEXT,
  month TEXT
);

CREATE TABLE stg_email_sends (
  "Дата отправки" TEXT,
  "Название выпуска" TEXT,
  "Получатели" TEXT,
  "Тема" TEXT,
  "Отправлено" INTEGER,
  "Доставлено" INTEGER,
  "Ошибок" INTEGER,
  "Открытий" INTEGER,
  "Уник. открытий" INTEGER,
  "Кликов" INTEGER,
  "Уник. кликов" INTEGER,
  "CTOR, %" REAL,
  "Отписок" INTEGER,
  "UTOR, %" REAL,
  "ID" INTEGER,
  "Номер задания" INTEGER,
  utm_campaign TEXT,
  utm_content REAL,
  utm_medium TEXT,
  utm_source TEXT,
  utm_term REAL,
  month TEXT
);

CREATE TABLE deals (
  "ID" TEXT PRIMARY KEY,
  "Воронка" TEXT,
  "Стадия сделки" TEXT,
  "Дата создания" TEXT,
  "UTM Source" TEXT,
  "UTM Medium" TEXT,
  "UTM Campaign" TEXT,
  "UTM Content" TEXT,
  "UTM Term" TEXT
);

CREATE TABLE yandex_stats (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  "Месяц" TEXT,
  "№ Кампании" TEXT,
  "Название кампании" TEXT,
  "№ Группы" TEXT,
  "Название группы" TEXT,
  "№ Объявления" TEXT,
  "Статус объявления" TEXT,
  "Тип объявления" TEXT,
  "Заголовок" TEXT,
  "Текст" TEXT,
  "Ссылка" TEXT,
  "Расход, ₽" REAL,
  "Клики" INTEGER,
  "Конверсии" INTEGER,
  "CR, %" REAL,
  "CPA, ₽" REAL
);

CREATE TABLE email_sends (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  "Дата отправки" TEXT,
  "Название выпуска" TEXT,
  "Получатели" TEXT,
  "Тема" TEXT,
  "Отправлено" INTEGER,
  "Доставлено" INTEGER,
  "Ошибок" INTEGER,
  "Открытий" INTEGER,
  "Уник. открытий" INTEGER,
  "Кликов" INTEGER,
  "Уник. кликов" INTEGER,
  "CTOR, %" REAL,
  "Отписок" INTEGER,
  "UTOR, %" REAL,
  "ID" TEXT,
  "Номер задания" TEXT,
  utm_campaign TEXT,
  utm_content TEXT,
  utm_medium TEXT,
  utm_source TEXT,
  utm_term TEXT
);

CREATE TABLE sheets_sync_log (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  sheet_id TEXT NOT NULL,
  worksheet_name TEXT NOT NULL,
  synced_at TEXT NOT NULL,
  row_count INTEGER
);
