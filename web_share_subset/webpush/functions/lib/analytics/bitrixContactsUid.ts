async function tableExists(db: D1Database, tableName: string): Promise<boolean> {
  const row = await db
    .prepare(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1`)
    .bind(tableName)
    .first<{ name: string }>();
  return !!row?.name;
}

/**
 * Build bitrix_contacts_uid dataset on-demand from D1.
 * Primary source is stg_contacts_uid (contact_uid/contact_id mapping loaded from CSV).
 */
export async function buildBitrixContactsUidRows(db: D1Database): Promise<Record<string, unknown>[]> {
  if (await tableExists(db, "stg_contacts_uid")) {
    const mapped = await db
      .prepare(
        `SELECT
           contact_uid AS uid,
           MAX(NULLIF(TRIM(COALESCE(all_names, '')), '')) AS name,
           MAX(NULLIF(TRIM(COALESCE(all_phones, '')), '')) AS phone,
           MAX(NULLIF(TRIM(COALESCE(all_emails, '')), '')) AS email
         FROM stg_contacts_uid
         GROUP BY contact_uid
         ORDER BY uid`,
      )
      .all<Record<string, unknown>>();
    return mapped.results ?? [];
  }

  const result = await db
    .prepare(
      `
      WITH combined_raw AS (
        SELECT "Контакт: ID", "Контакт", "Контакт: Имя", "Контакт: Фамилия", "Контакт: Отчество", "Контакт: Телефон", "Контакт: E-mail"
        FROM raw_bitrix_deals_p01
        UNION ALL
        SELECT "Контакт: ID", "Контакт", "Контакт: Имя", "Контакт: Фамилия", "Контакт: Отчество", "Контакт: Телефон", "Контакт: E-mail"
        FROM raw_bitrix_deals_p02
        UNION ALL
        SELECT "Контакт: ID", "Контакт", "Контакт: Имя", "Контакт: Фамилия", "Контакт: Отчество", "Контакт: Телефон", "Контакт: E-mail"
        FROM raw_bitrix_deals_p03
        UNION ALL
        SELECT "Контакт: ID", "Контакт", "Контакт: Имя", "Контакт: Фамилия", "Контакт: Отчество", "Контакт: Телефон", "Контакт: E-mail"
        FROM raw_bitrix_deals_p04
      ),
      grouped AS (
        SELECT
          COALESCE("Контакт: ID", '') AS uid,
          GROUP_CONCAT(DISTINCT TRIM(COALESCE("Контакт", '')), '; ') AS names_combined,
          GROUP_CONCAT(DISTINCT TRIM(COALESCE("Контакт: Телефон", '')), '; ') AS phone,
          GROUP_CONCAT(DISTINCT TRIM(COALESCE("Контакт: E-mail", '')), '; ') AS email
        FROM combined_raw
        GROUP BY uid
      )
      SELECT
        uid,
        NULLIF(NULLIF(names_combined, ''), '; ') AS name,
        NULLIF(NULLIF(phone, ''), '; ') AS phone,
        NULLIF(NULLIF(email, ''), '; ') AS email
      FROM grouped
      WHERE uid <> ''
      ORDER BY uid
      `
    )
    .all<Record<string, unknown>>();
  return result.results ?? [];
}
