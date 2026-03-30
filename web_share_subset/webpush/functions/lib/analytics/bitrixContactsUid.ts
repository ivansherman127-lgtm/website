/**
 * Build bitrix_contacts_uid dataset on-demand from D1
 */
export async function buildBitrixContactsUidRows(db: D1Database): Promise<Record<string, unknown>[]> {
  const result = await db
    .prepare(
      `SELECT DISTINCT COALESCE("Контакт: ID", '') AS uid
       FROM mart_deals_enriched
       WHERE COALESCE("Контакт: ID", '') <> ''
       ORDER BY uid`,
    )
    .all<Record<string, unknown>>();
  return result.results ?? [];
}
