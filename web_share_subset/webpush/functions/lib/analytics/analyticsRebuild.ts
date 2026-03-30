import { materializeSliceDatasets } from "./materializeDatasets";
import { rebuildMartDealsFromStaging } from "./rebuildMartDeals";
import { rebuildYandexMarts } from "./rebuildYandex";

export async function runAnalyticsRebuild(db: D1Database): Promise<Record<string, unknown>> {
  const mart = await rebuildMartDealsFromStaging(db);

  await db.prepare("DELETE FROM mart_attacking_january_cohort_deals").run();
  await db.prepare("DELETE FROM mart_attacking_january_contacts").run();

  await db
    .prepare(
      `INSERT INTO mart_attacking_january_contacts (contact_id)
       SELECT DISTINCT "Контакт: ID" AS contact_id
       FROM mart_deals_enriched
       WHERE is_attacking_january = 1 AND COALESCE("Контакт: ID", '') <> ''`,
    )
    .run();

  await db
    .prepare(
      `INSERT INTO mart_attacking_january_cohort_deals
       SELECT d.* FROM mart_deals_enriched d
       INNER JOIN mart_attacking_january_contacts c ON d."Контакт: ID" = c.contact_id`,
    )
    .run();

  const yx = await rebuildYandexMarts(db);
  const ds = await materializeSliceDatasets(db);

  const now = new Date().toISOString();
  await db
    .prepare(
      `INSERT OR REPLACE INTO analytics_build_meta (k, v, updated_at) VALUES ('last_rebuild_utc', ?, datetime('now'))`,
    )
    .bind(now)
    .run();
  await db
    .prepare(
      `INSERT OR REPLACE INTO analytics_build_meta (k, v, updated_at) VALUES ('logic_version', 'd1-workers-ts-v1', datetime('now'))`,
    )
    .run();

  return {
    mart_deals_rows: mart.rows,
    yandex_raw_rows: yx.raw_rows,
    yandex_dedup_rows: yx.dedup_rows,
    dataset_paths: ds.paths,
  };
}
