import { materializeSliceDatasets } from "./materializeDatasets";
import { rebuildMartDealsFromStaging } from "./rebuildMartDeals";
import { rebuildYandexMarts } from "./rebuildYandex";
import { ANALYTICS_LOGIC_VERSION, evaluateFreshness, saveFreshnessMeta } from "./sourceFreshness";

export async function runAnalyticsRebuild(
  db: D1Database,
  opts: { materializationOnly?: boolean; force?: boolean } = {},
): Promise<Record<string, unknown>> {
  if (!opts.force) {
    try {
      const freshness = await evaluateFreshness(db);
      if (!freshness.stale) {
        return {
          skipped: true,
          reason: freshness.reason,
          fingerprint: freshness.fingerprint,
          materialization_only: opts.materializationOnly === true,
        };
      }
    } catch {
      // If freshness check fails, continue with recomputation for correctness.
    }
  }

  let mart: Record<string, unknown> = { skipped: true };
  let yx: Record<string, unknown> = { skipped: true };

  if (!opts.materializationOnly) {
    mart = await rebuildMartDealsFromStaging(db);

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

    yx = await rebuildYandexMarts(db);
  }

  const ds = await materializeSliceDatasets(db);

  const freshnessAfter = await evaluateFreshness(db);
  await saveFreshnessMeta(db, freshnessAfter.fingerprint);

  const now = new Date().toISOString();
  await db
    .prepare(
      `INSERT OR REPLACE INTO analytics_build_meta (k, v, updated_at) VALUES ('last_rebuild_utc', ?, datetime('now'))`,
    )
    .bind(now)
    .run();
  await db
    .prepare(
      `INSERT OR REPLACE INTO analytics_build_meta (k, v, updated_at) VALUES ('logic_version', ?, datetime('now'))`,
    )
    .bind(ANALYTICS_LOGIC_VERSION)
    .run();

  return {
    mart_deals_rows: mart.rows,
    yandex_raw_rows: yx.raw_rows,
    yandex_dedup_rows: yx.dedup_rows,
    dataset_paths: ds.paths.length,
  };
}
