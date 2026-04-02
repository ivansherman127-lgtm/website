export const ANALYTICS_LOGIC_VERSION = "d1-workers-ts-v2";

const META_LAST_SOURCE_FINGERPRINT = "last_source_fingerprint";
const META_LAST_SOURCE_SEEN_UTC = "last_source_seen_utc";
const META_LAST_MATERIALIZE_UTC = "last_materialize_utc";
const META_LOGIC_VERSION = "logic_version";

type SourceSnapshot = {
  rawSourceBatchMaxCreatedAt: string;
  rawBitrixMaxIngestedAt: string;
  stgDealsAnalyticsCount: number;
  stgYandexStatsCount: number;
  stgContactsUidCount: number;
};

type BuildMeta = {
  lastSourceFingerprint: string;
  lastMaterializeUtc: string;
  logicVersion: string;
};

export type FreshnessDecision = {
  stale: boolean;
  reason: string;
  fingerprint: string;
  snapshot: SourceSnapshot;
  hasDatasets: boolean;
  meta: BuildMeta;
};

async function tableExists(db: D1Database, tableName: string): Promise<boolean> {
  const row = await db
    .prepare(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1`)
    .bind(tableName)
    .first<{ name: string }>();
  return !!row?.name;
}

async function maxTextOrEmpty(db: D1Database, tableName: string, columnName: string): Promise<string> {
  if (!(await tableExists(db, tableName))) return "";
  const row = await db
    .prepare(`SELECT MAX(${columnName}) AS v FROM ${tableName}`)
    .first<{ v: string | null }>();
  return String(row?.v ?? "").trim();
}

async function countOrZero(db: D1Database, tableName: string): Promise<number> {
  if (!(await tableExists(db, tableName))) return 0;
  const row = await db
    .prepare(`SELECT COUNT(*) AS c FROM ${tableName}`)
    .first<{ c: number | string | null }>();
  return Number(row?.c ?? 0) || 0;
}

async function datasetJsonHasRows(db: D1Database): Promise<boolean> {
  if (!(await tableExists(db, "dataset_json"))) return false;
  const row = await db
    .prepare(`SELECT 1 AS ok FROM dataset_json LIMIT 1`)
    .first<{ ok: number }>();
  return !!row?.ok;
}

async function readMeta(db: D1Database): Promise<BuildMeta> {
  const { results } = await db
    .prepare(
      `SELECT k, v
       FROM analytics_build_meta
       WHERE k IN (?, ?, ?)`
    )
    .bind(META_LAST_SOURCE_FINGERPRINT, META_LAST_MATERIALIZE_UTC, META_LOGIC_VERSION)
    .all<{ k: string; v: string }>();

  const out: BuildMeta = {
    lastSourceFingerprint: "",
    lastMaterializeUtc: "",
    logicVersion: "",
  };
  for (const row of results ?? []) {
    const k = String(row.k ?? "");
    const v = String(row.v ?? "");
    if (k === META_LAST_SOURCE_FINGERPRINT) out.lastSourceFingerprint = v;
    if (k === META_LAST_MATERIALIZE_UTC) out.lastMaterializeUtc = v;
    if (k === META_LOGIC_VERSION) out.logicVersion = v;
  }
  return out;
}

function fingerprintFromSnapshot(snapshot: SourceSnapshot): string {
  return [
    snapshot.rawSourceBatchMaxCreatedAt,
    snapshot.rawBitrixMaxIngestedAt,
    String(snapshot.stgDealsAnalyticsCount),
    String(snapshot.stgYandexStatsCount),
    String(snapshot.stgContactsUidCount),
  ].join("|");
}

export async function getSourceSnapshot(db: D1Database): Promise<SourceSnapshot> {
  const [rawSourceBatchMaxCreatedAt, rawBitrixMaxIngestedAt, stgDealsAnalyticsCount, stgYandexStatsCount, stgContactsUidCount] =
    await Promise.all([
      maxTextOrEmpty(db, "raw_source_batches", "created_at"),
      maxTextOrEmpty(db, "raw_bitrix_deals", "ingested_at"),
      countOrZero(db, "stg_deals_analytics"),
      countOrZero(db, "stg_yandex_stats"),
      countOrZero(db, "stg_contacts_uid"),
    ]);

  return {
    rawSourceBatchMaxCreatedAt,
    rawBitrixMaxIngestedAt,
    stgDealsAnalyticsCount,
    stgYandexStatsCount,
    stgContactsUidCount,
  };
}

export async function evaluateFreshness(db: D1Database): Promise<FreshnessDecision> {
  const [snapshot, meta, hasDatasets] = await Promise.all([
    getSourceSnapshot(db),
    readMeta(db),
    datasetJsonHasRows(db),
  ]);
  const fingerprint = fingerprintFromSnapshot(snapshot);

  if (!hasDatasets) {
    return { stale: true, reason: "missing_materialized_datasets", fingerprint, snapshot, hasDatasets, meta };
  }
  if (!meta.lastSourceFingerprint) {
    return { stale: true, reason: "missing_source_fingerprint", fingerprint, snapshot, hasDatasets, meta };
  }
  if (meta.logicVersion !== ANALYTICS_LOGIC_VERSION) {
    return { stale: true, reason: "logic_version_changed", fingerprint, snapshot, hasDatasets, meta };
  }
  if (meta.lastSourceFingerprint !== fingerprint) {
    return { stale: true, reason: "source_changed", fingerprint, snapshot, hasDatasets, meta };
  }
  return { stale: false, reason: "no_source_changes", fingerprint, snapshot, hasDatasets, meta };
}

export async function saveFreshnessMeta(db: D1Database, fingerprint: string): Promise<void> {
  const nowIso = new Date().toISOString();
  const stmt = db.prepare(
    `INSERT OR REPLACE INTO analytics_build_meta (k, v, updated_at) VALUES (?, ?, datetime('now'))`,
  );
  await db.batch([
    stmt.bind(META_LAST_SOURCE_FINGERPRINT, fingerprint),
    stmt.bind(META_LAST_SOURCE_SEEN_UTC, nowIso),
    stmt.bind(META_LAST_MATERIALIZE_UTC, nowIso),
    stmt.bind(META_LOGIC_VERSION, ANALYTICS_LOGIC_VERSION),
  ]);
}
