import { parseAmount } from "./amt";
import type { StgDealAnalytics } from "./stagingTypes";
import b24CrmSemanticMaps from "./b24CrmSemanticMaps.json";

type B24SemanticFile = { categories: Record<string, string>; stages: Record<string, string> };
const B24_SEM = b24CrmSemanticMaps as B24SemanticFile;

function resolveB24CategoryLabel(categoryId: string): string {
  const k = (categoryId || "").trim();
  if (!k) return "";
  const cats = B24_SEM.categories ?? {};
  const direct = cats[k];
  if (direct) return direct;
  if (/^\d+$/.test(k)) {
    const byNum = cats[String(Number(k))];
    if (byNum) return byNum;
  }
  return "";
}

/** Map STAGE_ID (e.g. C7:WON) to CRM title; keep id if unknown so revenue rules still match codes. */
function resolveB24StageLabel(stageId: string): string {
  const s = (stageId || "").trim();
  if (!s) return "";
  const st = B24_SEM.stages ?? {};
  return st[s] ?? st[s.toUpperCase()] ?? "";
}

const EXCLUDED_FUNNELS = new Set(["Спец. проекты", "Учебный центр", "Спецпроекты"]);
const REQUIRED_RAW_COLUMNS = ["ID", "Дата создания", "Сумма"];

/** Bitrix24 REST field names (raw_b24_deals) — mirrors db/run_all_slices._load_bitrix_from_raw_b24_sql */
const B24_UF_PAY = "UF_CRM_1744103866937";
const B24_UF_INSTALLMENT = "UF_CRM_1709817732193";
const B24_UF_CODE_SITE = "UF_CRM_1683566516628";
const B24_UF_CODE_COURSE = "UF_CRM_1674206786569";
const B24_UF_INVALID1 = "UF_CRM_1755599565032";
const B24_UF_INVALID2 = "UF_CRM_1755599720607";

async function tableExists(db: D1Database, tableName: string): Promise<boolean> {
  const row = await db
    .prepare(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1`)
    .bind(tableName)
    .first<{ name: string }>();
  return !!row?.name;
}

async function tableHasRows(db: D1Database, tableName: string): Promise<boolean> {
  if (!(await tableExists(db, tableName))) return false;
  const row = await db.prepare(`SELECT 1 AS ok FROM ${tableName} LIMIT 1`).first<{ ok: number }>();
  return !!row?.ok;
}

function textCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  const text = String(value).trim();
  return ["", "nan", "none", "null"].includes(text.toLowerCase()) ? "" : text;
}

function normalizeId(value: unknown): string {
  const text = textCell(value);
  if (/^\d+\.0$/.test(text)) return text.slice(0, -2);
  return text;
}

function sortBitrixKeys(keys: string[], base: string): string[] {
  const pattern = new RegExp(`^${base.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?:\\.(\\d+))?$`);
  return keys
    .filter((key) => pattern.test(key))
    .sort((left, right) => {
      const l = pattern.exec(left)?.[1];
      const r = pattern.exec(right)?.[1];
      const ln = l ? Number(l) : 0;
      const rn = r ? Number(r) : 0;
      return ln - rn;
    });
}

type RawBitrixKeySet = {
  funnelKeys: string[];
  stageKeys: string[];
};

function firstNonEmpty(row: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const value = textCell(row[key]);
    if (value) return value;
  }
  return "";
}

function b24PayDate(row: Record<string, unknown>): string {
  const uf = textCell(row[B24_UF_PAY]);
  if (uf) return uf;
  return textCell(row.CLOSEDATE);
}

function normalizeB24Row(row: Record<string, unknown>): StgDealAnalytics | null {
  const dealId = normalizeId(row.ID);
  if (!dealId) return null;

  const catId = textCell(row.CATEGORY_ID);
  const stageId = textCell(row.STAGE_ID);
  const funnelNamed = resolveB24CategoryLabel(catId);
  const funnelRaw = funnelNamed || catId;
  if (EXCLUDED_FUNNELS.has(funnelRaw)) return null;

  const inv1 = textCell(row[B24_UF_INVALID1]);
  const inv2 = textCell(row[B24_UF_INVALID2]);

  const stageNamed = resolveB24StageLabel(stageId);
  const stage_raw = stageNamed || stageId;

  return {
    deal_id: dealId,
    contact_id: normalizeId(row.CONTACT_ID),
    created_at: textCell(row.DATE_CREATE),
    funnel_raw: funnelRaw,
    stage_raw,
    closed_yes: textCell(row.CLOSED),
    pay_date: b24PayDate(row),
    installment_schedule: textCell(row[B24_UF_INSTALLMENT]),
    sum_text: textCell(row.OPPORTUNITY),
    utm_source: textCell(row.UTM_SOURCE),
    utm_medium: textCell(row.UTM_MEDIUM),
    utm_campaign: textCell(row.UTM_CAMPAIGN),
    utm_content: textCell(row.UTM_CONTENT),
    deal_name: textCell(row.TITLE),
    code_site: textCell(row[B24_UF_CODE_SITE]),
    code_course: textCell(row[B24_UF_CODE_COURSE]),
    source_detail: textCell(row.SOURCE_DESCRIPTION),
    source_inquiry: textCell(row.SOURCE_ID),
    invalid_type_lead: inv1 || inv2,
    responsible: textCell(row.ASSIGNED_BY_ID),
  };
}

function dedupeB24Rows(rawRows: Record<string, unknown>[]): StgDealAnalytics[] {
  const modKey = (r: Record<string, unknown>) =>
    textCell(r.DATE_MODIFY) || textCell(r.ingested_at) || "";
  const sorted = [...rawRows].sort((a, b) => modKey(b).localeCompare(modKey(a)));
  const seen = new Set<string>();
  const out: StgDealAnalytics[] = [];
  for (const row of sorted) {
    const id = normalizeId(row.ID);
    if (!id || seen.has(id)) continue;
    const n = normalizeB24Row(row);
    if (!n) continue;
    seen.add(id);
    out.push(n);
  }
  return out;
}

async function tryLoadRawB24Deals(
  db: D1Database,
): Promise<{ rows: StgDealAnalytics[]; source: "raw_b24_deals" } | null> {
  if (!(await tableHasRows(db, "raw_b24_deals"))) return null;
  const { results } = await db
    .prepare("SELECT * FROM raw_b24_deals")
    .all<Record<string, unknown>>();
  const rawRows = results ?? [];
  if (!rawRows.length) return null;
  const sample = rawRows[0] ?? {};
  const keys = Object.keys(sample);
  if (!keys.includes("ID") || !keys.includes("DATE_CREATE")) return null;
  const rows = dedupeB24Rows(rawRows);
  return rows.length ? { rows, source: "raw_b24_deals" } : null;
}

function normalizeRawRow(row: Record<string, unknown>, keySet: RawBitrixKeySet): StgDealAnalytics | null {
  const dealId = normalizeId(row.ID);
  if (!dealId) return null;

  const funnelRaw = firstNonEmpty(row, keySet.funnelKeys);
  if (EXCLUDED_FUNNELS.has(funnelRaw)) return null;

  return {
    deal_id: dealId,
    contact_id: normalizeId(row["Контакт: ID"]),
    created_at: textCell(row["Дата создания"]),
    funnel_raw: funnelRaw,
    stage_raw: firstNonEmpty(row, keySet.stageKeys),
    closed_yes: textCell(row["Сделка закрыта"]),
    pay_date: textCell(row["Дата оплаты"]),
    installment_schedule: textCell(row["Даты платежей по рассрочке "]),
    sum_text: textCell(row["Сумма"]),
    utm_source: textCell(row["UTM Source"]),
    utm_medium: textCell(row["UTM Medium"]),
    utm_campaign: textCell(row["UTM Campaign"]),
    utm_content: textCell(row["UTM Content"]),
    deal_name: textCell(row["Название сделки"]),
    code_site: textCell(row["Код_курса_сайт"]),
    code_course: textCell(row["Код курса"]),
    source_detail: textCell(row["Источник (подробно)"]),
    source_inquiry: textCell(row["Источник обращения"]),
    invalid_type_lead:
      textCell(row["Типы некачественного лида"]) || textCell(row["Типы некачественных лидов"]),
    responsible: textCell(row["Ответственный"]),
  };
}

export async function loadCanonicalBitrixRows(
  db: D1Database,
): Promise<{
  rows: StgDealAnalytics[];
  source: "raw_b24_deals" | "raw_bitrix_deals" | "stg_deals_analytics";
}> {
  const b24 = await tryLoadRawB24Deals(db);
  if (b24) return b24;

  if (await tableHasRows(db, "raw_bitrix_deals")) {
    const { results } = await db.prepare("SELECT rowid AS __rowid, * FROM raw_bitrix_deals ORDER BY rowid").all<Record<string, unknown>>();
    const rawRows = results ?? [];
    const keys = rawRows.length ? Object.keys(rawRows[0] ?? {}) : [];
    const keySet: RawBitrixKeySet = {
      funnelKeys: sortBitrixKeys(keys, "Воронка"),
      stageKeys: sortBitrixKeys(keys, "Стадия сделки"),
    };
    const hasRequiredColumns = REQUIRED_RAW_COLUMNS.every((column) => keys.includes(column));
    const hasBitrixFunnelColumns = keySet.funnelKeys.length > 0;
    const hasBitrixStageColumns = keySet.stageKeys.length > 0;
    if (hasRequiredColumns && hasBitrixFunnelColumns && hasBitrixStageColumns) {
      const deduped = new Map<string, { row: StgDealAnalytics; amount: number }>();
      for (const row of rawRows) {
        const normalized = normalizeRawRow(row, keySet);
        if (!normalized) continue;
        const amount = parseAmount(normalized.sum_text);
        const current = deduped.get(normalized.deal_id);
        if (!current || amount > current.amount) {
          deduped.set(normalized.deal_id, { row: normalized, amount });
        }
      }
      return { rows: [...deduped.values()].map((entry) => entry.row), source: "raw_bitrix_deals" };
    }
  }

  const { results } = await db.prepare("SELECT * FROM stg_deals_analytics").all<StgDealAnalytics>();
  return { rows: results ?? [], source: "stg_deals_analytics" };
}
