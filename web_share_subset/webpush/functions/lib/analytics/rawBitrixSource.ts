import { parseAmount } from "./amt";
import type { StgDealAnalytics } from "./stagingTypes";

const EXCLUDED_FUNNELS = new Set(["Спец. проекты", "Учебный центр", "Спецпроекты"]);
const REQUIRED_RAW_COLUMNS = ["ID", "Дата создания", "Сумма"];

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
  };
}

export async function loadCanonicalBitrixRows(
  db: D1Database,
): Promise<{ rows: StgDealAnalytics[]; source: "raw_bitrix_deals" | "stg_deals_analytics" }> {
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
