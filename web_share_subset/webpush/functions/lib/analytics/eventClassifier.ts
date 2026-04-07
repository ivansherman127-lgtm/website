/**
 * Port of db/event_classifier.py (classify_event_from_row, FIELD_PRIORITY, EVENT_RULES).
 */
export interface ClassificationResult {
  event: string;
  source_field: string;
  matched_pattern: string;
  confidence: string;
}

export const FIELD_PRIORITY = [
  "Название сделки",
  "Код_курса_сайт",
  "Код курса",
  "UTM Campaign",
  "Источник (подробно)",
  "Источник обращения",
] as const;

const EVENT_RULES: [string, RegExp[]][] = [
  ["Тренд репорты", [/\bтренд\b/i, /\btrend report/i]],
  ["Демо Ред", [/\bдемо ред\b/i, /\bdemo red\b/i, /\bprof pentest demo\b/i]],
  ["Демо Блю", [/\bдемо блю\b/i, /\bdemo blue\b/i, /\bprof soc demo\b/i]],
  ["ПБХ", [/\bпбх\b/i, /\bпрофессия белый хакер\b/i, /\bpbh\b/i, /\bwhite hacker\b/i]],
  ["Blue Team Stepik", [/\bblue team stepik\b/i, /\bstepik\b/i]],
  ["Опен дэй 24.03.26", [/\bopen day\b.*\b24\b.*\b03\b/i]],
  ["Опен дэй", [/\bопен деи\b/i, /\bопен дэй\b/i, /\bopen day\b/i]],
  ["Встреча с экспертом", [/\bвстреча с экспертом\b/i, /\bexpert meeting\b/i]],
];

// Dated open day UTM campaign patterns — checked before field-priority loop
// using raw (non-normalized) UTM value to preserve underscores
const OPEN_DAY_UTM_MAP: [RegExp, string][] = [
  [/\bopen_day_271025\b/i, "Опен дэй 27.10.25"],
  [/\bopen_day_281025\b/i, "Опен дэй 28.10.25"],
  [/\bopen_day_191225\b/i, "Опен дэй 19.12.25"],
  [/\bopen_day_231225\b/i, "Опен дэй 23.12.25"],
  [/\bopen_day_251225\b/i, "Опен дэй 25.12.25"],
];

export function normalizeText(v: unknown): string {
  let s = v === null || v === undefined ? "" : String(v);
  s = s.trim().toLowerCase().replace(/ё/g, "е");
  s = s.replace(/[_\-]+/g, " ");
  s = s.replace(/[^\w\s]/gu, " ");
  s = s.replace(/\s+/g, " ").trim();
  return s;
}

function confidenceForField(field: string): string {
  if (field === "Название сделки") return "high";
  if (field === "Код_курса_сайт" || field === "Код курса") return "high";
  if (field === "UTM Campaign") return "medium";
  return "low";
}

export function classifyEventFromRow(
  row: Record<string, unknown>,
  fields: readonly string[] = FIELD_PRIORITY,
): ClassificationResult {
  // UTM-based open day pre-check using raw value (normalizeText strips underscores)
  const utmRaw = String(row["UTM Campaign"] ?? "").trim().toLowerCase();
  if (utmRaw) {
    for (const [pattern, event] of OPEN_DAY_UTM_MAP) {
      if (pattern.test(utmRaw)) {
        return {
          event,
          source_field: "UTM Campaign",
          matched_pattern: pattern.source,
          confidence: "high",
        };
      }
    }
  }

  for (const field of fields) {
    const raw = row[field];
    const txt = normalizeText(raw);
    if (!txt) continue;
    for (const [event, patterns] of EVENT_RULES) {
      for (const pattern of patterns) {
        if (pattern.test(txt)) {
          return {
            event,
            source_field: field,
            matched_pattern: pattern.source,
            confidence: confidenceForField(field),
          };
        }
      }
    }
  }
  return {
    event: "Другое",
    source_field: "",
    matched_pattern: "",
    confidence: "low",
  };
}
