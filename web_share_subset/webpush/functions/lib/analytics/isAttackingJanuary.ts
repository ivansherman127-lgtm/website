import { FIELD_PRIORITY, normalizeText } from "./eventClassifier";

const ATTACKING_JANUARY_RE = /(атакующ\w*\s+январ\w*|attacking[_ ]?january)/i;

/** Port of db/event_classifier.is_attacking_january */
export function isAttackingJanuary(
  row: Record<string, unknown>,
  fields: readonly string[] = FIELD_PRIORITY,
): boolean {
  for (const field of fields) {
    const txt = normalizeText(row[field]);
    if (txt && ATTACKING_JANUARY_RE.test(txt)) return true;
  }
  return false;
}
