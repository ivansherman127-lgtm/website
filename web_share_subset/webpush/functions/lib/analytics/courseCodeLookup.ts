import { normalizeCourseCode } from "./normalizeCourseCode";

/** Known course codes from course_code_list.txt, pre-normalized. */
const RAW_KNOWN_CODES = [
  "В301-А", "B-207", "B-228", "B-410", "F-107", "F-109", "S-224",
  "R-319A", "R301-A", "Хакер", "B-101", "R-101", "Марафон профессий",
  "R301-M0A", "B301-M0A", "R-301", "R-301-A", "B-236", "F-401",
  "F-401-А", "pentester-new", "S-410", "TIA", "B-301", "main_page",
  "B-208", "F-110", "pentester-track", "R-218", "R301-GM1",
  "AttackingJanuary", "Групповая встреча", "Карьерная консультация",
  "Повышение цен",
];

const KNOWN_NORMALIZED = new Set(RAW_KNOWN_CODES.map(normalizeCourseCode).filter(Boolean));

/**
 * Try to extract a known course code from an arbitrary text string.
 * Splits on common separators, normalizes each token, and returns the first
 * match found in KNOWN_NORMALIZED. Returns "" if nothing matches.
 */
export function extractCourseCodeFromText(text: unknown): string {
  if (!text) return "";
  const s = String(text).trim();
  if (!s) return "";

  // Try the whole string first (handles codes without spaces)
  const full = normalizeCourseCode(s);
  if (full && KNOWN_NORMALIZED.has(full)) return full;

  // Split on whitespace, pipe, comma, slash
  const tokens = s.split(/[\s|,/]+/);
  for (const tok of tokens) {
    const norm = normalizeCourseCode(tok);
    if (norm && KNOWN_NORMALIZED.has(norm)) return norm;
  }

  // Try pairs of adjacent tokens (multi-word codes like "Марафон профессий")
  for (let i = 0; i < tokens.length - 1; i++) {
    const norm = normalizeCourseCode(`${tokens[i]} ${tokens[i + 1]}`);
    if (norm && KNOWN_NORMALIZED.has(norm)) return norm;
  }

  return "";
}
