/**
 * Shared SQL expression builders for analytics queries.
 *
 * Imported by both materializeDatasets.ts and the API handlers (assoc-revenue.ts, etc.)
 * so these helpers are defined exactly once.
 */

// ── Quoting & escaping ────────────────────────────────────────────────────────

/** Wraps a literal string value in single quotes, escaping any embedded single quotes. */
export function sqlQuote(value: string): string {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

// ── Date helpers ──────────────────────────────────────────────────────────────

/**
 * Returns a CASE expression that extracts YYYY-MM from a date column that may
 * be in ISO format (YYYY-MM-DD / YYYY-MM) or Russian dot format (DD.MM.YYYY).
 */
export function sqlMonthFromDateExpr(expr: string): string {
  return `CASE
    WHEN COALESCE(${expr}, '') LIKE '____-__%' THEN SUBSTR(${expr}, 1, 7)
    WHEN COALESCE(${expr}, '') LIKE '__.__.____%' THEN SUBSTR(${expr}, 7, 4) || '-' || SUBSTR(${expr}, 4, 2)
    ELSE ''
  END`;
}

// ── Arithmetic helpers ────────────────────────────────────────────────────────

/**
 * Safe integer division in SQL: returns numerator / denominator, or 0 when
 * denominator is NULL or 0.  Always produces a REAL result.
 */
export function sqlSafeDiv(numeratorExpr: string, denominatorExpr: string): string {
  return `CASE WHEN COALESCE(${denominatorExpr}, 0) = 0 THEN 0 ELSE ${numeratorExpr} * 1.0 / ${denominatorExpr} END`;
}

// ── Normalisation ─────────────────────────────────────────────────────────────

/**
 * Wraps a SQL expression with a chain of REPLACE() calls that lower-case and
 * strip punctuation / accents to produce a normalised lookup key.
 * Mirrors the TypeScript `normalizeLookupKey()` function on the TS side.
 */
export function sqlNormalizeLookupExpr(expr: string): string {
  const replacements: Array<[string, string]> = [
    ["ё", "е"],
    ["-", ""],
    ["_", ""],
    [" ", ""],
    [".", ""],
    ["/", ""],
    [":", ""],
    [",", ""],
    ["'", ""],
    ['"', ""],
    ["«", ""],
    ["»", ""],
    ["(", ""],
    [")", ""],
  ];
  let out = `LOWER(TRIM(COALESCE(${expr}, '')))`;
  for (const [from, to] of replacements) {
    out = `REPLACE(${out}, ${sqlQuote(from)}, ${sqlQuote(to)})`;
  }
  return out;
}

// ── Yandex ad ID validation ───────────────────────────────────────────────────

/** Returns true when the value looks like a valid Yandex ad ID (17 + 9 digits). */
export function isValidYandexAdId(value: unknown): boolean {
  return /^17\d{9}$/.test(String(value ?? "").trim());
}

// ── Invalid lead quality token check ─────────────────────────────────────────

/**
 * Builds an OR-joined SQL predicate that fires when either of the two
 * "invalid lead type" columns contains any of the given tokens.
 *
 * @param tokens     – List of lower-cased token strings to check for.
 * @param tableAlias – Prefix applied to column names: `""`, `"m."`, or `"p."`.
 * @param pattern    – `"instr"` uses `instr()` for a substring match (default);
 *                     `"like"` uses `LIKE '%token%'`.
 */
export function buildInvalidTokenCond(
  tokens: string[],
  tableAlias: "" | "m." | "p.",
  pattern: "instr" | "like" = "instr",
): string {
  const col1 = `${tableAlias}"Типы некачественного лида"`;
  const col2 = `${tableAlias}"Типы некачественных лидов"`;
  return tokens
    .flatMap((tok) => {
      if (pattern === "like") {
        return [
          `lower(COALESCE(${col1}, '')) LIKE ${sqlQuote("%" + tok + "%")}`,
          `lower(COALESCE(${col2}, '')) LIKE ${sqlQuote("%" + tok + "%")}`,
        ];
      }
      return [
        `instr(lower(COALESCE(${col1}, '')), ${sqlQuote(tok)}) > 0`,
        `instr(lower(COALESCE(${col2}, '')), ${sqlQuote(tok)}) > 0`,
      ];
    })
    .join(" OR ");
}
