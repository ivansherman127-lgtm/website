function toCleanString(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = String(v).trim();
  if (!s || s.toLowerCase() === "nan") return "";
  return s;
}

function stripFloatSuffix(v: string): string {
  return /^\d+\.0+$/.test(v) ? v.split(".")[0] : v;
}

function isYandexAdIdToken(v: string): boolean {
  return /^17\d{9}$/.test(v);
}

/**
 * Extract Yandex ad ID from UTM Content.
 * Supports both plain ad_id and tokenized strings like: cid|...|aid|17596885378|...
 */
export function extractYandexAdIdFromUtmContent(v: unknown): string {
  const raw = toCleanString(v);
  if (!raw) return "";

  const plain = stripFloatSuffix(raw);
  if (isYandexAdIdToken(plain)) return plain;

  const parts = raw.split("|").map((p) => stripFloatSuffix(p.trim()));
  const aidIndex = parts.findIndex((p) => p.toLowerCase() === "aid");
  if (aidIndex >= 0 && aidIndex + 1 < parts.length) {
    const candidate = parts[aidIndex + 1];
    if (isYandexAdIdToken(candidate)) return candidate;
  }

  for (const part of parts) {
    if (isYandexAdIdToken(part)) return part;
  }

  return "";
}

/**
 * SQL expression that extracts canonical ad_id from UTM Content-like source value.
 * Returns '' when no valid ad_id token is found.
 */
export function sqlExtractYandexAdId(rawSqlExpr: string): string {
  const trimmed = `TRIM(COALESCE(${rawSqlExpr}, ''))`;
  const plain = `REPLACE(${trimmed}, '.0', '')`;
  const afterAid = `SUBSTR(${trimmed}, INSTR(${trimmed}, '|aid|') + 5)`;
  const aidToken = `CASE
    WHEN INSTR(${trimmed}, '|aid|') > 0 THEN
      CASE
        WHEN INSTR(${afterAid}, '|') > 0 THEN SUBSTR(${afterAid}, 1, INSTR(${afterAid}, '|') - 1)
        ELSE ${afterAid}
      END
    ELSE ''
  END`;

  const plainIsValid = `LENGTH(${plain}) = 11 AND SUBSTR(${plain}, 1, 2) = '17' AND ${plain} NOT GLOB '*[^0-9]*'`;
  const aidIsValid = `LENGTH(${aidToken}) = 11 AND SUBSTR(${aidToken}, 1, 2) = '17' AND ${aidToken} NOT GLOB '*[^0-9]*'`;

  return `CASE
    WHEN ${plainIsValid} THEN ${plain}
    WHEN ${aidIsValid} THEN ${aidToken}
    ELSE ''
  END`;
}
