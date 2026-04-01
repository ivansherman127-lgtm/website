import leadLogic from "../../../../../bitrix_lead_logic.json";

type QualState = "qual" | "refusal" | "not_qual" | "not_yet" | "unassigned" | "qual_from_date";
type WorkingState = "yes" | "no";

type StageRule = {
  qual_state: QualState;
  working_state: WorkingState;
  qual_from_date?: string;
};

type LogicConfig = {
  normalization?: {
    funnel_aliases?: Record<string, string>;
    stage_aliases?: Record<string, string>;
  };
  funnels: Record<string, Record<string, StageRule>>;
};

type SqlBuildParams = {
  funnelExpr: string;
  stageExpr: string;
  monthExpr: string;
};

type LeadLogicSql = {
  qual: string;
  unqual: string;
  unknown: string;
  refusal: string;
  inWork: string;
  invalid: string;
};

const LOGIC = leadLogic as LogicConfig;

const INVALID_STAGE_TOKENS = [
  "неквал",
  "некачеств",
  "дубл",
  "спам",
  "чс",
  "тест",
  "неправильн%данн%",
];

function sqlQuote(value: string): string {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

function normalizeDateToMonth(dateLike: string | undefined): string {
  const src = String(dateLike ?? "").trim();
  if (!src) return "";
  const m = src.match(/^(\d{4})-(\d{2})/);
  if (!m) return "";
  return `${m[1]}-${m[2]}`;
}

function buildAliasCase(rawExpr: string, aliases: Record<string, string> | undefined): string {
  const trimmed = `TRIM(COALESCE(${rawExpr}, ''))`;
  const pairs = Object.entries(aliases ?? {});
  if (!pairs.length) return trimmed;
  const body = pairs.map(([from, to]) => `WHEN ${sqlQuote(from)} THEN ${sqlQuote(to)}`).join(" ");
  return `CASE ${trimmed} ${body} ELSE ${trimmed} END`;
}

function joinConds(conds: string[]): string {
  return conds.length ? conds.map((c) => `(${c})`).join(" OR ") : "0";
}

export function buildLeadLogicSql(params: SqlBuildParams): LeadLogicSql {
  const funnelNorm = buildAliasCase(params.funnelExpr, LOGIC.normalization?.funnel_aliases);
  const stageNorm = buildAliasCase(params.stageExpr, LOGIC.normalization?.stage_aliases);

  const qualConds: string[] = [];
  const unqualConds: string[] = [];
  const unknownConds: string[] = [];
  const refusalConds: string[] = [];
  const inWorkConds: string[] = [];

  for (const [funnelName, stages] of Object.entries(LOGIC.funnels || {})) {
    for (const [stageName, rule] of Object.entries(stages || {})) {
      const base = `${funnelNorm} = ${sqlQuote(funnelName)} AND ${stageNorm} = ${sqlQuote(stageName)}`;

      if (rule.qual_state === "qual") {
        qualConds.push(base);
      } else if (rule.qual_state === "qual_from_date") {
        const month = normalizeDateToMonth(rule.qual_from_date);
        if (month) {
          qualConds.push(`${base} AND COALESCE(${params.monthExpr}, '') >= ${sqlQuote(month)}`);
          unqualConds.push(`${base} AND COALESCE(${params.monthExpr}, '') < ${sqlQuote(month)}`);
        }
      } else if (rule.qual_state === "refusal") {
        // Refusal can only happen after qualification: count it in both refusal and qual.
        qualConds.push(base);
        refusalConds.push(base);
      } else if (rule.qual_state === "not_qual") {
        unqualConds.push(base);
      } else if (rule.qual_state === "not_yet") {
        unqualConds.push(base);
      } else if (rule.qual_state === "unassigned") {
        unknownConds.push(base);
      }

      if (rule.working_state === "yes") inWorkConds.push(base);
    }
  }

  const invalidCond = INVALID_STAGE_TOKENS
    .map((tok) => `lower(COALESCE(${params.stageExpr}, '')) LIKE ${sqlQuote(`%${tok}%`)}`)
    .join(" OR ");

  const qualCondSql = joinConds(qualConds);
  const unqualCondSql = joinConds(unqualConds);
  const unknownCondSql = joinConds(unknownConds);
  const refusalCondSql = joinConds(refusalConds);

  // Ensure every lead lands in qual/unqual/unknown. Any unmatched state is treated as unknown.
  const fallbackUnknown = `NOT ((${qualCondSql}) OR (${unqualCondSql}) OR (${refusalCondSql}))`;

  return {
    qual: `CASE WHEN ${qualCondSql} THEN 1 ELSE 0 END`,
    unqual: `CASE WHEN ${unqualCondSql} OR (${invalidCond}) THEN 1 ELSE 0 END`,
    unknown: `CASE WHEN ${unknownCondSql} OR (${fallbackUnknown}) THEN 1 ELSE 0 END`,
    refusal: `CASE WHEN ${refusalCondSql} THEN 1 ELSE 0 END`,
    inWork: `CASE WHEN ${joinConds(inWorkConds)} THEN 1 ELSE 0 END`,
    invalid: `CASE WHEN ${invalidCond} THEN 1 ELSE 0 END`,
  };
}
