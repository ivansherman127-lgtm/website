import groupsFile from "../../../yandex_campaign_groups.json";

export type YandexProjectGroupsFile = {
  version: number;
  updated_at: string;
  source_csv: string;
  groups: Record<string, string[]>;
};

export const YANDEX_PROJECT_GROUPS = groupsFile as YandexProjectGroupsFile;

function normalizeGroupLabel(label: string): string {
  const trimmed = String(label ?? "").trim();
  return trimmed || String(label ?? "");
}

function normalizeAliasKey(value: unknown): string {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/ё/g, "е")
    .replace(/\u00a0/g, " ")
    .replace(/[\s\-_./,:;'"«»()]+/g, "")
    .replace(/\.0+$/g, "");
}

const aliasToGroup = new Map<string, string>();
const aliasNormalizedToGroup = new Map<string, string>();

for (const [groupLabel, aliases] of Object.entries(YANDEX_PROJECT_GROUPS.groups)) {
  const normalizedGroup = normalizeGroupLabel(groupLabel);
  for (const aliasRaw of aliases) {
    const alias = String(aliasRaw ?? "").trim();
    if (!alias || aliasToGroup.has(alias)) continue;
    aliasToGroup.set(alias, normalizedGroup);
    const aliasNorm = normalizeAliasKey(alias);
    if (aliasNorm && !aliasNormalizedToGroup.has(aliasNorm)) {
      aliasNormalizedToGroup.set(aliasNorm, normalizedGroup);
    }
  }
}

export const YANDEX_PROJECT_GROUP_ALIAS_PAIRS: Array<[string, string]> = [...aliasToGroup.entries()];

/** Set of canonical group labels defined in yandex_campaign_groups.json (trimmed). */
export const YANDEX_KNOWN_GROUPS = new Set<string>(
  Object.keys(YANDEX_PROJECT_GROUPS.groups)
    .map((label) => String(label ?? "").trim())
    .filter(Boolean),
);

export function mapYandexProjectGroup(projectName: unknown): string {
  const project = String(projectName ?? "").trim();
  if (!project) return "UNMAPPED";
  // Return the project name as-is (fuzzy grouping is handled by Python post-processing).
  // JSON alias lookups have been removed in favour of normalized identity mapping.
  return project;
}

export function buildExplicitYandexProjectLabelMap(projectNames: Iterable<string>): Map<string, string> {
  const out = new Map<string, string>();
  for (const projectName of projectNames) {
    const project = String(projectName ?? "").trim();
    if (!project) continue;
    out.set(project, mapYandexProjectGroup(project));
  }
  return out;
}