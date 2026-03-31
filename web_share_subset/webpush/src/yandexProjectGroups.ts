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

const aliasToGroup = new Map<string, string>();

for (const [groupLabel, aliases] of Object.entries(YANDEX_PROJECT_GROUPS.groups)) {
  const normalizedGroup = normalizeGroupLabel(groupLabel);
  for (const aliasRaw of aliases) {
    const alias = String(aliasRaw ?? "").trim();
    if (!alias || aliasToGroup.has(alias)) continue;
    aliasToGroup.set(alias, normalizedGroup);
  }
}

export const YANDEX_PROJECT_GROUP_ALIAS_PAIRS: Array<[string, string]> = [...aliasToGroup.entries()];

export function mapYandexProjectGroup(projectName: unknown): string {
  const project = String(projectName ?? "").trim();
  if (!project) return "UNMAPPED";
  return aliasToGroup.get(project) || project;
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