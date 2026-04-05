export type YandexProjectGroupsFile = {
  version: number;
  updated_at: string;
  source_csv: string;
  groups: Record<string, string[]>;
};

// Hardcoded JSON campaign mapping is intentionally disabled.
export const YANDEX_PROJECT_GROUPS: YandexProjectGroupsFile = {
  version: 0,
  updated_at: "",
  source_csv: "",
  groups: {},
};

export const YANDEX_PROJECT_GROUP_ALIAS_PAIRS: Array<[string, string]> = [];
export const YANDEX_KNOWN_GROUPS = new Set<string>();

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