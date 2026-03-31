export type YandexProjectGroupsFile = {
  version: number;
  updated_at: string;
  source_csv: string;
  groups: Record<string, string[]>;
};

export const YANDEX_PROJECT_GROUPS: YandexProjectGroupsFile = {
  version: 1,
  updated_at: "2026-03-31",
  source_csv: "/Users/ivan/Downloads/mapping csv.csv",
  groups: {
    "Prof Pentest": [
      "vea_b2c_prof_pentest_demo1_RSYA_100226_rm261125-RF_LEAD_TILDA",
      "vea-professiya-pentester_RSYA-270226_1_rm221125-RF",
      "vea-professiya-pentester_RSYA-270226_1_rm221125_City_ml",
      "vea_b2c_prof_pentest_demo1_RSYA_261125_rm261125-RF_City_ml",
      "vea_b2c_prof_pentest_demo2_RSYA_100226_rm261125-RF_LEAD_TILDA",
      "vea_b2c_prof_pentest_demo1_RSYA_181225_rm181225-RF_City_ml",
      "vea_trek_pentester_RSYA_230126_rm230126-RF",
      "vea-apentester_RSYA-101225_rm201125-RF-City_ml (-авт.камп.-сайт.+расш.автотар)",
      "RED-PENTEST",
      "RED-PENTEST поиск",
      "vea-professiya-pentester_RSYA-270226_1_rm221125-RF-SR-PAY-CONV-200",
      "vea-professiya-pentester_RSYA-221125_rm221125-RF_City_ml",
      "vea_b2c_prof_pentest_demo1_RSYA_050226_rm261125-RF_KVAL",
      "vea_b2c_prof_pentest_demo1_POISK_261125_rm261125-RF_City_ml",
      "vea_trek_pentester_POISK_230126_rm230126-RF",
      "vea-apentester_RSYA-201125_rm201125-RF_City_ml",
      "vea_obhod_zashit_pentester_RSYA_300126_rm300126-RF_LEAD",
      "vea-professiya-pentester_RSYA-270226_1_rm221125-RF-PAY-CONV-200",
      "vea_b2c_prof_pentest_demo2_RSYA_070226_rm261125-RF_City_ml",
      "vea-b2c-courses_pentester_POISK-141125_rm141125-RF_City_ml",
      "vea_obhod_zashit_pentester_POISK_300126_rm300126-RF",
      "vea_b2c_prof_pentest_demo1_POISK_050226_rm261125-RF_City_ml",
      "TREK PENTESTER / поиск / keys / целевой",
      "vea-b2c-courses_pentester_RSYA-141125_rm141125-RF_City_ml",
      "vea-professiya-pentester_POISK-221125_rm221125-RF_City_ml",
      "vea-apentester_POISK-201125_rm201125-RF_City_ml"
    ],
    "not sure": [
      "vea_b2c_specialist_2025_RSYA_171225_rm171225-RF_City_ml"
    ],
    "January": [
      "vea_spec_red_cyber_ed_ru_yanvar_RSYA-241225_rm241225-RF_City_ml_SR_PAY_TGB_retarget",
      "vea_spec_red_cyber_ed_ru_yanvar_RSYA-241225_rm241225-RF_City_ml_SR_PAY_TGB",
      "vea_spec_red_cyber_ed_ru_yanvar_RSYA-241225_rm241225-RF_City_ml_SR_PAY_TGB_t2b2",
      "vea_spec_red_cyber_ed_ru_yanvar_POISK-241225_rm241225-RF_City_ml",
      "vea_spec_red_cyber_ed_ru_yanvar_RSYA-241225_rm241225-RF_City_ml_SR_PAY_TGB_t1b1",
      "vea_spec_red_cyber_ed_ru_yanvar_RSYA-241225_rm241225-RF_City_ml_SR_PAY_TGB_t3b3",
      "vea_spec_red_cyber_ed_ru_yanvar_RSYA-241225_rm241225-RF_City_ml_SR_PAY_TGB_lal",
      "vea_spec_red_cyber_ed_ru_yanvar_RSYA-241225_rm241225-RF_City_ml__CONV_CLIK_VIDEO_TGB"
    ],
    " Brand": [
      "vea_brand_POISK-271125_rm271125-RF",
      "Брендовые запросы / brand",
      "vea_brand_POISK-130226_rm271125-RF"
    ],
    "Meet a Mentor": [
      "CyberED/с опытом в ИБ/avto/18.09.25_vstrecha-s-nastavnikom",
      "vea_cybered_start_v_ib_RSYA-110326_rm110326-RF_retarget",
      "CyberED/без опыта в ИБ/сайты/06.10.25_vstrecha-s-nastavnikom",
      "CyberED/без опыта в ИБ/конкуренты/keys_bezavto/18.09.25_vstrecha-s-nastavnikom",
      "vea_kiberbez_RSYA-130226_rm130226-RF_City_ml_retarget",
      "CyberED/ПОИСК/целевые/18.09.25_vstrecha-s-nastavnikom"
    ],
    "SOC Demo": [
      "vea_b2c_courses_analytic_soc_demo_RSYA_261125_rm261125-RF_City_ml",
      "vea_b2c_courses_analytic_soc_demo_RSYA_050226_rm261125-RF_KVAL",
      "vea_b2c_prof_soc_demo2_RSYA_100226_rm261125-RF_LEAD_TILDA",
      "vea_b2c_courses_analytic_soc_demo_POISK_261125_rm261125-RF_City_ml",
      "vea-b2c_courses_analytic_soc_RSYA-141125_rm141125-RF_City_ml",
      "vea_b2c_prof-soc-demo_RSYA_171225_rm171225-RF_City_ml",
      "SOC поиск",
      "vea_b2c_prof_soc_demo2_RSYA_070226_rm261125-RF_City_ml",
      "vea_b2c_courses_analytic_soc_demo_POISK_050226_rm261125-RF_City_ml",
      "vea-b2c_courses_analytic_soc_POISK-141125_rm141125-RF_City_ml"
    ],
    "Statya": [
      "vea_TG_cybered_RSYA_270126_rm270126-RF",
      "vea_cybered_statya_RSYA-040326_rm040326-RF_retarget"
    ]
  }
};

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