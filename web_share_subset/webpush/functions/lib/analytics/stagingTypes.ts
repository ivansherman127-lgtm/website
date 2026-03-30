/** Row shape from stg_deals_analytics (SQLite / D1). */
export interface StgDealAnalytics {
  deal_id: string;
  contact_id: string;
  created_at: string;
  funnel_raw: string;
  stage_raw: string;
  closed_yes: string;
  pay_date: string;
  installment_schedule: string;
  sum_text: string;
  utm_source: string;
  utm_medium: string;
  utm_campaign: string;
  utm_content: string;
  deal_name: string;
  code_site: string;
  code_course: string;
  source_detail: string;
  source_inquiry: string;
}

export function rowForClassifier(s: StgDealAnalytics): Record<string, unknown> {
  return {
    "Название сделки": s.deal_name,
    "Код_курса_сайт": s.code_site,
    "Код курса": s.code_course,
    "UTM Campaign": s.utm_campaign,
    "Источник (подробно)": s.source_detail,
    "Источник обращения": s.source_inquiry,
  };
}
