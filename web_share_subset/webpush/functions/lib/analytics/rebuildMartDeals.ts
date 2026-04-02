import { parseAmount } from "./amt";
import { extractCourseCodeFromText } from "./courseCodeLookup";
import { classifyEventFromRow } from "./eventClassifier";
import { funnelReportBucket } from "./funnelBucket";
import { isAttackingJanuary } from "./isAttackingJanuary";
import { monthFromCreated } from "./month";
import { normalizeCourseCode } from "./normalizeCourseCode";
import { variant3RevenueMask } from "./revenue";
import { rowForClassifier, type StgDealAnalytics } from "./stagingTypes";

function chunks<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function buildMartRow(s: StgDealAnalytics) {
  const cls = classifyEventFromRow(rowForClassifier(s));
  let courseRaw = (s.code_site || "").trim();
  if (!courseRaw) courseRaw = (s.code_course || "").trim();
  if (!courseRaw) courseRaw = extractCourseCodeFromText(s.deal_name);
  if (!courseRaw) courseRaw = extractCourseCodeFromText(s.utm_campaign);
  if (!courseRaw) courseRaw = extractCourseCodeFromText(s.utm_content);
  const courseNorm = normalizeCourseCode(courseRaw);
  const revMask = variant3RevenueMask({
    stage_raw: s.stage_raw,
    closed_yes: s.closed_yes,
    pay_date: s.pay_date,
    installment_schedule: s.installment_schedule,
  });
  const amt = parseAmount(s.sum_text);
  const revenue = revMask ? amt : 0;
  const aj = isAttackingJanuary(rowForClassifier(s)) ? 1 : 0;
  const fg = funnelReportBucket(s.funnel_raw);
  const mo = monthFromCreated(s.created_at);

  return {
    ID: s.deal_id,
    contact: s.contact_id,
    created: s.created_at,
    month: mo,
    voronka: s.funnel_raw,
    funnel_group: fg,
    stage: s.stage_raw,
    closed: s.closed_yes,
    pay: s.pay_date,
    sum: s.sum_text,
    revenue_amount: revenue,
    is_revenue_variant3: revMask ? 1 : 0,
    utm_s: s.utm_source,
    utm_m: s.utm_medium,
    utm_c: s.utm_campaign,
    utm_ct: s.utm_content,
    name: s.deal_name,
    code_site: s.code_site,
    code_course: s.code_course,
    course_code_norm: courseNorm,
    event_class: cls.event,
    classification_source: cls.source_field,
    classification_pattern: cls.matched_pattern,
    classification_confidence: cls.confidence,
    is_attacking_january: aj,
    invalid_type_lead: s.invalid_type_lead ?? "",
  };
}

/** Rebuild mart_deals_enriched from stg_deals_analytics (batched). */
export async function rebuildMartDealsFromStaging(db: D1Database): Promise<{ rows: number }> {
  await db.prepare("DELETE FROM mart_deals_enriched").run();

  const { results } = await db.prepare("SELECT * FROM stg_deals_analytics").all<StgDealAnalytics>();
  const rows = results ?? [];
  if (!rows.length) return { rows: 0 };

  const stmt = db.prepare(
    `INSERT INTO mart_deals_enriched (
      "ID", "Контакт: ID", "Дата создания", month, "Воронка", funnel_group,
      "Стадия сделки", "Сделка закрыта", "Дата оплаты", "Сумма",
      revenue_amount, is_revenue_variant3,
      "UTM Source", "UTM Medium", "UTM Campaign", "UTM Content",
      "Название сделки", "Код_курса_сайт", "Код курса",
      course_code_norm, event_class, classification_source, classification_pattern, classification_confidence,
      is_attacking_january, "Типы некачественного лида"
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,  
  );

  for (const batch of chunks(rows, 80)) {
    const stmts: D1PreparedStatement[] = [];
    for (const s of batch) {
      const r = buildMartRow(s);
      stmts.push(
        stmt.bind(
          r.ID,
          r.contact,
          r.created,
          r.month,
          r.voronka,
          r.funnel_group,
          r.stage,
          r.closed,
          r.pay,
          r.sum,
          r.revenue_amount,
          r.is_revenue_variant3,
          r.utm_s,
          r.utm_m,
          r.utm_c,
          r.utm_ct,
          r.name,
          r.code_site,
          r.code_course,
          r.course_code_norm,
          r.event_class,
          r.classification_source,
          r.classification_pattern,
          r.classification_confidence,
          r.is_attacking_january,
          r.invalid_type_lead,
        ),
      );
    }
    await db.batch(stmts);
  }

  return { rows: rows.length };
}
