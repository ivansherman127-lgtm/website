/**
 * Port of db/run_all_slices.build_yandex_dedup_marts (merge + project marts + spend dedup).
 */
import { extractYandexAdIdFromUtmContent } from "./yandexAdId";

function str(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = String(v).trim();
  if (!s || s.toLowerCase() === "nan") return "";
  return s;
}

function idNorm(v: unknown): string {
  const s = str(v);
  if (!s) return "";
  return /^\d+\.0+$/.test(s) ? s.split(".")[0] : s;
}

interface MartRow {
  ID: string;
  contact_id: string;
  month: string;
  utm_campaign: string;
  utm_content: string;
  utm_source: string;
  revenue_amount: number | null;
  is_revenue_variant3: number | null;
  deal_name: string;
  funnel: string;
  stage: string;
}

interface YandexRow {
  ad_id: string;
  project_name: string;
  campaign_id: string;
  yandex_month: string;
  yandex_spend: number;
}

interface RawLead {
  ID: string;
  contact_id: string;
  lead_key: string;
  deal_month: string;
  utm_campaign: string;
  project_name: string;
  campaign_id: string;
  yandex_month: string;
  yandex_spend: number;
  deal_name: string;
  is_paid_deal: number;
  revenue_amount: number;
  funnel: string;
  stage: string;
}

function isYandexSource(v: unknown): boolean {
  const s = str(v).toLowerCase();
  return s.startsWith("y") && s !== "yah";
}

function parseSpend(v: unknown): number {
  const n = Number(String(v ?? 0).replace(",", "."));
  return Number.isFinite(n) ? n : 0;
}

function chunks<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export async function rebuildYandexMarts(db: D1Database): Promise<{
  raw_rows: number;
  dedup_rows: number;
}> {
  await db.prepare("DELETE FROM mart_yandex_leads_raw").run();
  await db.prepare("DELETE FROM mart_yandex_leads_dedup").run();
  await db.prepare("DELETE FROM mart_yandex_revenue_projects").run();

  const martRes = await db
    .prepare(
      `SELECT ID, "Контакт: ID" AS contact_id, month, "UTM Campaign" AS utm_campaign,
              "UTM Content" AS utm_content, "UTM Source" AS utm_source,
              revenue_amount, is_revenue_variant3, "Название сделки" AS deal_name,
              funnel_group AS funnel, "Стадия сделки" AS stage
       FROM mart_deals_enriched`,
    )
    .all<MartRow>();
  const bitrix = martRes.results ?? [];

  const yandexRes = await db
    .prepare(
      `SELECT "№ Объявления" AS ad_id,
              "Название кампании" AS project_name,
              "№ Кампании" AS campaign_id,
              COALESCE(month, "Месяц") AS yandex_month,
              "Расход, ₽" AS yandex_spend
       FROM stg_yandex_stats`,
    )
    .all<Record<string, unknown>>();
  const yandexLookup = new Map<string, YandexRow[]>();
  for (const row of yandexRes.results ?? []) {
    const adId = idNorm(row.ad_id);
    if (!adId) continue;
    const parsed: YandexRow = {
      ad_id: adId,
      project_name: str(row.project_name),
      campaign_id: idNorm(row.campaign_id),
      yandex_month: str(row.yandex_month),
      yandex_spend: parseSpend(row.yandex_spend),
    };
    const bucket = yandexLookup.get(adId);
    if (bucket) bucket.push(parsed);
    else yandexLookup.set(adId, [parsed]);
  }

  const merged: RawLead[] = [];
  for (const b of bitrix) {
    const did = idNorm(b.ID);
    const utmContent = extractYandexAdIdFromUtmContent(b.utm_content);
    if (!did || !utmContent || !isYandexSource(b.utm_source)) continue;
    const cid = idNorm(b.contact_id);
    const leadKey = cid || did;
    const rev = Number(b.revenue_amount ?? 0) || 0;
    const paid = Number(b.is_revenue_variant3 ?? 0) || 0;
    const rows = yandexLookup.get(utmContent) ?? [];
    for (const row of rows) {
      const pn = str(row.project_name);
      merged.push({
        ID: did,
        contact_id: cid,
        lead_key: leadKey,
        deal_month: str(b.month),
        utm_campaign: str(b.utm_campaign),
        project_name: pn === "" ? "UNMAPPED" : pn,
        campaign_id: idNorm(row.campaign_id),
        yandex_month: str(row.yandex_month),
        yandex_spend: Math.round(row.yandex_spend),
        deal_name: str(b.deal_name),
        is_paid_deal: paid,
        revenue_amount: rev,
        funnel: str(b.funnel),
        stage: str(b.stage),
      });
    }
  }

  const seen = new Set<string>();
  const rawDeduped: RawLead[] = [];
  for (const r of merged) {
    const k = `${r.ID}|${r.campaign_id}`;
    if (seen.has(k)) continue;
    seen.add(k);
    rawDeduped.push(r);
  }

  const insRaw = db.prepare(
    `INSERT INTO mart_yandex_leads_raw (
      "ID", contact_id, lead_key, deal_month, utm_campaign, project_name, campaign_id, yandex_month, yandex_spend,
      deal_name, is_paid_deal, revenue_amount, funnel, stage
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
  );
  for (const batch of chunks(rawDeduped, 80)) {
    await db.batch(
      batch.map((r) =>
        insRaw.bind(
          r.ID,
          r.contact_id,
          r.lead_key,
          r.deal_month,
          r.utm_campaign,
          r.project_name,
          r.campaign_id,
          r.yandex_month,
          r.yandex_spend,
          r.deal_name,
          r.is_paid_deal,
          r.revenue_amount,
          r.funnel,
          r.stage,
        ),
      ),
    );
  }

  const sorted = [...rawDeduped].sort((a, b) => {
    const x = a.lead_key.localeCompare(b.lead_key);
    if (x !== 0) return x;
    const y = a.project_name.localeCompare(b.project_name);
    if (y !== 0) return y;
    return a.ID.localeCompare(b.ID);
  });

  const insDedup = db.prepare(
    `INSERT INTO mart_yandex_leads_dedup (
      lead_key, project_name, deals_count, paid_deals, revenue, contact_id, campaign_id, yandex_month
    ) VALUES (?,?,?,?,?,?,?,?)`,
  );

  const dedupList: {
    lead_key: string;
    project_name: string;
    deals_count: number;
    paid_deals: number;
    revenue: number;
    contact_id: string;
    campaign_id: string;
    yandex_month: string;
  }[] = [];
  const g2 = new Map<
    string,
    { ids: Set<string>; paid: number; revenue: number; contact_id: string; campaign_id: string; yandex_month: string }
  >();
  for (const r of sorted) {
    const key = `${r.lead_key}\u0000${r.project_name}`;
    let gg = g2.get(key);
    if (!gg) {
      gg = {
        ids: new Set(),
        paid: 0,
        revenue: 0,
        contact_id: r.contact_id,
        campaign_id: r.campaign_id,
        yandex_month: r.yandex_month,
      };
      g2.set(key, gg);
    }
    gg.ids.add(r.ID);
    gg.paid += r.is_paid_deal;
    gg.revenue += r.revenue_amount;
  }
  for (const [key, gg] of g2) {
    const [lead_key, project_name] = key.split("\u0000");
    dedupList.push({
      lead_key,
      project_name,
      deals_count: gg.ids.size,
      paid_deals: gg.paid,
      revenue: gg.revenue,
      contact_id: gg.contact_id,
      campaign_id: gg.campaign_id,
      yandex_month: gg.yandex_month,
    });
  }

  for (const batch of chunks(dedupList, 80)) {
    await db.batch(
      batch.map((d) =>
        insDedup.bind(
          d.lead_key,
          d.project_name,
          d.deals_count,
          d.paid_deals,
          d.revenue,
          d.contact_id,
          d.campaign_id,
          d.yandex_month,
        ),
      ),
    );
  }

  if (rawDeduped.length) {
    const projMetrics = new Map<
      string,
      { leads_raw: number; deals_raw: Set<string>; paid_deals_raw: number; revenue_raw: number }
    >();
    for (const r of rawDeduped) {
      const pk = `${r.project_name}\u0000${r.yandex_month}`;
      let pm = projMetrics.get(pk);
      if (!pm) {
        pm = { leads_raw: 0, deals_raw: new Set(), paid_deals_raw: 0, revenue_raw: 0 };
        projMetrics.set(pk, pm);
      }
      pm.leads_raw += 1;
      pm.deals_raw.add(r.ID);
      pm.paid_deals_raw += r.is_paid_deal;
      pm.revenue_raw += r.revenue_amount;
    }
    const campFirst = new Map<string, { project_name: string; yandex_month: string; yandex_spend: number }>();
    for (const row of yandexRes.results ?? []) {
      const campaignId = idNorm(row.campaign_id);
      const projectName = str(row.project_name);
      const yandexMonth = str(row.yandex_month);
      if (!campaignId || !projectName || !yandexMonth) continue;
      const ck = `${campaignId}\u0000${yandexMonth}`;
      if (!campFirst.has(ck)) {
        campFirst.set(ck, {
          project_name: projectName,
          yandex_month: yandexMonth,
          yandex_spend: parseSpend(row.yandex_spend),
        });
      }
    }
    const spendByProj = new Map<string, number>();
    for (const v of campFirst.values()) {
      const pk = `${v.project_name}\u0000${v.yandex_month}`;
      spendByProj.set(pk, (spendByProj.get(pk) ?? 0) + v.yandex_spend);
    }

    // Build dedup-per-project map so we can merge in a single INSERT pass below.
    const dedupProj = new Map<string, { leads: Set<string>; paid_deals_dedup: number; revenue_dedup: number }>();
    for (const d of dedupList) {
      const pk = `${d.project_name}\u0000${d.yandex_month}`;
      let g = dedupProj.get(pk);
      if (!g) {
        g = { leads: new Set(), paid_deals_dedup: 0, revenue_dedup: 0 };
        dedupProj.set(pk, g);
      }
      g.leads.add(d.lead_key);
      g.paid_deals_dedup += d.paid_deals;
      g.revenue_dedup += d.revenue;
    }

    const insProj = db.prepare(
      `INSERT INTO mart_yandex_revenue_projects (
        project_name, yandex_month, leads_raw, deals_raw, paid_deals_raw, revenue_raw, spend,
        leads_dedup, paid_deals_dedup, revenue_dedup
      ) VALUES (?,?,?,?,?,?,?,?,?,?)`,
    );
    const stmts: D1PreparedStatement[] = [];
    for (const [pk, pm] of projMetrics) {
      const [project_name, yandex_month] = pk.split("\u0000");
      const spend = spendByProj.get(pk) ?? 0;
      const dup = dedupProj.get(pk);
      stmts.push(
        insProj.bind(
          project_name,
          yandex_month,
          pm.leads_raw,
          pm.deals_raw.size,
          pm.paid_deals_raw,
          pm.revenue_raw,
          spend,
          dup?.leads.size ?? null,
          dup?.paid_deals_dedup ?? null,
          dup?.revenue_dedup ?? null,
        ),
      );
    }
    for (const batch of chunks(stmts, 80)) {
      await db.batch(batch);
    }
  }

  return { raw_rows: rawDeduped.length, dedup_rows: dedupList.length };
}
