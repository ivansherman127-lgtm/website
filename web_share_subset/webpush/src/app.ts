import "./style.css";
import Chart from "chart.js/auto";
import { dataUrl, staticUrl } from "./data-source";
import { mapYandexProjectGroup } from "./yandexProjectGroups";
import mediumConfigJson from "../functions/api/utm_medium_sources.json";

const app = document.querySelector<HTMLDivElement>("#app")!;
const columnAliasesByView = new Map<string, Record<string, string>>();
let utmLatestTag = "";
let utmSessionRows: Record<string, unknown>[] = [];

type UtmMediumEntry = { value: string; label: string; sourceType: "select" | "freetext"; hasPartner?: boolean; sources: string[] };
const UTM_MEDIUM_CONFIG: UtmMediumEntry[] = (mediumConfigJson as { mediums: UtmMediumEntry[] }).mediums;


async function fetchJson<T>(path: string): Promise<T> {
  const parseBody = (urlLabel: string, ct: string, txt: string): T => {
    if (ct.includes("application/json")) {
      return JSON.parse(txt) as T;
    }
    try {
      return JSON.parse(txt) as T;
    } catch {
      throw new Error(`${urlLabel}: response is not valid JSON`);
    }
  };

  const url1 = dataUrl(path);
  const r1 = await fetch(url1, { cache: "no-store" });
  if (!r1.ok) {
    if (path.startsWith("/api/assoc-revenue") && r1.status === 404) {
      return [] as T;
    }
    throw new Error(`${path}: ${r1.status}`);
  }
  const ct1 = (r1.headers.get("content-type") || "").toLowerCase();
  const txt1 = await r1.text();

  // Some deployments expose Functions at /api/*, others under BASE_URL/api/*.
  // If we got HTML for an API request, retry with the alternate route automatically.
  if (path.startsWith("/api/") && /^\s*<!doctype\s+html/i.test(txt1)) {
    const base = import.meta.env.BASE_URL || "/";
    const altUrl = url1.startsWith("/api/") ? `${base}${path.slice(1)}` : path;
    if (altUrl !== url1) {
      const r2 = await fetch(altUrl, { cache: "no-store" });
      if (r2.ok) {
        const ct2 = (r2.headers.get("content-type") || "").toLowerCase();
        const txt2 = await r2.text();
        if (!/^\s*<!doctype\s+html/i.test(txt2)) {
          return parseBody(path, ct2, txt2);
        }
      }
    }
    if (path.startsWith("/api/assoc-revenue")) {
      throw new Error(
        `${path}: assoc report generation requires Cloudflare Functions + D1 runtime. Use \"npm run pages:dev\" locally or open deployed Pages URL.`,
      );
    }
    throw new Error(`${path}: API returned HTML instead of JSON on both route variants.`);
  }

  return parseBody(path, ct1, txt1);
}

function num(v: unknown): number {
  if (typeof v === "number" && !Number.isNaN(v)) return v;
  if (typeof v === "string" && v.trim() !== "") {
    const cleaned = v.replace(/\s+/g, "").replace(",", ".");
    const n = Number(cleaned);
    if (!Number.isNaN(n)) return n;
  }
  return 0;
}

function escapeHtml(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function isMoneyColumn(col: string): boolean {
  const c = col.trim().toLowerCase();
  return (
    c.includes("₽") ||
    c.includes("руб") ||
    c.includes("выруч") ||
    c === "средний_чек" ||
    c === "средний чек" ||
    c.startsWith("средняя") ||
    c === "sum" ||
    c === "profit" ||
    c.includes("прибыл") ||
    c === "qpa" ||
    c.includes("расход")
  );
}

function isPercentColumn(col: string): boolean {
  const c = col.trim().toLowerCase();
  return c.includes("конверсия") || c.endsWith("%") || c.includes("ctor");
}

function isCountColumn(col: string): boolean {
  const c = col.trim().toLowerCase();
  return (
    c === "сделок_с_выручкой" ||
    c === "сделок с выручкой" ||
    c === "контактов_с_выручкой" ||
    c === "контактов с выручкой" ||
    c === "контактов_в_пуле" ||
    c === "контактов в пуле" ||
    c === "лиды_yandex" ||
    c === "лиды yandex" ||
    c === "сделок_bitrix" ||
    c === "сделок bitrix" ||
    c === "рассылок за месяц" ||
    c === "лиды" ||
    c === "актуальная база email" ||
    c === "контактов email (db)"
  );
}

function prettyColName(col: string): string {
  return col.replaceAll("_", " ");
}

function displayColName(view: ViewKey, col: string): string {
  const aliases = columnAliasesByView.get(view) || {};
  return aliases[col] || prettyColName(col);
}

function isEditableDataColumn(col: string): boolean {
  const c = col.trim().toLowerCase();
  if (c === "level") return false;
  return !isMoneyColumn(col) && !isCountColumn(col) && !isPercentColumn(col) && !isDateColumn(col);
}

function formatRub(v: unknown): string {
  const n = num(v);
  if (!Number.isFinite(n)) return String(v ?? "");
  const rub = Math.round(n);
  return `${rub.toLocaleString("ru-RU", { maximumFractionDigits: 0 })} ₽`;
}

function formatPercent(v: unknown): string {
  const n = num(v);
  if (!Number.isFinite(n)) return String(v ?? "");
  const pct = Math.abs(n) <= 1 ? n * 100 : n;
  return `${pct.toLocaleString("ru-RU", { maximumFractionDigits: 1 })}%`;
}

function formatCell(col: string, v: unknown): string {
  if (v === null || v === undefined) return "";
  if (isCountColumn(col)) return Math.round(num(v)).toLocaleString("ru-RU");
  if (isMoneyColumn(col)) return formatRub(v);
  if (isPercentColumn(col)) return formatPercent(v);
  return String(v);
}

function isTotalValue(v: unknown): boolean {
  const s = String(v ?? "").trim().toLowerCase();
  return s === "total";
}

function monthNameToNumber(s: string): number {
  const t = s.toLowerCase();
  if (t.startsWith("янв")) return 1;
  if (t.startsWith("фев")) return 2;
  if (t.startsWith("мар")) return 3;
  if (t.startsWith("апр")) return 4;
  if (t.startsWith("май")) return 5;
  if (t.startsWith("июн")) return 6;
  if (t.startsWith("июл")) return 7;
  if (t.startsWith("авг")) return 8;
  if (t.startsWith("сен")) return 9;
  if (t.startsWith("окт")) return 10;
  if (t.startsWith("ноя")) return 11;
  if (t.startsWith("дек")) return 12;
  return 0;
}

function parseDateRank(v: unknown): number | null {
  const s = String(v ?? "").trim();
  if (!s) return null;
  if (isTotalValue(s)) return Number.POSITIVE_INFINITY;

  const ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (ymd) return Number(ymd[1]) * 100 + Number(ymd[2]);

  const ym = s.match(/^(\d{4})-(\d{2})$/);
  if (ym) return Number(ym[1]) * 100 + Number(ym[2]);
  const y = s.match(/^(\d{4})$/);
  if (y) return Number(y[1]) * 100;
  const ru = s.match(/^([А-Яа-яA-Za-z]+),\s*(\d{4})$/);
  if (ru) return Number(ru[2]) * 100 + monthNameToNumber(ru[1]);
  return null;
}

function monthSerialToRussianLabel(serial: number): string {
  const year = Math.floor(serial / 12);
  const monthIndex = serial % 12;
  const months = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"];
  return `${months[monthIndex]} ${year}`;
}

function monthsBackRangeLabel(rows: Record<string, unknown>[], dateCol: string, monthsBack: number): string {
  const ranks = rows
    .map((r) => parseDateRank(r[dateCol]))
    .filter((v): v is number => v !== null && Number.isFinite(v) && v !== Number.POSITIVE_INFINITY)
    .sort((a, b) => a - b);

  if (!ranks.length) return `${monthsBack} мес.`;

  const maxSerial = rankToMonthSerial(ranks[ranks.length - 1]);
  const normalizedBack = Math.max(1, Math.round(monthsBack));
  const minSerial = maxSerial - (normalizedBack - 1);
  return `${monthSerialToRussianLabel(minSerial)}:${monthSerialToRussianLabel(maxSerial)}`;
}

function isDateColumn(col: string): boolean {
  const c = col.trim().toLowerCase();
  return c === "месяц" || c === "год" || c === "период" || c.includes("date");
}

function compareCell(col: string, av: unknown, bv: unknown, dir: "asc" | "desc"): number {
  if (isTotalValue(av) && !isTotalValue(bv)) return -1;
  if (!isTotalValue(av) && isTotalValue(bv)) return 1;

  if (isDateColumn(col)) {
    const ar = parseDateRank(av);
    const br = parseDateRank(bv);
    if (ar !== null || br !== null) {
      const aa = ar ?? Number.NEGATIVE_INFINITY;
      const bb = br ?? Number.NEGATIVE_INFINITY;
      return dir === "asc" ? aa - bb : bb - aa;
    }
  }
  if (isMoneyColumn(col) || isCountColumn(col) || isPercentColumn(col) || (typeof av === "number" && typeof bv === "number")) {
    const an = num(av);
    const bn = num(bv);
    return dir === "asc" ? an - bn : bn - an;
  }
  return dir === "asc"
    ? String(av ?? "").localeCompare(String(bv ?? ""))
    : String(bv ?? "").localeCompare(String(av ?? ""));
}

function renderSingleRowTable(title: string, row: Record<string, unknown>): string {
  const entries = Object.entries(row).filter(([k]) => String(k).trim() !== "");
  const head = entries.map(([k]) => `<th>${escapeHtml(prettyColName(k))}</th>`).join("");
  const body = entries
    .map(([k, v]) => `<td>${escapeHtml(formatCell(k, v))}</td>`)
    .join("");
  return `
    <section class="chart-wrap">
      <h3>${escapeHtml(title)}</h3>
      <div class="table-scroll"><table><thead><tr>${head}</tr></thead><tbody><tr>${body}</tr></tbody></table></div>
    </section>
  `;
}

function rankToMonthSerial(rank: number): number {
  const y = Math.floor(rank / 100);
  let m = rank % 100;
  if (m < 1 || m > 12) m = 1;
  return y * 12 + (m - 1);
}

function filterRowsByMonthsBack(rows: Record<string, unknown>[], dateCol: string, monthsBack: number): Record<string, unknown>[] {
  if (!Number.isFinite(monthsBack) || monthsBack <= 0) return rows;
  const serials = rows
    .map((r) => parseDateRank(r[dateCol]))
    .filter((v): v is number => v !== null && Number.isFinite(v))
    .map((v) => rankToMonthSerial(v));
  if (!serials.length) return rows;
  const latest = Math.max(...serials);
  const cutoff = latest - (Math.max(1, Math.floor(monthsBack)) - 1);
  return rows.filter((r) => {
    const rk = parseDateRank(r[dateCol]);
    if (rk === null || !Number.isFinite(rk)) return true;
    return rankToMonthSerial(rk) >= cutoff;
  });
}

function filterRowsByDateRange(
  rows: Record<string, unknown>[],
  dateCol: string,
  from: string,
  to: string,
): Record<string, unknown>[] {
  if (!dateCol || (!from && !to)) return rows;
  return rows.filter((r) => {
    const v = String(r[dateCol] ?? "").slice(0, 7);
    if (!v || v.length < 7) return true;
    if (from && v < from) return false;
    if (to && v > to) return false;
    return true;
  });
}

function renderRowsTable(title: string, rows: Record<string, unknown>[]): string {
  if (!rows.length) {
    return `
      <section class="chart-wrap">
        <h3>${escapeHtml(title)}</h3>
        <p class="muted">Нет данных в выбранном диапазоне</p>
      </section>
    `;
  }
  const cols = Object.keys(rows[0]).filter((k) => k.trim() !== "");
  const head = cols.map((k) => `<th>${escapeHtml(prettyColName(k))}</th>`).join("");
  const body = rows
    .map((row) => `<tr>${cols.map((k) => `<td>${escapeHtml(formatCell(k, row[k]))}</td>`).join("")}</tr>`)
    .join("");
  return `
    <section class="chart-wrap">
      <h3>${escapeHtml(title)}</h3>
      <div class="table-scroll"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>
    </section>
  `;
}

function renderWeeklyBitrixExpandableTable(rows: Record<string, unknown>[], expandedWeeks: Set<string>): string {
  if (!rows.length) {
    return `
      <section class="chart-wrap">
        <h3>Bitrix: недели (раскрытие по воронкам)</h3>
        <p class="muted">Нет данных в выбранном диапазоне</p>
      </section>
    `;
  }

  const byWeek = new Map<string, Record<string, unknown>[]>();
  for (const r of rows) {
    const week = String(r["Неделя"] ?? "").trim();
    if (!week) continue;
    if (!byWeek.has(week)) byWeek.set(week, []);
    byWeek.get(week)!.push(r);
  }
  const weeks = [...byWeek.keys()].sort((a, b) => b.localeCompare(a));
  const allOpen = weeks.length > 0 && weeks.every((w) => expandedWeeks.has(w));
  const cols = [
    "Неделя",
    "Воронка",
    "Лиды",
    "Квал",
    "Конверсия в Квал",
    "Неквал",
    "Конверсия в Неквал",
    "Неизвестно",
    "Отказы",
    "Конверсия в Отказ",
    "В работе",
    "Конверсия в работе",
    "Сделок_с_выручкой",
    "Выручка_сделки_недели",
    "Выручка_получена_на_неделе",
  ];

  const bodyRows: string[] = [];
  for (const week of weeks) {
    const items = byWeek.get(week) || [];
    const totals: Record<string, unknown> = {
      "Неделя": week,
      "Воронка": "Всего",
      "Лиды": items.reduce((a, x) => a + num(x["Лиды"]), 0),
      "Квал": items.reduce((a, x) => a + num(x["Квал"]), 0),
      "Неквал": items.reduce((a, x) => a + num(x["Неквал"]), 0),
      "Неизвестно": items.reduce((a, x) => a + num(x["Неизвестно"]), 0),
      "Отказы": items.reduce((a, x) => a + num(x["Отказы"]), 0),
      "В работе": items.reduce((a, x) => a + num(x["В работе"]), 0),
      "Сделок_с_выручкой": items.reduce((a, x) => a + num(x["Сделок_с_выручкой"]), 0),
      "Выручка_сделки_недели": items.reduce((a, x) => a + num(x["Выручка_сделки_недели"]), 0),
      "Выручка_получена_на_неделе": items.reduce((a, x) => a + num(x["Выручка_получена_на_неделе"]), 0),
    };
    const leads = num(totals["Лиды"]);
    const qual = num(totals["Квал"]);
    const unqual = num(totals["Неквал"]);
    const refusal = num(totals["Отказы"]);
    const inWork = num(totals["В работе"]);
    totals["Конверсия в Квал"] = leads > 0 ? qual / leads : 0;
    totals["Конверсия в Неквал"] = leads > 0 ? unqual / leads : 0;
    totals["Конверсия в Отказ"] = leads > 0 ? refusal / leads : 0;
    totals["Конверсия в работе"] = leads > 0 ? inWork / leads : 0;
    const open = expandedWeeks.has(week);
    bodyRows.push(
      `<tr class="week-row"><td><button class="week-expand-btn" data-week="${escapeHtml(week)}">${open ? "−" : "+"}</button> ${escapeHtml(week)}</td>${cols
        .slice(1)
        .map((c) => `<td>${escapeHtml(formatCell(c, totals[c]))}</td>`)
        .join("")}</tr>`,
    );
    if (open) {
      const sorted = [...items].sort((a, b) => String(a["Воронка"] ?? "").localeCompare(String(b["Воронка"] ?? "")));
      for (const item of sorted) {
        bodyRows.push(
          `<tr class="week-child-row"><td>${escapeHtml(String(item["Неделя"] ?? ""))}</td>${cols
            .slice(1)
            .map((c) => `<td>${escapeHtml(formatCell(c, item[c]))}</td>`)
            .join("")}</tr>`,
        );
      }
    }
  }

  return `
    <section class="chart-wrap bitrix-weeks">
      <h3>Bitrix: недели (раскрытие по воронкам)</h3>
      <button class="week-expand-all-btn">${allOpen ? "Свернуть всё" : "Развернуть всё"}</button>
      <div class="table-scroll"><table><thead><tr>${cols.map((c) => `<th>${escapeHtml(prettyColName(c))}</th>`).join("")}</tr></thead><tbody>${bodyRows.join("")}</tbody></table></div>
    </section>
  `;
}

function renderWeeklyYandexExpandableTable(rows: Record<string, unknown>[], expandedWeeks: Set<string>): string {
  if (!rows.length) {
    return `
      <section class="chart-wrap">
        <h3>Yandex: последние 7 дней (раскрытие по кампаниям)</h3>
        <p class="muted">Нет данных в выбранном диапазоне</p>
      </section>
    `;
  }

  const byWeek = new Map<string, Record<string, unknown>[]>();
  for (const r of rows) {
    const week = String(r["Неделя"] ?? "").trim();
    if (!week) continue;
    if (!byWeek.has(week)) byWeek.set(week, []);
    byWeek.get(week)!.push(r);
  }
  const weeks = [...byWeek.keys()].sort((a, b) => b.localeCompare(a));
  const allOpen = weeks.length > 0 && weeks.every((w) => expandedWeeks.has(w));
  const cols = [
    "Неделя",
    "Кампания",
    "ID кампании",
    "Лиды",
    "Квал",
    "Конверсия в Квал",
    "Неквал",
    "Конверсия в Неквал",
    "Неизвестно",
    "Отказы",
    "Конверсия в Отказ",
    "Сделок_с_выручкой",
    "Ассоц_выручка",
    "Расход, ₽",
    "Прибыль",
  ];

  const bodyRows: string[] = [];
  for (const week of weeks) {
    const items = byWeek.get(week) || [];
    const totals: Record<string, unknown> = {
      "Неделя": week,
      "Кампания": "Всего",
      "ID кампании": "-",
      "Лиды": items.reduce((a, x) => a + num(x["Лиды"]), 0),
      "Квал": items.reduce((a, x) => a + num(x["Квал"]), 0),
      "Неквал": items.reduce((a, x) => a + num(x["Неквал"]), 0),
      "Неизвестно": items.reduce((a, x) => a + num(x["Неизвестно"]), 0),
      "Отказы": items.reduce((a, x) => a + num(x["Отказы"]), 0),
      "Сделок_с_выручкой": items.reduce((a, x) => a + num(x["Сделок_с_выручкой"]), 0),
      "Ассоц_выручка": items.reduce((a, x) => a + num(x["Ассоц_выручка"]), 0),
      "Расход, ₽": items.reduce((a, x) => a + num(x["Расход, ₽"]), 0),
      "Прибыль": items.reduce((a, x) => a + num(x["Прибыль"]), 0),
    };
    const leads = num(totals["Лиды"]);
    const qual = num(totals["Квал"]);
    const unqual = num(totals["Неквал"]);
    const refusal = num(totals["Отказы"]);
    totals["Конверсия в Квал"] = leads > 0 ? qual / leads : 0;
    totals["Конверсия в Неквал"] = leads > 0 ? unqual / leads : 0;
    totals["Конверсия в Отказ"] = leads > 0 ? refusal / leads : 0;
    const open = expandedWeeks.has(week);
    bodyRows.push(
      `<tr class="week-row"><td><button class="yweek-expand-btn" data-week="${escapeHtml(week)}">${open ? "−" : "+"}</button> ${escapeHtml(week)}</td>${cols
        .slice(1)
        .map((c) => `<td>${escapeHtml(formatCell(c, totals[c]))}</td>`)
        .join("")}</tr>`,
    );
    if (open) {
      const sorted = [...items].sort((a, b) => String(a["Кампания"] ?? "").localeCompare(String(b["Кампания"] ?? "")));
      for (const item of sorted) {
        bodyRows.push(
          `<tr class="week-child-row"><td>${escapeHtml(String(item["Неделя"] ?? ""))}</td>${cols
            .slice(1)
            .map((c) => `<td>${escapeHtml(formatCell(c, item[c]))}</td>`)
            .join("")}</tr>`,
        );
      }
    }
  }

  return `
    <section class="chart-wrap yandex-weeks">
      <h3>Yandex: последние 7 дней (раскрытие по кампаниям)</h3>
      <button class="yweek-expand-all-btn">${allOpen ? "Свернуть всё" : "Развернуть всё"}</button>
      <div class="table-scroll"><table><thead><tr>${cols.map((c) => `<th>${escapeHtml(prettyColName(c))}</th>`).join("")}</tr></thead><tbody>${bodyRows.join("")}</tbody></table></div>
    </section>
  `;
}

type TabKey = "assoc_builder" | "media" | "budget" | "months" | "managers" | "funnels" | "contacts" | "year" | "qa" | "utm";
type ViewKey =
  | "assoc_dynamic"
  | "media_email"
  | "media_yandex"
  | "media_yandex_month"
  | "budget_monthly"
  | "months_total"
  | "managers_sales_course"
  | "managers_sales_month"
  | "managers_firstline_course"
  | "managers_firstline_month"
  | "funnels_hierarchy"
  | "contacts_unique"
  | "year_total"
  | "email_ops_summary"
  | "qa_dedup_check"
  | "qa_raw_vs_dedup"
  | "qa_unmatched"
  | "qa_dedup_collisions"
  | "qa_campaign_mapping"
  | "qa_top50_cohort"
  | "qa_share_global"
  | "utm_constructor";

type ViewMeta = { tab: TabKey; label: string; path: string; rowsLabel: string; title: string; kind?: "assoc" | "email" | "generic" };
type PnlMode = "cohort" | "pnl";
type RenderOptions = {
  initialDateFrom?: string;
  initialDateTo?: string;
  initialPnlMode?: PnlMode;
};

const VIEW_META: Record<ViewKey, ViewMeta> = {
  assoc_dynamic: { tab: "assoc_builder", label: "Конструктор", path: "/api/assoc-revenue", rowsLabel: "Групп", title: "Ассоциативная выручка (конструктор)" },
  media_email: { tab: "media", label: "Имейл по месяцам", path: "data/email_hierarchy_by_send.json", rowsLabel: "Строк", title: "Рекламные медиумы" },
  media_yandex: { tab: "media", label: "Yandex по кампаниям (без месяцев)", path: "data/global/yandex_projects_revenue_no_month.json", rowsLabel: "Кампаний", title: "Рекламные медиумы" },
  media_yandex_month: { tab: "media", label: "Yandex по месяцам", path: "data/global/yandex_projects_revenue_by_month.json", rowsLabel: "Месяцев", title: "Рекламные медиумы" },

  email_ops_summary: { tab: "media", label: "Email: база, рассылки, лиды, выручка", path: "data/email_operational_summary.json", rowsLabel: "Периодов", title: "Рекламные медиумы" },
  budget_monthly: { tab: "budget", label: "Выручка / расход / прибыль по месяцам", path: "data/global/budget_monthly.json", rowsLabel: "Периодов", title: "Бюджет" },
  months_total: { tab: "months", label: "Bitrix по месяцам", path: "data/bitrix_month_total_full.json", rowsLabel: "Месяцев", title: "Отчеты по месяцам" },
  managers_sales_course: { tab: "managers", label: "Продажи по коду курса", path: "data/manager_sales_by_course.json", rowsLabel: "Строк", title: "Отчеты по менеджерам" },
  managers_sales_month: { tab: "managers", label: "Продажи по месяцу", path: "data/manager_sales_by_month.json", rowsLabel: "Строк", title: "Отчеты по менеджерам" },
  managers_firstline_course: { tab: "managers", label: "1-я линия по коду курса", path: "data/manager_firstline_by_course.json", rowsLabel: "Строк", title: "Отчеты по менеджерам" },
  managers_firstline_month: { tab: "managers", label: "1-я линия по месяцу", path: "data/manager_firstline_by_month.json", rowsLabel: "Строк", title: "Отчеты по менеджерам" },
  funnels_hierarchy: { tab: "funnels", label: "Воронка → Месяц → Код курса", path: "data/bitrix_funnel_month_code_full.json", rowsLabel: "Строк", title: "Отчеты по воронкам" },
  contacts_unique: { tab: "contacts", label: "Уникальные контакты", path: "data/bitrix_contacts_uid.json", rowsLabel: "Контактов", title: "Уникальные контакты" },
  year_total: { tab: "year", label: "Итоги по годам", path: "data/bitrix_month_total_full.json", rowsLabel: "Лет", title: "Отчет за год" },
  qa_dedup_check: { tab: "qa", label: "Дедупликация: итог", path: "data/qa/dedup_check.json", rowsLabel: "Строк", title: "Контроль качества" },
  qa_raw_vs_dedup: { tab: "qa", label: "Raw vs Dedup: дельта", path: "data/qa/yandex_raw_vs_dedup_delta.json", rowsLabel: "Строк", title: "Контроль качества" },
  qa_unmatched: { tab: "qa", label: "Yandex: несопоставленные кампании", path: "data/qa/yandex_unmatched_to_bitrix.json", rowsLabel: "Строк", title: "Контроль качества" },
  qa_dedup_collisions: { tab: "qa", label: "Ключи дедупликации: коллизии", path: "data/qa/yandex_dedup_keys_top_collisions.json", rowsLabel: "Ключей", title: "Контроль качества" },
  qa_campaign_mapping: { tab: "qa", label: "Маппинг кампаний", path: "data/qa/yandex_campaign_mapping_seed.json", rowsLabel: "Кампаний", title: "Контроль качества" },
  qa_top50_cohort: { tab: "qa", label: "Топ-50 когорт", path: "data/qa/other_top50_cohort.json", rowsLabel: "Строк", title: "Контроль качества" },
  qa_share_global: { tab: "qa", label: "Доля прочих (глобально)", path: "data/qa/other_share_global.json", rowsLabel: "Строк", title: "Контроль качества" },
  utm_constructor: { tab: "utm", label: "UTM Конструктор", path: "/api/utm", rowsLabel: "Тегов", title: "UTM Конструктор" },
};
const ALL_VIEWS = Object.keys(VIEW_META) as ViewKey[];
type MenuMode = "dashboard" | "reports" | "charts" | "utm";

const PNL_PATH_BY_VIEW: Partial<Record<ViewKey, string>> = {
  managers_sales_month: "data/manager_sales_by_month_pnl.json",
  managers_firstline_month: "data/manager_firstline_by_month_pnl.json",
  funnels_hierarchy: "data/bitrix_funnel_month_code_full_pnl.json",
};

function supportsPnlMode(view: ViewKey): boolean {
  return view === "assoc_dynamic" || view === "funnels_hierarchy" || view.startsWith("managers_");
}

function isViewKey(v: string): v is ViewKey {
  return (ALL_VIEWS as string[]).includes(v);
}

function readUrlState(): { menu: MenuMode; view?: ViewKey } {
  const q = new URLSearchParams(window.location.search);
  const m = (q.get("m") || "").trim().toLowerCase();
  const v = (q.get("v") || "").trim();
  const menu: MenuMode = m === "reports" ? "reports" : m === "charts" ? "charts" : m === "utm" ? "utm" : "dashboard";
  return { menu, view: isViewKey(v) ? v : undefined };
}

function writeUrlState(menu: MenuMode, view?: ViewKey): void {
  const url = new URL(window.location.href);
  url.searchParams.set("m", menu);
  if (view) url.searchParams.set("v", view);
  else url.searchParams.delete("v");
  window.history.replaceState({}, "", url.toString());
}

function viewPath(view: ViewKey, options?: { pnlMode?: PnlMode; dateFrom?: string; dateTo?: string }): string {
  if (view === "assoc_dynamic") {
    const params = new URLSearchParams({ dims: "event" });
    if (options?.dateFrom) params.set("from", options.dateFrom);
    if (options?.dateTo) params.set("to", options.dateTo);
    if (options?.pnlMode) params.set("pnlmode", options.pnlMode);
    return `/api/assoc-revenue?${params.toString()}`;
  }
  if (options?.pnlMode === "pnl" && PNL_PATH_BY_VIEW[view]) return PNL_PATH_BY_VIEW[view] as string;
  return VIEW_META[view].path;
}

async function openTableView(view: ViewKey, dealsIndex: DealsIndex): Promise<void> {
  if (view === "utm_constructor") {
    await renderTable(view, utmSessionRows, dealsIndex);
    return;
  }
  let rows: Record<string, unknown>[] = [];
  try {
    rows = await fetchJson<Record<string, unknown>[]>(viewPath(view));
  } catch (e) {
    // Show error inside the layout rather than breaking the whole app.
    app.innerHTML = `<div class="app-layout">
      <aside class="side-menu">
        <button class="side-btn" data-menu="dashboard">Главная</button>
        <button class="side-btn active" data-menu="reports">Детальные отчеты</button>
        <button class="side-btn" data-menu="charts">Графики</button>
        <button class="side-btn" data-menu="utm">UTM Конструктор</button>
      </aside>
      <main class="main-content">
        <div class="err">Ошибка загрузки данных: ${escapeHtml(String(e))}</div>
      </main>
    </div>`;
    app.querySelectorAll<HTMLButtonElement>(".side-btn").forEach((btn) => {
      btn.onclick = async () => {
        const m = btn.getAttribute("data-menu");
        if (m === "dashboard") await openMenu("dashboard", dealsIndex, view);
        else if (m === "reports") await openMenu("reports", dealsIndex, view);
        else if (m === "charts") await openMenu("charts", dealsIndex, view);
        else if (m === "utm") await openMenu("utm", dealsIndex, view);
      };
    });
    return;
  }
  await renderTable(view, rows, dealsIndex);
}

async function openMenu(menu: MenuMode, dealsIndex: DealsIndex, currentView?: ViewKey): Promise<void> {
  if (menu === "dashboard") {
    await renderDashboard(dealsIndex);
    return;
  }
  if (menu === "charts") {
    await renderCharts(dealsIndex);
    return;
  }
  if (menu === "utm") {
    await openTableView("utm_constructor", dealsIndex);
    return;
  }
  await openTableView(currentView && currentView !== "utm_constructor" ? currentView : "year_total", dealsIndex);
}

function managerFormulaNote(_view: ViewKey): string {
  return "";
}


type DealRow = Record<string, unknown>;
type DealsIndex = { month: Map<string, DealRow[]>; event: Map<string, DealRow[]>; course: Map<string, DealRow[]> };
type DealRevenue = { revenue: number; isRevenue: boolean };
const dealRevenueById = new Map<string, DealRevenue>();
type YandexLeadMetrics = { leads: number; qual: number; unqual: number; unknown: number; refusal: number; clicks: number; spend: number };
const yandexProjectLeadMetrics = new Map<string, YandexLeadMetrics>();
const yandexMonthLeadMetrics = new Map<string, YandexLeadMetrics>();
const emailGroupByLookup = new Map<string, string>();
const EMAIL_OTHER_GROUP = "Other";

type EmailOverridesFile = {
  groups?: Record<string, string[]>;
};

function normalizeId(v: unknown): string {
  const s = String(v ?? "").trim();
  return /^\d+\.0+$/.test(s) ? s.slice(0, s.indexOf(".")) : s;
}

function parseFlIds(v: unknown): string[] {
  const s = String(v ?? "").trim();
  if (!s) return [];
  return s
    .split(/[,\s;]+/)
    .map((x) => normalizeId(x))
    .filter(Boolean);
}

function normalizeEmailLookupKey(value: unknown): string {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/ё/g, "е")
    .replace(/[^a-zа-я0-9]+/giu, "");
}

function stripEmailDateLikeSuffix(value: string): string {
  let current = String(value ?? "").trim();
  let prev = "";
  while (current && current !== prev) {
    prev = current;
    current = current.replace(/(?:[_\-\s]+)?\d{1,2}:\d{2}$/u, "").trim();
    current = current.replace(/(?:[_\-\s]+)?\d{8}$/u, "").trim();
    current = current.replace(/(?:[_\-\s]+)?\d{6}$/u, "").trim();
    current = current.replace(/(?:[_\-\s]+)?\d{1,2}[_\-]\d{1,2}(?:[_\-]\d{2,4})?$/u, "").trim();
  }
  return current;
}

function cleanEmailLabel(value: unknown): string {
  let s = String(value ?? "").replace(/\u00a0/g, " ").trim();
  const quoted = s.match(/[«"]([^«»"]+)[»"]/u)?.[1]?.trim();
  if (quoted) s = quoted;
  s = s.replace(/[\s,]+\d{1,2}\.\d{1,2}\.\d{4}.*$/u, "").trim();
  return s;
}

function rememberEmailOverride(alias: string, group: string): void {
  const key = normalizeEmailLookupKey(alias);
  if (!key || !group) return;
  if (!emailGroupByLookup.has(key)) emailGroupByLookup.set(key, group);
}

function loadEmailOverridesMap(raw: EmailOverridesFile): void {
  emailGroupByLookup.clear();
  for (const [group, aliases] of Object.entries(raw.groups || {})) {
    if (!Array.isArray(aliases)) continue;
    for (const aliasRaw of aliases) {
      const alias = cleanEmailLabel(aliasRaw);
      if (!alias) continue;
      rememberEmailOverride(alias, group);
      rememberEmailOverride(stripEmailDateLikeSuffix(alias), group);
    }
  }
}

function resolveEmailGroup(label: unknown): string | null {
  const cleaned = cleanEmailLabel(label);
  if (!cleaned || cleaned === "-" || cleaned.toLowerCase() === "unmatched") return null;
  const exact = emailGroupByLookup.get(normalizeEmailLookupKey(cleaned));
  if (exact) return exact;
  const stripped = stripEmailDateLikeSuffix(cleaned);
  return emailGroupByLookup.get(normalizeEmailLookupKey(stripped)) || null;
}

function combineFlIds(a: unknown, b: unknown): string {
  const ids = new Set<string>([...parseFlIds(a), ...parseFlIds(b)]);
  return [...ids].join(",");
}

function mergeMetricRows(base: Record<string, unknown>, extra: Record<string, unknown>): Record<string, unknown> {
  const out = { ...base };
  for (const [k, v] of Object.entries(extra)) {
    if (["Level", "Месяц", "Название выпуска", "utm_campaign", "Тема", "QualCheck"].includes(k)) continue;
    if (k === "fl_IDs") {
      out[k] = combineFlIds(out[k], v);
      continue;
    }
    const a = num(out[k]);
    const b = num(v);
    if ((a !== 0 || String(out[k] ?? "").trim() === "0") || (b !== 0 || String(v ?? "").trim() === "0")) {
      out[k] = a + b;
    }
  }
  return out;
}

function regroupMediaEmailRows(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  let i = 0;
  while (i < rows.length) {
    const r = rows[i];
    const lvl = String(r["Level"] ?? "").trim();
    if (lvl !== "Month") {
      i += 1;
      continue;
    }

    const month = String(r["Месяц"] ?? "").trim();
    out.push({ ...r });
    i += 1;

    const grouped = new Map<string, Record<string, unknown>>();
    const groupOrder: string[] = [];
    const otherDetails: Record<string, unknown>[] = [];
    let spacerRow: Record<string, unknown> | null = null;

    while (i < rows.length) {
      const s = rows[i];
      const sLvl = String(s["Level"] ?? "").trim();
      const sMonth = String(s["Месяц"] ?? "").trim();
      if (sLvl === "Month" && sMonth !== month) break;
      if (sLvl === "Spacer") {
        spacerRow = { ...s };
        i += 1;
        break;
      }
      if (sLvl !== "Send") {
        i += 1;
        continue;
      }

      const release = String(s["Название выпуска"] ?? "").trim();
      const group = resolveEmailGroup(release) || EMAIL_OTHER_GROUP;
      if (!groupOrder.includes(group)) groupOrder.push(group);

      const template: Record<string, unknown> = {
        ...s,
        "Название выпуска": group,
        utm_campaign: group,
        "Тема": "-",
      };
      if (group === EMAIL_OTHER_GROUP) template["__email_other_group"] = 1;
      const prev = grouped.get(group);
      grouped.set(group, prev ? mergeMetricRows(prev, template) : template);

      if (group === EMAIL_OTHER_GROUP) {
        const detail = {
          ...s,
          Level: "SendOtherDetail",
          "Название выпуска": release || "(без названия)",
          utm_campaign: s.utm_campaign || "-",
          "__email_other_detail": 1,
        };
        otherDetails.push(detail);
      }

      i += 1;
    }

    for (const group of groupOrder) {
      const gRow = grouped.get(group);
      if (!gRow) continue;
      out.push(gRow);
      if (group === EMAIL_OTHER_GROUP) {
        out.push(...otherDetails);
      }
    }

    if (spacerRow) out.push(spacerRow);
  }
  return out;
}

const ASSOC_METRIC_KEYS = [
  "Сделок_всего",
  "Контактов_в_пуле",
  "Сделок_с_выручкой",
  "Контактов_с_выручкой",
  "Выручка",
  "Средний_чек",
] as const;

function regroupAssocEmailRows(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  if (!rows.length || !("Email кампания" in rows[0])) return rows;

  const dims = Object.keys(rows[0]).filter((k) => !ASSOC_METRIC_KEYS.includes(k as (typeof ASSOC_METRIC_KEYS)[number]));
  const dimWithoutEmail = dims.filter((k) => k !== "Email кампания");
  const ctxOrder: string[] = [];
  const rowOrder = new Map<string, string[]>();
  const grouped = new Map<string, Record<string, unknown>>();
  const details = new Map<string, Map<string, Record<string, unknown>>>();

  const ctxKeyOf = (row: Record<string, unknown>): string => dimWithoutEmail.map((k) => String(row[k] ?? "")).join("||");

  const addAssoc = (target: Map<string, Record<string, unknown>>, key: string, seed: Record<string, unknown>, inc: Record<string, unknown>) => {
    const prev = target.get(key);
    if (!prev) {
      const next = { ...seed };
      for (const mk of ASSOC_METRIC_KEYS) next[mk] = num(inc[mk]);
      target.set(key, next);
      return;
    }
    for (const mk of ASSOC_METRIC_KEYS) prev[mk] = num(prev[mk]) + num(inc[mk]);
  };

  for (const row of rows) {
    const rawEmail = String(row["Email кампания"] ?? "").trim();
    const ctxKey = ctxKeyOf(row);
    if (!ctxOrder.includes(ctxKey)) ctxOrder.push(ctxKey);

    const group = resolveEmailGroup(rawEmail) || EMAIL_OTHER_GROUP;
    const groupedKey = `${ctxKey}|||${group}`;
    if (!rowOrder.has(ctxKey)) rowOrder.set(ctxKey, []);
    const labels = rowOrder.get(ctxKey)!;
    if (!labels.includes(group)) labels.push(group);

    const groupedSeed: Record<string, unknown> = { ...row, "Email кампания": group };
    if (group === EMAIL_OTHER_GROUP) groupedSeed["__assoc_email_other_group"] = 1;
    addAssoc(grouped, groupedKey, groupedSeed, row);

    if (group === EMAIL_OTHER_GROUP) {
      if (!details.has(ctxKey)) details.set(ctxKey, new Map());
      const detailMap = details.get(ctxKey)!;
      const detailLabel = rawEmail || "(без названия)";
      const detailSeed = {
        ...row,
        "Email кампания": detailLabel,
        "__assoc_email_detail": 1,
        "__assoc_email_ctx": ctxKey,
      };
      addAssoc(detailMap, detailLabel, detailSeed, row);
    }
  }

  const out: Record<string, unknown>[] = [];
  for (const ctxKey of ctxOrder) {
    const labels = rowOrder.get(ctxKey) || [];
    for (const label of labels) {
      const key = `${ctxKey}|||${label}`;
      const row = grouped.get(key);
      if (!row) continue;
      if (label === EMAIL_OTHER_GROUP) row["__assoc_email_ctx"] = ctxKey;
      if (label === EMAIL_OTHER_GROUP && (details.get(ctxKey)?.size || 0) > 0) row["__assoc_email_has_details"] = 1;
      const deals = num(row["Сделок_с_выручкой"]);
      row["Средний_чек"] = deals > 0 ? num(row["Выручка"]) / deals : 0;
      out.push(row);
      if (label === EMAIL_OTHER_GROUP) {
        const detailRows = [...(details.get(ctxKey)?.values() || [])];
        detailRows.sort((a, b) => String(a["Email кампания"] ?? "").localeCompare(String(b["Email кампания"] ?? ""), "ru"));
        for (const d of detailRows) {
          const dDeals = num(d["Сделок_с_выручкой"]);
          d["Средний_чек"] = dDeals > 0 ? num(d["Выручка"]) / dDeals : 0;
          out.push(d);
        }
      }
    }
  }
  return out;
}

function yandexEmptyMetrics(): YandexLeadMetrics {
  return { leads: 0, qual: 0, unqual: 0, unknown: 0, refusal: 0, clicks: 0, spend: 0 };
}

function addYandexMetrics(a: YandexLeadMetrics, b: YandexLeadMetrics): YandexLeadMetrics {
  return {
    leads: a.leads + b.leads,
    qual: a.qual + b.qual,
    unqual: a.unqual + b.unqual,
    unknown: a.unknown + b.unknown,
    refusal: a.refusal + b.refusal,
    clicks: a.clicks + b.clicks,
    spend: a.spend + b.spend,
  };
}

function buildMediaYandexProjectRow(project: string, raw: Record<string, unknown>): Record<string, unknown> {
  const m = yandexProjectLeadMetrics.get(project) || yandexEmptyMetrics();
  const leads = m.leads > 0 ? m.leads : num(raw["leads_raw"]);
  const qual = m.qual > 0 ? m.qual : num(raw["qual"]);
  const unqual = m.unqual > 0 ? m.unqual : num(raw["unqual"]);
  const unknown = m.unknown > 0 ? m.unknown : num(raw["unknown"]);
  const refusal = m.refusal > 0 ? m.refusal : num(raw["refusal"]);
  const paid = num(raw["payments_count"] ?? raw["paid_deals_raw"]);
  const revenue = num(raw["revenue_raw"]);
  const spend = m.spend > 0 ? m.spend : num(raw["spend"]);
  const clicks = m.clicks > 0 ? m.clicks : num(raw["clicks"]);
  const assocRevenue = Math.max(num(raw["assoc_revenue"]), revenue);
  return {
    "Yandex кампания": project,
    "Yandex объявление": "-",
    "Заголовок": "-",
    "Первый месяц": String(raw["first_month"] ?? "").trim(),
    "Последний месяц": String(raw["last_month"] ?? "").trim(),
    "Лиды": leads,
    "Квал": qual,
    "Конверсия в Квал": leads > 0 ? qual / leads : 0,
    "Неквал": unqual,
    "Конверсия в Неквал": leads > 0 ? unqual / leads : 0,
    "Неизвестно": unknown,
    "Отказы": refusal,
    "Конверсия в Отказ": leads > 0 ? refusal / leads : 0,
    "Клики": clicks,
    "Расход, ₽": spend,
    "Оплаты": paid,
    "Конверсия в Оплаты": leads > 0 ? paid / leads : 0,
    "Выручка": revenue,
    "Прибыль": revenue - spend,
    "Ассоц. Выручка": assocRevenue,
    "Ассоц. Прибыль": assocRevenue - spend,
  };
}

function buildMediaYandexAdRow(project: string, raw: Record<string, unknown>): Record<string, unknown> {
  const adId = String(raw["ad_id"] ?? "").trim();
  const adTitle = String(raw["ad_title"] ?? "").trim();
  const leads = num(raw["leads_raw"]);
  const qual = num(raw["qual"]);
  const unqual = num(raw["unqual"]);
  const unknown = num(raw["unknown"]);
  const refusal = num(raw["refusal"]);
  const paid = num(raw["payments_count"] ?? raw["paid_deals_raw"]);
  const revenue = num(raw["revenue_raw"]);
  const spend = num(raw["spend"]);
  const assocRevenue = Math.max(num(raw["assoc_revenue"]), revenue);
  return {
    "Yandex кампания": project,
    "Yandex объявление": adId,
    "Заголовок": adTitle,
    "Первый месяц": String(raw["first_month"] ?? "").trim(),
    "Последний месяц": String(raw["last_month"] ?? "").trim(),
    "Лиды": leads,
    "Квал": qual,
    "Конверсия в Квал": leads > 0 ? qual / leads : 0,
    "Неквал": unqual,
    "Конверсия в Неквал": leads > 0 ? unqual / leads : 0,
    "Неизвестно": unknown,
    "Отказы": refusal,
    "Конверсия в Отказ": leads > 0 ? refusal / leads : 0,
    "Клики": num(raw["clicks"]),
    "Расход, ₽": spend,
    "Оплаты": paid,
    "Конверсия в Оплаты": leads > 0 ? paid / leads : 0,
    "Выручка": revenue,
    "Прибыль": revenue - spend,
    "Ассоц. Выручка": assocRevenue,
    "Ассоц. Прибыль": assocRevenue - spend,
    "__yandex_project_ctx": project,
    "__yandex_project_detail": 1,
  };
}

function toYandexMetrics(row: Record<string, unknown>): YandexLeadMetrics {
  return {
    leads: num(row["Leads"]),
    qual: num(row["Qual"]),
    unqual: num(row["Unqual"]),
    unknown: num(row["Unknown"]),
    refusal: num(row["Refusal"]),
    clicks: num(row["Клики"]),
    spend: num(row["Расход, ₽"]),
  };
}

function addKpi(row: Record<string, unknown>): Record<string, unknown> {
  const leads = num(row["Лиды"]);
  const qual = num(row["Квал"]);
  const unqual = num(row["Неквал"]);
  const unknown = num(row["Неизвестно"]);
  const refusal = num(row["Отказы"]);
  const inWork = num(row["В работе"]);
  const deals = num(row["Сделок_с_выручкой"]);
  const revenue = num(row["Выручка"]);
  return {
    ...row,
    "Конверсия в Квал": leads > 0 ? qual / leads : 0,
    "Конверсия в Неквал": leads > 0 ? unqual / leads : 0,
    "Конверсия в Отказ": leads > 0 ? refusal / leads : 0,
    "Конверсия в работе": leads > 0 ? inWork / leads : 0,
    "Средний_чек": deals > 0 ? revenue / deals : 0,
  };
}

function pickNum(row: Record<string, unknown>, keys: string[]): number {
  for (const k of keys) {
    if (k in row) return num(row[k]);
  }
  return 0;
}

function rowKey(_view: ViewKey, row: Record<string, unknown>): string {
  return [String(row["Level"] ?? ""), String(row["Месяц"] ?? ""), String(row["Название выпуска"] ?? ""), String(row["utm_campaign"] ?? "")].join("::");
}

function budgetPayMonthKey(row: Record<string, unknown>): string {
  return String(row["__pay_month"] ?? row["month"] ?? row["Период"] ?? "").trim();
}

function dealsForRow(view: ViewKey, row: Record<string, unknown>, dealsIndex: DealsIndex): DealRow[] {
  void view;
  void row;
  void dealsIndex;
  return [];
}

function renderDealsTable(deals: DealRow[]): string {
  if (!deals.length) return `<div class="dev-empty">No deals found for this row.</div>`;
  const cols = Object.keys(deals[0] as object);
  return `<div class="dev-table-wrap"><div class="dev-meta">${deals.length} deals</div><div class="table-scroll dev-scroll"><table><thead><tr>${cols.map((c) => `<th>${escapeHtml(prettyColName(c))}</th>`).join("")}</tr></thead><tbody>${deals.map((r) => `<tr>${cols.map((c) => `<td>${escapeHtml(formatCell(c, r[c]))}</td>`).join("")}</tr>`).join("")}</tbody></table></div></div>`;
}

function toViewRows(view: ViewKey, rows: Record<string, unknown>[]): Record<string, unknown>[] {
  const clean = rows.map((r) => {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(r)) {
      const lk = k.trim().toLowerCase();
      if (!k.startsWith("Unnamed:") && !lk.includes("остаток")) out[k] = v;
    }
    if (!("Неизвестно" in out)) {
      if ("Unknown" in out) out["Неизвестно"] = out["Unknown"];
      else if ("unknown" in out) out["Неизвестно"] = out["unknown"];
    }
    return out;
  });
  if (view === "months_total") return clean.map(addKpi);
  if (view === "budget_monthly") {
    return clean.map((r) => {
      const lvl = String(r["Level"] ?? "").trim();
      const isDetail = lvl === "Detail";
      const out: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(r)) {
        // Detail rows don't carry spend/profit; keep month-level spend/profit intact.
        if ((k === "Расход, ₽" || k === "Прибыль") && isDetail) continue;
        out[k] = v;
      }
      return out;
    });
  }
  if (view === "year_total") {
    const groups = new Map<string, Record<string, unknown>[]>();
    for (const r of clean) {
      const month = String(r["Месяц"] ?? "").trim();
      const year = /^\d{4}/.test(month) ? month.slice(0, 4) : "Невалидная дата";
      if (!groups.has(year)) groups.set(year, []);
      groups.get(year)!.push(r);
    }
    const out: Record<string, unknown>[] = [];
    for (const [year, rowsInYear] of groups.entries()) {
      const leads = rowsInYear.reduce((a, x) => a + num(x["Лиды"]), 0);
      const qual = rowsInYear.reduce((a, x) => a + num(x["Квал"]), 0);
      const unqual = rowsInYear.reduce((a, x) => a + num(x["Неквал"]), 0);
      const unknown = rowsInYear.reduce((a, x) => a + num(x["Неизвестно"]), 0);
      const refusal = rowsInYear.reduce((a, x) => a + num(x["Отказы"]), 0);
      const inWork = rowsInYear.reduce((a, x) => a + num(x["В работе"]), 0);
      const deals = rowsInYear.reduce((a, x) => a + num(x["Сделок_с_выручкой"]), 0);
      const revenue = rowsInYear.reduce((a, x) => a + num(x["Выручка"]), 0);
      const avgCheck = deals > 0 ? revenue / deals : 0;

      const acc: Record<string, unknown> = {
        "Год": year,
        "Лиды": leads,
        "Квал": qual,
        "Конверсия в Квал": leads > 0 ? qual / leads : 0,
        "Неквал": unqual,
        "Конверсия в Неквал": leads > 0 ? unqual / leads : 0,
        "Неизвестно": unknown,
        "Отказы": refusal,
        // User rule: refusal conversion is based on qualified leads.
        "Конверсия в Отказ": qual > 0 ? refusal / qual : 0,
        "В работе": inWork,
        "Конверсия в работе": leads > 0 ? inWork / leads : 0,
        "Сделок_с_выручкой": deals,
        "Конверсия в Сделки": leads > 0 ? deals / leads : 0,
        "Выручка": revenue,
        "Средний_чек": avgCheck,
      };
      out.push(acc);
    }
    out.sort((a, b) => String(a["Год"] ?? "").localeCompare(String(b["Год"] ?? "")));
    return out;
  }
  if (view === "media_email") {
    const grouped = regroupMediaEmailRows(clean);
    return grouped.map((r) => {
      const out = { ...r };
      delete out["Лиды с некорр. email"];
      delete out["Лидов с некорр. email"];
      delete out["Доля некорр. email (лиды)"];
      return out;
    });
  }
  if (view === "assoc_dynamic") {
    return regroupAssocEmailRows(clean);
  }
  if (view === "media_yandex") {
    const hasHierarchyRows = clean.some((r) => String(r["Level"] ?? "").trim() === "Project" || num(r["__yandex_project_detail"]) > 0);
    if (hasHierarchyRows) {
      const out: Record<string, unknown>[] = [];
      const parentProjects = new Set<string>();
      for (const r of clean) {
        const level = String(r["Level"] ?? "").trim();
        const project = String(r["project_name"] ?? r["Yandex кампания"] ?? r["Проект"] ?? "").trim();
        if (!project) continue;
        if (level === "Project") {
          parentProjects.add(project);
          out.push({
            ...buildMediaYandexProjectRow(project, r),
            Level: "Project",
            __yandex_project_ctx: project,
            __yandex_project_has_details: num(r["__yandex_project_has_details"]) > 0 ? 1 : 0,
          });
          continue;
        }
        if (num(r["__yandex_project_detail"]) > 0 || level === "Ad") {
          out.push(buildMediaYandexAdRow(project, r));
        }
      }

      for (const [project, m] of yandexProjectLeadMetrics.entries()) {
        if (!project || parentProjects.has(project)) continue;
        if (m.spend <= 0 && m.clicks <= 0 && m.leads <= 0) continue;
        out.push({
          ...buildMediaYandexProjectRow(project, { spend: m.spend }),
          Level: "Project",
          __yandex_project_ctx: project,
          __yandex_project_has_details: 0,
        });
      }
      return out;
    }

    const rowsByProject = new Map<string, Record<string, unknown>>();
    for (const r of clean) {
      const project = mapYandexProjectGroup(r["project_name"]);
      if (!project) continue;
      rowsByProject.set(project, buildMediaYandexProjectRow(project, r));
    }

    for (const [project, m] of yandexProjectLeadMetrics.entries()) {
      if (!project || rowsByProject.has(project)) continue;
      if (m.spend <= 0 && m.clicks <= 0 && m.leads <= 0) continue;
      rowsByProject.set(project, buildMediaYandexProjectRow(project, { spend: m.spend }));
    }

    return Array.from(rowsByProject.values());
  }
  if (view === "media_yandex_month") {
    return clean.map((r) => {
      const month = String(r["month"] ?? "").trim();
      const m = yandexMonthLeadMetrics.get(month) || yandexEmptyMetrics();
      const leads = m.leads > 0 ? m.leads : num(r["leads_raw"]);
      const qual = m.qual;
      const unqual = m.unqual;
      const unknown = m.unknown;
      const refusal = m.refusal;
      const paid = num(r["paid_deals_raw"]);
      const revenue = num(r["revenue_raw"]);
      const spend = m.spend > 0 ? m.spend : num(r["spend"]);
      const profit = revenue - spend;
      return {
        "Период": month,
        "Лиды": leads,
        "Квал": qual,
        "Конверсия в Квал": leads > 0 ? qual / leads : 0,
        "Неквал": unqual,
        "Конверсия в Неквал": leads > 0 ? unqual / leads : 0,
        "Неизвестно": unknown,
        "Отказы": refusal,
        "Конверсия в Отказ": leads > 0 ? refusal / leads : 0,
        "Клики": m.clicks,
        "Расход, ₽": spend,
        "Оплаты": paid,
        "Конверсия в Оплаты": leads > 0 ? paid / leads : 0,
        "Выручка": revenue,
        "Прибыль": profit,
      };
    });
  }
  if (view === "funnels_hierarchy") {
    const out: Record<string, unknown>[] = [];
    const funnelGroups = new Map<string, Record<string, unknown>[]>();
    for (const r of clean) {
      const k = String(r["Воронка"] ?? "").trim();
      if (!funnelGroups.has(k)) funnelGroups.set(k, []);
      funnelGroups.get(k)!.push(r);
    }
    for (const [funnel, funnelItems] of funnelGroups.entries()) {
      let funnelRow: Record<string, unknown> = {
        Level: "Воронка",
        "Воронка": funnel,
        "Месяц": "-",
        "Код_курса_норм": "-",
        "__node": "funnel",
        "__funnel": funnel,
      };
      funnelRow["Лиды"] = funnelItems.reduce((acc, x) => acc + num(x["Лиды"]), 0);
      funnelRow["Квал"] = funnelItems.reduce((acc, x) => acc + num(x["Квал"]), 0);
      funnelRow["Неквал"] = funnelItems.reduce((acc, x) => acc + num(x["Неквал"]), 0);
      funnelRow["Неизвестно"] = funnelItems.reduce((acc, x) => acc + num(x["Неизвестно"]), 0);
      funnelRow["Отказы"] = funnelItems.reduce((acc, x) => acc + num(x["Отказы"]), 0);
      funnelRow["В работе"] = funnelItems.reduce((acc, x) => acc + num(x["В работе"]), 0);
      funnelRow["Невалидные_лиды"] = funnelItems.reduce((acc, x) => acc + num(x["Невалидные_лиды"]), 0);
      funnelRow["Сделок_с_выручкой"] = funnelItems.reduce((acc, x) => acc + num(x["Сделок_с_выручкой"]), 0);
      funnelRow["Выручка"] = funnelItems.reduce((acc, x) => acc + num(x["Выручка"]), 0);
      funnelRow = addKpi(funnelRow);
      out.push(funnelRow);

      const monthGroups = new Map<string, Record<string, unknown>[]>();
      for (const r of funnelItems) {
        const m = String(r["Месяц"] ?? "").trim();
        if (!monthGroups.has(m)) monthGroups.set(m, []);
        monthGroups.get(m)!.push(r);
      }
      for (const [month, monthItems] of monthGroups.entries()) {
        let monthRow: Record<string, unknown> = {
          Level: "Месяц",
          "Воронка": funnel,
          "Месяц": month,
          "Код_курса_норм": "-",
          "__node": "month",
          "__funnel": funnel,
          "__month": month,
        };
        monthRow["Лиды"] = monthItems.reduce((acc, x) => acc + num(x["Лиды"]), 0);
        monthRow["Квал"] = monthItems.reduce((acc, x) => acc + num(x["Квал"]), 0);
        monthRow["Неквал"] = monthItems.reduce((acc, x) => acc + num(x["Неквал"]), 0);
        monthRow["Неизвестно"] = monthItems.reduce((acc, x) => acc + num(x["Неизвестно"]), 0);
        monthRow["Отказы"] = monthItems.reduce((acc, x) => acc + num(x["Отказы"]), 0);
        monthRow["В работе"] = monthItems.reduce((acc, x) => acc + num(x["В работе"]), 0);
        monthRow["Невалидные_лиды"] = monthItems.reduce((acc, x) => acc + num(x["Невалидные_лиды"]), 0);
        monthRow["Сделок_с_выручкой"] = monthItems.reduce((acc, x) => acc + num(x["Сделок_с_выручкой"]), 0);
        monthRow["Выручка"] = monthItems.reduce((acc, x) => acc + num(x["Выручка"]), 0);
        monthRow = addKpi(monthRow);
        out.push(monthRow);
        for (const r of monthItems) {
          out.push({
            ...r,
            Level: "Код курса",
            "__node": "code",
            "__funnel": funnel,
            "__month": month,
          });
        }
      }
    }
    return out;
  }
  return clean;
}

function isFlIdsColumn(col: string): boolean {
  return col.trim().toLowerCase() === "fl_ids";
}

function isHiddenUiColumn(col: string): boolean {
  return col.startsWith("__");
}

async function renderTable(view: ViewKey, rows: Record<string, unknown>[], dealsIndex: DealsIndex, options: RenderOptions = {}): Promise<void> {
  const isUtmConstructor = view === "utm_constructor";
  const _now = new Date();
  const _toY = _now.getFullYear();
  const _toM = String(_now.getMonth() + 1).padStart(2, "0");
  const _fromD = new Date(_now); _fromD.setMonth(_fromD.getMonth() - 11);
  let dateFrom = options.initialDateFrom ?? `${_fromD.getFullYear()}-${String(_fromD.getMonth() + 1).padStart(2, "0")}`;
  let dateTo = options.initialDateTo ?? `${_toY}-${_toM}`;
  let pnlMode: PnlMode = options.initialPnlMode ?? "cohort";

  writeUrlState(isUtmConstructor ? "utm" : "reports", view);
  const meta = VIEW_META[view];
  const tab = meta.tab;
  const tabViews = (Object.keys(VIEW_META) as ViewKey[]).filter((k) => VIEW_META[k].tab === tab && VIEW_META[k].tab !== "utm");
  const resolvedPath = viewPath(view, { pnlMode, dateFrom, dateTo });
  const aliasMetaRow = rows.length > 0 && String(rows[0]["__type"] ?? "") === "column_aliases" ? rows[0] : undefined;
  if (aliasMetaRow) {
    const viewAliases: Record<string, string> = {};
    for (const [k, v] of Object.entries(aliasMetaRow)) {
      if (k !== "__type" && typeof v === "string") viewAliases[k] = v;
    }
    columnAliasesByView.set(view, viewAliases);
  } else {
    columnAliasesByView.set(view, {});
  }
  const dataRows = aliasMetaRow ? rows.slice(1) : rows;
  const viewRows = toViewRows(view, dataRows);
  let mediaYandexMonthlyRows: Record<string, unknown>[] = [];
  let mediaYandexCampaignMonthRows: Record<string, unknown>[] = [];
  let managerCourseMonthRows: Record<string, unknown>[] = [];
  if (view === "media_yandex") {
    try {
      mediaYandexMonthlyRows = await fetchJson<Record<string, unknown>[]>("data/global/yandex_projects_revenue_raw_vs_dedup.json");
      const yd = await fetchJson<Record<string, unknown>[]>("data/yd_hierarchy.json");
      mediaYandexCampaignMonthRows = yd.filter((r) => String(r["Level"] ?? "").trim() === "Campaign");
    } catch {
      mediaYandexMonthlyRows = [];
      mediaYandexCampaignMonthRows = [];
    }
  }
  if (view === "managers_sales_course") {
    try {
      managerCourseMonthRows = await fetchJson<Record<string, unknown>[]>(
        pnlMode === "pnl" ? "data/manager_sales_by_course_month_pnl.json" : "data/manager_sales_by_course_month.json",
      );
    } catch {
      managerCourseMonthRows = [];
    }
  }
  if (view === "managers_firstline_course") {
    try {
      managerCourseMonthRows = await fetchJson<Record<string, unknown>[]>(
        pnlMode === "pnl" ? "data/manager_firstline_by_course_month_pnl.json" : "data/manager_firstline_by_course_month.json",
      );
    } catch {
      managerCourseMonthRows = [];
    }
  }
  if (view === "assoc_dynamic") {
    // Pre-populate so the column always appears in allCols even if the fetch fails
    for (const r of viewRows) {
      if (num(r["__assoc_event_detail"]) === 0 && String(r["Мероприятие"] ?? "").trim()) {
        r["Новых с мероприятия"] = 0;
      }
    }
    try {
      const ecRows = await fetchJson<Record<string, unknown>[]>("data/bitrix_new_event_contacts_by_event.json");
      const ecMap = new Map<string, number>();
      for (const r of ecRows) {
        const ev = String(r["Мероприятие"] ?? "").trim();
        const n = num(r["Новых с мероприятия"]);
        if (ev) ecMap.set(ev, n);
      }
      for (const r of viewRows) {
        if (num(r["__assoc_event_detail"]) === 0) {
          const ev = String(r["Мероприятие"] ?? "").trim();
          if (ev) r["Новых с мероприятия"] = ecMap.get(ev) ?? 0;
        }
      }
    } catch { /* ignore — pre-populated 0 remains */ }
  }
  const allCols = viewRows.length ? Object.keys(viewRows[0]) : [];
  let cols = allCols;
  const initialDateCol = ["Период", "Месяц", "Год"].find((c) => cols.includes(c));
  let sortCol = initialDateCol || cols[0] || "";
  let sortDir: "asc" | "desc" = initialDateCol ? "desc" : "asc";
  let filter = "";
  let contactsFullOnly = false;
  const expanded = new Set<string>();
  const expandedEmailMonths = new Set<string>();
  const expandedManagers = new Set<string>();
  const expandedManagerMonths = new Set<string>();
  const expandedManagerCodes = new Set<string>();
  const expandedYandexMonths = new Set<string>();
  const expandedYandexCampaigns = new Set<string>();
  const expandedFunnels = new Set<string>();
  const expandedFunnelMonths = new Set<string>();
  const expandedAssocOtherRows = new Set<string>();
  const expandedAssocEventRows = new Set<string>();
  const expandedAssocYandexRows = new Set<string>();
  const expandedYandexProjectRows = new Set<string>();
  const expandedBudget = new Set<string>();
  const assocEvents: string[] = [];
  let assocEventTab: string | null = null;
  if (view === "assoc_dynamic") {
    const uniqueEvents = [...new Set(
      viewRows
        .filter((r) => num(r["__assoc_event_detail"]) === 0)
        .map((r) => String(r["Мероприятие"] ?? "").trim())
        .filter((ev) => ev !== "" && ev !== "Другое"),
    )];
    assocEvents.push(...uniqueEvents);
    assocEventTab = assocEvents[0] ?? null;
  }
  const isEmailHierarchy = view === "media_email";
  const isAssocEmailHierarchy = view === "assoc_dynamic" && viewRows.some((r) => num(r["__assoc_email_detail"]) > 0);
  const isAssocEventHierarchy = view === "assoc_dynamic" && viewRows.some((r) => num(r["__assoc_event_detail"]) > 0);
  const isAssocYandexHierarchy = view === "assoc_dynamic" && viewRows.some((r) => num(r["__assoc_yandex_detail"]) > 0);
  const isYandexHierarchy = false;
  const isYandexProjectHierarchy = view === "media_yandex" && viewRows.some((r) => num(r["__yandex_project_detail"]) > 0);
  const isManagerHierarchy = view.startsWith("managers_");
  const isManagerCourseView = isManagerHierarchy && view.endsWith("_course");
  const isFunnelHierarchy = view === "funnels_hierarchy";
  const isBudgetHierarchy = view === "budget_monthly";
  const dateWindowCol = isManagerCourseView
    ? "month"
    : isManagerHierarchy
    ? (allCols.includes("month") ? "month" : "")
    : (["month", "\u041f\u0435\u0440\u0438\u043e\u0434", "\u041c\u0435\u0441\u044f\u0446", "\u0413\u043e\u0434"].find((c) => allCols.includes(c)) || "");
  const isAssocDynamic = view === "assoc_dynamic";
  const hasDateWindowControl = !!dateWindowCol || isAssocDynamic;
  const hasPnlToggle = supportsPnlMode(view);
  const canSaveViewJson = window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1";
  let visibleRows: Record<string, unknown>[] = [];
  const postJson = async (url: string, body: unknown): Promise<{ ok: boolean; rows?: number; error?: string }> => {
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      const text = await resp.text();
      let payload: { ok?: boolean; rows?: number; error?: string };
      try {
        payload = JSON.parse(text) as { ok?: boolean; rows?: number; error?: string };
      } catch {
        const compact = text.replace(/\s+/g, " ").trim();
        return {
          ok: false,
          error: compact.startsWith("<!DOCTYPE")
            ? "Локальное сохранение недоступно в этом окружении"
            : compact.slice(0, 200) || `${url} failed`,
        };
      }
      if (!resp.ok || !payload.ok) return { ok: false, error: payload.error || `${url} failed` };
      return { ok: true, rows: payload.rows };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  };
  const saveViewJson = async (): Promise<void> => {
    if (!canSaveViewJson) return;
    const aliases = columnAliasesByView.get(view) || {};
    const rowsToSave: Record<string, unknown>[] = Object.keys(aliases).length > 0
      ? [{ __type: "column_aliases", ...aliases }, ...viewRows]
      : [...viewRows];
    const local = await postJson("/api/save-view-json", { path: resolvedPath, rows: rowsToSave });
    const s = app.querySelector<HTMLDivElement>(".push-status");
    if (local.ok) {
      if (s) s.textContent = `JSON обновлен локально (${(local.rows ?? viewRows.length).toLocaleString("ru-RU")} строк)`;
      return;
    }
    if (s) s.textContent = `Ошибка сохранения JSON: ${local.error || "unknown"}`;
  };

  const draw = (): void => {
    cols = allCols.filter((c) => !isFlIdsColumn(c) && !isHiddenUiColumn(c));
    if (isBudgetHierarchy) cols = cols.filter((c) => c !== "month" && c !== "Level" && c !== "Расход, ₽" && c !== "Прибыль");
    if (sortCol && !cols.includes(sortCol)) sortCol = cols[0] || "";

    let data = [...viewRows];
    const effectiveDateCol = (pnlMode === "pnl" && allCols.includes("__pay_month")) ? "__pay_month" : dateWindowCol;
    if (hasDateWindowControl && effectiveDateCol && !isManagerCourseView) {
      data = filterRowsByDateRange(data, effectiveDateCol, dateFrom, dateTo);
    }

    if (view === "media_yandex" && hasDateWindowControl) {
      const monthly = filterRowsByDateRange(mediaYandexMonthlyRows, "month", dateFrom, dateTo);
      const mByProject = new Map<string, { leads: number; paid: number; revenue: number; spend: number }>();
      const assocRevenueByProject = new Map<string, number>();
      for (const r of viewRows) {
        const p = String(r["Yandex кампания"] ?? r["project_name"] ?? "").trim();
        if (!p) continue;
        const prev = assocRevenueByProject.get(p) || 0;
        assocRevenueByProject.set(p, prev + num(r["Ассоц. Выручка"] ?? r["assoc_revenue"]));
      }
      for (const r of monthly) {
        const p = mapYandexProjectGroup(r["project_name"]);
        if (!p) continue;
        const prev = mByProject.get(p) || { leads: 0, paid: 0, revenue: 0, spend: 0 };
        prev.leads += num(r["leads_raw"]);
        prev.paid += num(r["paid_deals_raw"]);
        prev.revenue += num(r["revenue_raw"]);
        prev.spend += num(r["spend"]);
        mByProject.set(p, prev);
      }

      const ymByProject = new Map<string, YandexLeadMetrics>();
      const campaignFiltered = filterRowsByDateRange(mediaYandexCampaignMonthRows, "month", dateFrom, dateTo);
      for (const r of campaignFiltered) {
        const p = mapYandexProjectGroup(r["Название кампании"]);
        if (!p) continue;
        const prev = ymByProject.get(p) || yandexEmptyMetrics();
        ymByProject.set(p, addYandexMetrics(prev, toYandexMetrics(r)));
      }

      const allProjects = new Set<string>([...mByProject.keys(), ...ymByProject.keys()]);
      data = [...allProjects.values()].map((project) => {
        const v = mByProject.get(project) || { leads: 0, paid: 0, revenue: 0, spend: 0 };
        const ym = ymByProject.get(project) || yandexEmptyMetrics();
        const leads = ym.leads > 0 ? ym.leads : v.leads;
        const qual = ym.qual;
        const unqual = ym.unqual;
        const refusal = ym.refusal;
        const clicks = ym.clicks;
        const assocRevenue = Math.max(v.revenue, assocRevenueByProject.get(project) || 0);
        return {
          "Yandex кампания": project,
          "Yandex объявление": "-",
          "Заголовок": "-",
          "Лиды": leads,
          "Квал": qual,
          "Конверсия в Квал": leads > 0 ? qual / leads : 0,
          "Неквал": unqual,
          "Конверсия в Неквал": leads > 0 ? unqual / leads : 0,
          "Отказы": refusal,
          "Конверсия в Отказ": leads > 0 ? refusal / leads : 0,
          "Клики": clicks,
          "Расход, ₽": v.spend,
          "Оплаты": v.paid,
          "Конверсия в Оплаты": leads > 0 ? v.paid / leads : 0,
          "Выручка": v.revenue,
          "Прибыль": v.revenue - v.spend,
          "Ассоц. Выручка": assocRevenue,
          "Ассоц. Прибыль": assocRevenue - v.spend,
        };
      });
    }

    if (isManagerHierarchy && hasDateWindowControl && view.endsWith("_month")) {
      const months = filterRowsByDateRange(
        viewRows.filter((r) => String(r["Level"] ?? "") === "Месяц"),
        "month",
        dateFrom,
        dateTo,
      );
      const mgr = new Map<string, Record<string, unknown>>();
      const monthsByManager = new Map<string, Record<string, unknown>[]>();
      for (const r of months) {
        const m = String(r["Менеджер"] ?? "").trim() || "Unassigned";
        if (!monthsByManager.has(m)) monthsByManager.set(m, []);
        monthsByManager.get(m)!.push(r);
        if (!mgr.has(m)) {
          mgr.set(m, {
            "Level": "Manager",
            "Менеджер": m,
            "Месяц": "-",
            "Лиды": 0,
            "Квал": 0,
            "Неквал": 0,
            "Отказы": 0,
            "В работе": 0,
            "Невалидные_лиды": 0,
            "Сделок_с_выручкой": 0,
            "Выручка": 0,
            "fl_IDs": "",
            "_sort_month": "-",
          });
        }
        const acc = mgr.get(m)!;
        for (const k of ["Лиды", "Квал", "Неквал", "Отказы", "В работе", "Невалидные_лиды", "Сделок_с_выручкой", "Выручка"]) {
          acc[k] = num(acc[k]) + num(r[k]);
        }
      }
      const managers = [...mgr.values()].map((r) => ({
        ...r,
        "Конверсия в Квал": num(r["Лиды"]) > 0 ? num(r["Квал"]) / num(r["Лиды"]) : 0,
        "Конверсия в Неквал": num(r["Лиды"]) > 0 ? num(r["Неквал"]) / num(r["Лиды"]) : 0,
        "Конверсия в Отказ": num(r["Лиды"]) > 0 ? num(r["Отказы"]) / num(r["Лиды"]) : 0,
        "Конверсия в работе": num(r["Лиды"]) > 0 ? num(r["В работе"]) / num(r["Лиды"]) : 0,
        "Средний_чек": num(r["Сделок_с_выручкой"]) > 0 ? num(r["Выручка"]) / num(r["Сделок_с_выручкой"]) : 0,
      }));
      const interleaved: Record<string, unknown>[] = [];
      for (const mRow of managers) {
        const manager = String((mRow as Record<string, unknown>)["Менеджер"] ?? "").trim() || "Unassigned";
        interleaved.push(mRow);
        interleaved.push(...(monthsByManager.get(manager) || []));
      }
      data = interleaved;
    }

    if (isManagerCourseView && hasDateWindowControl) {
      const monthRows = filterRowsByDateRange(managerCourseMonthRows, "month", dateFrom, dateTo);
      const byManager = new Map<string, Map<string, Record<string, unknown>[]>>();
      for (const r of monthRows) {
        const manager = String(r["Менеджер"] ?? "").trim() || "Unassigned";
        const code = String(r["Код курса"] ?? "").trim() || "—";
        if (!byManager.has(manager)) byManager.set(manager, new Map<string, Record<string, unknown>[]>());
        const byCode = byManager.get(manager)!;
        if (!byCode.has(code)) byCode.set(code, []);
        byCode.get(code)!.push(r);
      }
      const metricKeys = ["Лиды", "Квал", "Неквал", "Отказы", "В работе", "Невалидные_лиды", "Сделок_с_выручкой", "Выручка"];
      const rebuilt: Record<string, unknown>[] = [];
      for (const [manager, byCode] of byManager.entries()) {
        const managerItems = [...byCode.values()].flat();
        const managerRow: Record<string, unknown> = {
          "Level": "Manager",
          "Менеджер": manager,
          "Месяц": "-",
          "Код курса": "-",
          "fl_IDs": managerItems.map((x) => String(x["fl_IDs"] ?? "").trim()).filter(Boolean).join(",").slice(0, 50000),
        };
        for (const k of metricKeys) managerRow[k] = managerItems.reduce((a, x) => a + num(x[k]), 0);
        managerRow["Конверсия в Квал"] = num(managerRow["Лиды"]) > 0 ? num(managerRow["Квал"]) / num(managerRow["Лиды"]) : 0;
        managerRow["Конверсия в Неквал"] = num(managerRow["Лиды"]) > 0 ? num(managerRow["Неквал"]) / num(managerRow["Лиды"]) : 0;
        managerRow["Конверсия в Отказ"] = num(managerRow["Лиды"]) > 0 ? num(managerRow["Отказы"]) / num(managerRow["Лиды"]) : 0;
        managerRow["Конверсия в работе"] = num(managerRow["Лиды"]) > 0 ? num(managerRow["В работе"]) / num(managerRow["Лиды"]) : 0;
        managerRow["Средний_чек"] = num(managerRow["Сделок_с_выручкой"]) > 0 ? num(managerRow["Выручка"]) / num(managerRow["Сделок_с_выручкой"]) : 0;
        rebuilt.push(managerRow);

        const codes = [...byCode.keys()].sort((a, b) => a.localeCompare(b));
        for (const code of codes) {
          const items = byCode.get(code) || [];
          const codeRow: Record<string, unknown> = {
            "Level": "Код курса",
            "Менеджер": manager,
            "Месяц": "-",
            "Код курса": code,
            "fl_IDs": items.map((x) => String(x["fl_IDs"] ?? "").trim()).filter(Boolean).join(",").slice(0, 50000),
          };
          for (const k of metricKeys) codeRow[k] = items.reduce((a, x) => a + num(x[k]), 0);
          codeRow["Конверсия в Квал"] = num(codeRow["Лиды"]) > 0 ? num(codeRow["Квал"]) / num(codeRow["Лиды"]) : 0;
          codeRow["Конверсия в Неквал"] = num(codeRow["Лиды"]) > 0 ? num(codeRow["Неквал"]) / num(codeRow["Лиды"]) : 0;
          codeRow["Конверсия в Отказ"] = num(codeRow["Лиды"]) > 0 ? num(codeRow["Отказы"]) / num(codeRow["Лиды"]) : 0;
          codeRow["Конверсия в работе"] = num(codeRow["Лиды"]) > 0 ? num(codeRow["В работе"]) / num(codeRow["Лиды"]) : 0;
          codeRow["Средний_чек"] = num(codeRow["Сделок_с_выручкой"]) > 0 ? num(codeRow["Выручка"]) / num(codeRow["Сделок_с_выручкой"]) : 0;
          rebuilt.push(codeRow);
        }
      }
      data = rebuilt;
    }

    if (isFunnelHierarchy && hasDateWindowControl) {
      const codeRows = filterRowsByDateRange(
        viewRows.filter((r) => String(r["__node"] ?? "").trim() === "code"),
        "Месяц",
        dateFrom,
        dateTo,
      );
      const groupedByFunnel = new Map<string, Record<string, unknown>[]>();
      for (const r of codeRows) {
        const f = String(r["Воронка"] ?? "").trim();
        if (!groupedByFunnel.has(f)) groupedByFunnel.set(f, []);
        groupedByFunnel.get(f)!.push(r);
      }
      const rebuilt: Record<string, unknown>[] = [];
      for (const [funnel, items] of groupedByFunnel.entries()) {
        const fRow = addKpi({
          "Level": "Воронка",
          "Воронка": funnel,
          "Месяц": "-",
          "Код_курса_норм": "-",
          "__node": "funnel",
          "__funnel": funnel,
          "Лиды": items.reduce((a, x) => a + num(x["Лиды"]), 0),
          "Квал": items.reduce((a, x) => a + num(x["Квал"]), 0),
          "Неквал": items.reduce((a, x) => a + num(x["Неквал"]), 0),
          "Неизвестно": items.reduce((a, x) => a + num(x["Неизвестно"]), 0),
          "Отказы": items.reduce((a, x) => a + num(x["Отказы"]), 0),
          "В работе": items.reduce((a, x) => a + num(x["В работе"]), 0),
          "Невалидные_лиды": items.reduce((a, x) => a + num(x["Невалидные_лиды"]), 0),
          "Сделок_с_выручкой": items.reduce((a, x) => a + num(x["Сделок_с_выручкой"]), 0),
          "Выручка": items.reduce((a, x) => a + num(x["Выручка"]), 0),
        });
        rebuilt.push(fRow);
        const byMonth = new Map<string, Record<string, unknown>[]>();
        for (const r of items) {
          const m = String(r["Месяц"] ?? "").trim();
          if (!byMonth.has(m)) byMonth.set(m, []);
          byMonth.get(m)!.push(r);
        }
        for (const [month, mItems] of byMonth.entries()) {
          const mRow = addKpi({
            "Level": "Месяц",
            "Воронка": funnel,
            "Месяц": month,
            "Код_курса_норм": "-",
            "__node": "month",
            "__funnel": funnel,
            "__month": month,
            "Лиды": mItems.reduce((a, x) => a + num(x["Лиды"]), 0),
            "Квал": mItems.reduce((a, x) => a + num(x["Квал"]), 0),
            "Неквал": mItems.reduce((a, x) => a + num(x["Неквал"]), 0),
            "Неизвестно": mItems.reduce((a, x) => a + num(x["Неизвестно"]), 0),
            "Отказы": mItems.reduce((a, x) => a + num(x["Отказы"]), 0),
            "В работе": mItems.reduce((a, x) => a + num(x["В работе"]), 0),
            "Невалидные_лиды": mItems.reduce((a, x) => a + num(x["Невалидные_лиды"]), 0),
            "Сделок_с_выручкой": mItems.reduce((a, x) => a + num(x["Сделок_с_выручкой"]), 0),
            "Выручка": mItems.reduce((a, x) => a + num(x["Выручка"]), 0),
          });
          rebuilt.push(mRow);
          rebuilt.push(...mItems);
        }
      }
      data = rebuilt;
    }

    if (isBudgetHierarchy && hasDateWindowControl) {
      const months = filterRowsByDateRange(
        viewRows.filter((r) => String(r["Level"] ?? "") === "Month"),
        effectiveDateCol || "month",
        dateFrom,
        dateTo,
      );
      const allowed = new Set(months.map((r) => String(r["month"] ?? r["Период"] ?? "").trim()).filter(Boolean));
      const details = viewRows.filter((r) => String(r["Level"] ?? "") === "Detail" && allowed.has(String(r["month"] ?? "").trim()));
      data = [...months, ...details];
    }
    if (view === "contacts_unique" && contactsFullOnly) {
      data = data.filter((r) => {
        const hasName = String(r["all_names"] ?? "").trim() !== "";
        const hasPhone = String(r["all_phones"] ?? "").trim() !== "";
        const hasEmail = String(r["all_emails"] ?? "").trim() !== "";
        return hasName && hasPhone && hasEmail;
      });
    }
    if (filter.trim()) {
      const q = filter.trim().toLowerCase();
      data = data.filter((r) => cols.some((c) => String(r[c] ?? "").toLowerCase().includes(q)));
    }
    if (view === "assoc_dynamic" && assocEventTab !== null) {
      data = data.filter((r) =>
        num(r["__assoc_event_detail"]) > 0
          ? String(r["__assoc_event_ctx"] ?? "").trim() === assocEventTab
          : String(r["Мероприятие"] ?? "").trim() === assocEventTab,
      );
    }
    // Иерархии строят порядок строк сами; глобальная сортировка ломает вложенные таблицы.
    if (sortCol && !isEmailHierarchy && !isManagerHierarchy && !isFunnelHierarchy && !isYandexHierarchy && !isYandexProjectHierarchy && !isAssocEmailHierarchy && !isAssocEventHierarchy && !isAssocYandexHierarchy && !isBudgetHierarchy) {
      data.sort((a, b) => compareCell(sortCol, a[sortCol], b[sortCol], sortDir));
    }

    // KPI boxes should reflect active period/filter selection, not collapse state.
    const kpiBaseRows = [...data];
    if (isEmailHierarchy) {
      const ordered: Record<string, unknown>[] = [];
      for (const r of data) {
        const lvl = String(r["Level"] ?? "").trim();
        const m = String(r["Месяц"] ?? "").trim();
        if (lvl === "Month") ordered.push(r);
        else if ((lvl === "Send" || lvl === "Spacer") && expandedEmailMonths.has(m)) ordered.push(r);
        else if (lvl === "SendOtherDetail" && expandedEmailMonths.has(m) && expandedAssocOtherRows.has(`email-month||${m}`)) ordered.push(r);
      }
      data = ordered;
    }
    if (isAssocEmailHierarchy) {
      const ordered: Record<string, unknown>[] = [];
      for (const r of data) {
        const isDetail = num(r["__assoc_email_detail"]) > 0;
        const ctxKey = String(r["__assoc_email_ctx"] ?? "");
        const ctxToken = ctxKey || "__root__";
        if (!isDetail) ordered.push(r);
        else if (expandedAssocOtherRows.has(`assoc||${ctxToken}`)) ordered.push(r);
      }
      data = ordered;
    }
    if (isAssocEventHierarchy) {
      const ordered: Record<string, unknown>[] = [];
      for (const r of data) {
        const isDetail = num(r["__assoc_event_detail"]) > 0;
        const ctxKey = String(r["__assoc_event_ctx"] ?? "");
        if (!isDetail) ordered.push(r);
        else if (expandedAssocEventRows.has(ctxKey)) ordered.push(r);
      }
      data = ordered;
    }
    if (isAssocYandexHierarchy) {
      const ordered: Record<string, unknown>[] = [];
      for (const r of data) {
        const isDetail = num(r["__assoc_yandex_detail"]) > 0;
        const ctxKey = String(r["__assoc_yandex_ctx"] ?? r["Yandex кампания"] ?? "").trim();
        if (!isDetail) ordered.push(r);
        else if (expandedAssocYandexRows.has(ctxKey)) ordered.push(r);
      }
      data = ordered;
    }
    if (isYandexProjectHierarchy) {
      const ordered: Record<string, unknown>[] = [];
      for (const r of data) {
        const isDetail = num(r["__yandex_project_detail"]) > 0;
        const ctxKey = String(r["__yandex_project_ctx"] ?? r["Yandex кампания"] ?? r["Проект"] ?? "").trim();
        if (!isDetail) ordered.push(r);
        else if (expandedYandexProjectRows.has(`${view}||${ctxKey}`)) ordered.push(r);
      }
      data = ordered;
    }
    if (isManagerHierarchy) {
      const ordered: Record<string, unknown>[] = [];
      for (const r of data) {
        const lvl = String(r["Level"] ?? "");
        const mgr = String(r["Менеджер"] ?? "");
        const month = String(r["Месяц"] ?? "");
        const code = String(r["Код курса"] ?? "");
        const mm = `${mgr}||${month}`;
        const mmc = `${mgr}||${month}||${code}`;
        if (lvl === "Manager") ordered.push(r);
        else if ((lvl === "Месяц" || lvl === "Код курса") && expandedManagers.has(mgr)) ordered.push(r);
        else if (lvl === "Month" && expandedManagerMonths.has(mm)) ordered.push(r);
        else if (lvl === "Lead" && expandedManagerCodes.has(mmc)) ordered.push(r);
      }
      data = ordered;
    }
    if (isFunnelHierarchy) {
      const ordered: Record<string, unknown>[] = [];
      for (const r of data) {
        const node = String(r["__node"] ?? "").trim();
        const month = String(r["__month"] ?? r["Месяц"] ?? "").trim();
        const funnel = String(r["__funnel"] ?? r["Воронка"] ?? "").trim();
        const fm = `${funnel}||${month}`;
        if (node === "funnel") ordered.push(r);
        else if (node === "month" && expandedFunnels.has(funnel)) ordered.push(r);
        else if (node === "code" && expandedFunnelMonths.has(fm)) ordered.push(r);
      }
      data = ordered;
    }
    if (isYandexHierarchy) {
      const ordered: Record<string, unknown>[] = [];
      for (const r of data) {
        const lvl = String(r["Level"] ?? "").trim();
        const month = String(r["Месяц"] ?? "").trim();
        const campaign = String(r["№ Кампании"] ?? "").trim();
        const mc = `${month}||${campaign}`;
        if (lvl === "Month") ordered.push(r);
        else if ((lvl === "Campaign" || lvl === "Spacer") && expandedYandexMonths.has(month)) ordered.push(r);
        else if (lvl === "Ad" && expandedYandexCampaigns.has(mc)) ordered.push(r);
      }
      data = ordered;
    }
    if (isBudgetHierarchy) {
      const ordered: Record<string, unknown>[] = [];
      const months = data.filter((r) => String(r["Level"] ?? "").trim() === "Month");
      const detailsByMonth = new Map<string, Record<string, unknown>[]>();
      for (const r of data) {
        if (String(r["Level"] ?? "").trim() !== "Detail") continue;
        const payMonth = budgetPayMonthKey(r);
        if (!payMonth) continue;
        if (!detailsByMonth.has(payMonth)) detailsByMonth.set(payMonth, []);
        detailsByMonth.get(payMonth)!.push(r);
      }
      for (const m of months) {
        const payMonth = budgetPayMonthKey(m);
        ordered.push(m);
        if (!expandedBudget.has(payMonth)) continue;
        const details = detailsByMonth.get(payMonth) || [];
        ordered.push(...details);
      }
      data = ordered;
    }

    const kpiRows =
      view === "funnels_hierarchy"
        ? kpiBaseRows.filter((r) => String(r["__node"] ?? "").trim() === "code")
        : isManagerHierarchy
          ? kpiBaseRows.filter((r) => String(r["Level"] ?? "").trim() === "Manager")
        : isAssocEmailHierarchy
          ? kpiBaseRows.filter((r) => num(r["__assoc_email_detail"]) === 0)
        : isAssocEventHierarchy
          ? kpiBaseRows.filter((r) => num(r["__assoc_event_detail"]) === 0)
        : isAssocYandexHierarchy
          ? kpiBaseRows.filter((r) => num(r["__assoc_yandex_detail"]) === 0)
        : isYandexProjectHierarchy
          ? kpiBaseRows.filter((r) => num(r["__yandex_project_detail"]) === 0)
        : isBudgetHierarchy
          ? kpiBaseRows.filter((r) => String(r["Level"] ?? "").trim() === "Month")
          : kpiBaseRows;

    const topCountEl = app.querySelector<HTMLElement>(".kpi-grid .kpi:first-child .value");
    if (topCountEl) topCountEl.textContent = kpiRows.length.toLocaleString("ru-RU");

    const topDealsEl = app.querySelector<HTMLElement>(".kpi-grid .kpi:nth-child(2) .value");
    if (topDealsEl) {
      const dealsTotal = kpiRows.reduce((acc, r) => acc + pickNum(r, ["Сделок_с_выручкой", "Сделок с выручкой"]), 0);
      topDealsEl.textContent = dealsTotal.toLocaleString("ru-RU");
    }

    const topRevenueEl = app.querySelector<HTMLElement>(".kpi-grid .kpi:nth-child(3) .value");
    if (topRevenueEl) {
      const revenueTotal = kpiRows.reduce((acc, r) => acc + pickNum(r, ["Выручка", "выручка"]), 0);
      topRevenueEl.textContent = formatRub(revenueTotal);
    }

    const showCtrl = isEmailHierarchy || isManagerHierarchy || isFunnelHierarchy || isYandexHierarchy || isYandexProjectHierarchy || isAssocEmailHierarchy || isAssocEventHierarchy || isAssocYandexHierarchy || isBudgetHierarchy;
    visibleRows = data;
    const th = `${showCtrl ? '<th class="ctrl-col">#</th>' : ""}${cols.map((c) => `<th data-col="${escapeHtml(c)}" title="${escapeHtml(canSaveViewJson ? "клик: сортировка · Ctrl+клик: переименовать" : "клик: сортировка")}">${escapeHtml(displayColName(view, c))}</th>`).join("")}`;
    const rendered = data.slice(0, 5000);
    visibleRows = rendered;
    const body = rendered.map((r, idx) => {
      const key = `${rowKey(view, r)}::${idx}`;
      const isOpen = expanded.has(key);
      const lvl = String(r["Level"] ?? "").trim();
      const month = String(r["Месяц"] ?? "").trim();
      const emailBtn = isEmailHierarchy && lvl === "Month" ? `<button class="email-expand-btn" data-month="${escapeHtml(month)}">${expandedEmailMonths.has(month) ? "−" : "+"}</button>` : "";
      const isEmailOtherGroup = isEmailHierarchy && num(r["__email_other_group"]) > 0;
      const emailOtherBtn = isEmailOtherGroup
        ? `<button class="email-other-expand-btn" data-month="${escapeHtml(month)}">${expandedAssocOtherRows.has(`email-month||${month}`) ? "−" : "+"}</button>`
        : "";
      const mgr = String(r["Менеджер"] ?? "");
      const code = String(r["Код курса"] ?? "");
      const mm = `${mgr}||${month}`;
      const mmc = `${mgr}||${month}||${code}`;
      const managerBtn =
        isManagerHierarchy && lvl === "Manager"
          ? `<button class="mgr-expand-btn" data-lvl="${escapeHtml(lvl)}" data-mgr="${escapeHtml(mgr)}" data-mm="${escapeHtml(mm)}" data-mmc="${escapeHtml(mmc)}">${
              lvl === "Manager"
                ? expandedManagers.has(mgr) ? "−" : "+"
                : lvl === "Месяц"
                  ? expandedManagerMonths.has(mm) ? "−" : "+"
                  : expandedManagerCodes.has(mmc) ? "−" : "+"
            }</button>`
          : "";
      const fNode = String(r["__node"] ?? "").trim();
      const fFunnel = String(r["__funnel"] ?? r["Воронка"] ?? "").trim();
      const fMonth2 = String(r["__month"] ?? r["Месяц"] ?? "").trim();
      const fm = `${fFunnel}||${fMonth2}`;
      const funnelBtn =
        isFunnelHierarchy && (fNode === "funnel" || fNode === "month")
          ? `<button class="funnel-expand-btn" data-node="${escapeHtml(fNode)}" data-funnel="${escapeHtml(fFunnel)}" data-fm="${escapeHtml(fm)}">${
              fNode === "funnel"
                ? expandedFunnels.has(fFunnel) ? "−" : "+"
                : expandedFunnelMonths.has(fm) ? "−" : "+"
            }</button>`
          : "";
      const yMonth = String(r["Месяц"] ?? "").trim();
      const yCamp = String(r["№ Кампании"] ?? "").trim();
      const yMc = `${yMonth}||${yCamp}`;
      const yLevel = String(r["Level"] ?? "").trim();
      const yandexBtn =
        isYandexHierarchy && (yLevel === "Month" || yLevel === "Campaign")
          ? `<button class="yd-expand-btn" data-lvl="${escapeHtml(yLevel)}" data-month="${escapeHtml(yMonth)}" data-mc="${escapeHtml(yMc)}">${
              yLevel === "Month"
                ? expandedYandexMonths.has(yMonth) ? "−" : "+"
                : expandedYandexCampaigns.has(yMc) ? "−" : "+"
            }</button>`
          : "";
      const assocCtx = String(r["__assoc_email_ctx"] ?? "");
      const assocCtxToken = assocCtx || "__root__";
      const assocOtherBtn =
        isAssocEmailHierarchy && num(r["__assoc_email_other_group"]) > 0 && num(r["__assoc_email_has_details"]) > 0
          ? `<button class="assoc-email-expand-btn" data-ctx="${escapeHtml(assocCtxToken)}">${expandedAssocOtherRows.has(`assoc||${assocCtxToken}`) ? "−" : "+"}</button>`
          : "";
      const assocEventCtx = String(r["__assoc_event_ctx"] ?? r["Мероприятие"] ?? "").trim();
      const assocEventBtn =
        isAssocEventHierarchy && num(r["__assoc_event_detail"]) === 0 && num(r["__assoc_event_has_details"]) > 0
          ? `<button class="assoc-event-expand-btn" data-ctx="${escapeHtml(assocEventCtx)}">${expandedAssocEventRows.has(assocEventCtx) ? "−" : "+"}</button>`
          : "";
      const assocYandexCtx = String(r["__assoc_yandex_ctx"] ?? r["Yandex кампания"] ?? "").trim();
      const assocYandexBtn =
        isAssocYandexHierarchy && num(r["__assoc_yandex_detail"]) === 0 && num(r["__assoc_yandex_has_details"]) > 0
          ? `<button class="assoc-yandex-expand-btn" data-ctx="${escapeHtml(assocYandexCtx)}">${expandedAssocYandexRows.has(assocYandexCtx) ? "−" : "+"}</button>`
          : "";
      const yProjectCtx = String(r["__yandex_project_ctx"] ?? r["Yandex кампания"] ?? r["Проект"] ?? "").trim();
      const yProjectBtn =
        isYandexProjectHierarchy && num(r["__yandex_project_detail"]) === 0 && num(r["__yandex_project_has_details"]) > 0
          ? `<button class="yd-project-expand-btn" data-ctx="${escapeHtml(yProjectCtx)}">${expandedYandexProjectRows.has(`${view}||${yProjectCtx}`) ? "−" : "+"}</button>`
          : "";
      const budgetPayMonth = budgetPayMonthKey(r);
      const budgetBtn =
        isBudgetHierarchy && lvl === "Month"
          ? `<button class="budget-expand-btn" data-paymonth="${escapeHtml(budgetPayMonth)}">${expandedBudget.has(budgetPayMonth) ? "−" : "+"}</button>`
          : "";
      const row = `<tr>${showCtrl ? `<td class="ctrl-col">${emailBtn || emailOtherBtn || managerBtn || funnelBtn || yandexBtn || assocOtherBtn || assocEventBtn || assocYandexBtn || yProjectBtn || budgetBtn}</td>` : ""}${cols
        .map((c) => {
          const editable = canSaveViewJson && isEditableDataColumn(c) ? "1" : "0";
          return `<td data-row="${idx}" data-col="${escapeHtml(c)}" data-editable="${editable}">${escapeHtml(formatCell(c, r[c]))}</td>`;
        })
        .join("")}</tr>`;
      if (!isOpen || isEmailHierarchy || isManagerHierarchy) return row;
      return `${row}<tr class="dev-row"><td colspan="${cols.length + (showCtrl ? 1 : 0)}">${renderDealsTable(dealsForRow(view, r, dealsIndex))}</td></tr>`;
    }).join("");

    const table = app.querySelector(".table-scroll table");
    if (!table) return;
    table.innerHTML = `<thead><tr>${th}</tr></thead><tbody>${body}</tbody>`;
    app.querySelectorAll<HTMLButtonElement>(".pnl-btn").forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-mode") === pnlMode);
    });
    app.querySelectorAll<HTMLTableCellElement>("th[data-col]").forEach((h) => {
      h.style.color = h.getAttribute("data-col") === sortCol ? "var(--accent)" : "";
      h.onclick = (ev) => {
        const c = h.getAttribute("data-col") || "";
        if (!c) return;
        const me = ev as MouseEvent;
        if ((me.metaKey || me.ctrlKey) && canSaveViewJson) {
          if (h.querySelector("input")) return;
          const currentName = displayColName(view, c);
          const input = document.createElement("input");
          input.type = "text";
          input.value = currentName;
          input.className = "inline-col-editor";
          const commitHeader = (): void => {
            const newName = input.value.trim();
            const aliases = columnAliasesByView.get(view) || {};
            if (newName && newName !== prettyColName(c)) {
              aliases[c] = newName;
            } else {
              delete aliases[c];
            }
            columnAliasesByView.set(view, aliases);
            void saveViewJson();
            draw();
          };
          const cancelHeader = (): void => draw();
          input.onkeydown = (ke) => {
            if (ke.key === "Enter") {
              ke.preventDefault();
              commitHeader();
            } else if (ke.key === "Escape") {
              ke.preventDefault();
              cancelHeader();
            }
          };
          input.onblur = () => commitHeader();
          h.textContent = "";
          h.appendChild(input);
          input.focus();
          input.select();
          return;
        }
        if (sortCol === c) sortDir = sortDir === "asc" ? "desc" : "asc";
        else sortCol = c;
        draw();
      };
    });
    app.querySelectorAll<HTMLTableCellElement>('td[data-editable="1"]').forEach((td) => {
      td.onclick = () => {
        if (td.querySelector("input")) return;
        const rowIdx = Number(td.getAttribute("data-row") || "-1");
        const col = td.getAttribute("data-col") || "";
        if (rowIdx < 0 || !col || rowIdx >= visibleRows.length) return;
        const row = visibleRows[rowIdx];
        const current = String(row[col] ?? "");
        const input = document.createElement("input");
        input.type = "text";
        input.value = current;
        input.className = "inline-col-editor";
        const commit = (): void => {
          row[col] = input.value.trim();
          void saveViewJson();
          draw();
        };
        const cancel = (): void => draw();
        input.onkeydown = (ke) => {
          if (ke.key === "Enter") {
            ke.preventDefault();
            commit();
          } else if (ke.key === "Escape") {
            ke.preventDefault();
            cancel();
          }
        };
        input.onblur = () => commit();
        td.textContent = "";
        td.appendChild(input);
        input.focus();
        input.select();
      };
    });
    app.querySelectorAll<HTMLButtonElement>(".email-expand-btn").forEach((b) => (b.onclick = () => {
      const k = b.getAttribute("data-month") || "";
      if (!k) return;
      if (expandedEmailMonths.has(k)) expandedEmailMonths.delete(k); else expandedEmailMonths.add(k);
      draw();
    }));
    app.querySelectorAll<HTMLButtonElement>(".email-other-expand-btn").forEach((b) => (b.onclick = () => {
      const m = b.getAttribute("data-month") || "";
      if (!m) return;
      const k = `email-month||${m}`;
      if (expandedAssocOtherRows.has(k)) expandedAssocOtherRows.delete(k); else expandedAssocOtherRows.add(k);
      draw();
    }));
    app.querySelectorAll<HTMLButtonElement>(".mgr-expand-btn").forEach((b) => (b.onclick = () => {
      const lvl = b.getAttribute("data-lvl") || "";
      const mgr = b.getAttribute("data-mgr") || "";
      const mm = b.getAttribute("data-mm") || "";
      const mmc = b.getAttribute("data-mmc") || "";
      if (lvl === "Manager" && mgr) {
        if (expandedManagers.has(mgr)) expandedManagers.delete(mgr); else expandedManagers.add(mgr);
      } else if (lvl === "Месяц" && mm) {
        if (expandedManagerMonths.has(mm)) expandedManagerMonths.delete(mm); else expandedManagerMonths.add(mm);
      } else if (lvl === "Код курса" && mmc) {
        if (expandedManagerCodes.has(mmc)) expandedManagerCodes.delete(mmc); else expandedManagerCodes.add(mmc);
      }
      draw();
    }));
    app.querySelectorAll<HTMLButtonElement>(".funnel-expand-btn").forEach((b) => (b.onclick = () => {
      const node = b.getAttribute("data-node") || "";
      const funnel = b.getAttribute("data-funnel") || "";
      const fm = b.getAttribute("data-fm") || "";
      if (node === "funnel" && funnel) {
        if (expandedFunnels.has(funnel)) expandedFunnels.delete(funnel); else expandedFunnels.add(funnel);
      } else if (node === "month" && fm) {
        if (expandedFunnelMonths.has(fm)) expandedFunnelMonths.delete(fm); else expandedFunnelMonths.add(fm);
      }
      draw();
    }));
    app.querySelectorAll<HTMLButtonElement>(".yd-expand-btn").forEach((b) => (b.onclick = () => {
      const lvl = b.getAttribute("data-lvl") || "";
      const month = b.getAttribute("data-month") || "";
      const mc = b.getAttribute("data-mc") || "";
      if (lvl === "Month" && month) {
        if (expandedYandexMonths.has(month)) expandedYandexMonths.delete(month); else expandedYandexMonths.add(month);
      } else if (lvl === "Campaign" && mc) {
        if (expandedYandexCampaigns.has(mc)) expandedYandexCampaigns.delete(mc); else expandedYandexCampaigns.add(mc);
      }
      draw();
    }));
    app.querySelectorAll<HTMLButtonElement>(".assoc-email-expand-btn").forEach((b) => (b.onclick = () => {
      const ctx = b.getAttribute("data-ctx") || "";
      const k = `assoc||${ctx}`;
      if (expandedAssocOtherRows.has(k)) expandedAssocOtherRows.delete(k); else expandedAssocOtherRows.add(k);
      draw();
    }));
    app.querySelectorAll<HTMLButtonElement>(".assoc-event-expand-btn").forEach((b) => (b.onclick = () => {
      const ctx = b.getAttribute("data-ctx") || "";
      if (!ctx) return;
      if (expandedAssocEventRows.has(ctx)) expandedAssocEventRows.delete(ctx); else expandedAssocEventRows.add(ctx);
      draw();
    }));
    app.querySelectorAll<HTMLButtonElement>(".assoc-yandex-expand-btn").forEach((b) => (b.onclick = () => {
      const ctx = b.getAttribute("data-ctx") || "";
      if (!ctx) return;
      if (expandedAssocYandexRows.has(ctx)) expandedAssocYandexRows.delete(ctx); else expandedAssocYandexRows.add(ctx);
      draw();
    }));
    app.querySelectorAll<HTMLButtonElement>(".yd-project-expand-btn").forEach((b) => (b.onclick = () => {
      const ctx = b.getAttribute("data-ctx") || "";
      if (!ctx) return;
      const key = `${view}||${ctx}`;
      if (expandedYandexProjectRows.has(key)) expandedYandexProjectRows.delete(key); else expandedYandexProjectRows.add(key);
      draw();
    }));
    app.querySelectorAll<HTMLButtonElement>(".budget-expand-btn").forEach((b) => (b.onclick = () => {
      const pm = b.getAttribute("data-paymonth") || "";
      if (!pm) return;
      if (expandedBudget.has(pm)) expandedBudget.delete(pm); else expandedBudget.add(pm);
      draw();
    }));

    const expandAllBtn = app.querySelector<HTMLButtonElement>(".expand-all-toggle-btn");
    if (expandAllBtn && showCtrl) {
      const allExpanded = (() => {
        if (isEmailHierarchy) {
          const months = [...new Set(data.filter((r) => String(r["Level"] ?? "") === "Month").map((r) => String(r["Месяц"] ?? "").trim()).filter(Boolean))];
          return months.length > 0 && months.every((m) => expandedEmailMonths.has(m));
        }
        if (isManagerHierarchy) {
          const mgrs = [...new Set(data.filter((r) => String(r["Level"] ?? "") === "Manager").map((r) => String(r["Менеджер"] ?? "").trim()).filter(Boolean))];
          return mgrs.length > 0 && mgrs.every((m) => expandedManagers.has(m));
        }
        if (isFunnelHierarchy) {
          const funnels = [...new Set(data.filter((r) => String(r["__node"] ?? "").trim() === "funnel").map((r) => String(r["__funnel"] ?? r["Воронка"] ?? "").trim()).filter(Boolean))];
          return funnels.length > 0 && funnels.every((f) => expandedFunnels.has(f));
        }
        if (isYandexHierarchy) {
          const months = [...new Set(data.filter((r) => String(r["Level"] ?? "").trim() === "Month").map((r) => String(r["Месяц"] ?? "").trim()).filter(Boolean))];
          return months.length > 0 && months.every((m) => expandedYandexMonths.has(m));
        }
        if (isYandexProjectHierarchy) {
          const keys = [...new Set(data.filter((r) => num(r["__yandex_project_detail"]) === 0 && num(r["__yandex_project_has_details"]) > 0).map((r) => `${view}||${String(r["__yandex_project_ctx"] ?? r["Yandex кампания"] ?? r["Проект"] ?? "").trim()}`).filter((k) => !k.endsWith("||")))];
          return keys.length > 0 && keys.every((k) => expandedYandexProjectRows.has(k));
        }
        if (isAssocEmailHierarchy) {
          const keys = [...new Set(data.filter((r) => num(r["__assoc_email_other_group"]) > 0 && num(r["__assoc_email_has_details"]) > 0).map((r) => `assoc||${String(r["__assoc_email_ctx"] ?? "__root__")}`))];
          return keys.length > 0 && keys.every((k) => expandedAssocOtherRows.has(k));
        }
        if (isAssocEventHierarchy) {
          const keys = [...new Set(data.filter((r) => num(r["__assoc_event_detail"]) === 0 && num(r["__assoc_event_has_details"]) > 0).map((r) => String(r["__assoc_event_ctx"] ?? r["Мероприятие"] ?? "").trim()).filter(Boolean))];
          return keys.length > 0 && keys.every((k) => expandedAssocEventRows.has(k));
        }
        if (isAssocYandexHierarchy) {
          const keys = [...new Set(data.filter((r) => num(r["__assoc_yandex_detail"]) === 0 && num(r["__assoc_yandex_has_details"]) > 0).map((r) => String(r["__assoc_yandex_ctx"] ?? r["Yandex кампания"] ?? "").trim()).filter(Boolean))];
          return keys.length > 0 && keys.every((k) => expandedAssocYandexRows.has(k));
        }
        if (isBudgetHierarchy) {
          const keys = [...new Set(data.filter((r) => String(r["Level"] ?? "").trim() === "Month").map((r) => budgetPayMonthKey(r)).filter(Boolean))];
          return keys.length > 0 && keys.every((k) => expandedBudget.has(k));
        }
        return false;
      })();

      expandAllBtn.textContent = allExpanded ? "Свернуть всё" : "Развернуть всё";
      expandAllBtn.onclick = () => {
        const expand = !allExpanded;
        if (isEmailHierarchy) {
          const months = [...new Set(viewRows.filter((r) => String(r["Level"] ?? "") === "Month").map((r) => String(r["Месяц"] ?? "").trim()).filter(Boolean))];
          months.forEach((m) => expand ? expandedEmailMonths.add(m) : expandedEmailMonths.delete(m));
          const otherMonthKeys = [...new Set(viewRows.filter((r) => num(r["__email_other_group"]) > 0).map((r) => `email-month||${String(r["Месяц"] ?? "").trim()}`))];
          otherMonthKeys.forEach((k) => expand ? expandedAssocOtherRows.add(k) : expandedAssocOtherRows.delete(k));
        } else if (isManagerHierarchy) {
          const mgrs = [...new Set(viewRows.filter((r) => String(r["Level"] ?? "") === "Manager").map((r) => String(r["Менеджер"] ?? "").trim()).filter(Boolean))];
          mgrs.forEach((m) => expand ? expandedManagers.add(m) : expandedManagers.delete(m));
        } else if (isFunnelHierarchy) {
          const funnels = [...new Set(viewRows.filter((r) => String(r["__node"] ?? "").trim() === "funnel").map((r) => String(r["__funnel"] ?? r["Воронка"] ?? "").trim()).filter(Boolean))];
          funnels.forEach((f) => expand ? expandedFunnels.add(f) : expandedFunnels.delete(f));
          const funnelMonths = [...new Set(viewRows.filter((r) => String(r["__node"] ?? "").trim() === "month").map((r) => `${String(r["__funnel"] ?? r["Воронка"] ?? "").trim()}||${String(r["__month"] ?? r["Месяц"] ?? "").trim()}`).filter((k) => !k.startsWith("||")) )];
          funnelMonths.forEach((k) => expand ? expandedFunnelMonths.add(k) : expandedFunnelMonths.delete(k));
        } else if (isYandexHierarchy) {
          const months = [...new Set(viewRows.filter((r) => String(r["Level"] ?? "").trim() === "Month").map((r) => String(r["Месяц"] ?? "").trim()).filter(Boolean))];
          months.forEach((m) => expand ? expandedYandexMonths.add(m) : expandedYandexMonths.delete(m));
          const mcs = [...new Set(viewRows.filter((r) => String(r["Level"] ?? "").trim() === "Campaign").map((r) => `${String(r["Месяц"] ?? "").trim()}||${String(r["№ Кампании"] ?? "").trim()}`).filter((k) => !k.endsWith("||")) )];
          mcs.forEach((k) => expand ? expandedYandexCampaigns.add(k) : expandedYandexCampaigns.delete(k));
        } else if (isYandexProjectHierarchy) {
          const keys = [...new Set(viewRows.filter((r) => num(r["__yandex_project_detail"]) === 0 && num(r["__yandex_project_has_details"]) > 0).map((r) => `${view}||${String(r["__yandex_project_ctx"] ?? r["Yandex кампания"] ?? r["Проект"] ?? "").trim()}`).filter((k) => !k.endsWith("||")) )];
          keys.forEach((k) => expand ? expandedYandexProjectRows.add(k) : expandedYandexProjectRows.delete(k));
        } else if (isAssocEmailHierarchy) {
          const keys = [...new Set(viewRows.filter((r) => num(r["__assoc_email_other_group"]) > 0 && num(r["__assoc_email_has_details"]) > 0).map((r) => `assoc||${String(r["__assoc_email_ctx"] ?? "__root__")}`))];
          keys.forEach((k) => expand ? expandedAssocOtherRows.add(k) : expandedAssocOtherRows.delete(k));
        } else if (isAssocEventHierarchy) {
          const keys = [...new Set(viewRows.filter((r) => num(r["__assoc_event_detail"]) === 0 && num(r["__assoc_event_has_details"]) > 0).map((r) => String(r["__assoc_event_ctx"] ?? r["Мероприятие"] ?? "").trim()).filter(Boolean))];
          keys.forEach((k) => expand ? expandedAssocEventRows.add(k) : expandedAssocEventRows.delete(k));
        } else if (isAssocYandexHierarchy) {
          const keys = [...new Set(viewRows.filter((r) => num(r["__assoc_yandex_detail"]) === 0 && num(r["__assoc_yandex_has_details"]) > 0).map((r) => String(r["__assoc_yandex_ctx"] ?? r["Yandex кампания"] ?? "").trim()).filter(Boolean))];
          keys.forEach((k) => expand ? expandedAssocYandexRows.add(k) : expandedAssocYandexRows.delete(k));
        } else if (isBudgetHierarchy) {
          const keys = [...new Set(viewRows.filter((r) => String(r["Level"] ?? "").trim() === "Month").map((r) => budgetPayMonthKey(r)).filter(Boolean))];
          keys.forEach((k) => expand ? expandedBudget.add(k) : expandedBudget.delete(k));
        }
        draw();
      };
    }
  };

  const kpiRows =
    view === "funnels_hierarchy"
      ? viewRows.filter((r) => String(r["__node"] ?? "").trim() === "code")
      : isManagerHierarchy
        ? viewRows.filter((r) => String(r["Level"] ?? "").trim() === "Manager")
      : isAssocEmailHierarchy
        ? viewRows.filter((r) => num(r["__assoc_email_detail"]) === 0)
        : isAssocEventHierarchy
          ? viewRows.filter((r) => num(r["__assoc_event_detail"]) === 0)
          : isAssocYandexHierarchy
            ? viewRows.filter((r) => num(r["__assoc_yandex_detail"]) === 0)
          : isYandexProjectHierarchy
            ? viewRows.filter((r) => num(r["__yandex_project_detail"]) === 0)
            : isBudgetHierarchy
              ? viewRows.filter((r) => String(r["Level"] ?? "").trim() === "Month")
              : viewRows;
  const totalRevenue = kpiRows.reduce((acc, r) => acc + pickNum(r, ["Выручка", "выручка"]), 0);
  const deals = kpiRows.reduce((acc, r) => acc + pickNum(r, ["Сделок_с_выручкой", "Сделок с выручкой"]), 0);
  app.innerHTML = `<div class="app-layout">
    <aside class="side-menu">
      <button class="side-btn" data-menu="dashboard">Главная</button>
      <button class="side-btn ${isUtmConstructor ? "" : "active"}" data-menu="reports">Детальные отчеты</button>
      <button class="side-btn" data-menu="charts">Графики</button>
      <button class="side-btn ${isUtmConstructor ? "active" : ""}" data-menu="utm">UTM Конструктор</button>
    </aside>
    <main class="main-content">
    <header><h1>${escapeHtml(meta.title)}</h1><p class="sub">${escapeHtml(resolvedPath)} · ${viewRows.length} строк</p></header>
    ${isUtmConstructor ? "" : `<div class="kpi-grid"><div class="kpi"><div class="label">${escapeHtml(meta.rowsLabel)}</div><div class="value">${viewRows.length}</div></div><div class="kpi"><div class="label">Сделок с выручкой</div><div class="value">${deals.toLocaleString("ru-RU")}</div></div><div class="kpi"><div class="label">Выручка</div><div class="value">${formatRub(totalRevenue)}</div></div></div>`}
    ${managerFormulaNote(view)}
    <div class="toolbar">
      ${isUtmConstructor ? "" : `<div class="tabs-row top-tabs">
        <button class="tab-btn ${tab === "year" ? "active" : ""}" data-tab="year">Отчет за год</button>
        <button class="tab-btn ${tab === "assoc_builder" ? "active" : ""}" data-tab="assoc_builder">Ассоц. выручка</button>
        <button class="tab-btn ${tab === "media" ? "active" : ""}" data-tab="media">Рекламные медиумы</button>
        <button class="tab-btn ${tab === "budget" ? "active" : ""}" data-tab="budget">Бюджет</button>
        <button class="tab-btn ${tab === "months" ? "active" : ""}" data-tab="months">По месяцам</button>
        <button class="tab-btn ${tab === "managers" ? "active" : ""}" data-tab="managers">По менеджерам</button>
        <button class="tab-btn ${tab === "funnels" ? "active" : ""}" data-tab="funnels">По воронкам</button>
        <button class="tab-btn ${tab === "contacts" ? "active" : ""}" data-tab="contacts">Уникальные контакты</button>
        <button class="tab-btn ${tab === "qa" ? "active" : ""}" data-tab="qa">Контроль качества</button>
      </div>
      <div class="tabs-row sub-tabs">
        ${tabViews
          .map(
            (v) =>
              `<button class="tab-btn ${v === view ? "active" : ""}" data-view="${v}">${escapeHtml(VIEW_META[v].label)}</button>`,
          )
          .join("")}
      </div>`}
      ${
        view === "assoc_dynamic" && assocEvents.length > 0
          ? `<div class="tabs-row event-tabs">${assocEvents.map((ev) => `<button class="tab-btn${ev === assocEventTab ? " active" : ""}" data-event="${escapeHtml(ev)}">${escapeHtml(ev)}</button>`).join("")}</div>`
          : ""
      }
      ${
        isUtmConstructor
          ? ""
          : `<button class="copy-table-btn">Скопировать таблицу</button>
      <button class="download-table-btn">Загрузить таблицу</button>`
      }
      ${
        view === "contacts_unique"
          ? `<button class="contacts-full-btn">Контакты: имя + телефон + email (${contactsFullOnly ? "on" : "off"})</button>`
          : ""
      }
      ${
        hasDateWindowControl
          ? `<span class="date-range-controls">
              <label>С: <input type="month" class="date-from-input" value="${dateFrom}" /></label>
              <label>По: <input type="month" class="date-to-input" value="${dateTo}" /></label>
              ${
                hasPnlToggle
                  ? `<span class="pnl-toggle">
                       <button class="pnl-btn${pnlMode === "cohort" ? " active" : ""}" data-mode="cohort">Когорта</button>
                       <button class="pnl-btn${pnlMode !== "cohort" ? " active" : ""}" data-mode="pnl">PNL</button>
                     </span>`
                  : ""
              }
            </span>`
          : ""
      }
      ${
        (isEmailHierarchy || isManagerHierarchy || isFunnelHierarchy || isYandexHierarchy || isYandexProjectHierarchy || isAssocEmailHierarchy || isAssocEventHierarchy || isAssocYandexHierarchy || isBudgetHierarchy)
          ? `<button class="expand-all-toggle-btn">Развернуть всё</button>`
          : ""
      }
      <span class="row-note"></span>
    </div>
    ${isUtmConstructor ? "" : '<div class="table-filter-row"><input type="search" placeholder="Фильтр по строке…" class="filter-input" /></div>'}
    ${canSaveViewJson ? '<div class="push-status muted"></div>' : ""}
    ${
      isUtmConstructor
        ? `<section class="utm-builder">
      <h3>Создать UTM тег</h3>
      <div class="utm-grid">
        <label>Medium <span class="required-marker">*</span>
          <select class="utm-medium-select"></select>
        </label>
        <label>Source <span class="required-marker">*</span>
          <select class="utm-source-select"></select>
          <input class="utm-source-freetext" type="text" placeholder="Введите источник" style="display:none" />
        </label>
        <label>Name (Campaign) <span class="required-marker">*</span>
          <span class="utm-campaign-row">
            <input class="utm-campaign-input" type="text" placeholder="Например, spring_sale_2026" />
            <span class="utm-partner-field">
              <label class="utm-partner-toggle-wrap" style="display:none" title="Partner"><input type="checkbox" class="utm-partner-toggle" aria-label="Partner" /><span>Partner</span></label>
              <input class="utm-partner-input" type="text" placeholder="Partner" style="display:none" />
            </span>
          </span>
        </label>
        <label>Link <span class="required-marker">*</span>
          <input class="utm-link-input" type="url" placeholder="https://example.com/campaign" />
        </label>
        <label>Content
          <input class="utm-content-input" type="text" placeholder="Например, banner_a" />
        </label>
        <label>Term
          <input class="utm-term-input" type="text" placeholder="Например, python_course" />
        </label>
      </div>
      <div class="utm-actions">
        <button class="utm-write-btn" disabled>write</button>
        <span class="utm-write-status muted"></span>
      </div>
      ${utmLatestTag ? `<div class="utm-preview-wrap">
        <h4>Готовый UTM тег</h4>
        <table>
          <thead><tr><th>UTM Tag</th><th>Действие</th></tr></thead>
          <tbody><tr><td class="utm-preview-cell">${escapeHtml(utmLatestTag)}</td><td><button class="copy-utm-row-btn">Copy</button></td></tr></tbody>
        </table>
      </div>` : ""}
    </section>`
        : ""
    }
    ${!isUtmConstructor || viewRows.length > 0 ? `<div class="table-scroll"><table><thead><tr>${cols.map((c) => `<th>${escapeHtml(prettyColName(c))}</th>`).join("")}</tr></thead><tbody></tbody></table></div>` : ""}
    </main>
  </div>`;

  const filterInput = app.querySelector<HTMLInputElement>(".filter-input");
  const dateFromInput = app.querySelector<HTMLInputElement>(".date-from-input");
  const dateToInput = app.querySelector<HTMLInputElement>(".date-to-input");
  const rowNote = app.querySelector<HTMLSpanElement>(".row-note");
  const contactsFullBtn = app.querySelector<HTMLButtonElement>(".contacts-full-btn");
  const copyTableBtn = app.querySelector<HTMLButtonElement>(".copy-table-btn");
  const downloadTableBtn = app.querySelector<HTMLButtonElement>(".download-table-btn");
  const status = app.querySelector<HTMLDivElement>(".push-status");

  app.querySelectorAll<HTMLButtonElement>(".top-tabs .tab-btn").forEach((btn) => {
    btn.onclick = async () => {
      const nextTab = (btn.getAttribute("data-tab") || "") as TabKey;
      if (!nextTab) return;
      const first = (Object.keys(VIEW_META) as ViewKey[]).find((k) => VIEW_META[k].tab === nextTab);
      if (!first) return;
      try {
        const r = await fetchJson<Record<string, unknown>[]>(viewPath(first));
        void renderTable(first, r, dealsIndex);
      } catch (e) {
        if (status) status.textContent = `Ошибка загрузки: ${String(e)}`;
      }
    };
  });

  app.querySelectorAll<HTMLButtonElement>(".sub-tabs .tab-btn").forEach((btn) => {
    btn.onclick = async () => {
      const next = (btn.getAttribute("data-view") || "") as ViewKey;
      if (!next) return;
      try {
        const r = await fetchJson<Record<string, unknown>[]>(viewPath(next));
        void renderTable(next, r, dealsIndex);
      } catch (e) {
        if (status) status.textContent = `Ошибка загрузки: ${String(e)}`;
      }
    };
  });

  app.querySelectorAll<HTMLButtonElement>(".event-tabs .tab-btn").forEach((btn) => {
    btn.onclick = () => {
      const ev = btn.getAttribute("data-event") || null;
      assocEventTab = ev;
      app.querySelectorAll<HTMLButtonElement>(".event-tabs .tab-btn").forEach((b) =>
        b.classList.toggle("active", b.getAttribute("data-event") === assocEventTab),
      );
      draw();
    };
  });

  /* tabSelect.onchange = async () => {
    const nextTab = tabSelect.value as TabKey;
    const first = (Object.keys(VIEW_META) as ViewKey[]).find((k) => VIEW_META[k].tab === nextTab);
    if (!first) return;
    const r = await fetchJson<Record<string, unknown>[]>(VIEW_META[first].path);
    void renderTable(first, r, dealsIndex);
  };
  viewSelect.onchange = async () => {
    const next = viewSelect.value as ViewKey;
    const r = await fetchJson<Record<string, unknown>[]>(VIEW_META[next].path);
    void renderTable(next, r, dealsIndex);
  }; */
  if (filterInput) {
    filterInput.oninput = () => { filter = filterInput.value; draw(); };
  }

  let pnlRetryDone = false;
  const rerenderForCurrentState = async (): Promise<void> => {
    try {
      if (rowNote) rowNote.textContent = "";
      const nextRows = await fetchJson<Record<string, unknown>[]>(viewPath(view, { pnlMode, dateFrom, dateTo }));
      await renderTable(view, nextRows, dealsIndex, {
        initialDateFrom: dateFrom,
        initialDateTo: dateTo,
        initialPnlMode: pnlMode,
      });
      pnlRetryDone = false;
    } catch (e) {
      const msg = String(e ?? "");
      const canRetryMaterialize =
        !pnlRetryDone &&
        pnlMode === "pnl" &&
        !isAssocDynamic &&
        /:\s*404\b/.test(msg);

      if (canRetryMaterialize) {
        pnlRetryDone = true;
        try {
          if (rowNote) rowNote.textContent = "PNL данные не найдены, запускаю пересборку...";
          await fetch("/api/analytics/materialize?force=true", { method: "POST" });
          const nextRows = await fetchJson<Record<string, unknown>[]>(viewPath(view, { pnlMode, dateFrom, dateTo }));
          await renderTable(view, nextRows, dealsIndex, {
            initialDateFrom: dateFrom,
            initialDateTo: dateTo,
            initialPnlMode: pnlMode,
          });
          return;
        } catch (retryErr) {
          if (rowNote) rowNote.textContent = `PNL недоступен: ${String(retryErr)}`;
          if (status) status.textContent = `Ошибка загрузки: ${String(retryErr)}`;
          return;
        }
      }

      if (rowNote) rowNote.textContent = `Ошибка загрузки: ${msg}`;
      if (status) status.textContent = `Ошибка загрузки: ${String(e)}`;
    }
  };

  if (dateFromInput) {
    dateFromInput.onchange = () => {
      dateFrom = dateFromInput.value || "";
      if (isAssocDynamic) void rerenderForCurrentState();
      else draw();
    };
  }
  if (dateToInput) {
    dateToInput.onchange = () => {
      dateTo = dateToInput.value || "";
      if (isAssocDynamic) void rerenderForCurrentState();
      else draw();
    };
  }
  app.querySelectorAll<HTMLButtonElement>(".pnl-btn").forEach((btn) => {
    btn.onclick = () => {
      pnlMode = (btn.getAttribute("data-mode") || "cohort") as PnlMode;
      void rerenderForCurrentState();
    };
  });
  app.querySelectorAll<HTMLButtonElement>(".side-btn").forEach((btn) => {
    btn.onclick = async () => {
      const m = btn.getAttribute("data-menu");
      if (m === "dashboard" || m === "reports" || m === "charts" || m === "utm") {
        await openMenu(m, dealsIndex, view);
      }
    };
  });
  if (contactsFullBtn) {
    contactsFullBtn.onclick = () => {
      contactsFullOnly = !contactsFullOnly;
      contactsFullBtn.textContent = `Контакты: имя + телефон + email (${contactsFullOnly ? "on" : "off"})`;
      draw();
    };
  }
  const utmMediumSelect = app.querySelector<HTMLSelectElement>(".utm-medium-select");
  const utmSourceSelect = app.querySelector<HTMLSelectElement>(".utm-source-select");
  const utmSourceFreetext = app.querySelector<HTMLInputElement>(".utm-source-freetext");
  const utmCampaignInput = app.querySelector<HTMLInputElement>(".utm-campaign-input");
  const utmPartnerToggleWrap = app.querySelector<HTMLElement>(".utm-partner-toggle-wrap");
  const utmPartnerToggle = app.querySelector<HTMLInputElement>(".utm-partner-toggle");
  const utmPartnerInput = app.querySelector<HTMLInputElement>(".utm-partner-input");
  const utmLinkInput = app.querySelector<HTMLInputElement>(".utm-link-input");
  const utmContentInput = app.querySelector<HTMLInputElement>(".utm-content-input");
  const utmTermInput = app.querySelector<HTMLInputElement>(".utm-term-input");
  const utmWriteBtn = app.querySelector<HTMLButtonElement>(".utm-write-btn");
  const utmWriteStatus = app.querySelector<HTMLSpanElement>(".utm-write-status");
  const utmPreviewCell = app.querySelector<HTMLElement>(".utm-preview-cell");
  const copyUtmRowBtn = app.querySelector<HTMLButtonElement>(".copy-utm-row-btn");

  const setUtmPreview = (tag: string): void => {
    utmLatestTag = tag;
    if (utmPreviewCell) utmPreviewCell.textContent = tag;
  };

  const setSources = (medium: string): void => {
    if (!utmSourceSelect) return;
    const entry = UTM_MEDIUM_CONFIG.find(m => m.value === medium);
    if (entry?.sourceType === "freetext") {
      utmSourceSelect.style.display = "none";
      if (utmSourceFreetext) { utmSourceFreetext.style.display = ""; utmSourceFreetext.value = ""; }
    } else {
      utmSourceSelect.style.display = "";
      if (utmSourceFreetext) { utmSourceFreetext.style.display = "none"; utmSourceFreetext.value = ""; }
      const sources = entry?.sources || [];
      utmSourceSelect.innerHTML = sources.map((src) => `<option value="${escapeHtml(src)}">${escapeHtml(src)}</option>`).join("");
    }
    const showPartner = !!(entry?.hasPartner);
    if (utmPartnerToggleWrap) utmPartnerToggleWrap.style.display = showPartner ? "" : "none";
    if (!showPartner) {
      if (utmPartnerToggle) utmPartnerToggle.checked = false;
      if (utmPartnerInput) { utmPartnerInput.style.display = "none"; utmPartnerInput.value = ""; }
    }
  };

  const syncUtmWriteState = (): void => {
    if (!utmWriteBtn || !utmMediumSelect || !utmSourceSelect || !utmCampaignInput || !utmLinkInput || !utmContentInput || !utmTermInput) return;
    const entry = UTM_MEDIUM_CONFIG.find(m => m.value === utmMediumSelect.value);
    const sourceValue = entry?.sourceType === "freetext"
      ? (utmSourceFreetext?.value ?? "")
      : utmSourceSelect.value;
    const partnerRequired = !!(utmPartnerToggle?.checked);
    const partnerValue = partnerRequired ? (utmPartnerInput?.value ?? "") : "x";
    const ready = [
      utmMediumSelect.value,
      sourceValue,
      utmCampaignInput.value,
      utmLinkInput.value,
      partnerValue,
    ].every((value) => String(value || "").trim() !== "");
    utmWriteBtn.disabled = !ready;
  };

  if (utmMediumSelect && utmSourceSelect && utmWriteBtn && utmCampaignInput && utmLinkInput && utmContentInput && utmTermInput) {
    const mediums = UTM_MEDIUM_CONFIG.map(m => m.value);
    utmMediumSelect.innerHTML = mediums.map((m) => `<option value="${escapeHtml(m)}">${escapeHtml(m)}</option>`).join("");
    setSources(utmMediumSelect.value || mediums[0] || "");
    syncUtmWriteState();

    utmMediumSelect.onchange = () => {
      setSources(utmMediumSelect.value);
      syncUtmWriteState();
    };
    utmSourceSelect.onchange = syncUtmWriteState;
    if (utmSourceFreetext) utmSourceFreetext.oninput = syncUtmWriteState;
    utmCampaignInput.oninput = syncUtmWriteState;
    utmLinkInput.oninput = syncUtmWriteState;
    utmContentInput.oninput = syncUtmWriteState;
    utmTermInput.oninput = syncUtmWriteState;
    if (utmPartnerToggle) {
      utmPartnerToggle.onchange = () => {
        if (utmPartnerInput) utmPartnerInput.style.display = utmPartnerToggle.checked ? "" : "none";
        if (!utmPartnerToggle.checked && utmPartnerInput) utmPartnerInput.value = "";
        syncUtmWriteState();
      };
    }
    if (utmPartnerInput) utmPartnerInput.oninput = syncUtmWriteState;

    utmWriteBtn.onclick = async () => {
      const entry = UTM_MEDIUM_CONFIG.find(m => m.value === utmMediumSelect.value);
      const sourceValue = entry?.sourceType === "freetext"
        ? (utmSourceFreetext?.value || "").trim()
        : (utmSourceSelect.value || "").trim();
      const campaignBase = (utmCampaignInput.value || "").trim();
      const partnerVal = utmPartnerToggle?.checked ? (utmPartnerInput?.value || "").trim() : "";
      const campaignFinal = partnerVal ? `${campaignBase}|${partnerVal}` : campaignBase;
      const payload = {
        utm_medium: (utmMediumSelect.value || "").trim(),
        utm_source: sourceValue,
        utm_campaign: campaignFinal,
        campaign_link: (utmLinkInput.value || "").trim(),
        utm_content: (utmContentInput.value || "").trim(),
        utm_term: (utmTermInput.value || "").trim(),
      };
      if (!payload.utm_medium || !payload.utm_source || !payload.utm_campaign || !payload.campaign_link) {
        if (utmWriteStatus) utmWriteStatus.textContent = "Заполните обязательные поля";
        return;
      }
      utmWriteBtn.disabled = true;
      if (utmWriteStatus) utmWriteStatus.textContent = "Сохраняю...";
      try {
        const resp = await fetch("/api/utm", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await resp.json() as { ok?: boolean; row?: Record<string, unknown>; utm_tag?: string; error?: string };
        if (!resp.ok || !data.ok) {
          if (utmWriteStatus) utmWriteStatus.textContent = `Ошибка записи: ${String(data.error || resp.status)}`;
          return;
        }
        utmSessionRows = data.row ? [data.row] : [];
        setUtmPreview(String(data.row?.["UTM Tag"] ?? data.utm_tag ?? ""));
        if (utmWriteStatus) utmWriteStatus.textContent = "Сохранено";
        await renderTable(view, utmSessionRows, dealsIndex);
      } catch (e) {
        if (utmWriteStatus) utmWriteStatus.textContent = `Ошибка записи: ${String(e)}`;
      } finally {
        utmWriteBtn.disabled = false;
      }
    };
  }

  if (copyUtmRowBtn) {
    copyUtmRowBtn.onclick = async () => {
      if (!utmLatestTag) return;
      try {
        await navigator.clipboard.writeText(utmLatestTag);
        if (utmWriteStatus) utmWriteStatus.textContent = "UTM скопирован";
      } catch (e) {
        if (utmWriteStatus) utmWriteStatus.textContent = `Ошибка копирования: ${String(e)}`;
      }
    };
  }

  if (copyTableBtn) copyTableBtn.onclick = async () => {
    const headers = cols.map((c) => displayColName(view, c));
    const lines = [headers.join("\t")];
    for (const r of visibleRows) {
      lines.push(cols.map((c) => String(r[c] ?? "")).join("\t"));
    }
    try {
      await navigator.clipboard.writeText(lines.join("\n"));
      if (status) status.textContent = `Скопировано: ${visibleRows.length.toLocaleString("ru-RU")} строк`;
    } catch (e) {
      if (status) status.textContent = `Ошибка копирования: ${String(e)}`;
    }
  };
  if (downloadTableBtn) downloadTableBtn.onclick = () => {
    const headers = cols.map((c) => displayColName(view, c));
    const esc = (v: unknown): string => `"${String(v ?? "").replaceAll('"', '""')}"`;
    const csv = [
      headers.map(esc).join(","),
      ...visibleRows.map((r) => cols.map((c) => esc(r[c])).join(",")),
    ].join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `${view}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(a.href);
    if (status) status.textContent = `Скачано: ${visibleRows.length.toLocaleString("ru-RU")} строк`;
  };
  if (isUtmConstructor && !utmLatestTag) {
    const lastFromRows = String(viewRows[0]?.["UTM Tag"] ?? "");
    setUtmPreview(lastFromRows);
  }
  draw();
}

function toConvPct(v: unknown): number {
  const n = num(v);
  return Math.abs(n) <= 1 ? Math.round(n * 1000) / 10 : Math.round(n * 10) / 10;
}

async function renderCharts(dealsIndex: DealsIndex): Promise<void> {
  writeUrlState("charts");
  let chartsMonthsBack = 12;
  let bitrixMonths: Record<string, unknown>[] = [];
  let yandexMonths: Record<string, unknown>[] = [];
  let emailOps: Record<string, unknown>[] = [];
  let managerRows: Record<string, unknown>[] = [];

  app.innerHTML = `<div class="app-layout">
    <aside class="side-menu">
      <button class="side-btn" data-menu="dashboard">Главная</button>
      <button class="side-btn" data-menu="reports">Детальные отчеты</button>
      <button class="side-btn active" data-menu="charts">Графики</button>
      <button class="side-btn" data-menu="utm">UTM Конструктор</button>
    </aside>
    <main class="main-content">
      <header>
        <h1>Графики</h1>
        <p class="sub">Динамика продаж и выручки</p>
      </header>
      <div class="toolbar">
        <label class="date-window-inline">Период: <input class="charts-date-window-slider" type="range" min="1" max="24" step="1" value="${chartsMonthsBack}" /> <span class="charts-date-window-value">${escapeHtml(monthsBackRangeLabel(bitrixMonths, "Месяц", chartsMonthsBack))}</span></label>
      </div>
      <div class="charts-page"></div>
    </main>
  </div>`;

  app.querySelectorAll<HTMLButtonElement>(".side-btn").forEach((btn) => {
    btn.onclick = async () => {
      const m = btn.getAttribute("data-menu");
      if (m === "dashboard" || m === "reports" || m === "charts" || m === "utm") {
        await openMenu(m, dealsIndex, "year_total");
      }
    };
  });

  const CHART_COLORS = {
    revenue: "#6ee7b7",
    avgCheck: "#60a5fa",
    leads: "#a78bfa",
    qual: "#34d399",
    deals: "#f59e0b",
    spend: "#f87171",
    profit: "#6ee7b7",
    emailLeads: "#a78bfa",
    emailRevenue: "#6ee7b7",
    convRate: "#fbbf24",
    refusalRate: "#f87171",
    roi: "#34d399",
    managerBar: "#60a5fa",
  };

  const getXAxisConfig = (dataPointCount: number): Record<string, unknown> => {
    // Dynamically adjust x-axis based on number of data points for better readability
    let maxRotation = 0;
    let maxTicksLimit = undefined;
    let fontSize = 12;
    
    if (dataPointCount <= 3) {
      maxRotation = 0;
      maxTicksLimit = undefined;
      fontSize = 13;
    } else if (dataPointCount <= 6) {
      maxRotation = 45;
      maxTicksLimit = undefined;
      fontSize = 12;
    } else if (dataPointCount <= 12) {
      maxRotation = 45;
      maxTicksLimit = 12;
      fontSize = 11;
    } else if (dataPointCount <= 18) {
      maxRotation = 60;
      maxTicksLimit = 10;
      fontSize = 10;
    } else {
      maxRotation = 90;
      maxTicksLimit = 8;
      fontSize = 9;
    }
    
    return {
      ticks: { 
        color: "#8b92a8", 
        maxRotation,
        minRotation: maxRotation,
        font: { size: fontSize }
      },
      grid: { color: "#2a3142" },
      max: dataPointCount > 0 ? undefined : 0,
    };
  };

  const chartDefaults = {
    responsive: true,
    maintainAspectRatio: true,
    plugins: {
      legend: { labels: { color: "#e8eaef" } },
      tooltip: { mode: "index" as const, intersect: false },
    },
    scales: {
      x: { ticks: { color: "#8b92a8" }, grid: { color: "#2a3142" } },
      y: { ticks: { color: "#8b92a8" }, grid: { color: "#2a3142" } },
    },
  };

  try {
    [bitrixMonths, yandexMonths, emailOps, managerRows] = await Promise.all([
      fetchJson<Record<string, unknown>[]>("data/bitrix_month_total_full.json"),
      fetchJson<Record<string, unknown>[]>("data/global/yandex_projects_revenue_by_month.json"),
      fetchJson<Record<string, unknown>[]>("data/email_operational_summary.json"),
      fetchJson<Record<string, unknown>[]>("data/manager_sales_by_month.json"),
    ]);
  } catch {
    // Handled per-chart below.
  }

  const drawCharts = (): void => {
    const chartsPage = app.querySelector<HTMLElement>(".charts-page");
    if (!chartsPage) return;
    chartsPage.innerHTML = `
      <div class="chart-wrap"><h3>Выручка и средний чек по месяцам (Bitrix)</h3><canvas id="chart-revenue-month"></canvas></div>
      <div class="chart-wrap"><h3>Воронка лидов по месяцам (Bitrix)</h3><canvas id="chart-leads-month"></canvas></div>
      <div class="chart-wrap"><h3>Yandex: расход и выручка по месяцам</h3><canvas id="chart-yandex-month"></canvas></div>
      <div class="chart-wrap"><h3>Email: лиды и выручка по периодам</h3><canvas id="chart-email-period"></canvas></div>
      <div class="chart-wrap"><h3>Конверсия из лидов в квалифицированные (Bitrix)</h3><canvas id="chart-conversion-month"></canvas></div>
      <div class="chart-wrap"><h3>Yandex: ROI по месяцам (выручка / расход)</h3><canvas id="chart-yandex-roi"></canvas></div>
      <div class="chart-wrap chart-wrap--full"><h3>Топ менеджеров по выручке</h3><canvas id="chart-managers-revenue"></canvas></div>
    `;

    try {
      const sorted = filterRowsByMonthsBack(bitrixMonths, "Месяц", chartsMonthsBack)
        .filter((r) => !isTotalValue(r["Месяц"]))
        .sort((a, b) => compareCell("Месяц", a["Месяц"], b["Месяц"], "asc"));
      const monthLabels = sorted.map((r) => String(r["Месяц"] ?? ""));
      const revenueData = sorted.map((r) => num(r["Выручка"]));
      const avgCheckData = sorted.map((r) => num(r["Средний_чек"]));
      const leadsData = sorted.map((r) => num(r["Лиды"]));
      const qualData = sorted.map((r) => num(r["Квал"]));
      const dealsData = sorted.map((r) => num(r["Сделок_с_выручкой"]));
      const xAxisConfig = getXAxisConfig(monthLabels.length);

      const revenueCanvas = app.querySelector<HTMLCanvasElement>("#chart-revenue-month")!;
      new Chart(revenueCanvas, {
        type: "bar",
        data: {
          labels: monthLabels,
          datasets: [
            {
              label: "Выручка, ₽",
              data: revenueData,
              backgroundColor: CHART_COLORS.revenue + "99",
              borderColor: CHART_COLORS.revenue,
              borderWidth: 1,
              yAxisID: "y",
            },
            {
              label: "Средний чек, ₽",
              data: avgCheckData,
              type: "line" as const,
              borderColor: CHART_COLORS.avgCheck,
              backgroundColor: "transparent",
              pointRadius: 3,
              tension: 0.3,
              yAxisID: "y2",
            },
          ],
        },
        options: {
          ...chartDefaults,
          scales: {
            x: xAxisConfig,
            y: { ...chartDefaults.scales.y, position: "left" as const, title: { display: true, text: "Выручка, ₽", color: "#8b92a8" } },
            y2: { ...chartDefaults.scales.y, position: "right" as const, title: { display: true, text: "Средний чек, ₽", color: "#8b92a8" }, grid: { drawOnChartArea: false, color: "#2a3142" } },
          },
        },
      });

      const leadsCanvas = app.querySelector<HTMLCanvasElement>("#chart-leads-month")!;
      new Chart(leadsCanvas, {
        type: "bar",
        data: {
          labels: monthLabels,
          datasets: [
            { label: "Лиды", data: leadsData, backgroundColor: CHART_COLORS.leads + "99", borderColor: CHART_COLORS.leads, borderWidth: 1 },
            { label: "Квал", data: qualData, backgroundColor: CHART_COLORS.qual + "99", borderColor: CHART_COLORS.qual, borderWidth: 1 },
            { label: "Сделок с выручкой", data: dealsData, backgroundColor: CHART_COLORS.deals + "99", borderColor: CHART_COLORS.deals, borderWidth: 1 },
          ],
        },
        options: { ...chartDefaults, scales: { ...chartDefaults.scales, x: xAxisConfig } },
      });

      const convQualData = sorted.map((r) => toConvPct(r["Конверсия в Квал"]));
      const convRefusalData = sorted.map((r) => toConvPct(r["Конверсия в Отказ"]));
      const convCanvas = app.querySelector<HTMLCanvasElement>("#chart-conversion-month")!;
      new Chart(convCanvas, {
        type: "line",
        data: {
          labels: monthLabels,
          datasets: [
            {
              label: "Конверсия в Квал, %",
              data: convQualData,
              borderColor: CHART_COLORS.convRate,
              backgroundColor: CHART_COLORS.convRate + "33",
              pointRadius: 3,
              tension: 0.3,
              fill: true,
            },
            {
              label: "Конверсия в Отказ, %",
              data: convRefusalData,
              borderColor: CHART_COLORS.refusalRate,
              backgroundColor: "transparent",
              pointRadius: 3,
              tension: 0.3,
            },
          ],
        },
        options: {
          ...chartDefaults,
          scales: {
            x: xAxisConfig,
            y: { ...chartDefaults.scales.y, title: { display: true, text: "%", color: "#8b92a8" } },
          },
        },
      });
    } catch {
      const wrap = app.querySelector<HTMLElement>("#chart-revenue-month")?.closest(".chart-wrap");
      if (wrap) wrap.innerHTML += `<p class="muted">Данные недоступны</p>`;
    }

    try {
      const ySorted = filterRowsByMonthsBack(yandexMonths, "month", chartsMonthsBack)
        .filter((r) => !isTotalValue(r["month"]))
        .sort((a, b) => String(a["month"] ?? "").localeCompare(String(b["month"] ?? "")));
      const yLabels = ySorted.map((r) => String(r["month"] ?? ""));
      const yXAxisConfig = getXAxisConfig(yLabels.length);
      const spendData = ySorted.map((r) => {
        const month = String(r["month"] ?? "").trim();
        const m = yandexMonthLeadMetrics.get(month) || yandexEmptyMetrics();
        return m.spend > 0 ? m.spend : num(r["spend"]);
      });
      const yRevenueData = ySorted.map((r) => num(r["revenue_raw"]));

      const yCanvas = app.querySelector<HTMLCanvasElement>("#chart-yandex-month")!;
      new Chart(yCanvas, {
        type: "bar",
        data: {
          labels: yLabels,
          datasets: [
            { label: "Расход, ₽", data: spendData, backgroundColor: CHART_COLORS.spend + "99", borderColor: CHART_COLORS.spend, borderWidth: 1 },
            { label: "Выручка, ₽", data: yRevenueData, backgroundColor: CHART_COLORS.profit + "99", borderColor: CHART_COLORS.profit, borderWidth: 1 },
          ],
        },
        options: { ...chartDefaults, scales: { ...chartDefaults.scales, x: yXAxisConfig } },
      });

      const roiData = ySorted.map((r) => {
        const month = String(r["month"] ?? "").trim();
        const m = yandexMonthLeadMetrics.get(month) || yandexEmptyMetrics();
        const spend = m.spend > 0 ? m.spend : num(r["spend"]);
        const revenue = num(r["revenue_raw"]);
        return spend > 0 ? Math.round((revenue / spend) * 100) / 100 : 0;
      });
      const roiCanvas = app.querySelector<HTMLCanvasElement>("#chart-yandex-roi")!;
      new Chart(roiCanvas, {
        type: "line",
        data: {
          labels: yLabels,
          datasets: [
            {
              label: "ROI (выручка / расход)",
              data: roiData,
              borderColor: CHART_COLORS.roi,
              backgroundColor: CHART_COLORS.roi + "33",
              pointRadius: 4,
              tension: 0.3,
              fill: true,
            },
          ],
        },
        options: {
          ...chartDefaults,
          scales: {
            x: yXAxisConfig,
            y: { ...chartDefaults.scales.y, title: { display: true, text: "ROI", color: "#8b92a8" } },
          },
        },
      });
    } catch {
      const wrap = app.querySelector<HTMLElement>("#chart-yandex-month")?.closest(".chart-wrap");
      if (wrap) wrap.innerHTML += `<p class="muted">Данные недоступны</p>`;
    }

    try {
      const eSorted = filterRowsByMonthsBack(emailOps, "Период", chartsMonthsBack)
        .filter((r) => !isTotalValue(r["Период"]))
        .sort((a, b) => compareCell("Период", a["Период"], b["Период"], "asc"));
      const eLabels = eSorted.map((r) => String(r["Период"] ?? ""));
      const eXAxisConfig = getXAxisConfig(eLabels.length);
      const eLeadsData = eSorted.map((r) => num(r["Лиды"]));
      const eRevenueData = eSorted.map((r) => num(r["Выручка"]));

      const eCanvas = app.querySelector<HTMLCanvasElement>("#chart-email-period")!;
      new Chart(eCanvas, {
        type: "bar",
        data: {
          labels: eLabels,
          datasets: [
            { label: "Лиды", data: eLeadsData, backgroundColor: CHART_COLORS.emailLeads + "99", borderColor: CHART_COLORS.emailLeads, borderWidth: 1, yAxisID: "y" },
            { label: "Выручка, ₽", data: eRevenueData, backgroundColor: CHART_COLORS.emailRevenue + "99", borderColor: CHART_COLORS.emailRevenue, borderWidth: 1, yAxisID: "y2" },
          ],
        },
        options: {
          ...chartDefaults,
          scales: {
            x: eXAxisConfig,
            y: { ...chartDefaults.scales.y, position: "left" as const, title: { display: true, text: "Лиды", color: "#8b92a8" } },
            y2: { ...chartDefaults.scales.y, position: "right" as const, title: { display: true, text: "Выручка, ₽", color: "#8b92a8" }, grid: { drawOnChartArea: false, color: "#2a3142" } },
          },
        },
      });
    } catch {
      const wrap = app.querySelector<HTMLElement>("#chart-email-period")?.closest(".chart-wrap");
      if (wrap) wrap.innerHTML += `<p class="muted">Данные недоступны</p>`;
    }

    try {
      const monthRows = filterRowsByMonthsBack(
        managerRows.filter((r) => String(r["Level"] ?? "").trim() === "Месяц"),
        "Месяц",
        chartsMonthsBack,
      );
      const managerTotals = new Map<string, number>();
      for (const r of monthRows) {
        const name = String(r["Менеджер"] ?? "").trim();
        if (!name || name === "-") continue;
        managerTotals.set(name, (managerTotals.get(name) || 0) + num(r["Выручка"]));
      }
      const sorted = [...managerTotals.entries()].sort((a, b) => b[1] - a[1]).slice(0, 10);
      const mLabels = sorted.map(([name]) => name);
      const mData = sorted.map(([, revenue]) => revenue);
      const mYAxisConfig = getXAxisConfig(mLabels.length); // Y-axis shows labels for horizontal chart
      const mCanvas = app.querySelector<HTMLCanvasElement>("#chart-managers-revenue")!;
      new Chart(mCanvas, {
        type: "bar",
        data: {
          labels: mLabels,
          datasets: [
            {
              label: "Выручка, ₽",
              data: mData,
              backgroundColor: CHART_COLORS.managerBar + "99",
              borderColor: CHART_COLORS.managerBar,
              borderWidth: 1,
            },
          ],
        },
        options: {
          ...chartDefaults,
          indexAxis: "y" as const,
          scales: {
            x: { ...chartDefaults.scales.x, title: { display: true, text: "Выручка, ₽", color: "#8b92a8" } },
            y: mYAxisConfig,
          },
        },
      });
    } catch {
      const wrap = app.querySelector<HTMLElement>("#chart-managers-revenue")?.closest(".chart-wrap");
      if (wrap) wrap.innerHTML += `<p class="muted">Данные недоступны</p>`;
    }
  };

  const slider = app.querySelector<HTMLInputElement>(".charts-date-window-slider");
  const sliderValue = app.querySelector<HTMLElement>(".charts-date-window-value");
  if (slider && sliderValue) {
    slider.oninput = () => {
      const next = Number(slider.value);
      chartsMonthsBack = Number.isFinite(next) && next > 0 ? Math.round(next) : 12;
      sliderValue.textContent = monthsBackRangeLabel(bitrixMonths, "Месяц", chartsMonthsBack);
      drawCharts();
    };
  }

  drawCharts();
}

async function boot(): Promise<void> {
  // Show loading skeleton immediately — before any async operations so the page
  // is never a blank white/black void while waiting for data.
  app.innerHTML = `<div class="boot-loading"><div class="boot-spinner"></div><p class="boot-msg">Загрузка данных…</p></div>`;

  try {
    dealRevenueById.clear();
    yandexProjectLeadMetrics.clear();
    yandexMonthLeadMetrics.clear();

    // Fire-and-forget materialisation — do NOT block page load on it.
    // dataset_json already holds the last materialized data; b24-sync triggers a
    // full rebuild after each ingestion, keeping the cache warm between page loads.
    // The page renders immediately from existing cached rows in the DB.
    void fetch("/api/analytics/materialize", { method: "POST", cache: "no-store" }).catch(() => {});

    // Load email overrides and yandex hierarchy in parallel (independent of each other
    // and of the background materialisation above).
    const [emailOverridesRaw, yandexHierarchy] = await Promise.all([
      fetch(staticUrl("data/email_group_overrides.json"))
        .then((r) => r.json() as Promise<EmailOverridesFile>)
        .catch(() => ({ groups: {} } as EmailOverridesFile)),
      fetchJson<Record<string, unknown>[]>("data/yd_hierarchy.json").catch((err) => {
        console.warn("Yandex hierarchy unavailable; continuing without hierarchy metrics", err);
        return [] as Record<string, unknown>[];
      }),
    ]);

    loadEmailOverridesMap(emailOverridesRaw);
    for (const r of yandexHierarchy) {
      const lvl = String(r["Level"] ?? "").trim();
      if (lvl === "Campaign") {
        const key = mapYandexProjectGroup(r["Название кампании"]);
        if (key) {
          const prev = yandexProjectLeadMetrics.get(key) || yandexEmptyMetrics();
          yandexProjectLeadMetrics.set(key, addYandexMetrics(prev, toYandexMetrics(r)));
        }
      }
      if (lvl === "Month") {
        const key = String(r["month"] ?? r["Месяц"] ?? "").trim();
        if (key) {
          const prev = yandexMonthLeadMetrics.get(key) || yandexEmptyMetrics();
          yandexMonthLeadMetrics.set(key, addYandexMetrics(prev, toYandexMetrics(r)));
        }
      }
    }
    const dealsIndex: DealsIndex = { month: new Map(), event: new Map(), course: new Map() };
    const state = readUrlState();
    if (state.menu === "reports") {
      const v: ViewKey = state.view || "year_total";
      await openTableView(v, dealsIndex);
      return;
    }
    if (state.menu === "utm") {
      await openTableView("utm_constructor", dealsIndex);
      return;
    }
    if (state.menu === "charts") {
      await renderCharts(dealsIndex);
      return;
    }
    await renderDashboard(dealsIndex);
  } catch (e) {
    app.innerHTML = `<div class="err">Ошибка загрузки: ${escapeHtml(String(e))}</div>`;
  }
}

async function renderDashboard(dealsIndex: DealsIndex): Promise<void> {
  writeUrlState("dashboard");
  const [contacts, bitrixWeekFunnel, yandexWeekCampaign, contactsTotals] = await Promise.all([
    fetchJson<Record<string, unknown>[]>("data/bitrix_contacts_uid.json").catch(() => []),
    fetchJson<Record<string, unknown>[]>("data/bitrix_week_funnel_total.json").catch(() => []),
    fetchJson<Record<string, unknown>[]>("data/yandex_week_campaign_total.json").catch(() => []),
    fetchJson<Record<string, unknown>[]>("data/dashboard_contacts_total.json").catch(() => []),
  ]);

  const expandedWeeks = new Set<string>();
  const expandedYandexWeeks = new Set<string>();
  const totalsRow = contactsTotals[0] || {};
  const bitrixContactsActual = Math.round(num(totalsRow["bitrix_contacts_actual"])) || contacts.length;
  const emailContactsActual = Math.round(num(totalsRow["email_contacts_actual"]));
  const totalContactsActual = bitrixContactsActual + emailContactsActual;
  const yandexWeekFiltered = yandexWeekCampaign.filter((r) => {
    const campaignId = String(r["ID кампании"] ?? "").trim();
    const hasAd = campaignId !== "" && campaignId !== "(пусто)" && campaignId !== "-";
    const hasSpend = num(r["Расход, ₽"]) > 0;
    return hasAd || hasSpend;
  });

  const maxIsoDate = (rows: Record<string, unknown>[], col: string): string => {
    let best = "";
    for (const row of rows) {
      const s = String(row[col] ?? "").trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) continue;
      if (!best || s > best) best = s;
    }
    return best;
  };

  const drawDashboard = (): void => {
    const latestBitrixRecordDate = String(bitrixWeekFunnel[0]?.["Дата_последней_записи_Bitrix"] ?? "-").trim() || "-";
    const latestYandexRecordDate =
      maxIsoDate(yandexWeekCampaign, "Дата_последней_записи_Yandex") ||
      maxIsoDate(yandexWeekCampaign, "Макс_дата_в_строке") ||
      "-";

    app.innerHTML = `<div class="app-layout">
      <aside class="side-menu">
        <button class="side-btn active" data-menu="dashboard">Главная</button>
        <button class="side-btn" data-menu="reports">Детальные отчеты</button>
        <button class="side-btn" data-menu="charts">Графики</button>
        <button class="side-btn" data-menu="utm">UTM Конструктор</button>
      </aside>
      <main class="main-content">
        <header>
          <h1>Главный дашборд</h1>
          <p class="sub">Срез: последние 7 дней от последней доступной записи</p>
        </header>
        <div class="kpi-grid">
          <div class="kpi"><div class="label">Контакты Bitrix</div><div class="value">${bitrixContactsActual.toLocaleString("ru-RU")}</div></div>
          <div class="kpi"><div class="label">Контакты Email</div><div class="value">${emailContactsActual.toLocaleString("ru-RU")}</div></div>
          <div class="kpi"><div class="label">Последняя запись Bitrix</div><div class="value">${escapeHtml(latestBitrixRecordDate)}</div></div>
          <div class="kpi"><div class="label">Последняя запись Yandex</div><div class="value">${escapeHtml(latestYandexRecordDate)}</div></div>
        </div>
        ${renderWeeklyBitrixExpandableTable(bitrixWeekFunnel, expandedWeeks)}
        ${renderWeeklyYandexExpandableTable(yandexWeekFiltered, expandedYandexWeeks)}
      </main>
    </div>`;

    const bitrixWrap = app.querySelector<HTMLElement>(".bitrix-weeks");
    if (bitrixWrap) {
      bitrixWrap.querySelectorAll<HTMLButtonElement>(".week-expand-btn").forEach((btn) => {
        btn.onclick = () => {
          const week = btn.getAttribute("data-week") || "";
          if (!week) return;
          if (expandedWeeks.has(week)) expandedWeeks.delete(week);
          else expandedWeeks.add(week);
          drawDashboard();
        };
      });

      const bitrixExpandAllBtn = bitrixWrap.querySelector<HTMLButtonElement>(".week-expand-all-btn");
      if (bitrixExpandAllBtn) {
        bitrixExpandAllBtn.onclick = () => {
          const weeks = [...new Set(bitrixWeekFunnel.map((r) => String(r["Неделя"] ?? "").trim()).filter(Boolean))];
          const allOpen = weeks.length > 0 && weeks.every((w) => expandedWeeks.has(w));
          weeks.forEach((w) => allOpen ? expandedWeeks.delete(w) : expandedWeeks.add(w));
          drawDashboard();
        };
      }
    }

    const yandexWrap = app.querySelector<HTMLElement>(".yandex-weeks");
    if (yandexWrap) {
      yandexWrap.querySelectorAll<HTMLButtonElement>(".yweek-expand-btn").forEach((btn) => {
        btn.onclick = () => {
          const week = btn.getAttribute("data-week") || "";
          if (!week) return;
          if (expandedYandexWeeks.has(week)) expandedYandexWeeks.delete(week);
          else expandedYandexWeeks.add(week);
          drawDashboard();
        };
      });

      const yandexExpandAllBtn = yandexWrap.querySelector<HTMLButtonElement>(".yweek-expand-all-btn");
      if (yandexExpandAllBtn) {
        yandexExpandAllBtn.onclick = () => {
          const weeks = [...new Set(yandexWeekFiltered.map((r) => String(r["Неделя"] ?? "").trim()).filter(Boolean))];
          const allOpen = weeks.length > 0 && weeks.every((w) => expandedYandexWeeks.has(w));
          weeks.forEach((w) => allOpen ? expandedYandexWeeks.delete(w) : expandedYandexWeeks.add(w));
          drawDashboard();
        };
      }
    }

    app.querySelectorAll<HTMLButtonElement>(".side-btn").forEach((btn) => {
      btn.onclick = async () => {
        const m = btn.getAttribute("data-menu");
        if (m === "dashboard" || m === "reports" || m === "charts" || m === "utm") {
          await openMenu(m, dealsIndex, "year_total");
        }
      };
    });
  };

  drawDashboard();
}

void boot();

