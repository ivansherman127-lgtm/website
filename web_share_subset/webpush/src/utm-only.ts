import "./utm-only.css";
import mediumConfigJson from "../functions/api/utm_medium_sources.json";

type MediumEntry = {
  value: string;
  label: string;
  sourceType: "select" | "freetext";
  hasPartner?: boolean;
  sources: string[];
};

type UtmRow = {
  "Дата создания": string;
  "Создал"?: string;
  "UTM Source": string;
  "UTM Medium": string;
  "UTM Campaign": string;
  Link: string;
  "UTM Content": string;
  "UTM Term": string;
  "UTM Tag": string;
};

const MEDIUMS: MediumEntry[] = (mediumConfigJson as { mediums: MediumEntry[] }).mediums;
const app = document.querySelector<HTMLDivElement>("#app");

if (!app) {
  throw new Error("#app not found");
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function prettyColName(col: string): string {
  return col.replaceAll("_", " ");
}

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    cache: "no-store",
    ...init,
  });
  const body = await response.text();
  let data: unknown = null;
  try {
    data = body ? JSON.parse(body) : null;
  } catch {
    throw new Error(`Unexpected response: ${body.slice(0, 120)}`);
  }
  if (!response.ok) {
    const msg = typeof data === "object" && data && "error" in data ? String((data as Record<string, unknown>).error) : String(response.status);
    throw new Error(msg);
  }
  return data as T;
}

function renderRows(rows: UtmRow[]): string {
  if (!rows.length) return '<p class="muted">Пока нет записей</p>';
  const cols = Object.keys(rows[0]) as Array<keyof UtmRow>;
  const head = cols.map((c) => `<th>${escapeHtml(prettyColName(String(c)))}</th>`).join("");
  const body = rows
    .map((row) => `<tr>${cols.map((c) => `<td>${escapeHtml(String(row[c] ?? ""))}</td>`).join("")}</tr>`)
    .join("");
  return `<div class="table-wrap"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function renderShell(): void {
  app.innerHTML = `
    <main class="page">
      <section class="panel">
        <h1>UTM Builder</h1>
        <p class="muted">Отдельная страница без аналитических данных.</p>

        <div class="grid">
          <label><span class="label-text">Medium <span class="required-marker">*</span></span>
            <select class="medium-select"></select>
          </label>
          <label><span class="label-text">Source <span class="required-marker">*</span></span>
            <select class="source-select"></select>
            <input class="source-freetext" type="text" placeholder="Введите источник" style="display:none" />
          </label>
          <label><span class="label-text">Name (Campaign) <span class="required-marker">*</span></span>
            <input class="campaign-input" type="text" placeholder="Например, spring_sale_2026" />
          </label>
          <label><span class="label-text">Partner</span>
            <input class="partner-input" type="text" placeholder="Опционально" />
          </label>
          <label><span class="label-text">Автор</span>
            <input class="created-by-input" type="text" placeholder="Автор (опционально)" />
          </label>
          <label><span class="label-text">Link <span class="required-marker">*</span></span>
            <input class="link-input" type="url" placeholder="https://example.com/campaign" />
          </label>
          <label><span class="label-text">Content</span>
            <input class="content-input" type="text" placeholder="Например, banner_a" />
          </label>
          <label><span class="label-text">Term</span>
            <input class="term-input" type="text" placeholder="Например, python_course" />
          </label>
        </div>

        <div class="actions">
          <button class="write-btn" disabled>write</button>
          <span class="status muted"></span>
        </div>

        <div class="preview muted"></div>
        <div class="rows"></div>
      </section>
    </main>
  `;
}

async function main(): Promise<void> {
  renderShell();

  const mediumSelect = app.querySelector<HTMLSelectElement>(".medium-select")!;
  const sourceSelect = app.querySelector<HTMLSelectElement>(".source-select")!;
  const sourceFreetext = app.querySelector<HTMLInputElement>(".source-freetext")!;
  const campaignInput = app.querySelector<HTMLInputElement>(".campaign-input")!;
  const partnerInput = app.querySelector<HTMLInputElement>(".partner-input")!;
  const linkInput = app.querySelector<HTMLInputElement>(".link-input")!;
  const contentInput = app.querySelector<HTMLInputElement>(".content-input")!;
  const termInput = app.querySelector<HTMLInputElement>(".term-input")!;
  const createdByInput = app.querySelector<HTMLInputElement>(".created-by-input")!;
  const writeBtn = app.querySelector<HTMLButtonElement>(".write-btn")!;
  const status = app.querySelector<HTMLSpanElement>(".status")!;
  const preview = app.querySelector<HTMLDivElement>(".preview")!;
  const rowsContainer = app.querySelector<HTMLDivElement>(".rows")!;

  const setStatus = (text: string, kind: "muted" | "success" | "error" = "muted"): void => {
    status.textContent = text;
    status.className = `status ${kind}`;
  };

  const refreshRows = async (): Promise<void> => {
    try {
      const rows = await fetchJson<UtmRow[]>("/api/utm");
      rowsContainer.innerHTML = renderRows(rows);
    } catch (err) {
      rowsContainer.innerHTML = `<p class="status error">Ошибка загрузки истории: ${escapeHtml(String(err))}</p>`;
    }
  };

  const setSources = (medium: string): void => {
    const entry = MEDIUMS.find((m) => m.value === medium);
    if (!entry) {
      sourceSelect.innerHTML = "";
      sourceSelect.style.display = "";
      sourceFreetext.style.display = "none";
      sourceFreetext.value = "";
      partnerInput.value = "";
      partnerInput.disabled = true;
      return;
    }

    if (entry.sourceType === "freetext") {
      sourceSelect.style.display = "none";
      sourceFreetext.style.display = "";
      sourceFreetext.value = "";
    } else {
      sourceSelect.style.display = "";
      sourceFreetext.style.display = "none";
      sourceFreetext.value = "";
      sourceSelect.innerHTML = entry.sources
        .map((src) => `<option value="${escapeHtml(src)}">${escapeHtml(src)}</option>`)
        .join("");
    }

    if (entry.hasPartner) {
      partnerInput.disabled = false;
      partnerInput.placeholder = "Partner (опционально)";
    } else {
      partnerInput.value = "";
      partnerInput.disabled = true;
      partnerInput.placeholder = "Недоступно для выбранного medium";
    }
  };

  const syncWriteState = (): void => {
    const entry = MEDIUMS.find((m) => m.value === mediumSelect.value);
    const source = entry?.sourceType === "freetext" ? sourceFreetext.value : sourceSelect.value;
    const ready = [mediumSelect.value, source, campaignInput.value, linkInput.value]
      .every((value) => String(value || "").trim() !== "");
    writeBtn.disabled = !ready;
  };

  mediumSelect.innerHTML = MEDIUMS
    .map((m) => `<option value="${escapeHtml(m.value)}">${escapeHtml(m.label || m.value)}</option>`)
    .join("");
  setSources(mediumSelect.value);

  mediumSelect.onchange = () => {
    setSources(mediumSelect.value);
    syncWriteState();
  };
  sourceSelect.onchange = syncWriteState;
  sourceFreetext.oninput = syncWriteState;
  campaignInput.oninput = syncWriteState;
  linkInput.oninput = syncWriteState;
  contentInput.oninput = syncWriteState;
  termInput.oninput = syncWriteState;
  partnerInput.oninput = syncWriteState;

  writeBtn.onclick = async () => {
    const entry = MEDIUMS.find((m) => m.value === mediumSelect.value);
    const source = entry?.sourceType === "freetext" ? sourceFreetext.value.trim() : sourceSelect.value.trim();
    const partner = entry?.hasPartner ? partnerInput.value.trim() : "";
    const campaign = [campaignInput.value.trim(), partner].filter(Boolean).join("|");

    const payload = {
      utm_medium: mediumSelect.value.trim(),
      utm_source: source,
      utm_campaign: campaign,
      campaign_link: linkInput.value.trim(),
      utm_content: contentInput.value.trim(),
      utm_term: termInput.value.trim(),
      created_by: createdByInput.value.trim(),
    };

    if (!payload.utm_medium || !payload.utm_source || !payload.utm_campaign || !payload.campaign_link) {
      setStatus("Заполните обязательные поля", "error");
      return;
    }

    setStatus("Сохраняю...");
    writeBtn.disabled = true;

    try {
      const response = await fetchJson<{ ok?: boolean; row?: UtmRow; utm_tag?: string }>("/api/utm", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      });

      const tag = String(response.row?.["UTM Tag"] || response.utm_tag || "");
      preview.textContent = tag || "Тег не вернулся в ответе";
      setStatus("Сохранено", "success");
      await refreshRows();
    } catch (err) {
      setStatus(`Ошибка записи: ${String(err)}`, "error");
    } finally {
      syncWriteState();
    }
  };

  preview.textContent = "После записи здесь появится UTM тег";
  await refreshRows();
  syncWriteState();
}

void main();
