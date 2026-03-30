/** Port of db/event_classifier.normalize_course_code */
export function normalizeCourseCode(raw: unknown): string {
  let s0 = raw === null || raw === undefined ? "" : String(raw);
  let s = s0.trim().toUpperCase().replace(/Ё/g, "Е");
  if (!s) return "";

  s = s.replace(
    /(КОД\s*КУРСА|COURSE\s*CODE|CODE\s*DU\s*COURS|CODIGO\s*DO\s*CURSO|KURSUN\s*KODU|MA\s*KHOA\s*HOC|КОД\s*МЕРОПРИЯТИЯ|EVENT\s*CODE|ACTIVITY\s*CODE)\s*[:\-]?\s*/gi,
    "",
  );

  s = s.replace(/_/g, "-");
  s = s.replace(/\s+/g, "");

  const literalMap: Record<string, string> = {
    ATTACKINGJANUARY: "ATTACKINGJANUARY",
    ZIMOWN: "ZIM_OWN",
    MAINPAGE: "MAIN_PAGE",
    FORMINACTIVE: "FORM_INACTIVE",
    PHD21: "PHD21",
  };
  if (literalMap[s]) return literalMap[s];

  const m = /^([A-ZА-Я])[-_ ]?(\d{3})(?:[-_ ]?([A-ZА-Я0-9]{1,4}))?/u.exec(s);
  if (m) {
    const base = `${m[1]}-${m[2]}`;
    const suffix = m[3];
    return suffix ? `${base}-${suffix}` : base;
  }

  const mw = /\bW[-_ ]?(\d{1,3})\b/i.exec(s);
  if (mw) return `W-${parseInt(mw[1], 10)}`;

  const cleaned = s.replace(/[^A-ZА-Я0-9]+/gu, "_").replace(/^_+|_+$/g, "");
  return cleaned;
}
