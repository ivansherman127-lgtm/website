import { parseAmount } from "./amt";

/** B2C «Рассрочка» stage for this portal (matches March 2026 CSV ↔ raw_b24 STAGE_ID). */
const DEFAULT_EXTRA_VARIANT3_STAGES = new Set(["C7:UC_P7HXNZ"]);

/**
 * API ``STAGE_ID`` equivalent of CSV variant3 + positive amount.
 * Mirrors ``variant3_api_revenue_mask`` in ``db/revenue_variant3.py``.
 */
export function variant3ApiRevenueMask(row: { sum_text: string; stage_raw: string }): boolean {
  const amt = parseAmount(row.sum_text);
  if (amt <= 0) return false;
  const s = (row.stage_raw || "").trim();
  if (!s) return false;
  const up = s.toUpperCase();
  if (up === "WON" || up.endsWith("WON")) return true;
  if (DEFAULT_EXTRA_VARIANT3_STAGES.has(up)) return true;
  const sl = s.toLowerCase();
  return (
    sl.includes("сделка заключена") || sl.includes("постоплат") || sl.includes("рассроч")
  );
}

/** @deprecated Prefer ``variant3ApiRevenueMask`` for mart parity with Python API pipeline. */
export function paymentVerifiedRevenueMask(row: {
  sum_text: string;
  pay_date: string;
  installment_schedule: string;
}): boolean {
  const amt = parseAmount(row.sum_text);
  if (amt <= 0) return false;
  const junk = new Set(["", "-", "nan", "none", "null", "undefined"]);
  const pay = (row.pay_date || "").trim();
  const inst = (row.installment_schedule || "").trim();
  const payOk = pay.length > 0 && !junk.has(pay.toLowerCase());
  const instOk = inst.length > 0 && !junk.has(inst.toLowerCase());
  return payOk || instOk;
}

/** @deprecated Name kept for imports; delegates to ``variant3ApiRevenueMask``. */
export function variant3RevenueMask(row: {
  stage_raw: string;
  closed_yes: string;
  pay_date: string;
  installment_schedule: string;
  sum_text: string;
}): boolean {
  return variant3ApiRevenueMask({ sum_text: row.sum_text, stage_raw: row.stage_raw });
}
