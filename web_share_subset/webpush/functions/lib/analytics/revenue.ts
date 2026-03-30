/**
 * Canonical variant3 revenue mask — mirror db/revenue_variant3.py variant3_revenue_mask.
 */
export function variant3RevenueMask(row: {
  stage_raw: string;
  closed_yes: string;
  pay_date: string;
  installment_schedule: string;
}): boolean {
  const stage = (row.stage_raw || "").toLowerCase();
  const closedFlag = (row.closed_yes || "").trim().toLowerCase() === "да";
  const closedStage = stage.includes("сделка заключена");
  const postStage = stage.includes("постоплат");
  const instStage = stage.includes("рассроч");
  const payDates = (row.installment_schedule || "").trim() !== "";
  const payDatePresent = (row.pay_date || "").trim() !== "";
  const core =
    closedFlag ||
    closedStage ||
    ((postStage || instStage) && payDates);
  return core && payDatePresent;
}
