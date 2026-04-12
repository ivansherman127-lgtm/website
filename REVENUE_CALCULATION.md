# Revenue Calculation Logic

## Two Methods Compared

### 1. Variant3 (Legacy)

Location: `db/revenue_variant3.py`

**Logic:**

A deal is counted as "paid" (revenue) if `Стадия сделки` (deal stage) contains any of:
- "Сделка заключена" (Deal closed)
- "постоплат" (Post-payment)
- "рассрочка" (Installment/Payment plan)

```python
def variant3_revenue_mask(df):
    stage = df["Стадия сделки"].fillna("").str.lower()
    return (
        stage.str.contains("сделка заключена") |
        stage.str.contains("постоплат") |
        stage.str.contains("рассроч")
    )
```

**Problems:**
- Only checks stage name - does NOT verify actual payment
- Deals may be marked "Сделка заключена" but never actually paid
- Does not track payment date (when money was received)
- No temporal relationship to registration date

---

### 2. New Method (Paid After Registration)

Used for: Group consultations (career consultations) associated revenue

**Logic:**

A deal is counted as "paid" ONLY when:

1. **Payment date exists:** `Дата оплаты` is not empty
2. **Payment date ≥ Registration date:** The deal's `Дата оплаты` is AFTER when the contact first registered on the target URL

```python
# 1. Find contacts who registered on target URLs
# Target URLs: /sl-careerconsult, /vstrecha-s-nastavnikom, /consult_soc, etc.

# 2. Get earliest registration date per contact
reg_dates = deals.groupby('Контакт')['Дата создания'].min()

# 3. Filter deals with payment date
paid_deals = deals[deals['Дата оплаты'].notna()]

# 4. Compare payment date vs registration date
paid_deals['pay_dt'] = pd.to_datetime(paid_deals['Дата оплаты'], dayfirst=True)
paid_deals['reg_dt'] = paid_deals['Контакт'].map(reg_dates)
paid_deals['reg_dt'] = pd.to_datetime(paid_deals['reg_dt'], dayfirst=True)

# 5. Only count if payment AFTER registration
valid_paid = paid_deals[paid_deals['pay_dt'] >= paid_deals['reg_dt']]
```

**Key Differences:**

| Aspect | Variant3 | New Method |
|-------|---------|----------|
| What triggers revenue | Deal stage = "Заключена" | Actual payment date exists |
| Payment verification | No | Yes (Дата оплаты populated) |
| Timing | None | Payment must be AFTER registration |
| Excludes unpaid deals | No | Yes |
| False positives | Yes (deals marked "заключена" but unpaid) | Minimal |

---

## Filters Applied

Both methods also apply these exclusions:
- Exclude funnel "Учебный центр" (Education Center)
- Target periods only: January 2026, March 2026, April 2026
- Target source URLs (in `Дополнительно об источнике`):
  - `/sl-careerconsult`
  - `/vstrecha-s-nastavnikom`
  - `/consult_soc`
  - `/consult_r-301`

---

## Example Impact

A contact registers on `/consult_soc` on March 5, 2026.

- Deal A: Stage = "Сделка заключена", Дата оплаты = empty → **Variant3: counts as revenue, New: 0**
- Deal B: Stage = "Новая", Дата оплаты = March 10, 2026 → **Variant3: 0, New: counts if payment after registration**
- Deal C: Stage = "Сделка заключена", Дата оплаты = March 3, 2026 (BEFORE reg) → **Variant3: counts, New: 0**

New method is more conservative and accurate for attributing revenue to campaign attribution.