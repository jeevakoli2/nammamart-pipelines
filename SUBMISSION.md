# NammaMart ETL & ELT — Submission (owner: `jay`)

**Deliverables:** `dags/nammamart_etl_jay.py`, `dags/nammamart_elt_jay.py`, `include/nammamart_common.py`, `include/sql/elt_*.sql` (17 files), `experiments/run_experiments.py`.
**Warehouse:** `include/warehouse_jay.duckdb` (git-ignored: raw layer holds unmasked customer data by design).

**How this was verified.** Both DAGs were run end-to-end with Airflow 3.3.2 (`airflow dags test`, plus `airflow standalone` for Experiment 10) on a local install, *not* inside Astro/Docker. Numbers below are from those runs. Screenshots still have to be taken in your Astro UI (see the checklist at the end).

**Headline result.** On the same September data, ETL and ELT agree exactly: delivered revenue ₹1,060,547.95, gross margin ₹260,291.14, identical rejected-row counts for all 23 (file, reason) combinations, identical counts for every one of the 44 `dq_results` checks, identical reconciliation tables (also identical after the increment: ₹1,056,885.25).

---

## Part 0 — Data Issues Log

Row-level detail lives in `etl_quarantine` / `mart_rejected_rows` (critical) and `dq_results` (counts, warnings). Example keys shown.

| File | Row key (examples) | Column | Problem | Dimension | Rule | Sev. | Rows |
|---|---|---|---|---|---|---|---|
| orders | ORD… (12 ids) | order_id | exact duplicate rows | Uniqueness | `orders.order_id_unique` | Critical | 12 |
| orders | – | customer_id | null | Completeness | `orders.customer_id_not_null` | Critical | 18 |
| orders | – | delivery_minutes | DELIVERED but blank | Completeness | `orders.delivered_has_minutes` | Critical | 8 |
| orders | ORD000403, ORD000362, ORD001610, ORD002012 | order_ts | `N/A`, blank, `2026-13-05…`, `31/09/2026 10:00` | Validity | `orders.order_ts_valid` | Critical | 6 |
| orders | – | order_status | `Pending??` (invalid); `delivered` (casing) | Validity / Consistency | `status_valid` / `status_casing` | Critical / Warning | 3 / 2 |
| orders | – | payment_mode | `upi`, `UPI `, `CARD`, `cod`, `Cashh` | Consistency | `payment_mode_variant` (mapped) | Warning | 8 |
| orders | – | discount_pct | up to 250 | Validity | `orders.discount_range` | Critical | 3 |
| orders | – | quantity | ≤ 0 (min −3) | Accuracy | `orders.quantity_positive` | Critical | 10 |
| orders | – | order_amount | ≠ qty × price × (1−disc) | Accuracy | `orders.amount_formula` | Critical | 18 |
| orders | – | delivery_minutes | 480 / 900 / 1440 | Accuracy | `delivery_minutes_lt_120` | Critical | 6 |
| orders | – | product_id / store_id | `P9xxx` (9 ids) / `S99,S17,S00` | Consistency | `product_fk` / `store_fk` | Critical | 9 / 5 |
| orders | – | order_ts | after 2026-10-20 (up to 2027-01-01) | Timeliness | `orders.ts_not_future` | Critical | 7 |
| payments | 4 ids | payment_id | duplicate | Uniqueness | `payments.payment_id_unique` | Critical | 4 |
| payments | – | order_id | no such order | Consistency | `payments.order_exists` | Critical | 8 |
| payments | – | amount_paid | SUCCESS with amount ≤ 0 (mirrors negative-qty orders) | Accuracy | `payments.amount_positive` | Critical | 10 |
| payments | – | payment_ts | future (2027) | Timeliness | `payments.payment_ts_not_future` | Critical | 7 |
| payments | – | payment_mode | spelling variants | Consistency | `payment_mode_variant` | Warning | 8 |
| payments | – | (vs orders) | 44 FAILED payments on DELIVERED orders; 27 DELIVERED/RETURNED orders with no payment; 13 amount mismatches | Consistency | reconciliation table | – | 84 |
| customers | C00036, C00103, C00226, C00252, C00313 | customer_id | duplicate (5) | Uniqueness | `customers.customer_id_unique` | Critical | 5 |
| customers | – | signup_date | 2026-11-12, 2026-12-20, 2027-01-05 | Timeliness | `signup_not_future` | Critical | 3 |
| customers | – | email / phone / pincode | 12 bad emails (`N/A`, missing `@`), 7 bad phones, `5600XX` | Validity | `*_format` | Warning | 12 / 7 / 3 |
| customers | – | city; loyalty_tier; email | 6 null; `BRONZE`/`bronze`/`SILVER`; 8 shared emails | Completeness / Consistency / Uniqueness | `city_not_null`, `loyalty_tier_casing`, `email_unique` | Warning | 6 / 8 / 8 |
| products | P0030, P0140 | product_id | duplicated with an `(old)` row (different mrp) | Uniqueness | `products.product_id_unique` | Critical | 2 |
| products | P0051, P0060, P0068 | category | null | Completeness | `products.category_not_null` | Critical | 3 |
| products | – | mrp / cost_price | mrp ≤ 0 (1); cost > mrp (5) | Accuracy | `mrp_positive`, `cost_le_mrp` | Critical | 1 / 5 |
| stores | S07 | zone | null (176 orders) | Completeness | `stores.zone_not_null` → loaded as `Unknown` | Warning | 1 |
| stores | S12 | capacity_orders_per_day | 0 | Accuracy | `stores.capacity_positive` | Warning | 1 |

**Questions for the client**
1. Is a negative quantity a *return* or a data-entry error? (I reject all 10 as errors.)
2. **118 DELIVERED orders (₹76,446.43 revenue) are rejected only because their product's master row failed a rule** (3 products with no category, 5 with cost > mrp). Should a bad cost price or missing category block *revenue*, or only *margin*? This is the biggest judgement call in the build.
3. 42 DELIVERED orders (₹25,722.25) have a FAILED payment. Is that revenue, or an unpaid delivery? Also: are `Cashh` and `cod` both Cash on Delivery (I mapped them), and who owns the missing zone of store S07?

## Part A / B / D — what was built

* **ETL** (`nammamart_etl_jay`): 5 parallel `extract_*` → `validate_*` (masters in parallel, orders after the masters, payments in parallel) → `transform` → `load_warehouse` → `reconcile_payments` → `report` → `run_audit`. Star schema `etl_fact_orders` + `etl_dim_customer/product/store`; quarantine CSVs in `include/quarantine/jay/` with `failure_reason`; also `etl_quarantine`. Fact load is an upsert guarded by source rank so an older file can never overwrite a newer correction.
* **ELT** (`nammamart_elt_jay`): `extract_*` → `load_raw` (text, append-only, `_loaded_at/_source_file/_run_id`) → one task per SQL file: `stg_*` (SQL only), `MERGE INTO stg_orders` for the increment, `dq_results`, 7 `mart_*` tables. Marts: the five required + `mart_revenue_by_payment_mode` + `mart_rejected_rows` (needed for business questions 4 and 6).
* **Quality (Part D):** one catalog (`DQ_CHECKS`, 44 checks, all six dimensions, per-table, Critical/Warning) drives both pipelines; `dq_results` has the exact required columns and is written on every run by both DAGs.
* **Design decisions to review:** store `zone` missing is a *Warning* (loaded as `Unknown`) so one master-data gap cannot hide 176 orders; the same cascade for product rows is **Critical** as the assignment specifies (see question 2). Increment corrections that fail a critical check do not overwrite the good row but are kept in `stg_orders_rejected_corrections` (ELT) / quarantine (ETL) — found and fixed during testing, ELT originally dropped them silently.

## Part C — Scheduling

* **Cron:** `0 9 * * 1-6`, `start_date=pendulum.datetime(2026,10,1,tz="Asia/Kolkata")`, same in both DAGs.
* **UTC in the UI:** **03:30 UTC** (09:00 IST = UTC+5:30). Airflow itself reported `next_dagrun 03:30:00+00:00` for both DAGs.
* **If `start_date` were UTC:** the cron is interpreted in UTC, so the run fires at 09:00 UTC = **14:30 IST** — five hours after the 09:30 stand-up. Silent SLA breach, no error.
* **If the laptop/Docker is off at 09:00:** nothing runs and nothing alerts. `catchup=False` means missed days are never back-filled; at most the most recent missed slot is run once when the scheduler is back (after a fresh start Airflow's next run was already the current day's 03:30 UTC slot). So the SLA needs an always-on host plus an alert on a missing/late run.
* **`catchup=False` justification:** each run reads the *current* files and produces a point-in-time snapshot; back-filling 30 mornings would load identical data 30 times. Loads are idempotent anyway, so it would only waste compute.
* **Retries:** `retries=2`, `retry_delay=1 min`, `execution_timeout=10 min` — three attempts fit inside ~3 minutes, leaving ~25 min of slack before 09:30; the timeout stops a hung task from consuming it.

## Business questions (results from both pipelines, September)

| # | Query on final tables | Result (ETL = ELT) |
|---|---|---|
| 1 | `SELECT order_date, zone, sum(net_revenue) FROM mart_daily_store_kpis GROUP BY 1,2` (store level: same table) | East 273,415.95 · West 270,878.60 · South 226,467.30 · North 215,968.05 · Unknown (S07) 73,818.05 |
| 2 | `SELECT * FROM mart_category_margin` | Staples 246,178.95 rev / 62,198.41 margin (25.27%) · Personal Care 199,390.50 / 44,226.77 · Household 198,202.35 / 50,848.09 · Dairy & Bakery 167,733.65 / 44,415.46 · Snacks & Bev. 161,761.95 / 37,692.95 · Fruit & Veg 87,280.55 / 20,909.46 |
| 3 | `SELECT * FROM mart_delivery_sla` | 21.54 min mean of store averages; **28.13 %** of delivered orders within 15 min |
| 4 | `mart_revenue_by_payment_mode`; `SELECT sum(unreconciled_amount) FROM mart_payment_reconciliation` | UPI 580,448.90 · Card 213,759.95 · COD 151,262.40 · Wallet 115,076.70. Unreconciled **₹208,315.80**, of which ₹160,134.63 is payments for orders *we* rejected; **₹48,181.17 is true mismatch** (missing payment 17,193.35; failed 25,722.25; amount 685.00; orphan 4,580.57) |
| 5 | `SELECT * FROM mart_top_customers` | #1 C00136 ₹8,738.10 (`sa***@example.com`, Bronze), #2 C00310 ₹7,258.95, #3 C00236 ₹7,016.40 (Platinum) |
| 6 | `SELECT source_file, failure_reason, count(*) FROM mart_rejected_rows GROUP BY 1,2` | 280 rows rejected → 297 (row, reason) pairs; top reason `orders.product_fk` 135 |

## Part E — Governance

**1. Who owns what.**
Orders and payments belong to the Ops and Finance teams (payments mostly to Finance). Customers belong to whoever runs the CRM or marketing side. Products belong to the catalogue team and stores belong to Store Ops. The warehouse tables are owned by the data team, which is us for now. If someone wants to change a rule, like the 120-minute delivery limit, the owner of that metric should approve it, and Finance should be asked too. Every rule lives in one list in `nammamart_common.py`, so each change shows up in Git history.

**2. Data dictionary (short version).**
- `etl_fact_orders`: one row per order. It has order_id, order date and time, customer, product and store IDs, quantity, price, discount, order amount, payment mode, status, channel and delivery minutes. I also added net_revenue (the order amount if the order was delivered, otherwise 0), gross_margin (revenue minus quantity times cost price) and a flag for "delivered within 15 minutes".
- `etl_dim_customer`: customer_id, masked name, masked email, masked phone, city, area, pincode, signup date, loyalty tier and active flag.
- `etl_dim_product`: product details like name, category, brand, mrp, cost price and unit.
- `etl_dim_store`: store details like name, zone, area, manager and daily capacity.

No raw phone numbers or emails are stored in any of these tables.

**3. Lineage (tracing a number back to the source).**
In the ELT pipeline every row keeps the file name, run id and load time, so I can go from a mart number to the staging row to the raw row with simple joins. In the ETL pipeline the fact table only keeps the source file and run id, not the original row, so tracing takes more work. ELT is easier here.

**4. Personal data.**
Name, email and phone are personal data. Pincode and area are also sensitive because they can point to a person. I mask them (for example `pr***@example.com` and `98******21`) instead of deleting them, so analysts can still count and join customers without seeing real contact details. The law that applies in India is the Digital Personal Data Protection Act, 2023. It means we should collect only what we need, keep it only as long as needed, protect it, and delete it when asked.

**5. Who can see what.**
Raw data: only the pipeline account and one named data steward. Staging: the data team. Marts: Ops, Finance and leadership. Quarantine files: the data team and Finance reviewers (the customer quarantine file is masked). Nobody should read raw phone numbers day to day. If it is ever needed, the steward does it and writes down the reason.

**6. How long to keep data.**
Raw data for as long as Finance needs it for audit (I suggest 7 years, but Finance and legal should confirm). Quarantine files until someone has reviewed them, plus about 90 days. Customer data while the customer is active, then it should be anonymised or deleted. When the time is up, delete the raw personal data first and keep only the totals.

**7. Passwords and keys.**
Things like a Slack webhook go into Airflow Variables or Connections, not into the code. Git history is permanent and many people can copy it, so a secret pushed once is effectively leaked. There are no secrets in this repo.

**8. Checking a rejected order.**
In ELT I look up the order in `raw_orders` (exactly as it came in), then in `stg_orders` to see the failure reason, and then I can show that no mart contains it, because every mart ignores rows marked FAILED. In ETL I can see the reason in the quarantine file and in `etl_quarantine`, and I can show the order is missing from the fact table. One weakness: the ETL quarantine file stores cleaned values (for example `PENDING??` in capitals), not the exact original text. ELT is better for audits.

**9. When an order status changes later.**
An order delivered on 3 September can come back as RETURNED on 1 October. Both pipelines then show RETURNED, and September revenue goes down. In my test the 9 corrections that went through reduced September revenue from 1,060,547.95 to 1,056,885.25. Finance would probably want both versions: the number they already reported and the corrected one. ELT keeps both in the raw table. ETL keeps a small history table (`etl_order_status_history`). A proper fix would be a snapshot table that stores each version with dates.

## Part F — Observability results

Experiments run on both DAGs (`experiments/run_experiments.py`, scratch copies, originals untouched).

| # | Expected → Observed → Where I saw it → What I'd change |
|---|---|
| 1 Happy path | **Expected** green run, 5 files in. **Observed** ETL: orders 2,412→2,179 passed/233 quarantined; payments 2,228→2,199/29; customers 405→397/8; products 152→142/10; stores 15/0 (280 rejected). ELT: all 5,212 rows in raw, 280 `FAILED` in staging. ~7 s each. In the real scheduler the 5 extracts started in the same second and the 3 master validations plus `validate_payments` ran together. **Where** `pipeline_runs`, `dq_results`, task states. **Change:** none. |
| 2 Re-run | **Expected** no double-count. **Observed** ETL fact 2,179→2,179, revenue unchanged. ELT `raw_orders` 2,412→4,824 (raw is append-only), `stg_orders` 2,412 and marts unchanged. Correct by design. **Change:** raw grows ~2× per run — skip a load when the file hash is already in raw. |
| 3 Increment | **Observed** ETL fact +136 new, 9 status updates; ELT `stg_orders` 2,412→2,562 (+150 inserted, 14 of them rejected) and 9 updated; 0 duplicates in both; the 10th correction (ORD000822) is rejected because its product master row is invalid and is kept visible in the quarantine / `stg_orders_rejected_corrections`. Re-running *without* the increment flag keeps the corrections. **Where** `etl_order_status_history`, counts. **Change:** a failed correction should alert, not just log. |
| 4 Missing file | **Observed** `extract_products` `failed` after 3 attempts (1 + 2 retries; 60 s apart in prod ≈ 2 min), downstream `upstream_failed` in both; `run_audit` wrote a `FAILED` row and turned the run red. **Alert:** UI + `pipeline_runs` only. **Change:** Slack/email `on_failure_callback` (bonus) and a 08:45 file-presence check. |
| 5 New bad data | **Observed** all 5 rows quarantined with the right reason (`customer_id_not_null`, `quantity_positive`, `store_fk`, `status_valid`, `ts_not_future`) in both pipelines; `dq_results` counts rose by exactly 1 per check. **Change:** none. |
| 6 Schema change | **Observed** both failed loudly, nothing loaded: ETL `extract_orders` → `ValueError: Schema drift … missing columns ['order_amount']`; ELT `load_raw` → `Binder Error: Table "raw_orders" does not have a column with name "total"`; `raw_orders` still 2,412 rows. `INSERT … BY NAME` is what makes ELT fail (a positional insert would have loaded the data into the wrong column silently). **Change:** friendlier schema-contract task + alert. |
| 7 Volume (50 rows) | **Expected** warning. **Observed** both ran `SUCCESS`; `orders.volume_drop` = `WARNING`. **But nothing stopped the load**: ELT marts fell to ₹24,014.40 revenue while the ETL fact (upsert-only) kept all 2,179 old orders → the two pipelines now **disagree**. (I built the check up-front, it was not added after the experiment.) **Change:** make volume a *blocking* check and decide whether the fact is a snapshot or accumulates. |
| 8 Drift (store S03 ×100) | **(a) unit_price only:** `amount_formula` caught it — 175 rows rejected (+157), S03 vanished from revenue (₹73,343.05→0, total −6.9%); visible only as a metric drop. **(b) price *and* amount ×100:** **nothing caught it** — S03 ₹73,343.05→₹7,334,305.00, total ₹1.06 M→₹8.32 M (+685 %). **Change:** add a distribution check (unit_price vs `mrp`, store/day revenue vs trailing average) and block above a threshold. |
| 9 Freshness | **Observed** `orders.data_not_stale` = `WARNING` with latest order 30 days old; control run (latest <1 day) = `PASSED`. Run still `SUCCESS`. (It also warns on the normal run, because this dataset ends 30 Sep and the run is on 20 Oct.) **Change:** route to an alert. |
| 10 Recovery | **Observed** (real scheduler) `extract_products` failed ×3, 6 downstream `upstream_failed`, `run_audit` red. After restoring the file and **Clear** (task + downstream, only failed): only `extract_products` (try 4) and its downstream re-ran; the other 4 extracts and 3 validations stayed at try 1; run → `success`; `pipeline_runs` row replaced `FAILED`→`SUCCESS`. **Change:** none. |

**Five pillars.** Freshness: covered (`data_not_stale`, run timestamps). Volume: partly (warns, doesn't block). Schema: **best** (both fail loudly). Distribution: **weakest** (Experiment 8b passed undetected). Lineage: strong in ELT (`_source_file/_run_id/_loaded_at`), partial in ETL.

## Part G — Recommendation to Sameer (CTO)

I recommend ELT, with Finance reading only from the mart tables.

**Why.** I built both pipelines and ran them on the same data. Both gave the same September revenue (1,060,547.95), the same 280 rejected rows, and the same results for all 44 quality checks. So the main promise of ETL, that a bad row never reaches the CFO, also works in ELT, because every mart leaves out rows marked FAILED. And the rejected rows stay available to look at.

- **Audit:** ELT is better. The raw data stays exactly as it arrived, with file and run details. The ETL quarantine file holds cleaned values.
- **Data quality:** about equal. ETL blocks bad rows physically. ELT depends on the marts always filtering them out.
- **Changing rules:** ELT is easier. I edit one SQL file and rebuild from the raw data. In ETL, old rejected rows cannot be re-checked unless I read the source files again.
- **Growing 1000 times:** ELT is better, because the heavy work runs as SQL inside the warehouse. My ETL loads whole files into memory in one task.
- **Cost:** ETL stores less, since there is no raw copy. ELT adds about the same amount of raw data every run. At this size the storage cost is small compared to Finance spending half a day every week on reconciliation.

**Three risks with ELT.**
1. The raw customer table holds real phone numbers and emails. If that leaks, it is a serious problem under the DPDP Act. It needs tight access, encryption and a retention limit.
2. DuckDB is a single file that only one process can write to at a time. Both DAGs start at 09:00 and I had to add retries so they don't clash. It is also not a shared, backed-up warehouse. Before real use we should move to a proper database server.
3. Some problems only produce a warning and don't stop the load. When I cut the orders file to 50 rows, the run still finished successfully. When I multiplied one store's prices and amounts by 100, no check caught it and total revenue grew by about 685%. A broken product row can also quietly remove revenue (118 delivered orders worth 76,446.43 are held back for that reason). We need blocking checks for volume and big changes, and we need the client's answer on that product question.

---

## Still to do (not done by the build) — see the end of the hand-over message

Screenshots from your Astro UI; Slack alert / SCD2 / capacity check / DQ trend chart (bonus) not built; push to GitHub.
