-- STAGING: payments.
CREATE OR REPLACE TABLE stg_payments AS
WITH latest AS (
  SELECT *, rowid AS _rid FROM raw_payments
  WHERE (_source_file, _loaded_at) IN (SELECT _source_file, max(_loaded_at) FROM raw_payments GROUP BY 1)
), all_orders AS (
  SELECT DISTINCT nullif(trim(order_id), '') AS order_id FROM raw_orders
  WHERE (_source_file, _loaded_at) IN (SELECT _source_file, max(_loaded_at) FROM raw_orders GROUP BY 1)
), typed AS (
  SELECT nullif(trim(payment_id), '') AS payment_id, nullif(trim(order_id), '') AS order_id,
         TRY_STRPTIME(trim(payment_ts), '%Y-%m-%d %H:%M:%S') AS payment_ts,
         std_payment_mode(payment_mode) AS payment_mode, nullif(trim(payment_mode), '') AS payment_mode_raw,
         TRY_CAST(nullif(trim(amount_paid), '') AS DOUBLE) AS amount_paid,
         upper(nullif(trim(payment_status), '')) AS payment_status, nullif(trim(gateway_ref), '') AS gateway_ref,
         _source_file, _run_id, _loaded_at, _rid
  FROM latest
), ranked AS (
  SELECT *, row_number() OVER (PARTITION BY payment_id ORDER BY _rid) AS dup_rank FROM typed
)
SELECT payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status, gateway_ref, dup_rank,
       CASE WHEN length(failure_reason) > 0 THEN 'FAILED' WHEN length(warning_reason) > 0 THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       failure_reason, warning_reason, _source_file, _run_id, _loaded_at
FROM (
  SELECT *,
    concat_ws('; ',
      CASE WHEN dup_rank > 1 THEN 'payments.payment_id_unique' END,
      CASE WHEN coalesce(payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED'), true) THEN 'payments.status_valid' END,
      CASE WHEN coalesce(payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), true) THEN 'payments.payment_mode_valid' END,
      CASE WHEN payment_status = 'SUCCESS' AND amount_paid <= 0 THEN 'payments.amount_positive' END,
      CASE WHEN coalesce(order_id NOT IN (SELECT order_id FROM all_orders WHERE order_id IS NOT NULL), true) THEN 'payments.order_exists' END,
      CASE WHEN payment_ts > TIMESTAMP '{{as_of}}' THEN 'payments.payment_ts_not_future' END) AS failure_reason,
    concat_ws('; ',
      CASE WHEN payment_mode_raw IS DISTINCT FROM payment_mode AND payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'payments.payment_mode_variant' END,
      CASE WHEN payment_mode IS DISTINCT FROM 'Cash on Delivery' AND gateway_ref IS NULL THEN 'payments.gateway_ref_present' END) AS warning_reason
  FROM ranked
);
