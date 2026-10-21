-- Shared logic for stg_orders (base load) and the incremental MERGE: ONE definition of every order rule.
-- Reads the LATEST load of each source file from raw_orders (raw itself is append-only).
CREATE OR REPLACE VIEW v_orders_prepared AS
WITH latest AS (
  SELECT *, rowid AS _rid FROM raw_orders
  WHERE (_source_file, _loaded_at) IN (SELECT _source_file, max(_loaded_at) FROM raw_orders GROUP BY 1)
), typed AS (
  SELECT nullif(trim(order_id), '') AS order_id,
         TRY_STRPTIME(trim(order_ts), '%Y-%m-%d %H:%M:%S') AS order_ts,
         nullif(trim(customer_id), '') AS customer_id, nullif(trim(store_id), '') AS store_id,
         nullif(trim(product_id), '') AS product_id,
         TRY_CAST(nullif(trim(quantity), '') AS INTEGER) AS quantity,
         TRY_CAST(nullif(trim(unit_price), '') AS DOUBLE) AS unit_price,
         TRY_CAST(nullif(trim(discount_pct), '') AS DOUBLE) AS discount_pct,
         TRY_CAST(nullif(trim(order_amount), '') AS DOUBLE) AS order_amount,
         std_payment_mode(payment_mode) AS payment_mode, nullif(trim(payment_mode), '') AS payment_mode_raw,
         upper(nullif(trim(order_status), '')) AS order_status, nullif(trim(order_status), '') AS status_raw,
         nullif(trim(channel), '') AS channel,
         TRY_CAST(nullif(trim(delivery_minutes), '') AS DOUBLE) AS delivery_minutes,
         CASE WHEN _source_file = 'orders.csv' THEN 1 ELSE 2 END AS _src_rank,
         _source_file, _run_id, _loaded_at, _rid
  FROM latest
), ranked AS (
  SELECT *, row_number() OVER (PARTITION BY _source_file, order_id ORDER BY _rid) AS dup_rank FROM typed
), flagged AS (
  SELECT *,
    concat_ws('; ',
      CASE WHEN order_id IS NULL THEN 'orders.order_id_not_null' END,
      CASE WHEN customer_id IS NULL THEN 'orders.customer_id_not_null' END,
      CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 'orders.delivered_has_minutes' END,
      CASE WHEN dup_rank > 1 THEN 'orders.order_id_unique' END,
      CASE WHEN coalesce(order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED'), true) THEN 'orders.status_valid' END,
      CASE WHEN coalesce(payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), true) THEN 'orders.payment_mode_valid' END,
      CASE WHEN order_ts IS NULL THEN 'orders.order_ts_valid' END,
      CASE WHEN discount_pct < 0 OR discount_pct > 100 THEN 'orders.discount_range' END,
      CASE WHEN quantity <= 0 THEN 'orders.quantity_positive' END,
      CASE WHEN abs(round(quantity * unit_price * (1 - discount_pct / 100), 2) - order_amount) > 0.011 THEN 'orders.amount_formula' END,
      CASE WHEN delivery_minutes >= 120 THEN 'orders.delivery_minutes_lt_120' END,
      CASE WHEN customer_id IS NOT NULL AND customer_id NOT IN
           (SELECT customer_id FROM stg_customers WHERE dq_status <> 'FAILED' AND customer_id IS NOT NULL) THEN 'orders.customer_fk' END,
      CASE WHEN coalesce(product_id NOT IN
           (SELECT product_id FROM stg_products WHERE dq_status <> 'FAILED' AND product_id IS NOT NULL), true) THEN 'orders.product_fk' END,
      CASE WHEN coalesce(store_id NOT IN
           (SELECT store_id FROM stg_stores WHERE dq_status <> 'FAILED' AND store_id IS NOT NULL), true) THEN 'orders.store_fk' END,
      CASE WHEN order_ts > TIMESTAMP '{{as_of}}' THEN 'orders.ts_not_future' END) AS failure_reason,
    concat_ws('; ',
      CASE WHEN (delivery_minutes < 8 OR delivery_minutes > 35) AND delivery_minutes < 120 THEN 'orders.delivery_minutes_typical' END,
      CASE WHEN status_raw IS DISTINCT FROM order_status AND order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'orders.status_casing' END,
      CASE WHEN payment_mode_raw IS DISTINCT FROM payment_mode AND payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'orders.payment_mode_variant' END) AS warning_reason
  FROM ranked
)
SELECT order_id, order_ts, CAST(order_ts AS DATE) AS order_date, customer_id, store_id, product_id, quantity,
       unit_price, discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes, dup_rank,
       CASE WHEN length(failure_reason) > 0 THEN 'FAILED' WHEN length(warning_reason) > 0 THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       failure_reason, warning_reason, _src_rank, _source_file, _run_id, _loaded_at
FROM flagged;
