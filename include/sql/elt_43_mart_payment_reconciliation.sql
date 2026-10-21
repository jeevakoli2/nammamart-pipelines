-- MART: payment reconciliation (business question 4) - one row per mismatch reason. Same rules as the ETL pipeline.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH o AS (SELECT order_id, order_status, order_amount, payment_mode FROM stg_orders WHERE dq_status <> 'FAILED'),
     p AS (SELECT payment_id, order_id, payment_status, amount_paid, payment_mode FROM stg_payments WHERE dup_rank = 1),
     m AS (SELECT o.order_id, o.order_status, o.order_amount, o.payment_mode AS order_payment_mode,
                  p.payment_id, p.payment_status, p.amount_paid, p.payment_mode AS gateway_payment_mode
           FROM o LEFT JOIN p USING (order_id)),
     r AS (
  SELECT *, 'MISSING_PAYMENT' AS mismatch_reason, order_amount AS unreconciled_amount FROM m
   WHERE payment_id IS NULL AND order_status IN ('DELIVERED', 'RETURNED')
  UNION ALL SELECT *, 'PAYMENT_FOR_CANCELLED_ORDER', abs(amount_paid) FROM m
   WHERE payment_id IS NOT NULL AND order_status = 'CANCELLED'
  UNION ALL SELECT *, 'PAYMENT_FAILED', order_amount FROM m
   WHERE payment_id IS NOT NULL AND order_status = 'DELIVERED' AND payment_status = 'FAILED'
  UNION ALL SELECT *, 'STATUS_MISMATCH', abs(amount_paid) FROM m
   WHERE payment_id IS NOT NULL AND order_status = 'DELIVERED' AND payment_status = 'REFUNDED'
  UNION ALL SELECT *, 'RETURNED_NOT_REFUNDED', abs(amount_paid) FROM m
   WHERE payment_id IS NOT NULL AND order_status = 'RETURNED' AND payment_status <> 'REFUNDED'
  UNION ALL SELECT *, 'AMOUNT_MISMATCH', abs(order_amount - amount_paid) FROM m
   WHERE payment_id IS NOT NULL AND order_status IN ('DELIVERED', 'RETURNED') AND payment_status IN ('SUCCESS', 'REFUNDED')
     AND abs(order_amount - amount_paid) > 0.011
  UNION ALL SELECT *, 'PAYMENT_MODE_MISMATCH', 0.0 FROM m
   WHERE payment_id IS NOT NULL AND order_status IN ('DELIVERED', 'RETURNED') AND order_payment_mode <> gateway_payment_mode
)
SELECT order_id, payment_id, order_status, order_amount, payment_status, amount_paid, order_payment_mode,
       gateway_payment_mode, mismatch_reason, unreconciled_amount FROM r
UNION ALL
SELECT p.order_id, p.payment_id, NULL, NULL, p.payment_status, p.amount_paid, NULL, p.payment_mode,
       CASE WHEN p.order_id IN (SELECT order_id FROM stg_orders) THEN 'ORPHAN_PAYMENT_ORDER_REJECTED'
            ELSE 'ORPHAN_PAYMENT_NO_ORDER' END, abs(p.amount_paid)
FROM p WHERE p.order_id NOT IN (SELECT order_id FROM o);
