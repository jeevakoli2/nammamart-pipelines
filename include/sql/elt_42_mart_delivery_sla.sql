-- MART: delivery time per store (business question 3)
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT o.store_id, s.store_name, s.zone, count(*) AS delivered_orders,
       round(avg(o.delivery_minutes), 2) AS avg_delivery_minutes,
       round(100.0 * count(*) FILTER (WHERE o.delivery_minutes <= 15) / count(*), 2) AS pct_within_15_min
FROM stg_orders o JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status <> 'FAILED'
WHERE o.dq_status <> 'FAILED' AND o.order_status = 'DELIVERED'
GROUP BY 1, 2, 3 ORDER BY 1;
