-- MART: daily KPIs per store (delivered orders only, after discount). Answers business questions 1 and 3.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT o.order_date, o.store_id, s.store_name, s.zone,
       count(*) AS delivered_orders,
       round(sum(o.order_amount), 2) AS net_revenue,
       round(sum(o.order_amount - o.quantity * p.cost_price), 2) AS gross_margin,
       round(avg(o.delivery_minutes), 2) AS avg_delivery_minutes,
       count(*) FILTER (WHERE o.delivery_minutes <= 15) AS orders_within_15_min,
       round(100.0 * count(*) FILTER (WHERE o.delivery_minutes <= 15) / count(*), 2) AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status <> 'FAILED'
JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status <> 'FAILED'
WHERE o.dq_status <> 'FAILED' AND o.order_status = 'DELIVERED'
GROUP BY 1, 2, 3, 4;
