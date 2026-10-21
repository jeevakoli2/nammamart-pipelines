-- MART: revenue and gross margin by product category (business question 2). margin = revenue - quantity x cost_price
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category, round(sum(o.order_amount), 2) AS net_revenue,
       round(sum(o.order_amount - o.quantity * p.cost_price), 2) AS gross_margin,
       round(100.0 * sum(o.order_amount - o.quantity * p.cost_price) / nullif(sum(o.order_amount), 0), 2) AS margin_pct
FROM stg_orders o JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status <> 'FAILED'
WHERE o.dq_status <> 'FAILED' AND o.order_status = 'DELIVERED'
GROUP BY 1 ORDER BY 2 DESC;
