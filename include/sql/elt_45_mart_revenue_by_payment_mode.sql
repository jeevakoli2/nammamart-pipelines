-- MART: revenue by payment mode (first half of business question 4)
CREATE OR REPLACE TABLE mart_revenue_by_payment_mode AS
SELECT payment_mode, count(*) AS delivered_orders, round(sum(order_amount), 2) AS net_revenue
FROM stg_orders WHERE dq_status <> 'FAILED' AND order_status = 'DELIVERED'
GROUP BY 1 ORDER BY 3 DESC;
