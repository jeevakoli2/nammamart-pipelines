-- MART: top 10 customers by spend (business question 5). Masked contact details only.
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT row_number() OVER (ORDER BY sum(o.order_amount) DESC, o.customer_id) AS spend_rank,
       o.customer_id, c.full_name_masked, c.email_masked, c.loyalty_tier,
       count(*) AS delivered_orders, round(sum(o.order_amount), 2) AS total_spend
FROM stg_orders o JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status <> 'FAILED'
WHERE o.dq_status <> 'FAILED' AND o.order_status = 'DELIVERED'
GROUP BY o.customer_id, c.full_name_masked, c.email_masked, c.loyalty_tier
ORDER BY total_spend DESC, o.customer_id LIMIT 10;
