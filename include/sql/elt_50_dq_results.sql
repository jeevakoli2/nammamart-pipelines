-- DQ scorecard: one row per check per run -> dq_results (same table/columns the ETL pipeline writes).
-- Template vars: {{run_id}}, {{dag_id}}, {{as_of}}
DELETE FROM dq_results WHERE run_id = '{{run_id}}' AND dag_id = '{{dag_id}}';

INSERT INTO dq_results
WITH tot AS (
  SELECT 'orders' AS tbl, (SELECT count(*) FROM stg_orders) + (SELECT count(*) FROM stg_orders_rejected_corrections) AS n UNION ALL SELECT 'payments', count(*) FROM stg_payments
  UNION ALL SELECT 'customers', count(*) FROM stg_customers UNION ALL SELECT 'products', count(*) FROM stg_products
  UNION ALL SELECT 'stores', count(*) FROM stg_stores
), reasons AS (
  SELECT 'orders' AS tbl, failure_reason || '; ' || warning_reason AS r FROM stg_orders
  UNION ALL SELECT 'orders', failure_reason || '; ' || warning_reason FROM stg_orders_rejected_corrections
  UNION ALL SELECT 'payments', failure_reason || '; ' || warning_reason FROM stg_payments
  UNION ALL SELECT 'customers', failure_reason || '; ' || warning_reason FROM stg_customers
  UNION ALL SELECT 'products', failure_reason || '; ' || warning_reason FROM stg_products
  UNION ALL SELECT 'stores', failure_reason || '; ' || warning_reason FROM stg_stores
), hits AS (
  SELECT tbl, check_name, count(*) AS n
  FROM (SELECT tbl, unnest(string_split(r, '; ')) AS check_name FROM reasons)
  WHERE check_name <> '' GROUP BY 1, 2
)
SELECT '{{run_id}}', '{{dag_id}}', c.check_name, c.dimension, c.severity, t.n, coalesce(h.n, 0),
       CASE WHEN coalesce(h.n, 0) = 0 THEN 'PASSED' WHEN c.severity = 'Critical' THEN 'FAILED' ELSE 'WARNING' END,
       now()::TIMESTAMP
FROM dq_check_catalog c JOIN tot t ON t.tbl = c.table_name
LEFT JOIN hits h ON h.tbl = c.table_name AND h.check_name = c.check_name
WHERE NOT c.dataset_level;

-- dataset-level: freshness (latest order older than 24h at run time)
INSERT INTO dq_results
SELECT '{{run_id}}', '{{dag_id}}', 'orders.data_not_stale', 'Timeliness', 'Warning', 1, f, CASE WHEN f = 0 THEN 'PASSED' ELSE 'WARNING' END, now()::TIMESTAMP
FROM (SELECT CASE WHEN coalesce(
        TIMESTAMP '{{as_of}}' - (SELECT max(order_ts) FROM stg_orders WHERE order_ts <= TIMESTAMP '{{as_of}}') > INTERVAL 24 HOUR,
        true) THEN 1 ELSE 0 END AS f);

-- dataset-level: volume (more than 50% fewer order rows than the previous run)
INSERT INTO dq_results
SELECT '{{run_id}}', '{{dag_id}}', 'orders.volume_drop', 'Completeness', 'Warning', 1, f, CASE WHEN f = 0 THEN 'PASSED' ELSE 'WARNING' END, now()::TIMESTAMP
FROM (SELECT CASE WHEN prev IS NOT NULL AND prev > 0 AND (prev - cur) * 1.0 / prev > 0.5 THEN 1 ELSE 0 END AS f
      FROM (SELECT (SELECT rows_checked FROM dq_results WHERE dag_id = '{{dag_id}}' AND run_id <> '{{run_id}}'
                    AND check_name = 'orders.order_id_not_null' ORDER BY checked_at DESC LIMIT 1) AS prev,
                   (SELECT count(*) FROM stg_orders) AS cur));
