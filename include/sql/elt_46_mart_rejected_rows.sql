-- MART: every rejected (FAILED) staging row, one row per reason (business question 6).
CREATE OR REPLACE TABLE mart_rejected_rows AS
SELECT _source_file AS source_file, order_id AS row_key, unnest(string_split(failure_reason, '; ')) AS failure_reason,
       CAST(_loaded_at AS DATE) AS rejected_date, _run_id AS run_id FROM stg_orders WHERE dq_status = 'FAILED'
UNION ALL
SELECT _source_file, order_id, unnest(string_split(failure_reason, '; ')), CAST(_loaded_at AS DATE), _run_id
FROM stg_orders_rejected_corrections
UNION ALL
SELECT _source_file, payment_id, unnest(string_split(failure_reason, '; ')), CAST(_loaded_at AS DATE), _run_id
FROM stg_payments WHERE dq_status = 'FAILED'
UNION ALL
SELECT _source_file, customer_id, unnest(string_split(failure_reason, '; ')), CAST(_loaded_at AS DATE), _run_id
FROM stg_customers WHERE dq_status = 'FAILED'
UNION ALL
SELECT _source_file, product_id, unnest(string_split(failure_reason, '; ')), CAST(_loaded_at AS DATE), _run_id
FROM stg_products WHERE dq_status = 'FAILED'
UNION ALL
SELECT _source_file, store_id, unnest(string_split(failure_reason, '; ')), CAST(_loaded_at AS DATE), _run_id
FROM stg_stores WHERE dq_status = 'FAILED';
