-- STAGING: orders, INCREMENTAL load. MERGE (upsert) on order_id from every increment file present in raw:
--   matched + passes DQ  -> UPDATE (status corrections overwrite the old row, no duplicate)
--   not matched          -> INSERT (new order)
-- A correction that FAILS a critical check never overwrites a good row (it stays visible in raw).
-- Corrections that FAIL a critical check are not applied, but are never silently dropped: they are kept here
-- (and feed dq_results, mart_rejected_rows and the run audit).
CREATE OR REPLACE TABLE stg_orders_rejected_corrections AS
SELECT s.* FROM (
  SELECT * FROM v_orders_prepared WHERE _src_rank > 1
  QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _source_file DESC, dup_rank ASC) = 1
) s JOIN stg_orders t ON t.order_id = s.order_id
WHERE s.dq_status = 'FAILED';

MERGE INTO stg_orders AS t
USING (
  SELECT * FROM v_orders_prepared WHERE _src_rank > 1
  QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _source_file DESC, dup_rank ASC) = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.dq_status <> 'FAILED' THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
