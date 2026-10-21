-- STAGING: orders, BASE load (orders.csv). Rebuilt each run from the latest raw copy; increments are re-applied by elt_21.
CREATE OR REPLACE TABLE stg_orders AS
SELECT * FROM v_orders_prepared WHERE _src_rank = 1;
