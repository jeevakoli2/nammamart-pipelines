-- STAGING: products. A superseded "(old)" row loses to the current row when product_id is duplicated.
CREATE OR REPLACE TABLE stg_products AS
WITH latest AS (
  SELECT *, rowid AS _rid FROM raw_products
  WHERE (_source_file, _loaded_at) IN (SELECT _source_file, max(_loaded_at) FROM raw_products GROUP BY 1)
), typed AS (
  SELECT nullif(trim(product_id), '') AS product_id, nullif(trim(product_name), '') AS product_name,
         nullif(trim(category), '') AS category, nullif(trim(sub_category), '') AS sub_category,
         nullif(trim(brand), '') AS brand, TRY_CAST(nullif(trim(mrp), '') AS DOUBLE) AS mrp,
         TRY_CAST(nullif(trim(cost_price), '') AS DOUBLE) AS cost_price, nullif(trim(unit), '') AS unit,
         nullif(trim(is_perishable), '') AS is_perishable, nullif(trim(launch_date), '') AS launch_date,
         _source_file, _run_id, _loaded_at, _rid
  FROM latest
), ranked AS (
  SELECT *, row_number() OVER (PARTITION BY product_id
                               ORDER BY coalesce(product_name LIKE '%(old)%', false), _rid) AS dup_rank
  FROM typed
)
SELECT product_id, product_name, category, sub_category, brand, mrp, cost_price, unit, is_perishable, launch_date,
       dup_rank,
       CASE WHEN length(failure_reason) > 0 THEN 'FAILED' WHEN length(warning_reason) > 0 THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       failure_reason, warning_reason, _source_file, _run_id, _loaded_at
FROM (
  SELECT *,
    concat_ws('; ',
      CASE WHEN dup_rank > 1 THEN 'products.product_id_unique' END,
      CASE WHEN category IS NULL THEN 'products.category_not_null' END,
      CASE WHEN mrp <= 0 THEN 'products.mrp_positive' END,
      CASE WHEN cost_price > mrp THEN 'products.cost_le_mrp' END) AS failure_reason,
    '' AS warning_reason
  FROM ranked
);
