-- STAGING: stores. A missing zone is a WARNING (loaded as 'Unknown') so one master-data gap cannot hide a whole store's revenue.
CREATE OR REPLACE TABLE stg_stores AS
WITH latest AS (
  SELECT *, rowid AS _rid FROM raw_stores
  WHERE (_source_file, _loaded_at) IN (SELECT _source_file, max(_loaded_at) FROM raw_stores GROUP BY 1)
), typed AS (
  SELECT nullif(trim(store_id), '') AS store_id, nullif(trim(store_name), '') AS store_name,
         nullif(trim(zone), '') AS zone_raw, nullif(trim(area), '') AS area, nullif(trim(pincode), '') AS pincode,
         nullif(trim(open_date), '') AS open_date, nullif(trim(manager_name), '') AS manager_name,
         TRY_CAST(nullif(trim(capacity_orders_per_day), '') AS DOUBLE) AS capacity_orders_per_day,
         _source_file, _run_id, _loaded_at, _rid
  FROM latest
), ranked AS (
  SELECT *, row_number() OVER (PARTITION BY store_id ORDER BY _rid) AS dup_rank FROM typed
)
SELECT store_id, store_name, coalesce(zone_raw, 'Unknown') AS zone, area, pincode, open_date, manager_name,
       capacity_orders_per_day, dup_rank,
       CASE WHEN length(failure_reason) > 0 THEN 'FAILED' WHEN length(warning_reason) > 0 THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       failure_reason, warning_reason, _source_file, _run_id, _loaded_at
FROM (
  SELECT *,
    concat_ws('; ', CASE WHEN dup_rank > 1 THEN 'stores.store_id_unique' END) AS failure_reason,
    concat_ws('; ',
      CASE WHEN zone_raw IS NULL THEN 'stores.zone_not_null' END,
      CASE WHEN capacity_orders_per_day <= 0 THEN 'stores.capacity_positive' END,
      CASE WHEN NOT coalesce(regexp_full_match(pincode, '\d{6}'), false) THEN 'stores.pincode_format' END) AS warning_reason
  FROM ranked
);
