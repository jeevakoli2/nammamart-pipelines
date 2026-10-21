-- STAGING: customers. SQL only. Latest load of each source file; types cast, standardised, de-duplicated,
-- PII masked (raw name/email/phone are NOT carried into staging), DQ flags -> dq_status / failure_reason.
CREATE OR REPLACE TABLE stg_customers AS
WITH latest AS (
  SELECT *, rowid AS _rid FROM raw_customers
  WHERE (_source_file, _loaded_at) IN (SELECT _source_file, max(_loaded_at) FROM raw_customers GROUP BY 1)
), typed AS (
  SELECT nullif(trim(customer_id), '') AS customer_id, nullif(trim(full_name), '') AS full_name,
         nullif(trim(email), '') AS email, nullif(trim(phone), '') AS phone, nullif(trim(city), '') AS city,
         nullif(trim(area), '') AS area, nullif(trim(pincode), '') AS pincode,
         TRY_CAST(TRY_STRPTIME(trim(signup_date), '%Y-%m-%d') AS DATE) AS signup_date,
         nullif(trim(loyalty_tier), '') AS tier_raw,
         nullif(trim(is_active), '') AS is_active, _source_file, _run_id, _loaded_at, _rid
  FROM latest
), ranked AS (
  SELECT *, upper(left(tier_raw, 1)) || lower(substr(tier_raw, 2)) AS loyalty_tier,
         row_number() OVER (PARTITION BY customer_id ORDER BY _rid) AS dup_rank
  FROM typed
), flagged AS (
  SELECT *, count(*) FILTER (WHERE dup_rank = 1 AND email IS NOT NULL) OVER (PARTITION BY email) AS email_cnt
  FROM ranked
)
SELECT customer_id, mask_name(full_name) AS full_name_masked, mask_email(email) AS email_masked,
       mask_phone(phone) AS phone_masked, city, area, pincode, signup_date, loyalty_tier, is_active,
       dup_rank,
       CASE WHEN length(failure_reason) > 0 THEN 'FAILED' WHEN length(warning_reason) > 0 THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       failure_reason, warning_reason, _source_file, _run_id, _loaded_at
FROM (
  SELECT *,
    concat_ws('; ',
      CASE WHEN dup_rank > 1 THEN 'customers.customer_id_unique' END,
      CASE WHEN signup_date > TIMESTAMP '{{as_of}}' THEN 'customers.signup_not_future' END) AS failure_reason,
    concat_ws('; ',
      CASE WHEN city IS NULL THEN 'customers.city_not_null' END,
      CASE WHEN NOT coalesce(regexp_full_match(email, '[^@\s]+@[^@\s]+\.[^@\s]+'), false) THEN 'customers.email_format' END,
      CASE WHEN NOT coalesce(regexp_full_match(phone, '\d{10}'), false) THEN 'customers.phone_format' END,
      CASE WHEN NOT coalesce(regexp_full_match(pincode, '\d{6}'), false) THEN 'customers.pincode_format' END,
      CASE WHEN dup_rank = 1 AND email_cnt > 1 THEN 'customers.email_unique' END,
      CASE WHEN tier_raw IS DISTINCT FROM loyalty_tier THEN 'customers.loyalty_tier_casing' END) AS warning_reason
  FROM flagged
);
