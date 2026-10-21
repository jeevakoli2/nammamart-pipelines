-- PII masking macros (mirror of nammamart_common.mask_email / mask_phone / mask_name)
CREATE OR REPLACE MACRO mask_email(e) AS
  CASE WHEN e IS NULL THEN NULL
       WHEN regexp_full_match(e, '[^@\s]+@[^@\s]+\.[^@\s]+')
         THEN left(split_part(e, '@', 1), 2) || '***@' || split_part(e, '@', 2)
       ELSE '***' END;
CREATE OR REPLACE MACRO mask_phone(p) AS
  CASE WHEN p IS NULL THEN NULL
       WHEN regexp_full_match(p, '\d{10}') THEN left(p, 2) || '******' || right(p, 2)
       ELSE '**********' END;
CREATE OR REPLACE MACRO mask_name(n) AS
  CASE WHEN n IS NULL THEN NULL ELSE regexp_replace(n, '(\w)\w*', '\1***', 'g') END;
CREATE OR REPLACE MACRO std_payment_mode(x) AS
  CASE WHEN lower(trim(x)) = 'upi' THEN 'UPI'
       WHEN lower(trim(x)) = 'card' THEN 'Card'
       WHEN lower(trim(x)) IN ('cash on delivery', 'cod', 'cashh') THEN 'Cash on Delivery'
       WHEN lower(trim(x)) = 'wallet' THEN 'Wallet'
       ELSE nullif(trim(x), '') END;
