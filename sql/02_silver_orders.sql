-- =====================================================================
-- 02_silver_orders.sql  -  clean, quarantine, dedup, MERGE silver.orders
-- Rule 1: the version with the latest updated_at wins (file order irrelevant).
-- Rule 5: missing currency is derived from shipping_country, case-insensitive.
-- Session time zone is UTC (set by run_sql.py and the Glue --conf).
-- =====================================================================

-- ---------- 1. type + standardise this batch
CREATE OR REPLACE TEMPORARY VIEW orders_typed AS
SELECT
  nullif(trim(order_id), '')                                            AS order_id,
  nullif(trim(customer_id), '')                                         AS customer_id,
  coalesce(
    to_timestamp(trim(order_ts), "yyyy-MM-dd'T'HH:mm:ssXXX"),           -- 2026-09-01T13:48:48+05:30 and ...Z
    to_timestamp(trim(order_ts), 'dd/MM/yyyy HH:mm'))                    -- 01/09/2026 08:18 (day/month, UTC)
                                                                        AS order_ts_utc,
  lower(trim(status))                                                   AS status,
  coalesce(nullif(upper(trim(currency)), ''),
           CASE upper(trim(shipping_country))
                WHEN 'IN' THEN 'INR' WHEN 'US' THEN 'USD' WHEN 'GB' THEN 'GBP'
                WHEN 'DE' THEN 'EUR' WHEN 'SG' THEN 'SGD' END)          AS currency,
  (nullif(trim(currency), '') IS NULL)                                  AS currency_derived,
  upper(trim(shipping_country))                                         AS shipping_country,
  nullif(trim(promo_code), '')                                          AS promo_code,
  to_timestamp(trim(updated_at), "yyyy-MM-dd'T'HH:mm:ssXXX")            AS updated_at,
  CAST(_batch_id AS INT)                                                AS _batch_id,
  order_ts                                                              AS order_ts_raw,
  _source_file
FROM bronze.orders
WHERE _batch_id = '{{batch_id}}';

-- ---------- 2. one quarantine reason per row (first failing rule)
CREATE OR REPLACE TEMPORARY VIEW orders_checked AS
SELECT *,
  CASE
    WHEN order_id IS NULL                       THEN 'missing order_id'
    WHEN order_ts_utc IS NULL                   THEN 'unparseable order_ts'
    WHEN updated_at IS NULL                     THEN 'missing or invalid updated_at'
    WHEN status IS NULL
      OR status NOT IN ('placed','paid','shipped','delivered','cancelled')
                                                THEN 'invalid status'
    WHEN currency IS NULL                       THEN 'currency missing and not derivable'
    WHEN currency NOT IN ('INR','USD','GBP','EUR','SGD')
                                                THEN 'unsupported currency'
  END AS dq_reason
FROM orders_typed;

DELETE FROM quarantine.records WHERE source_table = 'orders' AND batch_id = '{{batch_id}}';

INSERT INTO quarantine.records
SELECT 'orders', '{{batch_id}}', dq_reason, to_json(struct(*)), current_timestamp()
FROM orders_checked
WHERE dq_reason IS NOT NULL;

-- ---------- 3. keep one row per order within the batch (MERGE needs a unique source key)
CREATE OR REPLACE TEMPORARY VIEW orders_ranked AS
SELECT *,
  row_number() OVER (
    PARTITION BY order_id
    ORDER BY updated_at DESC,
             CASE status WHEN 'delivered' THEN 5 WHEN 'shipped' THEN 4 WHEN 'paid' THEN 3
                         WHEN 'cancelled' THEN 2 WHEN 'placed' THEN 1 ELSE 0 END DESC,
             _source_file DESC) AS rn
FROM orders_checked
WHERE dq_reason IS NULL;

CREATE OR REPLACE TEMPORARY VIEW orders_latest AS
SELECT order_id, customer_id, order_ts_utc, to_date(order_ts_utc) AS order_date,
       status, currency, currency_derived, shipping_country, promo_code, updated_at,
       _batch_id, current_timestamp() AS _merged_at
FROM orders_ranked
WHERE rn = 1;

-- ---------- 4. upsert: only a strictly newer version may replace the stored one
MERGE INTO silver.orders AS t
USING orders_latest AS s
  ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;

-- ---------- 5. data-quality rows for this table and batch
DELETE FROM silver.dq_report WHERE batch_id = '{{batch_id}}' AND table_name = 'orders';

INSERT INTO silver.dq_report
SELECT '{{batch_id}}', 'orders', metric, rule, row_count, current_timestamp()
FROM (
  SELECT 'rows_in' AS metric, 'all rows read from bronze' AS rule, count(*) AS row_count FROM orders_checked
  UNION ALL
  SELECT 'rows_quarantined', dq_reason, count(*) FROM orders_checked WHERE dq_reason IS NOT NULL GROUP BY dq_reason
  UNION ALL
  SELECT 'rows_deduplicated', 'older version or exact duplicate within batch', count(*) FROM orders_ranked WHERE rn > 1
  UNION ALL
  SELECT 'rows_out', 'candidates sent to MERGE', count(*) FROM orders_latest
  UNION ALL
  SELECT 'info', 'currency derived from shipping_country', count(*) FROM orders_latest WHERE currency_derived
  UNION ALL
  SELECT 'info', 'batch candidates older than stored version (not applied)', count(*)
  FROM orders_latest s JOIN silver.orders t ON t.order_id = s.order_id AND t.updated_at > s.updated_at
);

SELECT metric, rule, row_count FROM silver.dq_report
WHERE batch_id = '{{batch_id}}' AND table_name = 'orders' ORDER BY metric, rule;
