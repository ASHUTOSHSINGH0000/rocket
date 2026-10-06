-- =====================================================================
-- 03_silver_reference.sql  -  FX rates, products, CRM customers, payments
-- Reference files are full snapshots, so each silver table is rebuilt
-- from the latest bronze slice (deterministic, idempotent).
-- Runs after 02 because payments are checked against silver.orders.
-- =====================================================================

-- ---------- FX rates: typed, upper-case currency, one rate per (date, currency)
INSERT OVERWRITE silver.fx_rates
SELECT rate_date, currency, rate_to_usd
FROM (
  SELECT to_date(trim(rate_date))                   AS rate_date,
         upper(trim(currency))                      AS currency,
         CAST(trim(rate_to_usd) AS DECIMAL(18,8))   AS rate_to_usd,
         row_number() OVER (PARTITION BY to_date(trim(rate_date)), upper(trim(currency))
                            ORDER BY _ingested_at DESC) AS rn
  FROM bronze.fx_rates
  WHERE _batch_id = '{{batch_id}}')
WHERE rn = 1 AND rate_date IS NOT NULL AND rate_to_usd IS NOT NULL;

-- ---------- products: one row per product_id
INSERT OVERWRITE silver.products
SELECT product_id, sku, product_name, category, list_price_usd
FROM (
  SELECT trim(product_id) AS product_id, trim(sku) AS sku, trim(product_name) AS product_name,
         trim(category) AS category, CAST(trim(list_price_usd) AS DECIMAL(18,2)) AS list_price_usd,
         row_number() OVER (PARTITION BY trim(product_id) ORDER BY _ingested_at DESC) AS rn
  FROM bronze.products
  WHERE _batch_id = '{{batch_id}}')
WHERE rn = 1 AND product_id IS NOT NULL;

-- ---------- customers: Rule 8 (no updated_at -> quarantine), exact duplicates removed
CREATE OR REPLACE TEMPORARY VIEW customers_checked AS
SELECT nullif(trim(customer_id), '')                                   AS customer_id,
       trim(full_name)                                                  AS full_name,
       lower(trim(email))                                               AS email,
       initcap(trim(tier))                                              AS tier,
       upper(trim(country))                                             AS country,
       to_timestamp(trim(updated_at), "yyyy-MM-dd'T'HH:mm:ssXXX")       AS updated_at,
       updated_at                                                       AS updated_at_raw,
       CASE
         WHEN nullif(trim(customer_id), '') IS NULL THEN 'missing customer_id'
         WHEN nullif(trim(updated_at), '') IS NULL  THEN 'CRM row without updated_at'
         WHEN to_timestamp(trim(updated_at), "yyyy-MM-dd'T'HH:mm:ssXXX") IS NULL THEN 'invalid updated_at'
       END                                                              AS dq_reason
FROM bronze.customers
WHERE _batch_id = '{{batch_id}}';

CREATE OR REPLACE TEMPORARY VIEW customers_ranked AS
SELECT *, row_number() OVER (PARTITION BY customer_id, updated_at
                             ORDER BY tier, country, email, full_name) AS rn
FROM customers_checked
WHERE dq_reason IS NULL;

INSERT OVERWRITE silver.customers
SELECT customer_id, full_name, email, tier, country, updated_at
FROM customers_ranked
WHERE rn = 1;

DELETE FROM quarantine.records WHERE source_table = 'customers' AND batch_id = '{{batch_id}}';

INSERT INTO quarantine.records
SELECT 'customers', '{{batch_id}}', dq_reason, to_json(struct(*)), current_timestamp()
FROM customers_checked
WHERE dq_reason IS NOT NULL;

-- ---------- payments: typed; payments for orders that do not exist go to quarantine
CREATE OR REPLACE TEMPORARY VIEW payments_checked AS
SELECT p.*,
       CASE
         WHEN p.payment_id IS NULL OR p.order_id IS NULL THEN 'missing payment_id or order_id'
         WHEN p.amount IS NULL OR p.amount < 0           THEN 'invalid amount'
         WHEN p.status NOT IN ('success','failed','refunded') OR p.status IS NULL
                                                         THEN 'invalid status'
         WHEN o.order_id IS NULL                         THEN 'payment for unknown order'
       END AS dq_reason
FROM (
  SELECT nullif(trim(payment_id), '')                                  AS payment_id,
         nullif(trim(order_id), '')                                    AS order_id,
         lower(trim(method))                                           AS method,
         CAST(trim(amount) AS DECIMAL(18,2))                           AS amount,
         upper(trim(currency))                                         AS currency,
         lower(trim(status))                                           AS status,
         to_timestamp(trim(paid_at), "yyyy-MM-dd'T'HH:mm:ssXXX")       AS paid_at
  FROM bronze.payments
  WHERE _batch_id = '{{batch_id}}') p
LEFT JOIN silver.orders o ON o.order_id = p.order_id;

CREATE OR REPLACE TEMPORARY VIEW payments_ranked AS
SELECT *, row_number() OVER (PARTITION BY payment_id ORDER BY paid_at DESC) AS rn
FROM payments_checked
WHERE dq_reason IS NULL;

INSERT OVERWRITE silver.payments
SELECT payment_id, order_id, method, amount, currency, status, paid_at
FROM payments_ranked
WHERE rn = 1;

DELETE FROM quarantine.records WHERE source_table = 'payments' AND batch_id = '{{batch_id}}';

INSERT INTO quarantine.records
SELECT 'payments', '{{batch_id}}', dq_reason, to_json(struct(*)), current_timestamp()
FROM payments_checked
WHERE dq_reason IS NOT NULL;

-- ---------- data-quality rows
DELETE FROM silver.dq_report
WHERE batch_id = '{{batch_id}}' AND table_name IN ('customers', 'payments', 'fx_rates', 'products');

INSERT INTO silver.dq_report
SELECT '{{batch_id}}', table_name, metric, rule, row_count, current_timestamp()
FROM (
  SELECT 'customers' AS table_name, 'rows_in' AS metric, 'all rows read from bronze' AS rule, count(*) AS row_count FROM customers_checked
  UNION ALL SELECT 'customers', 'rows_quarantined', dq_reason, count(*) FROM customers_checked WHERE dq_reason IS NOT NULL GROUP BY dq_reason
  UNION ALL SELECT 'customers', 'rows_deduplicated', 'duplicate (customer_id, updated_at)', count(*) FROM customers_ranked WHERE rn > 1
  UNION ALL SELECT 'customers', 'rows_out', 'loaded to silver.customers', count(*) FROM customers_ranked WHERE rn = 1
  UNION ALL SELECT 'payments', 'rows_in', 'all rows read from bronze', count(*) FROM payments_checked
  UNION ALL SELECT 'payments', 'rows_quarantined', dq_reason, count(*) FROM payments_checked WHERE dq_reason IS NOT NULL GROUP BY dq_reason
  UNION ALL SELECT 'payments', 'rows_deduplicated', 'duplicate payment_id', count(*) FROM payments_ranked WHERE rn > 1
  UNION ALL SELECT 'payments', 'rows_out', 'loaded to silver.payments', count(*) FROM payments_ranked WHERE rn = 1
  UNION ALL SELECT 'fx_rates', 'info', 'rates loaded', count(*) FROM silver.fx_rates
  UNION ALL SELECT 'products', 'info', 'products loaded', count(*) FROM silver.products
);

SELECT table_name, metric, rule, row_count FROM silver.dq_report
WHERE batch_id = '{{batch_id}}' AND table_name IN ('customers', 'payments', 'fx_rates', 'products')
ORDER BY table_name, metric, rule;
