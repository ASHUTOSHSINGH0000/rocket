-- =====================================================================
-- 05_dim_customer.sql  -  SCD Type 2 customer dimension
-- A new version only when tier or country changes. Name or email changes
-- alone create no version. Rebuilt from the full CRM history each run, with
-- a hash surrogate key, so the output is identical on every re-run.
-- Rule 6: unknown member customer_sk = -1, country UNKNOWN.
-- =====================================================================

CREATE OR REPLACE TEMPORARY VIEW cust_changes AS
SELECT customer_id, full_name, email, tier, country, updated_at
FROM (
  SELECT *,
         row_number()       OVER w AS seq,
         lag(tier)          OVER w AS prev_tier,
         lag(country)       OVER w AS prev_country
  FROM silver.customers
  WINDOW w AS (PARTITION BY customer_id ORDER BY updated_at))
WHERE seq = 1                                  -- first known version
   OR NOT (tier    <=> prev_tier)              -- tier changed (null-safe)
   OR NOT (country <=> prev_country);          -- country changed

INSERT OVERWRITE silver.dim_customer
SELECT xxhash64(customer_id, updated_at)                                  AS customer_sk,
       customer_id, full_name, email, tier, country,
       updated_at                                                         AS valid_from,
       coalesce(lead(updated_at) OVER (PARTITION BY customer_id ORDER BY updated_at),
                TIMESTAMP '9999-12-31 00:00:00')                          AS valid_to,
       lead(updated_at) OVER (PARTITION BY customer_id ORDER BY updated_at) IS NULL AS is_current
FROM cust_changes
UNION ALL
SELECT CAST(-1 AS BIGINT), 'UNKNOWN', 'Unknown customer', NULL, 'UNKNOWN', 'UNKNOWN',
       TIMESTAMP '1900-01-01 00:00:00', TIMESTAMP '9999-12-31 00:00:00', true;

DELETE FROM silver.dq_report WHERE batch_id = '{{batch_id}}' AND table_name = 'dim_customer';

INSERT INTO silver.dq_report
SELECT '{{batch_id}}', 'dim_customer', 'info', rule, row_count, current_timestamp()
FROM (
  SELECT 'CRM rows (valid)' AS rule, count(*) AS row_count FROM silver.customers
  UNION ALL SELECT 'rows dropped: no tier/country change', (SELECT count(*) FROM silver.customers) - (SELECT count(*) FROM cust_changes)
  UNION ALL SELECT 'SCD2 versions (excl. unknown member)', count(*) FROM silver.dim_customer WHERE customer_sk <> -1
  UNION ALL SELECT 'customers with more than one version', count(*) FROM (
              SELECT customer_id FROM silver.dim_customer WHERE customer_sk <> -1 GROUP BY customer_id HAVING count(*) > 1)
);

SELECT rule, row_count FROM silver.dq_report
WHERE batch_id = '{{batch_id}}' AND table_name = 'dim_customer';
