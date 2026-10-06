-- =====================================================================
-- 06_gold_fact_order_line.sql  -  order-line grain fact (full rebuild)
-- Rule 2: revenue = current status in (paid, shipped, delivered); returns
--         are negative lines on the original order, so they hit its date.
-- Rule 3: net_local = qty * unit_price * (1 - discount_pct/100)
-- Rule 4: FX rate of the order date (UTC); no rate that day -> most recent
--         earlier rate. USD is not listed, its rate is 1.
-- Point-in-time customer: the dim_customer version valid at order_ts_utc.
-- =====================================================================

-- daily calendar per currency, forward-filled with the last business-day rate
CREATE OR REPLACE TEMPORARY VIEW fx_daily AS
WITH bounds AS (
  SELECT currency,
         min(rate_date) AS d_from,
         greatest(DATE '2026-09-30', (SELECT max(order_date) FROM silver.orders)) AS d_to
  FROM silver.fx_rates
  GROUP BY currency),
cal AS (
  SELECT currency, explode(sequence(d_from, d_to, INTERVAL 1 DAY)) AS cal_date
  FROM bounds)
SELECT c.currency,
       c.cal_date,
       last(f.rate_to_usd, true) OVER w AS rate_to_usd,      -- true = ignore nulls
       last(f.rate_date,   true) OVER w AS fx_rate_date       -- business day actually used
FROM cal c
LEFT JOIN silver.fx_rates f ON f.currency = c.currency AND f.rate_date = c.cal_date
WINDOW w AS (PARTITION BY c.currency ORDER BY c.cal_date
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

INSERT OVERWRITE gold.fact_order_line
WITH base AS (
  SELECT i.order_id, i.line_no, o.order_date, o.order_ts_utc, o.customer_id,
         coalesce(d.customer_sk, CAST(-1 AS BIGINT))                           AS customer_sk,
         i.product_id,
         coalesce(p.category, 'UNKNOWN')                                        AS category,
         i.line_type, i.qty, i.unit_price, i.discount_pct, o.currency, o.status,
         CAST(i.qty * i.unit_price * (1 - i.discount_pct / 100) AS DECIMAL(18,4)) AS net_local,
         CAST(CASE WHEN o.currency = 'USD' THEN 1 ELSE fx.rate_to_usd END AS DECIMAL(18,8)) AS fx_rate,
         CASE WHEN o.currency = 'USD' THEN o.order_date ELSE fx.fx_rate_date END AS fx_rate_date,
         i.is_unknown_product
  FROM silver.order_items i
  JOIN silver.orders o           ON o.order_id = i.order_id
  LEFT JOIN silver.products p    ON p.product_id = i.product_id
  LEFT JOIN fx_daily fx          ON fx.currency = o.currency AND fx.cal_date = o.order_date
  LEFT JOIN silver.dim_customer d
         ON d.customer_id   = o.customer_id
        AND o.order_ts_utc >= d.valid_from
        AND o.order_ts_utc <  d.valid_to)                                       -- half-open: one match
SELECT order_id, line_no, order_date, order_ts_utc, customer_id, customer_sk, product_id, category,
       line_type, qty, unit_price, discount_pct, currency, status, net_local, fx_rate, fx_rate_date,
       CAST(net_local * fx_rate AS DECIMAL(18,4))                               AS net_usd,
       status IN ('paid', 'shipped', 'delivered')                              AS is_revenue,
       is_unknown_product,
       fx_rate IS NULL                                                          AS fx_missing
FROM base;

DELETE FROM silver.dq_report WHERE batch_id = '{{batch_id}}' AND table_name = 'fact_order_line';

INSERT INTO silver.dq_report
SELECT '{{batch_id}}', 'fact_order_line', 'info', rule, row_count, current_timestamp()
FROM (
  SELECT 'fact lines' AS rule, count(*) AS row_count FROM gold.fact_order_line
  UNION ALL SELECT 'revenue lines', count(*) FROM gold.fact_order_line WHERE is_revenue
  UNION ALL SELECT 'lines with unknown product (category UNKNOWN)', count(*) FROM gold.fact_order_line WHERE is_unknown_product
  UNION ALL SELECT 'lines with unknown customer (sk -1)', count(*) FROM gold.fact_order_line WHERE customer_sk = -1
  UNION ALL SELECT 'lines using an earlier-day FX rate', count(*) FROM gold.fact_order_line WHERE fx_rate_date < order_date
  UNION ALL SELECT 'lines with no FX rate (excluded from USD)', count(*) FROM gold.fact_order_line WHERE fx_missing
);

SELECT rule, row_count FROM silver.dq_report
WHERE batch_id = '{{batch_id}}' AND table_name = 'fact_order_line';
