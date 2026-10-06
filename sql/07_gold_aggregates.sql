-- =====================================================================
-- 07_gold_aggregates.sql  -  business tables, all derived from the fact
-- Full rebuild every run, so re-runs give identical results.
-- =====================================================================

-- USD revenue per order date (returns already negative on their original date)
INSERT OVERWRITE gold.daily_revenue
SELECT order_date,
       CAST(round(sum(net_usd), 2) AS DECIMAL(18,2)) AS revenue_usd,
       count(DISTINCT order_id)                      AS revenue_orders,
       count(*)                                      AS revenue_lines
FROM gold.fact_order_line
WHERE is_revenue AND NOT fx_missing
GROUP BY order_date;

-- USD revenue per product category; unknown products land in UNKNOWN
INSERT OVERWRITE gold.revenue_by_category
SELECT category,
       CAST(round(sum(net_usd), 2) AS DECIMAL(18,2)) AS revenue_usd,
       count(*)                                      AS revenue_lines
FROM gold.fact_order_line
WHERE is_revenue AND NOT fx_missing
GROUP BY category;

-- top 10 customers by net USD revenue (unknown customer -1 excluded);
-- name, tier and country shown from the customer's current version
INSERT OVERWRITE gold.top_customers
SELECT revenue_rank, customer_id, full_name, tier, country, net_revenue_usd, revenue_orders
FROM (
  SELECT r.customer_id, d.full_name, d.tier, d.country, r.net_revenue_usd, r.revenue_orders,
         CAST(row_number() OVER (ORDER BY r.net_revenue_usd DESC, r.customer_id) AS INT) AS revenue_rank
  FROM (
    SELECT customer_id,
           CAST(round(sum(net_usd), 2) AS DECIMAL(18,2)) AS net_revenue_usd,
           count(DISTINCT order_id)                      AS revenue_orders
    FROM gold.fact_order_line
    WHERE is_revenue AND NOT fx_missing AND customer_sk <> -1
    GROUP BY customer_id) r
  LEFT JOIN silver.dim_customer d ON d.customer_id = r.customer_id AND d.is_current)
WHERE revenue_rank <= 10;

-- orders whose (successful - refunded) payments differ from the order net total by > 0.50
-- Scope: every order in a revenue status, plus any other order that has net money paid.
INSERT OVERWRITE gold.payment_mismatches
WITH ord AS (
  SELECT o.order_id, o.status, o.currency,
         coalesce(sum(f.net_local), 0) AS order_net
  FROM silver.orders o
  LEFT JOIN gold.fact_order_line f ON f.order_id = o.order_id
  GROUP BY o.order_id, o.status, o.currency),
pay AS (
  SELECT order_id,
         sum(CASE WHEN status = 'success'  THEN amount ELSE 0 END)
       - sum(CASE WHEN status = 'refunded' THEN amount ELSE 0 END)   AS net_paid,   -- failed ignored
         sum(CASE WHEN status = 'success'  THEN 1 ELSE 0 END)        AS success_payments,
         sum(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END)        AS refunded_payments,
         max(currency)                                               AS pay_currency,
         count(DISTINCT currency)                                    AS pay_currencies
  FROM silver.payments
  GROUP BY order_id),
cmp AS (
  SELECT o.order_id, o.status, o.currency,
         CAST(round(o.order_net, 2) AS DECIMAL(18,2))                             AS order_net_local,
         CAST(round(coalesce(p.net_paid, 0), 2) AS DECIMAL(18,2))                 AS net_paid,
         CAST(round(coalesce(p.net_paid, 0) - o.order_net, 2) AS DECIMAL(18,2))   AS difference,
         coalesce(p.success_payments, 0)                                          AS success_payments,
         coalesce(p.refunded_payments, 0)                                         AS refunded_payments,
         p.pay_currency, p.pay_currencies
  FROM ord o
  LEFT JOIN pay p ON p.order_id = o.order_id)
SELECT order_id, status, currency, order_net_local, net_paid, difference,
       success_payments, refunded_payments,
       CASE
         WHEN pay_currencies > 1 OR pay_currency <> currency THEN 'payment currency differs from order currency'
         WHEN success_payments = 0                           THEN 'no successful payment'
         WHEN net_paid > order_net_local                     THEN 'paid more than order total'
         ELSE                                                     'paid less than order total'
       END AS reason
FROM cmp
WHERE (status IN ('paid', 'shipped', 'delivered') OR net_paid <> 0)
  AND abs(difference) > 0.50;

SELECT 'daily_revenue' AS gold_table, count(*) AS row_count, CAST(sum(revenue_usd) AS DECIMAL(18,2)) AS total_usd FROM gold.daily_revenue
UNION ALL SELECT 'revenue_by_category', count(*), CAST(sum(revenue_usd) AS DECIMAL(18,2)) FROM gold.revenue_by_category
UNION ALL SELECT 'top_customers', count(*), CAST(sum(net_revenue_usd) AS DECIMAL(18,2)) FROM gold.top_customers
UNION ALL SELECT 'payment_mismatches', count(*), CAST(sum(difference) AS DECIMAL(18,2)) FROM gold.payment_mismatches;
