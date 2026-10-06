-- =====================================================================
-- part5_evidence.sql  -  run cell by cell in a Glue Studio notebook (%%sql)
-- after: batch 1 run, batch 2 run, batch 2 re-run. Screenshot each result.
-- Expected versions of silver.orders: 0 CREATE TABLE, 1 MERGE (batch 1),
-- 2 MERGE (batch 2), 3 MERGE (batch 2 re-run, 0 rows updated/inserted).
-- =====================================================================

-- 1. change history
DESCRIBE HISTORY silver.orders;

-- the DataFrame version in run_evidence.py prints version / operation / rows inserted / rows updated

-- 2. point-in-time read: the table exactly as it was right after batch 1
SELECT count(*) AS orders_after_batch_1 FROM silver.orders VERSION AS OF 1;
SELECT count(*) AS orders_now           FROM silver.orders;

-- orders whose state batch 2 changed (same order, newer version)
SELECT b1.order_id, b1.status AS status_after_batch_1, cur.status AS status_now,
       b1.updated_at AS updated_after_batch_1, cur.updated_at AS updated_now
FROM silver.orders VERSION AS OF 1 b1
JOIN silver.orders cur ON cur.order_id = b1.order_id
WHERE cur.updated_at <> b1.updated_at
ORDER BY b1.order_id
LIMIT 20;

-- 3. schema enforcement: the extra column is rejected and no new version is written
--    (SQL form; the DataFrame form with the Delta error text is in run_evidence.py)
INSERT INTO silver.orders
SELECT *, 'FESTIVE10' AS coupon_type FROM silver.orders LIMIT 1;

-- 4. storage optimisation on the fact table
DESCRIBE DETAIL gold.fact_order_line;
OPTIMIZE gold.fact_order_line ZORDER BY (order_date);
DESCRIBE DETAIL gold.fact_order_line;
VACUUM gold.fact_order_line RETAIN 168 HOURS DRY RUN;

-- 5. idempotency proof: one row per key, and gold unchanged by the batch 2 re-run
SELECT count(*) AS rows, count(DISTINCT order_id) AS distinct_orders FROM silver.orders;
SELECT count(*) AS rows, count(DISTINCT order_id, line_no) AS distinct_lines FROM silver.order_items;
SELECT * FROM gold.daily_revenue ORDER BY order_date;
SELECT * FROM gold.revenue_by_category ORDER BY revenue_usd DESC;
SELECT * FROM gold.top_customers ORDER BY revenue_rank;
SELECT * FROM gold.payment_mismatches ORDER BY abs(difference) DESC;
SELECT * FROM silver.dq_report WHERE batch_id = '2' ORDER BY table_name, metric, rule;
SELECT source_table, batch_id, reason, count(*) AS n FROM quarantine.records GROUP BY 1, 2, 3 ORDER BY 2, 1, 3;
