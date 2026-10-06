-- =====================================================================
-- 08_dq_and_optimize.sql  -  reconciliation checks, DQ export, compaction
-- assert_true() raises an error when a check fails, so the Glue job fails,
-- Airflow retries, and the SNS alert fires after the last retry.
-- =====================================================================

-- every row read from bronze is accounted for: in = out + quarantined + deduplicated
SELECT table_name,
       assert_true(
         sum(CASE WHEN metric = 'rows_in' THEN row_count ELSE 0 END)
           = sum(CASE WHEN metric IN ('rows_out', 'rows_quarantined', 'rows_deduplicated') THEN row_count ELSE 0 END),
         concat('DQ reconciliation failed for ', table_name)) AS reconciliation_check
FROM silver.dq_report
WHERE batch_id = '{{batch_id}}' AND table_name IN ('orders', 'order_items', 'customers', 'payments')
GROUP BY table_name;

-- silver must hold exactly one row per order and per order line
SELECT assert_true(count(*) = count(DISTINCT order_id), 'duplicate order_id in silver.orders') AS orders_unique
FROM silver.orders;

SELECT assert_true(count(*) = count(DISTINCT order_id, line_no), 'duplicate (order_id, line_no) in silver.order_items') AS items_unique
FROM silver.order_items;

-- gold totals agree with each other
SELECT assert_true(
         (SELECT coalesce(sum(revenue_usd), 0) FROM gold.daily_revenue)
       = (SELECT coalesce(sum(revenue_usd), 0) FROM gold.revenue_by_category)
       OR abs((SELECT coalesce(sum(revenue_usd), 0) FROM gold.daily_revenue)
            - (SELECT coalesce(sum(revenue_usd), 0) FROM gold.revenue_by_category)) < 1,
         'daily_revenue and revenue_by_category totals disagree') AS gold_totals_agree;

-- DQ report for this batch, exported as JSON for submission
INSERT OVERWRITE DIRECTORY '{{lake}}/reports/dq_report_batch_{{batch_id}}'
USING json
SELECT batch_id, table_name, metric, rule, row_count, run_ts
FROM silver.dq_report
WHERE batch_id = '{{batch_id}}'
ORDER BY table_name, metric, rule;

-- quarantine summary for this batch (also visible in the log)
SELECT source_table, reason, count(*) AS rows_quarantined
FROM quarantine.records
WHERE batch_id = '{{batch_id}}'
GROUP BY source_table, reason
ORDER BY source_table, reason;

-- storage optimisation: compact small files and co-locate rows by order_date
OPTIMIZE gold.fact_order_line ZORDER BY (order_date);
