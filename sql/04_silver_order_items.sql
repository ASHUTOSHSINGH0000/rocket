-- =====================================================================
-- 04_silver_order_items.sql  -  type, flag, quarantine, MERGE silver.order_items
-- Rule 3: a null discount means 0.
-- Rule 7: unknown product -> load and flag;  order does not exist -> quarantine.
-- The order check uses the cumulative silver.orders, so a batch 2 line for a
-- batch 1 order is accepted. Runs after 02 (orders) and 03 (products).
-- =====================================================================

CREATE OR REPLACE TEMPORARY VIEW items_typed AS
SELECT nullif(trim(i.order_id), '')                                       AS order_id,
       CAST(trim(i.line_no) AS INT)                                        AS line_no,
       nullif(trim(i.product_id), '')                                     AS product_id,
       CAST(trim(i.qty) AS INT)                                            AS qty,
       CAST(trim(i.unit_price) AS DECIMAL(18,4))                           AS unit_price,
       CAST(coalesce(nullif(trim(i.discount_pct), ''), '0') AS DECIMAL(5,2)) AS discount_pct,
       lower(trim(i.line_type))                                           AS line_type,
       from_json(i.attributes, 'map<string,string>')                      AS attributes,
       CAST(i._batch_id AS INT)                                            AS _batch_id,
       i._raw                                                             AS raw_line,
       o.order_id IS NOT NULL                                             AS order_exists,
       p.product_id IS NULL                                               AS is_unknown_product
FROM bronze.order_items i
LEFT JOIN silver.orders   o ON o.order_id   = trim(i.order_id)
LEFT JOIN silver.products p ON p.product_id = trim(i.product_id)
WHERE i._batch_id = '{{batch_id}}';

CREATE OR REPLACE TEMPORARY VIEW items_checked AS
SELECT *,
  CASE
    WHEN order_id IS NULL OR line_no IS NULL          THEN 'missing key (order_id, line_no)'
    WHEN NOT order_exists                             THEN 'order does not exist'
    WHEN qty IS NULL OR qty = 0                       THEN 'invalid qty'
    WHEN unit_price IS NULL OR unit_price < 0         THEN 'invalid unit_price'
    WHEN discount_pct IS NULL OR discount_pct < 0 OR discount_pct > 100
                                                      THEN 'discount_pct out of range'
    WHEN line_type IS NULL OR line_type NOT IN ('sale', 'return')
                                                      THEN 'invalid line_type'
    WHEN line_type = 'return' AND qty > 0             THEN 'return with positive qty'
    WHEN line_type = 'sale'   AND qty < 0             THEN 'sale with negative qty'
  END AS dq_reason
FROM items_typed;

DELETE FROM quarantine.records WHERE source_table = 'order_items' AND batch_id = '{{batch_id}}';

INSERT INTO quarantine.records
SELECT 'order_items', '{{batch_id}}', dq_reason, raw_line, current_timestamp()
FROM items_checked
WHERE dq_reason IS NOT NULL;

-- one row per (order_id, line_no) within the batch; exact duplicates collapse here
CREATE OR REPLACE TEMPORARY VIEW items_ranked AS
SELECT *, row_number() OVER (PARTITION BY order_id, line_no
                             ORDER BY _batch_id DESC, sha2(raw_line, 256)) AS rn
FROM items_checked
WHERE dq_reason IS NULL;

CREATE OR REPLACE TEMPORARY VIEW items_latest AS
SELECT order_id, line_no, product_id, qty, unit_price, discount_pct, line_type, attributes,
       is_unknown_product, _batch_id, current_timestamp() AS _merged_at
FROM items_ranked
WHERE rn = 1;

-- a later batch replaces a line (items have no updated_at); a re-run of the
-- same batch matches no branch, so nothing is rewritten
MERGE INTO silver.order_items AS t
USING items_latest AS s
  ON t.order_id = s.order_id AND t.line_no = s.line_no
WHEN MATCHED AND s._batch_id > t._batch_id THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;

DELETE FROM silver.dq_report WHERE batch_id = '{{batch_id}}' AND table_name = 'order_items';

INSERT INTO silver.dq_report
SELECT '{{batch_id}}', 'order_items', metric, rule, row_count, current_timestamp()
FROM (
  SELECT 'rows_in' AS metric, 'all rows read from bronze' AS rule, count(*) AS row_count FROM items_checked
  UNION ALL SELECT 'rows_quarantined', dq_reason, count(*) FROM items_checked WHERE dq_reason IS NOT NULL GROUP BY dq_reason
  UNION ALL SELECT 'rows_deduplicated', 'duplicate (order_id, line_no) within batch', count(*) FROM items_ranked WHERE rn > 1
  UNION ALL SELECT 'rows_out', 'candidates sent to MERGE', count(*) FROM items_latest
  UNION ALL SELECT 'info', 'unknown product flagged (loaded)', count(*) FROM items_latest WHERE is_unknown_product
  UNION ALL SELECT 'info', 'return lines', count(*) FROM items_latest WHERE line_type = 'return'
);

SELECT metric, rule, row_count FROM silver.dq_report
WHERE batch_id = '{{batch_id}}' AND table_name = 'order_items' ORDER BY metric, rule;
