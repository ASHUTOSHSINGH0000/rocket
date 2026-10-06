-- =====================================================================
-- 01_bronze.sql  -  raw ingestion, values kept exactly as received
-- Idempotent: each table first deletes its own _batch_id slice, then
-- inserts it again, so re-running a batch never duplicates rows.
-- Placeholders: {{input}} = s3://novacart-lakehouse/input   {{batch_id}}
-- Batch files      : {{input}}/batch_<n>/orders_batch_<n>.csv, order_items_batch_<n>.jsonl
-- Reference files  : {{input}}/reference/  (reloaded on every run under the run's batch_id)
-- =====================================================================

-- ---------- orders (CSV, every column read as STRING, no schema inference)
CREATE OR REPLACE TEMPORARY VIEW src_orders
USING csv
OPTIONS (path '{{input}}/batch_{{batch_id}}/orders_batch_{{batch_id}}.csv',
         header 'true', inferSchema 'false', mode 'PERMISSIVE');

DELETE FROM bronze.orders WHERE _batch_id = '{{batch_id}}';

INSERT INTO bronze.orders
SELECT order_id, customer_id, order_ts, status, currency, shipping_country, updated_at, promo_code,
       input_file_name(), '{{batch_id}}', current_timestamp()
FROM src_orders;

-- ---------- order items (JSON lines, read as text so every value stays as sent)
CREATE OR REPLACE TEMPORARY VIEW src_items
USING text
OPTIONS (path '{{input}}/batch_{{batch_id}}/order_items_batch_{{batch_id}}.jsonl');

DELETE FROM bronze.order_items WHERE _batch_id = '{{batch_id}}';

INSERT INTO bronze.order_items
SELECT get_json_object(value, '$.order_id'),
       get_json_object(value, '$.line_no'),
       get_json_object(value, '$.product_id'),
       get_json_object(value, '$.qty'),
       get_json_object(value, '$.unit_price'),
       get_json_object(value, '$.discount_pct'),
       get_json_object(value, '$.line_type'),
       get_json_object(value, '$.attributes'),
       value,
       input_file_name(), '{{batch_id}}', current_timestamp()
FROM src_items
WHERE trim(value) <> '';

-- ---------- customers (CRM change log)
CREATE OR REPLACE TEMPORARY VIEW src_customers
USING csv
OPTIONS (path '{{input}}/reference/customers_changes.csv', header 'true', inferSchema 'false');

DELETE FROM bronze.customers WHERE _batch_id = '{{batch_id}}';

INSERT INTO bronze.customers
SELECT customer_id, full_name, email, tier, country, updated_at,
       input_file_name(), '{{batch_id}}', current_timestamp()
FROM src_customers;

-- ---------- products
CREATE OR REPLACE TEMPORARY VIEW src_products
USING csv
OPTIONS (path '{{input}}/reference/products.csv', header 'true', inferSchema 'false');

DELETE FROM bronze.products WHERE _batch_id = '{{batch_id}}';

INSERT INTO bronze.products
SELECT product_id, sku, product_name, category, list_price_usd,
       input_file_name(), '{{batch_id}}', current_timestamp()
FROM src_products;

-- ---------- fx rates (business days only)
CREATE OR REPLACE TEMPORARY VIEW src_fx
USING csv
OPTIONS (path '{{input}}/reference/fx_rates.csv', header 'true', inferSchema 'false');

DELETE FROM bronze.fx_rates WHERE _batch_id = '{{batch_id}}';

INSERT INTO bronze.fx_rates
SELECT rate_date, currency, rate_to_usd,
       input_file_name(), '{{batch_id}}', current_timestamp()
FROM src_fx;

-- ---------- payments (one multi-line JSON array)
CREATE OR REPLACE TEMPORARY VIEW src_payments
USING json
OPTIONS (path '{{input}}/reference/payments.json', multiLine 'true', primitivesAsString 'true');

DELETE FROM bronze.payments WHERE _batch_id = '{{batch_id}}';

INSERT INTO bronze.payments
SELECT payment_id, order_id, method, amount, currency, status, paid_at, gateway_ref,
       input_file_name(), '{{batch_id}}', current_timestamp()
FROM src_payments;

-- ---------- run summary (printed to the Glue / CloudWatch log)
SELECT 'orders' AS bronze_table, count(*) AS rows_this_batch FROM bronze.orders WHERE _batch_id = '{{batch_id}}'
UNION ALL SELECT 'order_items', count(*) FROM bronze.order_items WHERE _batch_id = '{{batch_id}}'
UNION ALL SELECT 'customers',   count(*) FROM bronze.customers   WHERE _batch_id = '{{batch_id}}'
UNION ALL SELECT 'products',    count(*) FROM bronze.products    WHERE _batch_id = '{{batch_id}}'
UNION ALL SELECT 'fx_rates',    count(*) FROM bronze.fx_rates    WHERE _batch_id = '{{batch_id}}'
UNION ALL SELECT 'payments',    count(*) FROM bronze.payments    WHERE _batch_id = '{{batch_id}}';
