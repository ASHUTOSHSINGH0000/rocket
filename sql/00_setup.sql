-- =====================================================================
-- 00_setup.sql  -  one-time DDL for the NovaCart lakehouse (idempotent)
-- Placeholders filled by run_sql.py:  {{lake}} = s3://novacart-lakehouse
-- All tables are Delta tables at explicit S3 locations, registered in the
-- AWS Glue Data Catalog (databases bronze, silver, gold, quarantine).
-- =====================================================================

CREATE DATABASE IF NOT EXISTS bronze     LOCATION '{{lake}}/bronze/';
CREATE DATABASE IF NOT EXISTS silver     LOCATION '{{lake}}/silver/';
CREATE DATABASE IF NOT EXISTS gold       LOCATION '{{lake}}/gold/';
CREATE DATABASE IF NOT EXISTS quarantine LOCATION '{{lake}}/quarantine/';

-- ---------------- BRONZE: raw strings + lineage --------------------------
CREATE TABLE IF NOT EXISTS bronze.orders (
  order_id STRING, customer_id STRING, order_ts STRING, status STRING, currency STRING,
  shipping_country STRING, updated_at STRING, promo_code STRING,
  _source_file STRING, _batch_id STRING, _ingested_at TIMESTAMP)
USING DELTA LOCATION '{{lake}}/bronze/orders';

CREATE TABLE IF NOT EXISTS bronze.order_items (
  order_id STRING, line_no STRING, product_id STRING, qty STRING, unit_price STRING,
  discount_pct STRING, line_type STRING, attributes STRING, _raw STRING,
  _source_file STRING, _batch_id STRING, _ingested_at TIMESTAMP)
USING DELTA LOCATION '{{lake}}/bronze/order_items';

CREATE TABLE IF NOT EXISTS bronze.customers (
  customer_id STRING, full_name STRING, email STRING, tier STRING, country STRING, updated_at STRING,
  _source_file STRING, _batch_id STRING, _ingested_at TIMESTAMP)
USING DELTA LOCATION '{{lake}}/bronze/customers';

CREATE TABLE IF NOT EXISTS bronze.products (
  product_id STRING, sku STRING, product_name STRING, category STRING, list_price_usd STRING,
  _source_file STRING, _batch_id STRING, _ingested_at TIMESTAMP)
USING DELTA LOCATION '{{lake}}/bronze/products';

CREATE TABLE IF NOT EXISTS bronze.fx_rates (
  rate_date STRING, currency STRING, rate_to_usd STRING,
  _source_file STRING, _batch_id STRING, _ingested_at TIMESTAMP)
USING DELTA LOCATION '{{lake}}/bronze/fx_rates';

CREATE TABLE IF NOT EXISTS bronze.payments (
  payment_id STRING, order_id STRING, method STRING, amount STRING, currency STRING,
  status STRING, paid_at STRING, gateway_ref STRING,
  _source_file STRING, _batch_id STRING, _ingested_at TIMESTAMP)
USING DELTA LOCATION '{{lake}}/bronze/payments';

-- ---------------- SILVER: typed, cleaned, merged --------------------------
CREATE TABLE IF NOT EXISTS silver.orders (
  order_id STRING NOT NULL, customer_id STRING, order_ts_utc TIMESTAMP, order_date DATE,
  status STRING, currency STRING, currency_derived BOOLEAN, shipping_country STRING,
  promo_code STRING, updated_at TIMESTAMP, _batch_id INT, _merged_at TIMESTAMP)
USING DELTA LOCATION '{{lake}}/silver/orders'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS silver.order_items (
  order_id STRING NOT NULL, line_no INT NOT NULL, product_id STRING, qty INT,
  unit_price DECIMAL(18,4), discount_pct DECIMAL(5,2), line_type STRING,
  attributes MAP<STRING,STRING>, is_unknown_product BOOLEAN, _batch_id INT, _merged_at TIMESTAMP)
USING DELTA LOCATION '{{lake}}/silver/order_items'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS silver.customers (
  customer_id STRING, full_name STRING, email STRING, tier STRING, country STRING, updated_at TIMESTAMP)
USING DELTA LOCATION '{{lake}}/silver/customers';

CREATE TABLE IF NOT EXISTS silver.products (
  product_id STRING, sku STRING, product_name STRING, category STRING, list_price_usd DECIMAL(18,2))
USING DELTA LOCATION '{{lake}}/silver/products';

CREATE TABLE IF NOT EXISTS silver.fx_rates (
  rate_date DATE, currency STRING, rate_to_usd DECIMAL(18,8))
USING DELTA LOCATION '{{lake}}/silver/fx_rates';

CREATE TABLE IF NOT EXISTS silver.payments (
  payment_id STRING, order_id STRING, method STRING, amount DECIMAL(18,2), currency STRING,
  status STRING, paid_at TIMESTAMP)
USING DELTA LOCATION '{{lake}}/silver/payments';

CREATE TABLE IF NOT EXISTS silver.dim_customer (
  customer_sk BIGINT, customer_id STRING, full_name STRING, email STRING, tier STRING, country STRING,
  valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN)
USING DELTA LOCATION '{{lake}}/silver/dim_customer';

CREATE TABLE IF NOT EXISTS silver.dq_report (
  batch_id STRING, table_name STRING, metric STRING, rule STRING, row_count BIGINT, run_ts TIMESTAMP)
USING DELTA LOCATION '{{lake}}/silver/dq_report';

CREATE TABLE IF NOT EXISTS quarantine.records (
  source_table STRING, batch_id STRING, reason STRING, record STRING, quarantined_at TIMESTAMP)
USING DELTA PARTITIONED BY (source_table)
LOCATION '{{lake}}/quarantine/records';

-- ---------------- GOLD ------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_order_line (
  order_id STRING, line_no INT, order_date DATE, order_ts_utc TIMESTAMP, customer_id STRING,
  customer_sk BIGINT, product_id STRING, category STRING, line_type STRING, qty INT,
  unit_price DECIMAL(18,4), discount_pct DECIMAL(5,2), currency STRING, status STRING,
  net_local DECIMAL(18,4), fx_rate DECIMAL(18,8), fx_rate_date DATE, net_usd DECIMAL(18,4),
  is_revenue BOOLEAN, is_unknown_product BOOLEAN, fx_missing BOOLEAN)
USING DELTA LOCATION '{{lake}}/gold/fact_order_line';

CREATE TABLE IF NOT EXISTS gold.daily_revenue (
  order_date DATE, revenue_usd DECIMAL(18,2), revenue_orders BIGINT, revenue_lines BIGINT)
USING DELTA LOCATION '{{lake}}/gold/daily_revenue';

CREATE TABLE IF NOT EXISTS gold.revenue_by_category (
  category STRING, revenue_usd DECIMAL(18,2), revenue_lines BIGINT)
USING DELTA LOCATION '{{lake}}/gold/revenue_by_category';

CREATE TABLE IF NOT EXISTS gold.top_customers (
  revenue_rank INT, customer_id STRING, full_name STRING, tier STRING, country STRING,
  net_revenue_usd DECIMAL(18,2), revenue_orders BIGINT)
USING DELTA LOCATION '{{lake}}/gold/top_customers';

CREATE TABLE IF NOT EXISTS gold.payment_mismatches (
  order_id STRING, status STRING, currency STRING, order_net_local DECIMAL(18,2),
  net_paid DECIMAL(18,2), difference DECIMAL(18,2), success_payments BIGINT,
  refunded_payments BIGINT, reason STRING)
USING DELTA LOCATION '{{lake}}/gold/payment_mismatches';
