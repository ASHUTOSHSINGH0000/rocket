# NovaCart Medallion Lakehouse — Design Note

## 1. Platform choice
**AWS:** S3, Delta Lake, AWS Glue 5.0 (Spark SQL), Glue Data Catalog, MWAA (Airflow), EventBridge + Lambda, and SNS.
- **Delta Lake** gives ACID MERGE, time travel, schema enforcement, OPTIMIZE/VACUUM and Change Data Feed (CDF) on plain S3. There is no warehouse to run.
- **Glue** is serverless Spark. One generic runner (`glue/run_sql.py`) executes versioned `.sql` files, so the pipeline logic is all reviewable SQL.
- **MWAA** gives the batch-parameterised DAG retries, alerting and a run history.

## 2. Architecture
`input/batch_n/` → **Bronze** (raw strings plus `_source_file`, `_batch_id`, `_ingested_at`) → **Silver** → **Gold**.
- **Silver** holds typed, deduplicated, MERGEd tables, the SCD2 `dim_customer`, quarantine and DQ rows.
- **Gold** holds `fact_order_line`, `daily_revenue`, `revenue_by_category`, `top_customers` and `payment_mismatches`.
- Each step is one SQL file (`00`–`08`). The DAG runs one Glue task per step, in this order: setup → bronze → silver_orders → silver_reference → (items ‖ dim_customer) → fact → aggregates → DQ/optimize.

## 3. Correctness and idempotency
- **Bronze:** `DELETE WHERE _batch_id = n` then `INSERT`. Re-running a batch replaces its slice and never duplicates it.
- **Silver orders:** a `row_number()` dedup inside the batch, then `MERGE … WHEN MATCHED AND s.updated_at > t.updated_at`. Older or out-of-order versions are rejected: 10 such rows in batch 2.
- **Order items:** lines are matched on (order_id, line_no) and updated only from a newer batch.
- **SCD2 `dim_customer`:**
  - A new version opens only when tier or country changes; e-mail-only changes don't create one.
  - It has half-open validity `[valid_from, valid_to)`, a hash surrogate key, and an unknown member `-1`.
- **Point-in-time joins:**
  - The customer version is the one valid at the order timestamp.
  - FX uses the last available business-day rate: a daily calendar forward-filled with `last(rate, true)`.
- **Reconciliation:** `assert_true(rows_in = rows_out + quarantined + deduplicated)` fails the job, and so the DAG task, on any leak.
- **Proven on the real files:** batch 1 → 2 → 2 re-run gives a re-run with **0 changed rows** in every table. Total revenue of **295,585.58 USD** matches an independent pandas recalculation per day.

## 4. Orchestration and trigger
- **DAG settings:** `batch_id` param, 2 retries with exponential backoff, and `max_active_runs=1`, so two batches never write the same Delta table concurrently.
- **Alerts:** SNS on the final task failure, plus a run-level `ONE_FAILED` alert task.
- **Trigger:** S3 `_SUCCESS` → EventBridge → Lambda → MWAA CLI `dags trigger -c {"batch_id": n}`.
  - We chose the `_SUCCESS` marker over per-file events so a half-uploaded batch never starts a run.
  - The marker is uploaded last.

## 5. Assumptions
- **Timestamps:** formats `Z`, `+05:30` and `dd/MM/yyyy HH:mm` are all normalised to UTC. The bare format is assumed to be UTC.
- **Currency:** values are upper-cased. A blank currency is derived from `shipping_country`.
- **Revenue:** counts statuses `PLACED`, `SHIPPED` and `DELIVERED`. Cancelled and returned orders are excluded.
- **Discounts:** a null discount is treated as 0.
- **Unknown products** (P101–P105): kept with category `UNKNOWN`, not dropped, so revenue is not understated.
- **Unknown customers** (C0901–C0906): mapped to `customer_sk = -1`.
- **Quarantined, with a reason column:**
  - order lines whose order does not exist: 18 orphans;
  - payments for unknown orders;
  - CRM rows without `updated_at`.
- **Payment mismatch:** `abs(order_total_usd − net_paid_usd) > 0.50` on revenue orders, or on any order with a non-zero net payment.

## 6. What blocked us
The AWS infrastructure was created: S3 lakehouse, Glue databases, IAM role and SNS topic.

**`glue:CreateJob` failed with *"Account … is denied access"*.** This is an account-level restriction on Glue, typical of new or free-plan accounts. It is not an IAM-policy or code issue. As a result, the Glue job, the MWAA DAG and the Lambda trigger could not run in the cloud within the time.

**How we compensated:**
- We validated the full SQL pipeline on the real files with a local Spark harness.
- We included `local_delta/run_local_delta.sh`, which runs the identical SQL on real Delta Lake on a laptop, together with the Delta evidence.

## 7. Improvements with more time
- Get Glue enabled through AWS Support and run `infra/quick_run.sh` end to end. Then deploy the MWAA DAG and the trigger.
- Infrastructure as code (CDK/Terraform) instead of CLI scripts, plus CI that runs the local harness on every PR.
- Lake Formation permissions on the catalog; Great Expectations or Deequ for declarative DQ.
- Incremental gold driven by CDF instead of a full gold rebuild.
- Partition `fact_order_line` by `order_date` once volume grows.

## 8. AI usage
We used Claude for architecture brainstorming and for drafting SQL, scripts and docs.
- The team profiled the input files and checked every rule against the data.
- We ran the pipeline end to end and cross-checked the results independently.
