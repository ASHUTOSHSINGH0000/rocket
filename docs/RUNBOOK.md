# NovaCart Order Analytics on AWS - Medallion Lakehouse

S3 + Delta Lake + AWS Glue 5.0 (Spark SQL) + Glue Data Catalog + Amazon MWAA (Airflow) + EventBridge/Lambda + SNS.
All transformation logic is plain Spark SQL in `sql/`; `glue/run_sql.py` only renders and runs those files.

```
novacart-aws/
├── sql/                      00_setup … 08_dq_and_optimize  (the whole pipeline, in order)
├── glue/run_sql.py           Glue job script: --batch_id, --steps, --lake, --input, --sql_root
├── dags/novacart_medallion_dag.py   MWAA DAG: batch_id param, 2 retries/task, max_active_runs=1, SNS alerts
├── lambda/trigger_dag_lambda.py     S3 _SUCCESS -> EventBridge -> Lambda -> DAG run
├── infra/                    01_create_core.sh, 02_create_trigger_and_mwaa_access.sh,
│   └── iam/                  03_upload_batch.sh, 99_teardown.sh + IAM / EventBridge JSON
├── evidence/                 part5_evidence.sql (notebook cells), run_evidence.py (all Part 5/6 evidence)
├── input/                    the hackathon files, renamed to the brief's names
└── local_test/               Spark-only logic test + expected_results/ from the real files
```

## Pipeline steps (one Glue run per step, orchestrated by the DAG)

| Step file | Layer | What it does |
|---|---|---|
| `00_setup` | all | Creates Glue databases `bronze`, `silver`, `gold`, `quarantine` and every Delta table (idempotent). CDF on for `silver.orders` / `silver.order_items`. |
| `01_bronze` | bronze | Reads every file as STRING, adds `_source_file`, `_batch_id`, `_ingested_at`. `DELETE` the batch slice, then `INSERT` → no duplicates on re-run. |
| `02_silver_orders` | silver | Parses 3 timestamp formats to UTC, fixes/derives currency, quarantines bad rows, keeps 1 row per order, `MERGE` with `s.updated_at > t.updated_at`. |
| `03_silver_reference` | silver | FX, products, CRM customers (no `updated_at` → quarantine), payments (unknown order → quarantine). |
| `04_silver_order_items` | silver | Types lines, null discount = 0, unknown product flagged, unknown order quarantined, dedup, `MERGE`. |
| `05_dim_customer` | silver | SCD2: new version only on tier/country change, `valid_from/valid_to/is_current`, hash `customer_sk`, unknown member `-1`. |
| `06_gold_fact_order_line` | gold | Order-line fact: net local, FX as-of (last business day), net USD, revenue flag, point-in-time customer. |
| `07_gold_aggregates` | gold | `daily_revenue`, `revenue_by_category`, `top_customers`, `payment_mismatches`. |
| `08_dq_and_optimize` | all | `assert_true` reconciliation checks, DQ JSON export to `reports/`, `OPTIMIZE … ZORDER BY (order_date)`. |

---

## FASTEST PATH (about 20 minutes, no MWAA)
```bash
aws configure                      # your access key, secret, region ap-south-1
ALERT_EMAIL=you@example.com BUCKET=novacart-lakehouse-<yourname> ./infra/quick_run.sh
```
Creates everything, runs batch 1, batch 2, batch 2 again and the Delta evidence job on Glue, and downloads the DQ reports.
Add MWAA later (Phase 1.5 and 4) if you have time; it takes about 25 minutes to create.

## PHASE 1 - Software and environment setup

### 1.1 Local prerequisites
| Tool | Why | Install |
|---|---|---|
| AWS CLI v2 | create resources, upload files | https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html, then `aws configure` |
| Python 3.10–3.12 + Java 17 | optional local logic test | `pip install pyspark==3.5.3 pandas` |
| zip, bash | packaging the Lambda, running scripts | standard on macOS/Linux (Windows: use WSL or Git Bash) |

Check: `aws sts get-caller-identity` returns your account.

### 1.2 – 1.4 S3 layout, Glue catalog, IAM, SNS, Glue job (one script)
```bash
export AWS_REGION=ap-south-1          # any region with Glue 5.0 and MWAA
export BUCKET=novacart-lakehouse      # bucket names are global: add a suffix if taken
ALERT_EMAIL=you@example.com ./infra/01_create_core.sh
```
It creates (idempotently):
- `s3://$BUCKET/` with `input/batch_1/`, `input/batch_2/`, `input/reference/`, `bronze/`, `silver/`, `gold/`, `quarantine/`, `reports/`, `scripts/`, plus public-access block, SSE-S3, and EventBridge notifications on.
- Glue databases `bronze`, `silver`, `gold`, `quarantine`. These names match the brief's table names (`silver.orders`, `gold.fact_order_line`); `00_setup.sql` also creates them if missing.
- IAM role `NovaCartGlueRole` (managed `AWSGlueServiceRole` + `infra/iam/glue-lakehouse-policy.json`: bucket read/write, Glue catalog, logs).
- SNS topic `novacart-pipeline-alerts` with your e-mail. **Confirm the subscription e-mail.**
- Glue job `novacart_run_sql`: Glue 5.0, 2 × G.1X, 30 min timeout, Glue retries 0 (Airflow owns retries), max 3 concurrent runs, and `--datalake-formats delta` with the Delta `--conf`.

IAM summary:

| Principal | Permissions | File |
|---|---|---|
| Glue job role | S3 bucket R/W, Glue catalog, CloudWatch logs | `iam/glue-lakehouse-policy.json` |
| MWAA execution role (added) | `glue:StartJobRun/GetJobRun/GetJob`, read Glue logs, `sns:Publish` | `iam/mwaa-execution-extra-policy.json` |
| Lambda role | `airflow:CreateCliToken` on the environment, logs | `iam/lambda-trigger-policy.json` |
| EventBridge → Lambda | resource policy `lambda:InvokeFunction` (added by script 02) | – |

**Smoke test without Airflow** (runs every step for batch 1, about 5–8 minutes):
```bash
./infra/03_upload_batch.sh 1
aws glue start-job-run --job-name novacart_run_sql --arguments '{"--batch_id":"1","--steps":"all"}'
```

### 1.5 MWAA environment (console, one time, about 25 minutes to create)
1. S3 → create bucket `novacart-mwaa-<suffix>` with versioning **on** (MWAA requires it) and a `dags/` folder.
2. MWAA → Create environment: name `novacart-mwaa`, the latest Airflow 2.x version, DAG folder `s3://novacart-mwaa-<suffix>/dags`, "Create MWAA VPC" (CloudFormation quick-create), web server access **Public**, class `mw1.small` (or `mw1.micro` where offered), "Create new role".
3. Then:
```bash
MWAA_ENV=novacart-mwaa MWAA_DAGS_BUCKET=novacart-mwaa-<suffix> ./infra/02_create_trigger_and_mwaa_access.sh
```
4. Airflow UI → Admin → Variables: `novacart_sns_topic_arn` = the ARN printed by script 01, `novacart_aws_region` = your region.

> Cost: MWAA bills per hour while the environment exists. Create it for the demo and delete it right after.
> Cheaper fallback: run the same DAG on local Airflow in Docker; it still starts the Glue jobs.

---

## PHASE 2 - Input data
The real hackathon files are in `input/`, renamed to the brief's names (`order_items_batch_1.jsonl`, `payments.json`). No synthetic generator is needed. Profiling found these deliberate traps; each is handled and counted in the DQ report:

| Trap in the data | Handling |
|---|---|
| 3 timestamp formats (`…Z`, `…+05:30`, `dd/MM/yyyy HH:mm`) | `coalesce` of two `to_timestamp` patterns, session TZ = UTC |
| Lower-case currencies (`inr`, `eur`) and 194 blank currencies | `upper(trim())`, blank → derived from `shipping_country` |
| Many versions per order (923 / 948 superseded rows) and exact duplicate rows | `row_number` by `updated_at` within the batch |
| 10 orders in batch 2 older than batch 1's version | `MERGE … WHEN MATCHED AND s.updated_at > t.updated_at` → not applied |
| 36 batch 2 lines for batch 1 orders | orphan check against cumulative `silver.orders` |
| Orphan lines `NC-9900xx` (10 + 8) | quarantine "order does not exist" |
| Unknown products P101–P105 | loaded, `is_unknown_product`, category `UNKNOWN` |
| `unit_price` / `discount_pct` sometimes JSON strings, discount null | cast; null discount = 0 |
| Exact duplicate item lines (25 per batch) | dedup on `(order_id, line_no)` |
| CRM: 5 rows without `updated_at`, 4 duplicates, email-only changes | quarantine, dedup, no new SCD2 version |
| Customers `C0901–C0906` not in CRM | `customer_sk = -1`, country `UNKNOWN` |
| FX on business days only (no 5–7, 12–13, 19–20, 26–27 Sep) | daily calendar forward-filled with `last(rate, true)` |
| Payments for unknown orders `NC-8000xx` | quarantine "payment for unknown order" |

---

## PHASE 3 - Glue processing code
`glue/run_sql.py` + `sql/*.sql`. The runner:
- reads `--batch_id --steps --lake --input --sql_root` (Glue `getResolvedOptions`, or argparse locally);
- downloads each SQL file from S3, fills `{{batch_id}}`, `{{lake}}`, `{{input}}`;
- splits statements safely (ignores `;` inside quotes/comments) and runs them with `spark.sql`;
- prints the result of every `SELECT` to the Glue log, which also forces the `assert_true` checks.

Any failed statement fails the Glue run, so Airflow retries and then sends the alert.

## PHASE 4 - Orchestration and automation
- **DAG** `dags/novacart_medallion_dag.py`:
  - Task order: `setup_tables → bronze_ingest → silver_orders → silver_reference → [silver_order_items, scd2_dim_customer] → gold_fact_order_line → gold_aggregates → dq_and_optimize`.
  - Each task is a `GlueJobOperator`, with `retries=2` and exponential backoff.
  - `on_failure_callback` sends SNS after the last retry, plus an `alert_on_failure` task (`trigger_rule=one_failed`).
  - `max_active_runs=1`.
- **Trigger** `lambda/trigger_dag_lambda.py`:
  - Path: `input/batch_<n>/_SUCCESS` → EventBridge rule `novacart-batch-success` (wildcard key match) → Lambda → MWAA CLI endpoint `dags trigger novacart_medallion -c {"batch_id":"<n>"}`.
  - `03_upload_batch.sh` writes `_SUCCESS` last, so the run only starts once the batch is complete.
- **Manual run:** Airflow UI → ▶ Trigger DAG w/ config → `{"batch_id": "1"}`.

Run order for the demo: batch 1 → batch 2 → batch 2 again (manual trigger). Screenshot the Grid view of each run.
Failure demo: trigger `{"batch_id": "9"}`. Bronze fails (no files), retries twice, then the SNS e-mail arrives.

## PHASE 5 - Delta evidence and viva
- Glue Studio notebook (Glue 5.0, `%%configure` with the same `--datalake-formats delta` / `--conf`), run `evidence/part5_evidence.sql` cell by cell, **or**
- Run `evidence/run_evidence.py` as a Glue job (same settings). It prints:
  1. `DESCRIBE HISTORY silver.orders`. Expected: v0 CREATE, v1 MERGE (495 inserted), v2 MERGE (137 updated, 450 inserted), v3 MERGE re-run (**0 updated, 0 inserted**).
  2. `VERSION AS OF 1` vs current: 495 vs 945 orders, plus the 137 orders batch 2 changed.
  3. Schema enforcement: appending `coupon_type` is rejected (`AnalysisException: A schema mismatch detected…`), and the version count is unchanged.
  4. `OPTIMIZE gold.fact_order_line ZORDER BY (order_date)` with file counts before and after, plus `VACUUM … DRY RUN`.
  5. Change Data Feed of batch 2: `update_preimage/postimage` per changed order.
  6. One row per key in silver.

### Expected results (validated on the real files, see `local_test/expected_results/`)
| Check | Batch 1 | Batch 2 | Batch 2 re-run |
|---|---|---|---|
| `silver.orders` rows | 495 | 945 | 945 (0 changed) |
| orders MERGE updated / inserted | 0 / 495 | 137 / 450 (10 stale not applied) | **0 / 0** |
| `silver.order_items` rows | 971 | 1,933 | 1,933 (0 changed) |
| items quarantined (order does not exist) | 10 | 8 | 8 |
| fact lines / revenue lines | 971 / 766 | 1,933 / 1,660 | identical |
| total revenue USD | – | **295,585.58** | identical |
| payment mismatches | – | 14 orders | identical |

Revenue by category after batch 2:

| Category | USD |
|---|---|
| Electronics | 223,943.51 |
| Fashion | 20,869.65 |
| Sports | 18,413.67 |
| Home | 15,924.47 |
| Beauty | 9,639.45 |
| Books | 3,834.98 |
| UNKNOWN | 2,959.85 |

Daily revenue matches an independent pandas calculation to the cent on all 30 days.

### Assumptions (copy into the design note)
1. Order date = UTC date of `order_ts`. In this data every order falls on 1–30 Sep.
2. Equal `updated_at` tie-break: status progression, then source file.
3. Items have no `updated_at`: a later batch replaces a line, and a re-run changes nothing.
4. Reference files are full snapshots, reloaded every run. Payments for orders not yet received are quarantined for that run: batch 1 has 537 (they belong to batch 2 orders), batch 2 has 5 genuine orphans.
5. A provided currency is kept even if it differs from the shipping country. Only missing currencies are derived.
6. SCD2 tracks tier and country only. A version keeps the name and email it had at that time.
7. Orders before a customer's first CRM version map to `-1`. There are 0 such orders in this data.
8. FX: a weekend or holiday uses the last earlier business-day rate. USD rate = 1. A missing rate sets `fx_missing` and excludes the line from USD totals (0 lines here).
9. Payment mismatch scope:
   - Covers every order in a revenue status, plus any other order with money paid.
   - `net paid = success − refunded`; failed payments are ignored.
   - Compared in the order currency. Payment currency equals order currency for all 1,159 payments.
10. `top_customers` excludes the unknown customer and shows each customer's current tier and country.
11. Items whose order arrives in a later batch are quarantined, not held. None exist in this data; the improvement is re-processing quarantined orphans.

### Local logic test (optional, no AWS)
```bash
pip install pyspark==3.5.3 pandas
./local_test/run_local_test.sh
```
`local_test/harness.py` runs the **same SQL files** on plain Spark. Only the Delta-only statements are emulated: `USING DELTA` becomes parquet, and `DELETE` / `MERGE` are rewritten with identical semantics. `OPTIMIZE` is skipped. Delta features (history, time travel, schema enforcement, OPTIMIZE) are proven on AWS with `evidence/`.

### Teardown
```bash
CONFIRM=yes ./infra/99_teardown.sh     # then delete the MWAA environment in the console
```
