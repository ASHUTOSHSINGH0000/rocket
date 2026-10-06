"""
novacart_medallion_dag.py - Amazon MWAA (Airflow 2.x) DAG for the NovaCart medallion pipeline.

One Glue job (novacart_run_sql, script glue/run_sql.py) is started once per step with
--batch_id and --steps. Every task retries twice; when a task fails for the last time its
on_failure_callback publishes to SNS, and alert_on_failure (trigger_rule=one_failed) sends a
run-level alert. max_active_runs=1 serialises runs so two batches never write the same Delta
table at the same time.

Upload to the MWAA environment's dags/ folder. Set the Airflow Variables (Admin > Variables):
    novacart_sns_topic_arn   arn:aws:sns:<region>:<account>:novacart-pipeline-alerts
    novacart_glue_job_name   novacart_run_sql            (optional, this is the default)
    novacart_aws_region      ap-south-1                  (optional, this is the default)
Trigger manually with config {"batch_id": "1"} or from the S3 -> EventBridge -> Lambda path.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.sns import SnsHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.utils.trigger_rule import TriggerRule

REGION = Variable.get("novacart_aws_region", default_var="ap-south-1")
GLUE_JOB = Variable.get("novacart_glue_job_name", default_var="novacart_run_sql")
SNS_TOPIC = Variable.get("novacart_sns_topic_arn", default_var="")


def notify_failure(context):
    """Runs only after the task's final retry has failed."""
    ti = context["task_instance"]
    batch_id = context["params"].get("batch_id")
    message = (
        f"NovaCart pipeline task FAILED\n"
        f"DAG run : {context['run_id']}\n"
        f"Task    : {ti.task_id} (attempt {ti.try_number})\n"
        f"Batch   : {batch_id}\n"
        f"Error   : {context.get('exception')}\n"
        f"Log     : {ti.log_url}"
    )
    if SNS_TOPIC:
        SnsHook(region_name=REGION).publish_to_target(
            target_arn=SNS_TOPIC, subject=f"NovaCart FAILED: {ti.task_id} (batch {batch_id})", message=message)
    else:
        print(message)


default_args = {
    "owner": "novacart-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="novacart_medallion",
    description="NovaCart bronze -> silver -> SCD2 -> gold -> DQ on Glue + Delta Lake",
    start_date=datetime(2026, 9, 1),
    schedule=None,                      # started manually or by the S3 _SUCCESS event (Lambda)
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={"batch_id": Param("1", type="string", pattern=r"^\d+$", description="Batch number to process")},
    tags=["novacart", "medallion", "delta"],
) as dag:

    def glue_step(task_id, steps):
        return GlueJobOperator(
            task_id=task_id,
            job_name=GLUE_JOB,
            region_name=REGION,
            script_args={"--batch_id": "{{ params.batch_id }}", "--steps": steps},
            wait_for_completion=True,
            verbose=True,                # stream the Glue driver log into the Airflow task log
        )

    setup = glue_step("setup_tables", "00_setup")
    bronze = glue_step("bronze_ingest", "01_bronze")
    silver_orders = glue_step("silver_orders", "02_silver_orders")
    silver_reference = glue_step("silver_reference", "03_silver_reference")
    silver_items = glue_step("silver_order_items", "04_silver_order_items")
    dim_customer = glue_step("scd2_dim_customer", "05_dim_customer")
    gold_fact = glue_step("gold_fact_order_line", "06_gold_fact_order_line")
    gold_aggs = glue_step("gold_aggregates", "07_gold_aggregates")
    dq_optimize = glue_step("dq_and_optimize", "08_dq_and_optimize")

    alert = SnsPublishOperator(
        task_id="alert_on_failure",
        target_arn=SNS_TOPIC or "arn:aws:sns:ap-south-1:000000000000:set-novacart_sns_topic_arn",
        region_name=REGION,
        subject="NovaCart pipeline run failed",
        message="Run {{ run_id }} for batch_id={{ params.batch_id }} has at least one failed task. "
                "Open the Airflow Grid view for details.",
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0,
        on_failure_callback=None,
    )

    setup >> bronze >> silver_orders >> silver_reference >> [silver_items, dim_customer]
    [silver_items, dim_customer] >> gold_fact >> gold_aggs >> dq_optimize
    [setup, bronze, silver_orders, silver_reference, silver_items, dim_customer,
     gold_fact, gold_aggs, dq_optimize] >> alert
