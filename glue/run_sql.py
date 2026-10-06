"""
run_sql.py - NovaCart medallion pipeline runner (AWS Glue 5.0 / any Spark + Delta Lake)

Runs one or more SQL step files in order, substituting {{batch_id}}, {{lake}} and {{input}}.
All pipeline logic lives in the sql/*.sql files; this script only reads, renders and executes them.

On AWS Glue (job parameters):
    --batch_id 2
    --steps    02_silver_orders            (comma-separated SQL file stems, or 'all')
    --lake     s3://novacart-lakehouse
    --input    s3://novacart-lakehouse/input
    --sql_root s3://novacart-lakehouse/scripts/sql
  plus the Glue job settings in infra/create_glue_job.sh (--datalake-formats delta, --conf ...).

Locally (Spark 3.5 + delta-spark installed, internet access to Maven for the Delta jar):
    python glue/run_sql.py --batch_id 1 --steps all \
        --lake /tmp/novacart-lake --input ./input --sql_root ./sql
"""

import argparse
import os
import re
import sys
import time

ALL_STEPS = [
    "00_setup",
    "01_bronze",
    "02_silver_orders",
    "03_silver_reference",
    "04_silver_order_items",
    "05_dim_customer",
    "06_gold_fact_order_line",
    "07_gold_aggregates",
    "08_dq_and_optimize",
]

RESULT_PREFIXES = ("SELECT", "WITH", "DESCRIBE", "SHOW")


# --------------------------------------------------------------------------- arguments
def parse_args():
    """Glue passes job parameters as --key value; getResolvedOptions reads them there."""
    try:
        from awsglue.utils import getResolvedOptions  # only present on AWS Glue

        opts = getResolvedOptions(sys.argv, ["batch_id", "steps", "lake", "input", "sql_root"])
        opts["on_glue"] = True
        return opts
    except ImportError:
        p = argparse.ArgumentParser(description="NovaCart SQL step runner")
        p.add_argument("--batch_id", required=True)
        p.add_argument("--steps", default="all")
        p.add_argument("--lake", required=True)
        p.add_argument("--input", required=True)
        p.add_argument("--sql_root", default=os.path.join(os.path.dirname(__file__), "..", "sql"))
        a = p.parse_args()
        return {**vars(a), "on_glue": False}


def resolve_steps(steps_arg):
    if steps_arg.strip().lower() == "all":
        return list(ALL_STEPS)
    steps = [s.strip().replace(".sql", "") for s in steps_arg.split(",") if s.strip()]
    unknown = [s for s in steps if s not in ALL_STEPS]
    if unknown:
        raise ValueError(f"Unknown step(s) {unknown}; valid steps: {ALL_STEPS}")
    return steps


# --------------------------------------------------------------------------- SQL handling
def read_sql(sql_root, step):
    path = f"{sql_root.rstrip('/')}/{step}.sql"
    if path.startswith("s3://"):
        import boto3

        bucket, key = path[len("s3://"):].split("/", 1)
        return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def render(sql_text, params):
    out = sql_text
    for key, value in params.items():
        out = out.replace("{{" + key + "}}", str(value))
    left = re.findall(r"\{\{\w+\}\}", out)
    if left:
        raise ValueError(f"Unfilled placeholders: {sorted(set(left))}")
    return out


def split_statements(sql_text):
    """Split on ';' outside quotes. '--' comments outside quotes are removed."""
    statements, buf, i, quote = [], [], 0, None
    while i < len(sql_text):
        ch = sql_text[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
        elif ch == "-" and sql_text[i:i + 2] == "--":
            while i < len(sql_text) and sql_text[i] != "\n":
                i += 1
            continue
        elif ch == ";":
            statements.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    statements.append("".join(buf))
    return [s.strip() for s in statements if s.strip()]


def first_keyword(stmt):
    return stmt.lstrip("( \n\t").split(None, 1)[0].upper()


# --------------------------------------------------------------------------- Spark session
def build_spark(on_glue):
    from pyspark.sql import SparkSession

    if on_glue:
        # Glue creates the session; Delta + Glue Data Catalog come from the job's --conf
        # and --datalake-formats delta / --enable-glue-datacatalog settings.
        spark = SparkSession.builder.getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.appName("novacart-local")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "4")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --------------------------------------------------------------------------- execution
def run_step(spark, step, sql_text, executor=None):
    statements = split_statements(sql_text)
    print(f"\n===== STEP {step}: {len(statements)} statements =====", flush=True)
    for n, stmt in enumerate(statements, 1):
        preview = " ".join(stmt.split())[:140]
        print(f"[{step} #{n}] {preview}", flush=True)
        t0 = time.time()
        if executor is not None:
            executor(stmt)
        else:
            df = spark.sql(stmt)
            if first_keyword(stmt) in RESULT_PREFIXES:
                df.show(50, truncate=False)          # also forces lazy checks such as assert_true
        print(f"    ok in {time.time() - t0:.1f}s", flush=True)


def main():
    opts = parse_args()
    steps = resolve_steps(opts["steps"])
    params = {"batch_id": opts["batch_id"], "lake": opts["lake"].rstrip("/"), "input": opts["input"].rstrip("/")}
    print(f"NovaCart run: batch_id={params['batch_id']} steps={steps} lake={params['lake']}", flush=True)

    spark = build_spark(opts["on_glue"])
    for step in steps:
        run_step(spark, step, render(read_sql(opts["sql_root"], step), params))
    print("\nAll steps finished successfully.", flush=True)


if __name__ == "__main__":
    main()
