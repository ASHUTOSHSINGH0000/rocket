"""Exports the harness results (after the last run) and checks idempotency between run 2 and run 3."""
import argparse
import json
import os

from pyspark.sql import SparkSession, functions as F

p = argparse.ArgumentParser()
p.add_argument("--lake", required=True)
p.add_argument("--out", required=True)
a = p.parse_args()
os.makedirs(a.out, exist_ok=True)

spark = (SparkSession.builder.master("local[1]").config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
snap = os.path.join(a.lake, "_snapshots")
runs = sorted(os.listdir(snap))
last = runs[-1]


def norm(df):
    df = df.drop("_merged_at")
    if "attributes" in df.columns:
        df = df.withColumn("attributes", F.to_json(F.map_from_entries(F.array_sort(F.map_entries("attributes")))))
    return df


for t in ["gold.daily_revenue", "gold.revenue_by_category", "gold.top_customers", "gold.payment_mismatches"]:
    pdf = spark.read.parquet(os.path.join(snap, last, t)).toPandas()
    sort_col = {"gold.daily_revenue": "order_date", "gold.revenue_by_category": "revenue_usd",
                "gold.top_customers": "revenue_rank", "gold.payment_mismatches": "order_id"}[t]
    pdf.sort_values(sort_col, ascending=(t != "gold.revenue_by_category")).to_csv(
        os.path.join(a.out, t.replace("gold.", "") + ".csv"), index=False)

for b in ("1", "2"):
    d = os.path.join(a.lake, "reports", f"dq_report_batch_{b}")
    rows = [json.loads(l) for f in sorted(os.listdir(d)) if f.endswith(".json") for l in open(os.path.join(d, f))]
    with open(os.path.join(a.out, f"dq_report_batch_{b}.json"), "w") as fh:
        json.dump(rows, fh, indent=1)

history = [json.loads(l) for l in open(os.path.join(a.lake, "_emulated_history.jsonl"))]
merges = [h for h in history if h["operation"] in ("RUN_START", "MERGE")]
check = {"runs": runs, "merge_metrics_by_run": merges, "rerun_differences": {}}
if len(runs) >= 3:
    r2, r3 = runs[-2], runs[-1]
    for t in ["silver.orders", "silver.order_items", "gold.daily_revenue", "gold.revenue_by_category",
              "gold.top_customers", "gold.payment_mismatches"]:
        x = norm(spark.read.parquet(os.path.join(snap, r2, t)))
        y = norm(spark.read.parquet(os.path.join(snap, r3, t)))
        check["rerun_differences"][t] = {"rows_run2": x.count(), "rows_run3": y.count(),
                                        "differing_rows": x.exceptAll(y).count() + y.exceptAll(x).count()}
with open(os.path.join(a.out, "idempotency_check.json"), "w") as fh:
    json.dump(check, fh, indent=1)
print(json.dumps(check["rerun_differences"], indent=1))
print(f"Results written to {a.out}")
