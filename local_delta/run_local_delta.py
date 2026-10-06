"""
run_local_delta.py - runs the REAL pipeline (same sql/*.sql, real Delta Lake) on your laptop:
batch 1 -> batch 2 -> batch 2 again, then all Delta evidence (history, time travel,
schema enforcement, OPTIMIZE, Change Data Feed). One process, so the catalog persists.
Needs internet once (Spark downloads the Delta jar from Maven).
Usage: python local_delta/run_local_delta.py --lake ./lake
"""
import argparse
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "glue"))
import run_sql  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--lake", default=os.path.join(ROOT, "lake"))
p.add_argument("--batches", default="1,2,2")
a = p.parse_args()
lake = os.path.abspath(a.lake)
os.makedirs(lake, exist_ok=True)

spark = run_sql.build_spark(on_glue=False)          # real Delta Lake session
for n, batch in enumerate([b.strip() for b in a.batches.split(",")], 1):
    print(f"\n#################### RUN {n}: batch {batch} ####################")
    params = {"batch_id": batch, "lake": lake, "input": os.path.join(ROOT, "input")}
    for step in run_sql.ALL_STEPS:
        run_sql.run_step(spark, step, run_sql.render(run_sql.read_sql(os.path.join(ROOT, "sql"), step), params))

print("\n#################### DELTA EVIDENCE ####################")
exec(open(os.path.join(ROOT, "evidence", "run_evidence.py")).read())   # reuses the same session

print("\nGold results:")
for t in ["gold.daily_revenue", "gold.revenue_by_category", "gold.top_customers", "gold.payment_mismatches"]:
    print(t)
    spark.table(t).show(40, truncate=False)
print(f"\nLakehouse written to {lake}  (DQ JSON in {lake}/reports/)")
