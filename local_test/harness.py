"""
harness.py - LOCAL TEST ONLY. Runs the real sql/*.sql files on plain Spark 3.5 without Delta.

Why: validates the pipeline logic (parsing, quarantine, dedup, MERGE rules, SCD2, FX, gold numbers)
against the real input files on a machine that cannot download the Delta jar.
How: the SQL text is identical to production; only the four Delta-only statement types are emulated:
  CREATE TABLE ... USING DELTA  -> same table USING PARQUET (TBLPROPERTIES / PARTITIONED BY / NOT NULL dropped)
  DELETE FROM t WHERE c         -> rewrite t keeping rows where c is not true
  MERGE INTO t USING s ...      -> rewrite t with Delta MERGE semantics (pattern used in this project)
  OPTIMIZE / VACUUM             -> skipped (Delta storage operations)
Delta-only evidence (DESCRIBE HISTORY, VERSION AS OF, schema enforcement, OPTIMIZE) must be produced on AWS.

Usage: python local_test/harness.py --batches 1,2,2 --lake /tmp/lake --input ./input --sql_root ./sql
"""

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "glue"))
import run_sql  # noqa: E402

from pyspark.sql import SparkSession  # noqa: E402

MERGE_RE = re.compile(
    r"MERGE\s+INTO\s+(\S+)\s+AS\s+t\s+USING\s+(\S+)\s+AS\s+s\s+ON\s+(.+?)\s+"
    r"WHEN\s+MATCHED\s+AND\s+(.+?)\s+THEN\s+UPDATE\s+SET\s+\*\s+"
    r"WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s+\*\s*$",
    re.S | re.I,
)
DELETE_RE = re.compile(r"DELETE\s+FROM\s+(\S+)\s+WHERE\s+(.+)$", re.S | re.I)


class Emulator:
    def __init__(self, spark, history_file):
        self.spark = spark
        self.history_file = history_file

    def log(self, table, operation, metrics):
        rec = {"table": table, "operation": operation, "metrics": metrics}
        with open(self.history_file, "a") as fh:
            fh.write(json.dumps(rec) + "\n")
        print(f"    [emulated {operation}] {table} {metrics}", flush=True)

    def overwrite(self, table, df):
        schema = self.spark.table(table).schema
        rows = df.select(*[f.name for f in schema.fields]).collect()   # materialise before overwrite
        self.spark.createDataFrame(rows, schema).write.insertInto(table, overwrite=True)

    def __call__(self, stmt):
        kw = run_sql.first_keyword(stmt)
        if kw == "CREATE" and re.search(r"USING\s+DELTA", stmt, re.I):
            s = re.sub(r"USING\s+DELTA", "USING PARQUET", stmt, flags=re.I)
            s = re.sub(r"TBLPROPERTIES\s*\([^)]*\)", "", s, flags=re.I)
            s = re.sub(r"PARTITIONED\s+BY\s*\([^)]*\)", "", s, flags=re.I)
            s = re.sub(r"\s+NOT\s+NULL", "", s, flags=re.I)
            self.spark.sql(s)
            return
        if kw in ("OPTIMIZE", "VACUUM"):
            print("    [skipped locally: Delta storage operation]", flush=True)
            return
        if kw == "DELETE":
            table, cond = DELETE_RE.match(stmt.strip()).groups()
            before = self.spark.table(table).count()
            keep = self.spark.sql(f"SELECT * FROM {table} WHERE NOT coalesce(({cond}), false)")
            self.overwrite(table, keep)
            self.log(table, "DELETE", {"numDeletedRows": before - self.spark.table(table).count()})
            return
        if kw == "MERGE":
            m = MERGE_RE.match(stmt.strip())
            if not m:
                raise ValueError("MERGE pattern not supported by the local harness:\n" + stmt)
            target, source, on, cond = m.groups()
            cols = [f.name for f in self.spark.table(target).schema.fields]
            src = f"(SELECT * FROM {source})"
            matched_upd = self.spark.sql(
                f"SELECT count(*) c FROM {target} t JOIN {src} s ON {on} WHERE {cond}").first().c
            inserted = self.spark.sql(
                f"SELECT count(*) c FROM {src} s LEFT ANTI JOIN {target} t ON {on}").first().c
            pick = ", ".join(f"CASE WHEN {cond} THEN s.{c} ELSE t.{c} END AS {c}" for c in cols)
            sel_t = ", ".join(f"t.{c}" for c in cols)
            sel_s = ", ".join(f"s.{c}" for c in cols)
            new = self.spark.sql(
                f"SELECT {sel_t} FROM {target} t LEFT ANTI JOIN {src} s ON {on} "
                f"UNION ALL SELECT {pick} FROM {target} t JOIN {src} s ON {on} "
                f"UNION ALL SELECT {sel_s} FROM {src} s LEFT ANTI JOIN {target} t ON {on}")
            self.overwrite(target, new)
            self.log(target, "MERGE", {"numTargetRowsUpdated": matched_upd, "numTargetRowsInserted": inserted,
                                       "numTargetRowsAfter": self.spark.table(target).count()})
            return
        df = self.spark.sql(stmt)
        if kw in run_sql.RESULT_PREFIXES:
            df.show(50, truncate=False)


def snapshot(spark, lake, label):
    """Save the state after a run, so the harness can compare runs (stand-in for VERSION AS OF)."""
    out = os.path.join(lake, "_snapshots", label)
    for t in ["silver.orders", "silver.order_items", "gold.daily_revenue", "gold.revenue_by_category",
              "gold.top_customers", "gold.payment_mismatches"]:
        spark.table(t).write.mode("overwrite").parquet(os.path.join(out, t))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--batches", default="1,2,2", help="runs to execute in order, e.g. 1,2,2")
    p.add_argument("--lake", required=True)
    p.add_argument("--input", required=True)
    p.add_argument("--sql_root", default=os.path.join(os.path.dirname(__file__), "..", "sql"))
    a = p.parse_args()

    lake = os.path.abspath(a.lake)
    os.makedirs(lake, exist_ok=True)
    spark = (SparkSession.builder.appName("novacart-harness").master("local[2]")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.warehouse.dir", os.path.join(lake, "_warehouse"))
             .getOrCreate())
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("ERROR")

    emu = Emulator(spark, os.path.join(lake, "_emulated_history.jsonl"))
    for run_no, batch_id in enumerate([b.strip() for b in a.batches.split(",")], 1):
        params = {"batch_id": batch_id, "lake": lake, "input": os.path.abspath(a.input)}
        emu.log("-", "RUN_START", {"run": run_no, "batch_id": batch_id})
        for step in run_sql.ALL_STEPS:
            run_sql.run_step(spark, step, run_sql.render(run_sql.read_sql(a.sql_root, step), params), executor=emu)
        snapshot(spark, lake, f"run{run_no}_batch{batch_id}")
    print("\nAll runs finished successfully (local harness).")


if __name__ == "__main__":
    main()
