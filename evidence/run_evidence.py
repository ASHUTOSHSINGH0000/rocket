"""
run_evidence.py - prints all Part 5 / Part 6 evidence for silver.orders and the fact table.
Run in a Glue Studio notebook (paste cells) or as a Glue job using the same settings as
novacart_run_sql (--datalake-formats delta, --conf ..., --enable-glue-datacatalog true).
Run it after: batch 1, batch 2, batch 2 re-run. It only reads, except for the schema test,
which is expected to FAIL (that failure is the evidence) and writes nothing.
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")


def title(t):
    print("\n" + "=" * 90 + f"\n{t}\n" + "=" * 90)


# 1. change history --------------------------------------------------------------------------
title("1. DESCRIBE HISTORY silver.orders")
hist = spark.sql("DESCRIBE HISTORY silver.orders")
hist.select("version", "timestamp", "operation",
            F.col("operationMetrics.numSourceRows").alias("source_rows"),
            F.col("operationMetrics.numTargetRowsInserted").alias("inserted"),
            F.col("operationMetrics.numTargetRowsUpdated").alias("updated")) \
    .orderBy("version").show(truncate=False)

merges = [r.version for r in hist.filter("operation = 'MERGE'").orderBy("version").collect()]
v_after_b1 = merges[0] if merges else 1

# 2. point-in-time read ----------------------------------------------------------------------
title(f"2. Point-in-time read: VERSION AS OF {v_after_b1} (right after batch 1) vs current")
b1 = spark.sql(f"SELECT * FROM silver.orders VERSION AS OF {v_after_b1}")
cur = spark.table("silver.orders")
print(f"orders after batch 1: {b1.count()}   orders now: {cur.count()}")
b1.alias("a").join(cur.alias("c"), "order_id") \
  .where("c.updated_at <> a.updated_at") \
  .select("order_id", F.col("a.status").alias("status_after_b1"), F.col("c.status").alias("status_now"),
          F.col("a.updated_at").alias("updated_after_b1"), F.col("c.updated_at").alias("updated_now")) \
  .orderBy("order_id").show(15, truncate=False)

# 3. schema enforcement ----------------------------------------------------------------------
title("3. Schema enforcement: append a row with an unexpected column")
versions_before = spark.sql("DESCRIBE HISTORY silver.orders").count()
bad = spark.table("silver.orders").limit(1).withColumn("coupon_type", F.lit("FESTIVE10"))
try:
    bad.write.format("delta").mode("append").saveAsTable("silver.orders")
    print("UNEXPECTED: write succeeded")
except AnalysisException as exc:
    print("Write REJECTED by Delta schema enforcement (expected):")
    print(str(exc).splitlines()[0][:600])
versions_after = spark.sql("DESCRIBE HISTORY silver.orders").count()
print(f"table versions before: {versions_before}, after: {versions_after} -> nothing was committed")

# 4. storage optimisation --------------------------------------------------------------------
title("4. OPTIMIZE gold.fact_order_line (before / after file counts)")
spark.sql("DESCRIBE DETAIL gold.fact_order_line").select("numFiles", "sizeInBytes").show()
spark.sql("OPTIMIZE gold.fact_order_line ZORDER BY (order_date)").show(truncate=False)
spark.sql("DESCRIBE DETAIL gold.fact_order_line").select("numFiles", "sizeInBytes").show()
print("VACUUM dry run (lists files that a 7-day VACUUM would delete; nothing is deleted):")
spark.sql("VACUUM gold.fact_order_line RETAIN 168 HOURS DRY RUN").show(truncate=False)

# 5. row-level changes from the Change Data Feed (Part 6) ------------------------------------
title("5. Change Data Feed: rows changed in silver.orders by batch 2")
if len(merges) >= 2:
    cdf = (spark.read.format("delta").option("readChangeFeed", "true")
           .option("startingVersion", merges[1]).option("endingVersion", merges[1])
           .table("silver.orders"))
    cdf.groupBy("_change_type").count().show()
    cdf.where("_change_type IN ('update_preimage', 'update_postimage')") \
       .select("order_id", "_change_type", "status", "updated_at", "_commit_version") \
       .orderBy("order_id", "_change_type").show(12, truncate=False)
else:
    print("Run batch 2 first.")

# 6. idempotency summary ---------------------------------------------------------------------
title("6. Idempotency: one row per key")
spark.sql("SELECT count(*) rows, count(DISTINCT order_id) distinct_orders FROM silver.orders").show()
spark.sql("SELECT count(*) rows, count(DISTINCT order_id, line_no) distinct_lines FROM silver.order_items").show()
