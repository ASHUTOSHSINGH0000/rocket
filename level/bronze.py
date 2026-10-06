import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


# ============================================================
# 1. INITIALIZE GLUE / SPARK
# ============================================================

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder \
    .appName("NovaCart Bronze Layer") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# ============================================================
# 2. S3 PATHS
# ============================================================

SOURCE_BASE = "s3://novakart-lakehouse12/raw"

BRONZE_BASE = "s3://novakart-lakehouse12/bronze"


# ============================================================
# 3. READ ORDERS
# ============================================================

orders_path = f"{SOURCE_BASE}/orders/"

orders_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(orders_path)


# ============================================================
# 4. ADD BRONZE METADATA
# ============================================================

orders_bronze = orders_df \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_file", input_file_name())


# ============================================================
# 5. WRITE ORDERS TO BRONZE
# ============================================================

orders_bronze.write \
    .format("delta") \
    .mode("append") \
    .save(f"{BRONZE_BASE}/orders")


# ============================================================
# 6. READ ORDER ITEMS
# ============================================================

order_items_path = f"{SOURCE_BASE}/order_items/"

order_items_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(order_items_path)


# ============================================================
# 7. ADD BRONZE METADATA
# ============================================================

order_items_bronze = order_items_df \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_file", input_file_name())


# ============================================================
# 8. WRITE ORDER ITEMS TO BRONZE
# ============================================================

order_items_bronze.write \
    .format("delta") \
    .mode("append") \
    .save(f"{BRONZE_BASE}/order_items")


# ============================================================
# 9. COMPLETE JOB
# ============================================================

job.commit()

