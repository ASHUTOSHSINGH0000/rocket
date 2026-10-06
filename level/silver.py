import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder \
    .appName("NovaCart Silver Gold") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BRONZE = "s3://novakart-lakehouse12/bronze"
SILVER = "s3://novakart-lakehouse12/silver"
GOLD = "s3://novakart-lakehouse12/gold"

# READ BRONZE
orders = spark.read.format("delta").load(f"{BRONZE}/orders")
items = spark.read.format("delta").load(f"{BRONZE}/order_items")

# SILVER CLEANING
orders_clean = orders.dropDuplicates().na.drop(how="all")
items_clean = items.dropDuplicates().na.drop(how="all")

# WRITE SILVER
orders_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{SILVER}/orders")

items_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{SILVER}/order_items")

# GOLD JOIN
gold_df = orders_clean.join(
    items_clean,
    orders_clean.order_id == items_clean.order_id,
    "left"
)

# WRITE GOLD
gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{GOLD}/sales")

job.commit()