import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    current_timestamp,
    to_timestamp,
    row_number,
)
from pyspark.sql.window import Window

# ===============================
# Initialize Glue Context
# ===============================

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("silver_enrichment_job", {})

# ===============================
# Bronze Layer Input
# ===============================

customer_bronze_path = "s3://zentric-customer-platform-sunil/bronze/customer/"
country_bronze_path = "s3://zentric-customer-platform-sunil/bronze/country/"

# ===============================
# Silver Layer Output
# ===============================

customer_silver_path = "s3://zentric-customer-platform-sunil/silver/customer_enriched/"
country_silver_path = "s3://zentric-customer-platform-sunil/silver/country/"

# ===============================
# Read Bronze Data
# ===============================

customer_df = spark.read.parquet(customer_bronze_path)
country_df = spark.read.parquet(country_bronze_path)

# ===============================
# Clean Customer Data
# ===============================

customer_clean_df = (
    customer_df
    .withColumn("cust_id", col("cust_id").cast("string"))
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("email", trim(col("email")))
    .withColumn("phone", trim(col("phone")))
    .withColumn("address", trim(col("address")))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .withColumn("country_code", upper(trim(col("country_code"))))
    .withColumn("postal_code", trim(col("postal_code")))
    .withColumn("updated_at", to_timestamp(col("updated_at")))
)

# ===============================
# Clean Country Lookup Data
# ===============================

country_clean_df = (
    country_df
    .withColumn("country_code", upper(trim(col("country_code"))))
    .withColumn("country_name", trim(col("country_name")))
    .withColumn("region", trim(col("region")))
)

# ===============================
# Remove Duplicate Customer Records
# Keep Latest Record Per Customer
# ===============================

window_spec = Window.partitionBy("cust_id").orderBy(col("updated_at").desc())

customer_dedup_df = (
    customer_clean_df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# ===============================
# Enrich Customer Data
# ===============================

customer_enriched_df = (
    customer_dedup_df.alias("customer")
    .join(
        country_clean_df.alias("country"),
        col("customer.country_code") == col("country.country_code"),
        "left"
    )
    .select(
        col("customer.cust_id"),
        col("customer.first_name"),
        col("customer.last_name"),
        col("customer.email"),
        col("customer.phone"),
        col("customer.address"),
        col("customer.city"),
        col("customer.state"),
        col("customer.postal_code"),
        col("country.country_name"),
        col("country.region"),
        col("customer.updated_at"),
        current_timestamp().alias("load_date")
    )
)

# ===============================
# Write Customer Data to Silver
# ===============================

customer_enriched_df.write \
    .mode("overwrite") \
    .parquet(customer_silver_path)

# ===============================
# Write Country Lookup to Silver
# ===============================

country_clean_df.write \
    .mode("overwrite") \
    .parquet(country_silver_path)

# ===============================
# Commit Glue Job
# ===============================

job.commit()
