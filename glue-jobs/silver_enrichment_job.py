import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, upper, current_timestamp, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

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
# Deduplicate Customer Records
# Keep latest updated_at per cust_id
# ===============================

window_spec = Window.partitionBy("cust_id").orderBy(col("updated_at").desc())

customer_dedup_df = (
    customer_clean_df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# ===============================
# Enrich Customer with Country Lookup
# ===============================

customer_enriched_df = (
    customer_dedup_df.alias("c")
    .join(
        country_clean_df.alias("l"),
        col("c.country_code") == col("l.country_code"),
        "left"
    )
    .select(
        col("c.cust_id"),
        col("c.first_name"),
        col("c.last_name"),
        col("c.email"),
        col("c.phone"),
        col("c.address"),
        col("c.city"),
        col("c.state"),
        col("c.postal_code"),
        col("l.country_name"),
        col("l.region"),
        col("c.updated_at"),
        current_timestamp().alias("load_date")
    )
)

# ===============================
# Write to Silver Layer
# Truncate & Load = overwrite
# ===============================

customer_enriched_df.write \
    .mode("overwrite") \
    .parquet(customer_silver_path)

country_clean_df.write \
    .mode("overwrite") \
    .parquet(country_silver_path)

# ===============================
# Commit Glue Job
# ===============================

job.commit()