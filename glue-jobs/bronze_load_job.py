import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_load_job", {})

# ===============================
# Landing Layer (Input)
# ===============================

customer_landing_path = "s3://zentric-customer-platform-sunil/landing/customer/"
country_landing_path = "s3://zentric-customer-platform-sunil/landing/country/"

# ===============================
# Bronze Layer (Output)
# ===============================

customer_bronze_path = "s3://zentric-customer-platform-sunil/bronze/customer/"
country_bronze_path = "s3://zentric-customer-platform-sunil/bronze/country/"

customer_df = spark.read.csv(
    "s3://zentric-customer-platform-sunil/landing/customer/",
    header=True
)

customer_landing_path = "s3://zentric-customer-platform-sunil/landing/customer/"

customer_df = spark.read.csv(customer_landing_path, header=True)

# ===============================
# Read Country Lookup Data
# ===============================

country_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(country_landing_path)
)

# ===============================
# Add Load Date
# ===============================

customer_bronze_df = customer_df.withColumn("load_date", current_timestamp())

country_bronze_df = country_df.withColumn("load_date", current_timestamp())

# ===============================
# Write Customer Data to Bronze
# Truncate & Load = overwrite
# ===============================

customer_bronze_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .parquet(customer_bronze_path)


# ===============================
# Write Country Data to Bronze
# Truncate & Load = overwrite
# ===============================

country_bronze_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .parquet(country_bronze_path)
    
# ===============================
# Commit Glue Job
# ===============================

job.commit()