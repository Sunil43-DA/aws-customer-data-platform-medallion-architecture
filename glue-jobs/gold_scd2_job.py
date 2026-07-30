import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    row_number,
    sha2,
    concat_ws,
    coalesce,
    max as spark_max,
    to_timestamp
)

from pyspark.sql.window import Window


# ============================================================
# 1. START SPARK AND GLUE
# ============================================================

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("gold_scd2_job", {})


# ============================================================
# 2. S3 PATHS
# ============================================================

customer_silver_path = (
    "s3://zentric-customer-platform-sunil/"
    "silver/customer_enriched/"
)

gold_customer_path = (
    "s3://zentric-customer-platform-sunil/"
    "gold/dim_customer/"
)

etl_tracker_path = (
    "s3://zentric-customer-platform-sunil/"
    "control/etl_tracker/"
)


# ============================================================
# 3. GOLD TABLE COLUMN ORDER
# ============================================================

gold_columns = [
    "customer_sk",
    "cust_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "address",
    "city",
    "state",
    "postal_code",
    "country_name",
    "region",
    "current_flag",
    "valid_from",
    "valid_to",
    "load_date",
    "updated_at"
]


# ============================================================
# 4. COLUMNS USED FOR CHANGE DETECTION
# ============================================================

compare_columns = [
    "first_name",
    "last_name",
    "email",
    "phone",
    "address",
    "city",
    "state",
    "postal_code",
    "country_name",
    "region"
]


# ============================================================
# 5. READ SILVER DATA
# ============================================================

silver_df = spark.read.parquet(customer_silver_path).cache()

silver_row_count = silver_df.count()

print("Silver records received:", silver_row_count)


# ============================================================
# 6. CHECK WHETHER GOLD DATA EXISTS
# ============================================================

try:
    gold_df = spark.read.parquet(gold_customer_path)

    # Convert valid_to to timestamp.
    # This also supports your earlier Gold output where valid_to was a date.
    gold_df = (
        gold_df
        .withColumn("valid_to", col("valid_to").cast("timestamp"))
        .cache()
    )

    gold_df.count()

    gold_exists = True

    print("Gold table exists. Incremental processing will run.")

except Exception as error:
    gold_exists = False
    gold_df = None

    print("Gold table does not exist. Initial load will run.")
    print("Initial read message:", str(error))


# ============================================================
# 7. INITIAL LOAD — DAY 1
# ============================================================

if not gold_exists:

    initial_key_window = Window.orderBy("cust_id")

    final_gold_df = (
        silver_df
        .withColumn(
            "customer_sk",
            row_number().over(initial_key_window)
        )
        .withColumn(
            "current_flag",
            lit("Y")
        )
        .withColumn(
            "valid_from",
            current_timestamp()
        )
        .withColumn(
            "valid_to",
            to_timestamp(lit("9999-12-31 23:59:59"))
        )
        .withColumn(
            "load_date",
            current_timestamp()
        )
        .select(*gold_columns)
    )

    print("Initial Gold records:", final_gold_df.count())


# ============================================================
# 8. INCREMENTAL LOAD — DAY 2 AND DAY 3
# ============================================================

else:

    # --------------------------------------------------------
    # 8.1 Separate current and historical Gold records
    # --------------------------------------------------------

    gold_current_df = (
        gold_df
        .filter(col("current_flag") == "Y")
        .cache()
    )

    gold_history_df = (
        gold_df
        .filter(col("current_flag") == "N")
        .cache()
    )

    print("Current Gold records:", gold_current_df.count())
    print("Historical Gold records:", gold_history_df.count())


    # --------------------------------------------------------
    # 8.2 Generate hash for incoming Silver records
    # --------------------------------------------------------

    silver_hash_df = silver_df.withColumn(
        "source_hash",
        sha2(
            concat_ws(
                "||",
                *[
                    coalesce(
                        col(column_name).cast("string"),
                        lit("")
                    )
                    for column_name in compare_columns
                ]
            ),
            256
        )
    )


    # --------------------------------------------------------
    # 8.3 Generate hash for current Gold records
    # --------------------------------------------------------

    gold_hash_df = gold_current_df.withColumn(
        "target_hash",
        sha2(
            concat_ws(
                "||",
                *[
                    coalesce(
                        col(column_name).cast("string"),
                        lit("")
                    )
                    for column_name in compare_columns
                ]
            ),
            256
        )
    )


    # --------------------------------------------------------
    # 8.4 Compare Silver with current Gold
    # --------------------------------------------------------

    comparison_df = (
        silver_hash_df.alias("s")
        .join(
            gold_hash_df.alias("g"),
            col("s.cust_id") == col("g.cust_id"),
            "left"
        )
        .cache()
    )

    comparison_df.count()


    # --------------------------------------------------------
    # 8.5 Identify new customers
    # Customer is present in Silver but absent from Gold
    # --------------------------------------------------------

    new_customers_df = (
        comparison_df
        .filter(col("g.cust_id").isNull())
        .select("s.*")
        .drop("source_hash")
        .cache()
    )


    # --------------------------------------------------------
    # 8.6 Identify changed customers
    # Customer exists, but tracked attributes differ
    # --------------------------------------------------------

    changed_customers_df = (
        comparison_df
        .filter(
            col("g.cust_id").isNotNull()
            & (
                col("s.source_hash")
                != col("g.target_hash")
            )
        )
        .select("s.*")
        .drop("source_hash")
        .cache()
    )


    # --------------------------------------------------------
    # 8.7 Identify unchanged customers
    # --------------------------------------------------------

    unchanged_customers_df = (
        comparison_df
        .filter(
            col("g.cust_id").isNotNull()
            & (
                col("s.source_hash")
                == col("g.target_hash")
            )
        )
        .select("s.*")
        .drop("source_hash")
        .cache()
    )

    new_count = new_customers_df.count()
    changed_count = changed_customers_df.count()
    unchanged_count = unchanged_customers_df.count()

    print("New customers:", new_count)
    print("Changed customers:", changed_count)
    print("Unchanged customers:", unchanged_count)


    # --------------------------------------------------------
    # 8.8 Get IDs of changed customers
    # --------------------------------------------------------

    changed_customer_ids_df = (
        changed_customers_df
        .select("cust_id")
        .distinct()
    )


    # --------------------------------------------------------
    # 8.9 Preserve current records that did not change
    #
    # This also keeps customers that are absent from a source
    # snapshot because soft-delete handling is not required.
    # --------------------------------------------------------

    unchanged_current_gold_df = (
        gold_current_df
        .join(
            changed_customer_ids_df,
            on="cust_id",
            how="left_anti"
        )
        .select(*gold_columns)
    )


    # --------------------------------------------------------
    # 8.10 Expire previous versions of changed customers
    # --------------------------------------------------------

    expired_gold_df = (
        gold_current_df
        .join(
            changed_customer_ids_df,
            on="cust_id",
            how="inner"
        )
        .withColumn(
            "current_flag",
            lit("N")
        )
        .withColumn(
            "valid_to",
            current_timestamp()
        )
        .withColumn(
            "load_date",
            current_timestamp()
        )
        .select(*gold_columns)
    )


    # --------------------------------------------------------
    # 8.11 Read last surrogate ID from ETL Tracker
    # --------------------------------------------------------

    try:
        existing_tracker_df = (
            spark.read
            .parquet(etl_tracker_path)
            .cache()
        )

        existing_tracker_df.count()

        last_surrogate_id = (
            existing_tracker_df
            .agg(
                spark_max("last_surrogate_id")
                .alias("last_surrogate_id")
            )
            .collect()[0]["last_surrogate_id"]
        )

        print(
            "Last surrogate ID read from tracker:",
            last_surrogate_id
        )

    except Exception as tracker_error:

        last_surrogate_id = (
            gold_df
            .agg(
                spark_max("customer_sk")
                .alias("last_surrogate_id")
            )
            .collect()[0]["last_surrogate_id"]
        )

        print(
            "Tracker could not be read. "
            "Using maximum Gold surrogate key instead."
        )

        print("Tracker read message:", str(tracker_error))


    if last_surrogate_id is None:
        last_surrogate_id = 0


    # --------------------------------------------------------
    # 8.12 Combine new and changed incoming records
    # --------------------------------------------------------

    records_requiring_new_version_df = (
        new_customers_df
        .unionByName(changed_customers_df)
    )


    # --------------------------------------------------------
    # 8.13 Generate new surrogate keys
    # --------------------------------------------------------

    new_key_window = Window.orderBy("cust_id")

    new_versions_df = (
        records_requiring_new_version_df
        .withColumn(
            "customer_sk",
            lit(last_surrogate_id)
            + row_number().over(new_key_window)
        )
        .withColumn(
            "current_flag",
            lit("Y")
        )
        .withColumn(
            "valid_from",
            current_timestamp()
        )
        .withColumn(
            "valid_to",
            to_timestamp(lit("9999-12-31 23:59:59"))
        )
        .withColumn(
            "load_date",
            current_timestamp()
        )
        .select(*gold_columns)
    )


    # --------------------------------------------------------
    # 8.14 Create final Gold table
    # --------------------------------------------------------

    final_gold_df = (
        gold_history_df
        .select(*gold_columns)
        .unionByName(
            unchanged_current_gold_df
        )
        .unionByName(
            expired_gold_df
        )
        .unionByName(
            new_versions_df
        )
    )


# ============================================================
# 9. MATERIALISE FINAL DATA BEFORE OVERWRITING GOLD
# ============================================================

final_gold_df = final_gold_df.cache()

final_gold_count = final_gold_df.count()

print("Final Gold row count:", final_gold_count)


# ============================================================
# 10. DATA QUALITY CHECKS
# ============================================================

null_customer_count = (
    final_gold_df
    .filter(col("cust_id").isNull())
    .count()
)

duplicate_sk_count = (
    final_gold_df
    .groupBy("customer_sk")
    .count()
    .filter(col("count") > 1)
    .count()
)

multiple_current_customer_count = (
    final_gold_df
    .filter(col("current_flag") == "Y")
    .groupBy("cust_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if null_customer_count > 0:
    raise Exception(
        f"Data-quality failure: "
        f"{null_customer_count} null cust_id values found."
    )

if duplicate_sk_count > 0:
    raise Exception(
        f"Data-quality failure: "
        f"{duplicate_sk_count} duplicate customer_sk values found."
    )

if multiple_current_customer_count > 0:
    raise Exception(
        f"Data-quality failure: "
        f"{multiple_current_customer_count} customers have "
        f"more than one current record."
    )

print("Data-quality checks passed.")


# ============================================================
# 11. CALCULATE TRACKER VALUES BEFORE WRITES
# ============================================================

processed_rows = silver_row_count

last_surrogate_id = (
    final_gold_df
    .agg(
        spark_max("customer_sk")
        .alias("last_surrogate_id")
    )
    .collect()[0]["last_surrogate_id"]
)

last_source_updated_at = (
    silver_df
    .agg(
        spark_max("updated_at")
        .alias("last_source_updated_at")
    )
    .collect()[0]["last_source_updated_at"]
)


# ============================================================
# 12. WRITE FINAL GOLD TABLE
# ============================================================

final_gold_df.write \
    .mode("overwrite") \
    .parquet(gold_customer_path)

print("Gold table written successfully.")


# ============================================================
# 13. CREATE AND WRITE ETL TRACKER
# ============================================================

tracker_df = (
    spark.createDataFrame(
        [(
            "dim_customer",
            int(processed_rows),
            int(last_surrogate_id),
            last_source_updated_at
        )],
        [
            "table_name",
            "processed_rows",
            "last_surrogate_id",
            "last_source_updated_at"
        ]
    )
    .withColumn(
        "load_date",
        current_timestamp()
    )
)

tracker_df.write \
    .mode("overwrite") \
    .parquet(etl_tracker_path)

print("ETL Tracker updated successfully.")


# ============================================================
# 14. COMPLETE GLUE JOB
# ============================================================

job.commit()

print("Gold SCD Type 2 job completed successfully.")