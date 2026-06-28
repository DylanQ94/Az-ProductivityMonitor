# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 05_gold_aggregate_facts
# MAGIC
# MAGIC This notebook builds the aggregated Gold fact tables for the Productivity Monitor project.
# MAGIC
# MAGIC The notebook reads `fact_productivity_interval`, which is the row-level analytical source of truth in the Gold layer, and creates aggregated fact tables for monthly, daily, hourly, activity, and source file audit analysis.
# MAGIC
# MAGIC The main goals of this notebook are:
# MAGIC
# MAGIC 1. Read `fact_productivity_interval` from the Gold layer.
# MAGIC 2. Build monthly productivity metrics.
# MAGIC 3. Build daily productivity metrics.
# MAGIC 4. Build hourly productivity metrics.
# MAGIC 5. Build activity-level productivity metrics.
# MAGIC 6. Build source file audit metrics.
# MAGIC 7. Write all aggregated fact tables as Delta tables in the Gold layer.
# MAGIC
# MAGIC Input:
# MAGIC - `gold/fact_productivity_interval/`
# MAGIC
# MAGIC Outputs:
# MAGIC - `gold/fact_productivity_monthly/`
# MAGIC - `gold/fact_productivity_daily/`
# MAGIC - `gold/fact_productivity_hourly/`
# MAGIC - `gold/fact_productivity_activity/`
# MAGIC - `gold/fact_source_file_audit/`
# MAGIC
# MAGIC The most valuable productivity analysis is based on duration:
# MAGIC - session time
# MAGIC - attentive time
# MAGIC - productive time
# MAGIC - inactive time
# MAGIC
# MAGIC The `fact_productivity_activity` table already answers the key business question by activity:
# MAGIC
# MAGIC How much time was session, attentive, productive, and inactive for each activity category?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 - Receive and validate the ingestion date parameter
# MAGIC
# MAGIC This step receives the `p_ingest_date` and `p_source_file_date` parameters from the orchestrator notebook.
# MAGIC
# MAGIC The parameter is originally sent by Azure Data Factory to `00_orchestrate_transformations`, and then forwarded to this notebook using `dbutils.notebook.run()`.
# MAGIC
# MAGIC Expected format: `yyyy-MM-dd`
# MAGIC
# MAGIC Example: `2026-06-26`
# MAGIC
# MAGIC This parameter controls which ingestion date will be processed by this notebook.

# COMMAND ----------

# DBTITLE 1,Receive and validate the ingestion date parameter
dbutils.widgets.text("p_ingest_date", "")
dbutils.widgets.text("p_source_file_date", "")

p_ingest_date = dbutils.widgets.get("p_ingest_date").strip()
p_source_file_date = dbutils.widgets.get("p_source_file_date").strip()

if p_ingest_date == "":
    raise ValueError("Parameter p_ingest_date is required. Expected format: yyyy-MM-dd")
if p_source_file_date == "":
    raise ValueError("Parameter p_source_file_date is required. Expected format: yyyy-MM-dd")

print(f"Landing ingest date: {p_ingest_date}")
print(f"Expected source file date: {p_source_file_date}")

date_key = int(p_source_file_date.replace("-", ""))
month_key = int(p_source_file_date[:7].replace("-", ""))
print(month_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Import required PySpark functions
# MAGIC
# MAGIC This cell imports the PySpark functions needed to calculate row-level increments, inactive duration, ratios, and aggregated Gold metrics.

# COMMAND ----------

# DBTITLE 1,Import required PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Establishing a connection with a ADLS
# MAGIC
# MAGIC This cell establishes communication with the ADLS Gen2.

# COMMAND ----------

# DBTITLE 1,Establishing a connection with a ADLS
storage_account_name = "saproductivitymonitor"
container_name = "productivity"
storage_account_key = "STORAGE KEY"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Define storage paths
# MAGIC
# MAGIC This cell defines the ADLS Gen2 paths used to read the Silver Delta table and write the Gold dimensions Delta tables.

# COMMAND ----------

# DBTITLE 1,Define storage paths
base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"

gold_base_path = f"{base_path}/gold/"
gold_fact_productivity_interval_path = f"{gold_base_path}/fact_productivity_interval/"

gold_fact_productivity_monthly_path = f"{gold_base_path}/fact_productivity_monthly/"
gold_fact_productivity_daily_path = f"{gold_base_path}/fact_productivity_daily/"
gold_fact_productivity_hourly_path = f"{gold_base_path}/fact_productivity_hourly/"
gold_fact_productivity_activity_path = f"{gold_base_path}/fact_productivity_activity/"
gold_fact_source_file_audit_path = f"{gold_base_path}/fact_source_file_audit/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Read the gold fact productivity interval Delta table
# MAGIC
# MAGIC This cell reads the cleaned and normalized productivity events from the gold fact productivity interval Delta table.

# COMMAND ----------

# DBTITLE 1,Read the gold fact productivity interval Delta table
df_fact_interval = (
    spark.read
    .format("delta")
    .load(gold_fact_productivity_interval_path)
    .filter(F.col("_ingest_date") == p_ingest_date)
)

display(df_fact_interval)

df_fact_interval.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Define reusable aggregation logic
# MAGIC
# MAGIC This cell defines reusable functions to build aggregated productivity fact tables.
# MAGIC
# MAGIC The aggregation logic uses interval-level duration columns from `fact_productivity_interval`.
# MAGIC
# MAGIC The helper function calculates:
# MAGIC - Total records.
# MAGIC - Source file count.
# MAGIC - Total duration metrics in seconds.
# MAGIC - Final duration metrics in hours.
# MAGIC - Productivity ratios.
# MAGIC - Record-level signal ratios.
# MAGIC - Data quality counters.
# MAGIC - First and last event timestamps.
# MAGIC
# MAGIC The final aggregated fact tables expose duration metrics in hours and remove internal calculation columns in seconds.

# COMMAND ----------

# DBTITLE 1,Define reusable aggregation logic
seconds_calc_columns = [
    "total_session_seconds_calc",
    "total_attentive_seconds_calc",
    "total_productive_seconds_calc",
    "total_inactive_seconds_calc",
    "total_active_seconds_calc",
    "total_duration_balance_difference_seconds_calc"
]


def add_duration_metrics(df):
    return (
        df

        .withColumn(
            "total_session_hours",
            F.round(F.col("total_session_seconds_calc") / 3600, 4)
        )

        .withColumn(
            "total_attentive_hours",
            F.round(F.col("total_attentive_seconds_calc") / 3600, 4)
        )

        .withColumn(
            "total_productive_hours",
            F.round(F.col("total_productive_seconds_calc") / 3600, 4)
        )

        .withColumn(
            "total_inactive_hours",
            F.round(F.col("total_inactive_seconds_calc") / 3600, 4)
        )

        .withColumn(
            "attentive_ratio",
            F.when(
                F.col("total_session_seconds_calc") > 0,
                F.round(F.col("total_attentive_seconds_calc") / F.col("total_session_seconds_calc"), 4)
            ).otherwise(F.lit(None))
        )

        .withColumn(
            "productive_ratio",
            F.when(
                F.col("total_session_seconds_calc") > 0,
                F.round(F.col("total_productive_seconds_calc") / F.col("total_session_seconds_calc"), 4)
            ).otherwise(F.lit(None))
        )

        .withColumn(
            "inactive_ratio",
            F.when(
                F.col("total_session_seconds_calc") > 0,
                F.round(F.col("total_inactive_seconds_calc") / F.col("total_session_seconds_calc"), 4)
            ).otherwise(F.lit(None))
        )

        .withColumn(
            "duration_balance_difference_hours",
            F.round(F.col("total_duration_balance_difference_seconds_calc") / 3600, 4)
        )

        .withColumn(
            "_gold_processed_timestamp",
            F.current_timestamp()
        )

        .drop(*seconds_calc_columns)
    )


def build_productivity_fact(group_columns):
    fact_df = (
        df_fact_interval
        .groupBy(*group_columns)
        .agg(
            F.count("*").alias("total_records"),
            F.countDistinct("source_file_key").alias("source_files_count"),

            F.sum("session_increment_seconds").alias("total_session_seconds_calc"),
            F.sum("attentive_increment_seconds").alias("total_attentive_seconds_calc"),
            F.sum("productive_increment_seconds").alias("total_productive_seconds_calc"),
            F.sum("inactive_increment_seconds").alias("total_inactive_seconds_calc"),

            F.round(F.avg("face_front_record_flag"), 4).alias("face_front_record_ratio"),
            F.round(F.avg("input_active_record_flag"), 4).alias("input_active_record_ratio"),

            F.sum(F.col("has_counter_reset").cast("int")).alias("counter_reset_records"),
            F.sum(F.col("has_duration_inconsistency").cast("int")).alias("duration_inconsistency_records"),
            F.sum("duration_balance_difference_seconds").alias("total_duration_balance_difference_seconds_calc"),

            F.min("event_timestamp").alias("first_event_timestamp"),
            F.max("event_timestamp").alias("last_event_timestamp")
        )
    )

    return add_duration_metrics(fact_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Build monthly, daily, hourly, and activity aggregated facts
# MAGIC
# MAGIC This cell builds the main aggregated Gold fact tables from `fact_productivity_interval`.
# MAGIC
# MAGIC The generated fact tables are:
# MAGIC
# MAGIC 1. `fact_productivity_monthly`
# MAGIC    - One row per month.
# MAGIC
# MAGIC 2. `fact_productivity_daily`
# MAGIC    - One row per day.
# MAGIC
# MAGIC 3. `fact_productivity_hourly`
# MAGIC    - One row per day and hour.
# MAGIC
# MAGIC 4. `fact_productivity_activity`
# MAGIC    - One row per day and activity category.
# MAGIC
# MAGIC These fact tables contain duration-based productivity metrics and keep the analytical keys needed for Power BI relationships.

# COMMAND ----------

# DBTITLE 1,Build monthly, daily, hourly, and activity aggregated facts
df_fact_productivity_monthly = (
    build_productivity_fact(
        ["month_key"]
    )
)

df_fact_productivity_daily = (
    build_productivity_fact(
        ["date_key", "month_key"]
    )
)

df_fact_productivity_hourly = (
    build_productivity_fact(
        ["date_key", "month_key", "event_hour"]
    )
)

df_fact_productivity_activity = (
    build_productivity_fact(
        ["date_key", "month_key", "activity_category_key"]
    )
)

display(df_fact_productivity_monthly)
df_fact_productivity_monthly.printSchema()

display(df_fact_productivity_daily)
df_fact_productivity_daily.printSchema()

display(df_fact_productivity_hourly)
df_fact_productivity_hourly.printSchema()

display(df_fact_productivity_activity)
df_fact_productivity_activity.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Build source file audit fact
# MAGIC
# MAGIC This cell builds `fact_source_file_audit`.
# MAGIC
# MAGIC This fact table contains one row per processed source file and provides file-level metrics such as:
# MAGIC - Total records.
# MAGIC - Distinct activity categories.
# MAGIC - Distinct productivity states.
# MAGIC - Duration metrics.
# MAGIC - Data quality counters.
# MAGIC - Processing timestamps from Bronze and Silver.
# MAGIC
# MAGIC The descriptive file metadata is stored in `dim_source_file`, so this fact keeps only `source_file_key` and measurable audit fields.

# COMMAND ----------

# DBTITLE 1,Build source file audit fact
df_fact_source_file_audit_base = (
    df_fact_interval
    .groupBy("source_file_key")
    .agg(
        F.count("*").alias("total_records"),

        F.countDistinct("activity_category_key").alias("activity_category_count"),

        F.sum("session_increment_seconds").alias("total_session_seconds_calc"),
        F.sum("attentive_increment_seconds").alias("total_attentive_seconds_calc"),
        F.sum("productive_increment_seconds").alias("total_productive_seconds_calc"),
        F.sum("inactive_increment_seconds").alias("total_inactive_seconds_calc"),

        F.sum(F.col("has_counter_reset").cast("int")).alias("counter_reset_records"),
        F.sum(F.col("has_duration_inconsistency").cast("int")).alias("duration_inconsistency_records"),
        F.sum("duration_balance_difference_seconds").alias("total_duration_balance_difference_seconds_calc"),

        F.min("event_timestamp").alias("first_event_timestamp"),
        F.max("event_timestamp").alias("last_event_timestamp"),

        F.min("_ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        F.max("_silver_processed_timestamp").alias("silver_processed_timestamp")
    )
)

df_fact_source_file_audit = add_duration_metrics(df_fact_source_file_audit_base)

display(df_fact_source_file_audit)

df_fact_source_file_audit.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Write all aggregated Gold fact tables
# MAGIC
# MAGIC This cell writes all aggregated fact DataFrames as Delta tables in the Gold layer.
# MAGIC
# MAGIC The write uses selective overwrite with `replaceWhere` based on their respective date key.
# MAGIC
# MAGIC This approach makes the notebook idempotent:
# MAGIC
# MAGIC - If the ingestion date does not exist yet, the data is inserted.
# MAGIC - If the same ingestion date is processed again, only that ingestion date is replaced.
# MAGIC - Data from other ingestion dates remains unchanged.
# MAGIC
# MAGIC This avoids duplicated records when the pipeline is re-executed for the same ingestion date.

# COMMAND ----------

# DBTITLE 1,Write all aggregated Gold fact tables
(
    df_fact_productivity_monthly.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"month_key = {month_key}")
    .save(gold_fact_productivity_monthly_path)
)

(
    df_fact_productivity_daily.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"date_key = {date_key}")
    .save(gold_fact_productivity_daily_path)
)

(
    df_fact_productivity_hourly.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"date_key = {date_key}")
    .save(gold_fact_productivity_hourly_path)
)

(
    df_fact_productivity_activity.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"date_key = {date_key}")
    .save(gold_fact_productivity_activity_path)
)

(
    df_fact_source_file_audit.write
    .format("delta")
    .mode("overwrite")
    .save(gold_fact_source_file_audit_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Read and validate final aggregated Gold fact tables
# MAGIC
# MAGIC This cell reads the written aggregated Gold fact tables from ADLS Gen2 to confirm that all write operations were successful.
# MAGIC
# MAGIC It also displays each final fact table for visual validation.

# COMMAND ----------

# DBTITLE 1,Read and validate final aggregated Gold fact tables
df_check_fact_productivity_monthly = spark.read.format("delta").load(gold_fact_productivity_monthly_path)
df_check_fact_productivity_daily = spark.read.format("delta").load(gold_fact_productivity_daily_path)
df_check_fact_productivity_hourly = spark.read.format("delta").load(gold_fact_productivity_hourly_path)
df_check_fact_productivity_activity = spark.read.format("delta").load(gold_fact_productivity_activity_path)
df_check_fact_source_file_audit = spark.read.format("delta").load(gold_fact_source_file_audit_path)

print("Final Monthly fact rows:", df_check_fact_productivity_monthly.count())
print("Final Daily fact rows:", df_check_fact_productivity_daily.count())
print("Final Hourly fact rows:", df_check_fact_productivity_hourly.count())
print("Final Activity fact rows:", df_check_fact_productivity_activity.count())
print("Final Source file audit fact rows:", df_check_fact_source_file_audit.count())

display(df_check_fact_productivity_monthly)
display(df_check_fact_productivity_daily)
display(df_check_fact_productivity_hourly)
display(df_check_fact_productivity_activity)
display(df_check_fact_source_file_audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Finish notebook execution
# MAGIC
# MAGIC This step returns a success message to the orchestrator notebook.
# MAGIC
# MAGIC If this message is returned, the notebook finished successfully.

# COMMAND ----------

# DBTITLE 1,Finish notebook execution
dbutils.notebook.exit("OK")