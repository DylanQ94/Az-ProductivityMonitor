# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03_silver_to_gold_fact_interval
# MAGIC
# MAGIC This notebook builds the base analytical fact table for the Gold layer of the Productivity Monitor project.
# MAGIC
# MAGIC The notebook reads cleaned productivity events from the Silver Delta table and creates `fact_productivity_interval`, which is the row-level analytical source of truth for all future Gold aggregate fact tables.
# MAGIC
# MAGIC The main goals of this notebook are:
# MAGIC
# MAGIC 1. Read productivity events from the Silver layer.
# MAGIC 2. Create analytical keys for Date, Month, Activity Category, Productivity State, and Source File.
# MAGIC 3. Convert cumulative duration counters into real row-level duration increments.
# MAGIC 4. Calculate inactive duration as session duration minus attentive duration minus productive duration.
# MAGIC 5. Create duration metrics in seconds and hours.
# MAGIC 6. Create interval-level ratios.
# MAGIC 7. Create record-level flags for signals and productivity states.
# MAGIC 8. Add data quality columns to detect counter resets and duration inconsistencies.
# MAGIC 9. Write the final interval-level fact table as Delta in the Gold layer.
# MAGIC
# MAGIC Input:
# MAGIC - `silver/productivity_events/`
# MAGIC
# MAGIC Output:
# MAGIC - `gold/fact_productivity_interval/`
# MAGIC
# MAGIC Important business logic:
# MAGIC The source duration columns are cumulative counters. Because of that, this notebook uses `lag()` inside each source file to calculate real row-level increments.
# MAGIC
# MAGIC The correct duration model is:
# MAGIC
# MAGIC - session duration = attentive duration + productive duration + inactive duration
# MAGIC
# MAGIC Inactive duration is calculated as:
# MAGIC
# MAGIC - inactive duration = session duration - attentive duration - productive duration
# MAGIC
# MAGIC The `productivity_state` column is kept as a record classification field, but it is not used as the main source for duration calculations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 - Receive and validate the ingestion date parameter
# MAGIC
# MAGIC This step receives the `p_ingest_date` parameter from the orchestrator notebook.
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

p_ingest_date = dbutils.widgets.get("p_ingest_date").strip()

if p_ingest_date == "":
    raise ValueError("Parameter p_ingest_date is required. Expected format: yyyy-MM-dd")

print(f"Processing ingest date: {p_ingest_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Import required PySpark functions
# MAGIC
# MAGIC This cell imports the PySpark functions needed to calculate row-level increments, inactive duration, ratios, and aggregated Gold metrics.

# COMMAND ----------

# DBTITLE 1,Import required PySpark functions
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
# MAGIC This cell defines the ADLS Gen2 paths used to read the Silver Delta table and write the Gold Delta tables.

# COMMAND ----------

# DBTITLE 1,Define storage paths
base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
silver_path = f"{base_path}/silver/productivity_events/"

gold_fact_productivity_interval_path = f"{base_path}/gold//fact_productivity_interval/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Read the Silver Delta table
# MAGIC
# MAGIC This cell reads the cleaned and normalized productivity events from the Silver layer.

# COMMAND ----------

# DBTITLE 1,Read the Silver Delta table
df_silver = (
    spark.read
    .format("delta")
    .load(silver_path)
    .filter(F.col("_ingest_date") == p_ingest_date)
)

df_silver.printSchema()
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Create previous cumulative values and counter reset flags
# MAGIC
# MAGIC This cell creates auxiliary columns with the previous cumulative duration values from the same source file.
# MAGIC
# MAGIC It also creates data quality flags to detect whether any cumulative counter decreased compared with the previous row. A decrease usually means that the counter was reset.

# COMMAND ----------

# DBTITLE 1,Create previous cumulative values and counter reset flags
window_by_source_file = (
    Window
    .partitionBy("_source_file")
    .orderBy("event_timestamp", "record_hash")
)

df_gold_interval_base = (
    df_silver

    .withColumn(
        "previous_session_seconds",
        F.lag(F.col("session_seconds")).over(window_by_source_file)
    )

    .withColumn(
        "previous_attentive_seconds",
        F.lag(F.col("attentive_seconds")).over(window_by_source_file)
    )

    .withColumn(
        "previous_productive_seconds",
        F.lag(F.col("productive_seconds")).over(window_by_source_file)
    )

    .withColumn(
        "is_first_record_in_file",
        F.col("previous_session_seconds").isNull()
    )

    .withColumn(
        "has_session_counter_reset",
        F.when(
            F.col("previous_session_seconds").isNotNull()
            & (F.col("session_seconds") < F.col("previous_session_seconds")),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    .withColumn(
        "has_attentive_counter_reset",
        F.when(
            F.col("previous_attentive_seconds").isNotNull()
            & (F.col("attentive_seconds") < F.col("previous_attentive_seconds")),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    .withColumn(
        "has_productive_counter_reset",
        F.when(
            F.col("previous_productive_seconds").isNotNull()
            & (F.col("productive_seconds") < F.col("previous_productive_seconds")),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    .withColumn(
        "has_counter_reset",
        F.col("has_session_counter_reset")
        | F.col("has_attentive_counter_reset")
        | F.col("has_productive_counter_reset")
    )
)

display(df_gold_interval_base)

df_gold_interval_base.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Calculate real duration increments
# MAGIC
# MAGIC This cell converts cumulative counters into real row-level increments.
# MAGIC
# MAGIC For the first record of each source file, the increment is set to zero because there is no previous row to compare against.
# MAGIC
# MAGIC If a counter reset is detected, the current cumulative value is used as the increment after the reset.

# COMMAND ----------

# DBTITLE 1,Calculate real duration increments
df_gold_interval_base = (
    df_gold_interval_base

    .withColumn(
        "session_increment_seconds",
        F.when(F.col("is_first_record_in_file"), F.lit(0))
        .when(F.col("session_seconds").isNull(), F.lit(0))
        .when(
            F.col("session_seconds") >= F.col("previous_session_seconds"),
            F.col("session_seconds") - F.col("previous_session_seconds")
        )
        .otherwise(F.col("session_seconds"))
    )

    .withColumn(
        "attentive_increment_seconds",
        F.when(F.col("is_first_record_in_file"), F.lit(0))
        .when(F.col("attentive_seconds").isNull(), F.lit(0))
        .when(
            F.col("attentive_seconds") >= F.col("previous_attentive_seconds"),
            F.col("attentive_seconds") - F.col("previous_attentive_seconds")
        )
        .otherwise(F.col("attentive_seconds"))
    )

    .withColumn(
        "productive_increment_seconds",
        F.when(F.col("is_first_record_in_file"), F.lit(0))
        .when(F.col("productive_seconds").isNull(), F.lit(0))
        .when(
            F.col("productive_seconds") >= F.col("previous_productive_seconds"),
            F.col("productive_seconds") - F.col("previous_productive_seconds")
        )
        .otherwise(F.col("productive_seconds"))
    )

    .withColumn(
        "inactive_increment_seconds_raw",
        F.col("session_increment_seconds")
        - F.col("attentive_increment_seconds")
        - F.col("productive_increment_seconds")
    )

    .withColumn(
        "inactive_increment_seconds",
        F.when(
            F.col("inactive_increment_seconds_raw") < 0,
            F.lit(0)
        ).otherwise(F.col("inactive_increment_seconds_raw"))
    )

    .withColumn(
        "duration_balance_difference_seconds",
        F.col("session_increment_seconds")
        - F.col("attentive_increment_seconds")
        - F.col("productive_increment_seconds")
        - F.col("inactive_increment_seconds")
    )

    .withColumn(
        "has_duration_inconsistency",
        F.col("inactive_increment_seconds_raw") < 0
    )
)

display(df_gold_interval_base)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Create analytical keys, duration hours, ratios, and record flags
# MAGIC
# MAGIC This cell enriches the interval fact table with analytical keys, duration metrics in hours, interval-level ratios, and record-level flags.
# MAGIC
# MAGIC The keys will later be used to relate this fact table with Gold dimension tables in Power BI or Synapse.

# COMMAND ----------

# DBTITLE 1,Create analytical keys, duration hours, ratios, and record flags
df_gold_fact_productivity_interval = (
    df_gold_interval_base

    .withColumn(
        "date_key",
        F.date_format(F.col("event_date"), "yyyyMMdd").cast("int")
    )

    .withColumn(
        "month_key",
        F.date_format(F.col("event_date"), "yyyyMM").cast("int")
    )

    .withColumn(
        "activity_category_key",
        F.sha2(
            F.coalesce(F.col("activity_category"), F.lit("unknown")),
            256
        )
    )

    .withColumn(
        "source_file_key",
        F.sha2(
            F.coalesce(F.col("_source_file"), F.lit("unknown")),
            256
        )
    )

    .withColumn(
        "session_increment_hours",
        F.round(F.col("session_increment_seconds") / 3600, 6)
    )

    .withColumn(
        "attentive_increment_hours",
        F.round(F.col("attentive_increment_seconds") / 3600, 6)
    )

    .withColumn(
        "productive_increment_hours",
        F.round(F.col("productive_increment_seconds") / 3600, 6)
    )

    .withColumn(
        "inactive_increment_hours",
        F.round(F.col("inactive_increment_seconds") / 3600, 6)
    )

    .withColumn(
        "attentive_increment_ratio",
        F.when(
            F.col("session_increment_seconds") > 0,
            F.round(F.col("attentive_increment_seconds") / F.col("session_increment_seconds"), 6)
        ).otherwise(F.lit(None))
    )

    .withColumn(
        "productive_increment_ratio",
        F.when(
            F.col("session_increment_seconds") > 0,
            F.round(F.col("productive_increment_seconds") / F.col("session_increment_seconds"), 6)
        ).otherwise(F.lit(None))
    )

    .withColumn(
        "inactive_increment_ratio",
        F.when(
            F.col("session_increment_seconds") > 0,
            F.round(F.col("inactive_increment_seconds") / F.col("session_increment_seconds"), 6)
        ).otherwise(F.lit(None))
    )

    .withColumn(
        "face_front_record_flag",
        F.when(F.col("face_front_bool") == True, F.lit(1)).otherwise(F.lit(0))
    )

    .withColumn(
        "input_active_record_flag",
        F.when(F.col("input_active_bool") == True, F.lit(1)).otherwise(F.lit(0))
    )

    .withColumn(
        "inactive_record_flag",
        F.when(F.col("productivity_state") == "inactive", F.lit(1)).otherwise(F.lit(0))
    )

    .withColumn(
        "partial_active_record_flag",
        F.when(F.col("productivity_state") == "partial_active", F.lit(1)).otherwise(F.lit(0))
    )

    .withColumn(
        "fully_active_record_flag",
        F.when(F.col("productivity_state") == "fully_active", F.lit(1)).otherwise(F.lit(0))
    )

    .withColumn(
        "_gold_processed_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Select final columns for the interval fact table
# MAGIC
# MAGIC This cell selects the final columns of `fact_productivity_interval` in a clean and organized order.
# MAGIC
# MAGIC The table includes:
# MAGIC - Analytical keys.
# MAGIC - Natural date and category columns.
# MAGIC - Original cumulative counters.
# MAGIC - Real interval duration metrics.
# MAGIC - Ratio metrics.
# MAGIC - Record-level flags.
# MAGIC - Data quality fields.
# MAGIC - Technical lineage columns.

# COMMAND ----------

# DBTITLE 1,Select final columns for the interval fact table
df_gold_fact_productivity_interval = df_gold_fact_productivity_interval.select(
    # Technical record identifier
    "record_hash",

    # Analytical keys
    "date_key",
    "month_key",
    "event_hour",
    "activity_category_key",
    "source_file_key",

    # Natural date columns
    "event_timestamp",
    "event_date",
    "event_year_month",
    "event_minute",

    # Natural category columns
    "activity_category",
    "productivity_state",
    "interaction_state",

    # Original cumulative counters
    F.col("session_seconds").alias("session_cumulative_seconds"),
    F.col("attentive_seconds").alias("attentive_cumulative_seconds"),
    F.col("productive_seconds").alias("productive_cumulative_seconds"),

    # Duration increments in seconds
    "session_increment_seconds",
    "attentive_increment_seconds",
    "productive_increment_seconds",
    "inactive_increment_seconds",

    # Duration increments in hours
    "session_increment_hours",
    "attentive_increment_hours",
    "productive_increment_hours",
    "inactive_increment_hours",

    # Interval-level ratios
    "attentive_increment_ratio",
    "productive_increment_ratio",
    "inactive_increment_ratio",

    # Record-level flags
    "level",
    "face_front_bool",
    "input_active_bool",
    "face_front_record_flag",
    "input_active_record_flag",
    "inactive_record_flag",
    "partial_active_record_flag",
    "fully_active_record_flag",

    # Data quality columns
    "is_first_record_in_file",
    "has_session_counter_reset",
    "has_attentive_counter_reset",
    "has_productive_counter_reset",
    "has_counter_reset",
    "inactive_increment_seconds_raw",
    "duration_balance_difference_seconds",
    "has_duration_inconsistency",

    # Lineage columns
    "_source_format",
    "_source_file",
    "_source_file_name",
    "_ingest_date",
    "_ingestion_timestamp",
    "_silver_processed_timestamp",
    "_gold_processed_timestamp"
)

display(df_gold_fact_productivity_interval)

df_gold_fact_productivity_interval.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Run quality checks before writing
# MAGIC
# MAGIC This cell validates the final interval fact table before writing it to the Gold layer.
# MAGIC
# MAGIC The checks include:
# MAGIC - Total row count.
# MAGIC - Number of first records per source file.
# MAGIC - Number of counter reset records.
# MAGIC - Number of duration inconsistency records.
# MAGIC - Duration balance validation.
# MAGIC - Distribution by activity category and productivity state.

# COMMAND ----------

# DBTITLE 1,Run quality checks before writing
print("Interval fact rows:", df_gold_fact_productivity_interval.count())

print(
    "First records in source files:",
    df_gold_fact_productivity_interval
    .filter(F.col("is_first_record_in_file") == True)
    .count()
)

print(
    "Counter reset records:",
    df_gold_fact_productivity_interval
    .filter(F.col("has_counter_reset") == True)
    .count()
)

print(
    "Duration inconsistency records:",
    df_gold_fact_productivity_interval
    .filter(F.col("has_duration_inconsistency") == True)
    .count()
)

df_duration_balance_check = (
    df_gold_fact_productivity_interval
    .withColumn(
        "duration_sum_seconds",
        F.col("attentive_increment_seconds")
        + F.col("productive_increment_seconds")
        + F.col("inactive_increment_seconds")
    )
    .withColumn(
        "duration_difference_seconds",
        F.col("session_increment_seconds") - F.col("duration_sum_seconds")
    )
)

display(
    df_duration_balance_check
    .select(
        "record_hash",
        "session_increment_seconds",
        "attentive_increment_seconds",
        "productive_increment_seconds",
        "inactive_increment_seconds",
        "duration_sum_seconds",
        "duration_difference_seconds",
        "has_duration_inconsistency"
    )
    .orderBy(F.desc(F.abs(F.col("duration_difference_seconds"))))
)

display(
    df_gold_fact_productivity_interval
    .groupBy("activity_category")
    .count()
    .orderBy("activity_category")
)

display(
    df_gold_fact_productivity_interval
    .groupBy("productivity_state")
    .count()
    .orderBy("productivity_state")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Write the interval fact table to the Gold layer
# MAGIC
# MAGIC This cell writes `fact_productivity_interval` as a Delta table in the Gold layer.
# MAGIC
# MAGIC The write uses selective overwrite with `replaceWhere` based on `_ingest_date`.
# MAGIC
# MAGIC This approach makes the notebook idempotent:
# MAGIC
# MAGIC - If the ingestion date does not exist yet, the data is inserted.
# MAGIC - If the same ingestion date is processed again, only that ingestion date is replaced.
# MAGIC - Data from other ingestion dates remains unchanged.
# MAGIC
# MAGIC This avoids duplicated records when the pipeline is re-executed for the same ingestion date.

# COMMAND ----------

# DBTITLE 1,Write the interval fact table to the Gold layer
(
    df_gold_fact_productivity_interval.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_ingest_date = '{p_ingest_date}'")
    .save(gold_fact_productivity_interval_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Read and validate the final Gold interval fact table
# MAGIC
# MAGIC This cell reads the written Delta table from ADLS Gen2 to confirm that the write operation was successful.
# MAGIC
# MAGIC It also displays the final table and prints its schema for validation.

# COMMAND ----------

# DBTITLE 1,Read and validate the final Gold interval fact table
df_check_gold_fact_productivity_interval = (
    spark.read
    .format("delta")
    .load(gold_fact_productivity_interval_path)
)

print(
    "Final interval fact rows:",
    df_check_gold_fact_productivity_interval.count()
)

display(df_check_gold_fact_productivity_interval)

df_check_gold_fact_productivity_interval.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - Finish notebook execution
# MAGIC
# MAGIC This step returns a success message to the orchestrator notebook.
# MAGIC
# MAGIC If this message is returned, the notebook finished successfully.

# COMMAND ----------

# DBTITLE 1,Finish notebook execution
dbutils.notebook.exit("OK")