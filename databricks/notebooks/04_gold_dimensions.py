# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04_gold_dimensions
# MAGIC
# MAGIC This notebook builds the Gold dimension tables for the Productivity Monitor project.
# MAGIC
# MAGIC The notebook reads the Gold interval fact table and creates reusable dimensions for the analytical model.
# MAGIC
# MAGIC The main goals of this notebook are:
# MAGIC
# MAGIC 1. Read `fact_productivity_interval` from the Gold layer.
# MAGIC 2. Create `dim_date` from the event dates available in the interval fact.
# MAGIC 3. Create `dim_month` from the month values available in the interval fact.
# MAGIC 4. Create `dim_hour` with the 24 hours of the day.
# MAGIC 5. Create `dim_activity_category` with activity metadata.
# MAGIC 6. Create `dim_productivity_state` with productivity state metadata.
# MAGIC 7. Create `dim_source_file` for source file traceability.
# MAGIC 8. Write all dimension tables as Delta tables in the Gold layer.
# MAGIC
# MAGIC Input:
# MAGIC - `gold/fact_productivity_interval/`
# MAGIC
# MAGIC Outputs:
# MAGIC - `gold/dim_date/`
# MAGIC - `gold/dim_month/`
# MAGIC - `gold/dim_hour/`
# MAGIC - `gold/dim_activity_category/`
# MAGIC - `gold/dim_productivity_state/`
# MAGIC - `gold/dim_source_file/`
# MAGIC
# MAGIC Important design decision:
# MAGIC The dimensions are created from `fact_productivity_interval` because this fact is the row-level analytical source of truth in the Gold layer.
# MAGIC
# MAGIC These dimensions will later be used in Synapse Serverless SQL and Power BI to create cleaner relationships, slicers, sort orders, labels, and business-friendly metadata.

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

gold_dim_date_path = f"{gold_base_path}/dim_date/"
gold_dim_month_path = f"{gold_base_path}/dim_month/"
gold_dim_hour_path = f"{gold_base_path}/dim_hour/"
gold_dim_activity_category_path = f"{gold_base_path}/dim_activity_category/"
gold_dim_source_file_path = f"{gold_base_path}/dim_source_file/"

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
)

display(df_fact_interval)

df_fact_interval.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Create Date dimensions
# MAGIC
# MAGIC This cell creates the temporal dimension tables used for time-based analysis.
# MAGIC
# MAGIC The generated dimensions are:
# MAGIC
# MAGIC 1. `dim_date`
# MAGIC    - One row per event date.
# MAGIC    - Includes year, month, day, week, weekend flag, and month relationship fields.
# MAGIC
# MAGIC 2. `dim_month`
# MAGIC    - One row per event month.
# MAGIC    - Includes month key, month start date, month name, and sorting fields.
# MAGIC
# MAGIC 3. `dim_hour`
# MAGIC    - One row per hour of the day from 0 to 23.
# MAGIC    - Includes visual labels and day period classification.
# MAGIC
# MAGIC These dimensions are useful for Power BI slicers, time filters, date hierarchies, and correct sorting.

# COMMAND ----------

# DBTITLE 1,Create Date dimensions
df_gold_dim_date = (
    df_fact_interval
    .select(
        "date_key",
        "event_date",
        "month_key",
        "event_year_month"
    )
    .dropna(subset=["date_key", "event_date"])
    .distinct()
    .withColumn(
        "event_year",
        F.year(F.col("event_date"))
    )
    .withColumn(
        "event_month",
        F.month(F.col("event_date"))
    )
    .withColumn(
        "event_day",
        F.dayofmonth(F.col("event_date"))
    )
    .withColumn(
        "event_month_name",
        F.date_format(F.col("event_date"), "MMMM")
    )
    .withColumn(
        "event_day_name",
        F.date_format(F.col("event_date"), "EEEE")
    )
    .withColumn(
        "event_day_of_week",
        F.dayofweek(F.col("event_date"))
    )
    .withColumn(
        "event_week_of_year",
        F.weekofyear(F.col("event_date"))
    )
    .withColumn(
        "is_weekend",
        F.when(F.col("event_day_of_week").isin(1, 7), F.lit(True))
        .otherwise(F.lit(False))
    )
    .withColumn(
        "_gold_processed_timestamp",
        F.current_timestamp()
    )
    .orderBy("event_date")
)

df_gold_dim_month = (
    df_fact_interval
    .select(
        "month_key",
        "event_year_month"
    )
    .dropna(subset=["month_key", "event_year_month"])
    .distinct()
    .withColumn(
        "event_year",
        F.substring(F.col("event_year_month"), 1, 4).cast("int")
    )
    .withColumn(
        "event_month",
        F.substring(F.col("event_year_month"), 6, 2).cast("int")
    )
    .withColumn(
        "month_start_date",
        F.to_date(F.concat(F.col("event_year_month"), F.lit("-01")))
    )
    .withColumn(
        "month_name",
        F.date_format(F.col("month_start_date"), "MMMM")
    )
    .withColumn(
        "month_sort",
        F.col("month_key")
    )
    .withColumn(
        "_gold_processed_timestamp",
        F.current_timestamp()
    )
    .orderBy("month_start_date")
)

df_gold_dim_hour = (
    spark.range(0, 24)
    .withColumnRenamed("id", "event_hour")
    .withColumn(
        "event_hour",
        F.col("event_hour").cast("int")
    )
    .withColumn(
        "hour_label",
        F.format_string("%02d:00", F.col("event_hour"))
    )
    .withColumn(
        "day_period",
        F.when(
            (F.col("event_hour") >= 0) & (F.col("event_hour") < 6),
            F.lit("Night")
        )
        .when(
            (F.col("event_hour") >= 6) & (F.col("event_hour") < 12),
            F.lit("Morning")
        )
        .when(
            (F.col("event_hour") >= 12) & (F.col("event_hour") < 18),
            F.lit("Afternoon")
        )
        .otherwise(F.lit("Evening"))
    )
    .withColumn(
        "day_period_sort_order",
        F.when(F.col("day_period") == "Night", F.lit(1))
        .when(F.col("day_period") == "Morning", F.lit(2))
        .when(F.col("day_period") == "Afternoon", F.lit(3))
        .when(F.col("day_period") == "Evening", F.lit(4))
        .otherwise(F.lit(99))
    )
    .withColumn(
        "_gold_processed_timestamp",
        F.current_timestamp()
    )
    .orderBy("event_hour")
)

display(df_gold_dim_date)
df_gold_dim_date.printSchema()

display(df_gold_dim_month)
df_gold_dim_month.printSchema()

display(df_gold_dim_hour)
df_gold_dim_hour.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Create Activity Category and Productivity State dimensions
# MAGIC
# MAGIC This cell creates categorical dimensions from the interval fact table.
# MAGIC
# MAGIC The generated dimensions are:
# MAGIC
# MAGIC 1. `dim_activity_category`
# MAGIC    - One row per activity category.
# MAGIC    - Includes a display name, activity group, sort order, and business description.
# MAGIC
# MAGIC 2. `dim_productivity_state`
# MAGIC    - One row per productivity state.
# MAGIC    - Includes a display name, sort order, active-state flag, and business description.
# MAGIC
# MAGIC These dimensions are useful for cleaner Power BI labels, controlled sorting, reusable slicers, and centralized business definitions.

# COMMAND ----------

# DBTITLE 1,Create Activity Category and Productivity State dimensions
df_gold_dim_activity_category = (
    df_fact_interval
    .select(
        "activity_category_key",
        "activity_category"
    )
    .dropna(subset=["activity_category_key", "activity_category"])
    .distinct()
    .withColumn(
        "activity_display_name",
        F.initcap(F.regexp_replace(F.col("activity_category"), "_", " "))
    )
    .withColumn(
        "activity_group",
        F.when(F.col("activity_category") == "studying", F.lit("Learning"))
        .when(F.col("activity_category") == "working", F.lit("Work"))
        .otherwise(F.lit("Other"))
    )
    .withColumn(
        "activity_sort_order",
        F.when(F.col("activity_category") == "studying", F.lit(1))
        .when(F.col("activity_category") == "working", F.lit(2))
        .otherwise(F.lit(99))
    )
    .withColumn(
        "activity_description",
        F.when(
            F.col("activity_category") == "studying",
            F.lit("Time registered while the user was studying.")
        )
        .when(
            F.col("activity_category") == "working",
            F.lit("Time registered while the user was working.")
        )
        .otherwise(
            F.lit("Other activity category registered by the source application.")
        )
    )
    .withColumn(
        "_gold_processed_timestamp",
        F.current_timestamp()
    )
    .orderBy("activity_sort_order", "activity_category")
)

display(df_gold_dim_activity_category)
df_gold_dim_activity_category.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Create Source File dimension
# MAGIC
# MAGIC This cell creates the `dim_source_file` table for lineage and audit analysis.
# MAGIC
# MAGIC The dimension contains one row per processed source file and includes:
# MAGIC
# MAGIC - Source file key.
# MAGIC - Source file path.
# MAGIC - Source file name.
# MAGIC - Source format.
# MAGIC - Ingest date.
# MAGIC - First and last event timestamps.
# MAGIC - Total records in the file.
# MAGIC - Data quality indicators at file level.
# MAGIC
# MAGIC This dimension helps trace analytical results back to the original files processed by the pipeline.

# COMMAND ----------

# DBTITLE 1,Create Source File dimension
df_gold_dim_source_file = (
    df_fact_interval
    .groupBy(
        "source_file_key",
        "_source_file",
        "_source_file_name",
        "_source_format",
        "_ingest_date"
    )
    .agg(
        F.min("event_timestamp").alias("first_event_timestamp"),
        F.max("event_timestamp").alias("last_event_timestamp"),
        F.count("*").alias("total_records_in_file"),
        F.max(F.col("has_counter_reset").cast("int")).alias("has_any_counter_reset_int"),
        F.max(F.col("has_duration_inconsistency").cast("int")).alias("has_any_duration_inconsistency_int")
    )
    .withColumn(
        "has_any_counter_reset",
        F.col("has_any_counter_reset_int").cast("boolean")
    )
    .withColumn(
        "has_any_duration_inconsistency",
        F.col("has_any_duration_inconsistency_int").cast("boolean")
    )
    .withColumn(
        "_gold_processed_timestamp",
        F.current_timestamp()
    )
    .drop(
        "has_any_counter_reset_int",
        "has_any_duration_inconsistency_int"
    )
    .orderBy("_source_file_name")
)

display(df_gold_dim_source_file)

df_gold_dim_source_file.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Run dimension quality checks before writing
# MAGIC
# MAGIC This cell validates all dimension DataFrames before writing them to the Gold layer.
# MAGIC
# MAGIC The validation checks include:
# MAGIC - Row counts for each dimension.
# MAGIC - Duplicate key checks.
# MAGIC - Visual inspection of final dimension tables.

# COMMAND ----------

# DBTITLE 1,Run dimension quality checks before writing
print("Dim Date rows:", df_gold_dim_date.count())
print("Dim Month rows:", df_gold_dim_month.count())
print("Dim Hour rows:", df_gold_dim_hour.count())
print("Dim Activity Category rows:", df_gold_dim_activity_category.count())
print("Dim Source File rows:", df_gold_dim_source_file.count())

print(
    "Dim Date duplicated keys:",
    df_gold_dim_date.count() - df_gold_dim_date.select("date_key").distinct().count()
)

print(
    "Dim Month duplicated keys:",
    df_gold_dim_month.count() - df_gold_dim_month.select("month_key").distinct().count()
)

print(
    "Dim Hour duplicated keys:",
    df_gold_dim_hour.count() - df_gold_dim_hour.select("event_hour").distinct().count()
)

print(
    "Dim Activity Category duplicated keys:",
    df_gold_dim_activity_category.count()
    - df_gold_dim_activity_category.select("activity_category_key").distinct().count()
)

print(
    "Dim Source File duplicated keys:",
    df_gold_dim_source_file.count()
    - df_gold_dim_source_file.select("source_file_key").distinct().count()
)

display(df_gold_dim_date)
display(df_gold_dim_month)
display(df_gold_dim_hour)
display(df_gold_dim_activity_category)
display(df_gold_dim_source_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Write all Gold dimension Delta tables
# MAGIC
# MAGIC This cell writes all dimension DataFrames as Delta tables in the Gold layer.
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

# DBTITLE 1,Write all Gold dimension Delta tables
(
    df_gold_dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_dim_date_path)
)

(
    df_gold_dim_month.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_dim_month_path)
)

(
    df_gold_dim_hour.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_dim_hour_path)
)

(
    df_gold_dim_activity_category.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_dim_activity_category_path)
)

(
    df_gold_dim_source_file.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_dim_source_file_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Read and validate final Gold dimension tables
# MAGIC
# MAGIC This cell reads the written Gold dimension Delta tables from ADLS Gen2 to confirm that all write operations were successful.
# MAGIC
# MAGIC It also displays each final dimension table for visual validation.

# COMMAND ----------

# DBTITLE 1,Read and validate final Gold dimension tables
df_check_dim_date = spark.read.format("delta").load(gold_dim_date_path)
df_check_dim_month = spark.read.format("delta").load(gold_dim_month_path)
df_check_dim_hour = spark.read.format("delta").load(gold_dim_hour_path)
df_check_dim_activity_category = spark.read.format("delta").load(gold_dim_activity_category_path)
df_check_dim_source_file = spark.read.format("delta").load(gold_dim_source_file_path)

print("Final Dim Date rows:", df_check_dim_date.count())
print("Final Dim Month rows:", df_check_dim_month.count())
print("Final Dim Hour rows:", df_check_dim_hour.count())
print("Final Dim Activity Category rows:", df_check_dim_activity_category.count())
print("Final Dim Source File rows:", df_check_dim_source_file.count())

display(df_check_dim_date)
display(df_check_dim_month)
display(df_check_dim_hour)
display(df_check_dim_activity_category)
display(df_check_dim_source_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Finish notebook execution
# MAGIC
# MAGIC This step returns a success message to the orchestrator notebook.
# MAGIC
# MAGIC If this message is returned, the notebook finished successfully.

# COMMAND ----------

# DBTITLE 1,Finish notebook execution
dbutils.notebook.exit("OK")