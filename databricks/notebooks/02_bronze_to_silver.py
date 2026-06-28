# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 02_bronze_to_silver
# MAGIC
# MAGIC This notebook builds the Silver layer of the Productivity Monitor project.
# MAGIC
# MAGIC The notebook reads raw productivity events from the Bronze Delta table and transforms them into a cleaner, typed, and business-friendly Silver Delta table.
# MAGIC
# MAGIC The main goals of this notebook are:
# MAGIC
# MAGIC 1. Read productivity events from the Bronze layer.
# MAGIC 2. Convert raw timestamp strings into real timestamp and date columns.
# MAGIC 3. Clean and standardize activity values.
# MAGIC 4. Convert numeric 0/1 fields into boolean indicators.
# MAGIC 5. Create productivity state columns based on the `level` field.
# MAGIC 6. Create interaction state columns based on `face_front` and `input_active`.
# MAGIC 7. Create row-level quality and traceability columns.
# MAGIC 8. Remove invalid records, duplicated records, and records without activity information.
# MAGIC 9. Write the final cleaned dataset as a Delta table in the Silver layer.
# MAGIC
# MAGIC Input:
# MAGIC - Bronze Delta table: `bronze/productivity_events/`
# MAGIC
# MAGIC Output:
# MAGIC - Silver Delta table: `silver/productivity_events/`
# MAGIC
# MAGIC Important business logic:
# MAGIC - `level = 0` means inactive.
# MAGIC - `level = 1` means partially active. This happens when either face detection or input activity is active.
# MAGIC - `level = 2` means fully active. This happens when both face detection and input activity are active.
# MAGIC - `face_front = 1` means the user is looking at the screen.
# MAGIC - `input_active = 1` means the user is using keyboard or mouse.
# MAGIC - Records without activity information are removed because they belong to an older incomplete version of the source app data.

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
# MAGIC This cell imports the PySpark functions required to clean data, cast columns, create timestamps, calculate ratios, create hashes, and build productivity state fields.

# COMMAND ----------

# DBTITLE 1,Import required PySpark functions
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    when,
    lit,
    to_timestamp,
    to_date,
    hour,
    minute,
    date_format,
    current_timestamp,
    sha2,
    concat_ws,
    round
)

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Define storage access configuration and session timezone
# MAGIC
# MAGIC This cell defines the ADLS Gen2 paths used to read the Bronze Delta table and write the Silver Delta table.
# MAGIC
# MAGIC It also sets the Spark session timezone to `America/Bogota` so timestamp parsing and display are aligned with Colombia local time.

# COMMAND ----------

# DBTITLE 1,Define storage access configuration and session timezone
storage_account_name = "saproductivitymonitor"
container_name = "productivity"
storage_account_key = "STORAGE KEY"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)
spark.conf.set("spark.sql.session.timeZone", "America/Bogota")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Define storage paths
# MAGIC
# MAGIC This cell defines the ADLS Gen2 paths used by the notebook.

# COMMAND ----------

# DBTITLE 1,Define storage paths
base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
landing_root_path = f"{base_path}/landing/source=desktop_app/"
bronze_path = f"{base_path}/bronze/productivity_events/"
silver_path = f"{base_path}/silver/productivity_events/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Read the Bronze Delta table
# MAGIC
# MAGIC This cell reads the raw productivity events from the Bronze layer.
# MAGIC
# MAGIC Bronze data is expected to preserve the original source structure plus technical metadata columns such as source file path, source file name, ingestion timestamp, and ingest date.

# COMMAND ----------

# DBTITLE 1,Read the Bronze Delta table
df_bronze = (
    spark.read
    .format("delta")
    .load(bronze_path)
    .filter(F.col("_ingest_date") == p_ingest_date)
)

df_bronze.printSchema()

display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Create typed and cleaned base columns
# MAGIC
# MAGIC This cell converts raw Bronze columns into cleaner and typed Silver columns.
# MAGIC
# MAGIC The main transformations are:
# MAGIC - Convert the raw `timestamp` string into `event_timestamp`.
# MAGIC - Create date and time helper columns.
# MAGIC - Cast numeric fields to the expected data types.
# MAGIC - Clean the raw activity text.
# MAGIC - Convert `face_front` and `input_active` from 0/1 values into boolean columns.

# COMMAND ----------

# DBTITLE 1,Create typed and cleaned base columns
df_silver = (
    df_bronze

    .withColumn(
        "event_timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    )

    .withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    )

    .withColumn(
        "event_hour",
        hour(col("event_timestamp"))
    )

    .withColumn(
        "event_minute",
        minute(col("event_timestamp"))
    )

    .withColumn(
        "event_year_month",
        date_format(col("event_timestamp"), "yyyy-MM")
    )

    .withColumn(
        "level",
        col("level").cast("int")
    )

    .withColumn(
        "face_front",
        col("face_front").cast("int")
    )

    .withColumn(
        "input_active",
        col("input_active").cast("int")
    )

    .withColumn(
        "session_seconds",
        col("session_seconds").cast("long")
    )

    .withColumn(
        "attentive_seconds",
        col("attentive_seconds").cast("long")
    )

    .withColumn(
        "productive_seconds",
        col("productive_seconds").cast("long")
    )

    .withColumn(
        "activity_clean",
        lower(trim(col("activity")))
    )

    .withColumn(
        "face_front_bool",
        when(col("face_front") == 1, lit(True))
        .when(col("face_front") == 0, lit(False))
        .otherwise(lit(None))
    )

    .withColumn(
        "input_active_bool",
        when(col("input_active") == 1, lit(True))
        .when(col("input_active") == 0, lit(False))
        .otherwise(lit(None))
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Standardize activity categories
# MAGIC
# MAGIC This cell creates a normalized `activity_category` column.
# MAGIC
# MAGIC The source app may store activities in Spanish or English. This step maps equivalent values into controlled categories such as `working` and `studying`.
# MAGIC
# MAGIC Records with empty or missing activity values will be filtered later because they belong to an incomplete version of the source dataset.

# COMMAND ----------

# DBTITLE 1,Standardize activity categories
df_silver = (
    df_silver
    .withColumn(
        "activity_category",
        when(
            col("activity_clean").isin("trabajando", "work", "working"),
            lit("working")
        )
        .when(
            col("activity_clean").isin("estudiando", "study", "studying"),
            lit("studying")
        )
        .when(
            col("activity_clean").isNull() | (col("activity_clean") == ""),
            lit("None")
        )
        .otherwise(col("activity_clean"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Create productivity state columns
# MAGIC
# MAGIC This cell creates business-friendly productivity state columns based on the `level` field.
# MAGIC
# MAGIC Business logic:
# MAGIC - `level = 0` means inactive.
# MAGIC - `level = 1` means attentive.
# MAGIC - `level = 2` means fully active.
# MAGIC
# MAGIC These columns are created in the Silver layer because they represent business meaning, not just final aggregation logic.

# COMMAND ----------

# DBTITLE 1,Create productivity state columns
df_silver = (
    df_silver
    .withColumn(
        "productivity_state",
        when(col("level") == 0, lit("inactive"))
        .when(col("level") == 1, lit("attentive"))
        .when(col("level") == 2, lit("fully_active"))
        .otherwise(lit("unknown"))
    )

    .withColumn(
        "inactive",
        when(col("level") == 0, lit(True)).otherwise(lit(False))
    )

    .withColumn(
        "attentive",
        when(col("level") == 1, lit(True)).otherwise(lit(False))
    )

    .withColumn(
        "fully_active",
        when(col("level") == 2, lit(True)).otherwise(lit(False))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Create interaction state columns
# MAGIC
# MAGIC This cell creates a detailed interaction state based on `face_front_bool` and `input_active_bool`.
# MAGIC
# MAGIC The interaction state explains which productivity signals were active:
# MAGIC - No face detected and no input activity.
# MAGIC - Face detected only.
# MAGIC - Input activity only.
# MAGIC - Both face detection and input activity.

# COMMAND ----------

# DBTITLE 1,Create interaction state columns
df_silver = (
    df_silver
    .withColumn(
        "interaction_state",
        when(
            (col("face_front_bool") == False) & (col("input_active_bool") == False),
            lit("no_face_no_input")
        )
        .when(
            (col("face_front_bool") == True) & (col("input_active_bool") == False),
            lit("face_only")
        )
        .when(
            (col("face_front_bool") == False) & (col("input_active_bool") == True),
            lit("input_only")
        )
        .when(
            (col("face_front_bool") == True) & (col("input_active_bool") == True),
            lit("face_and_input")
        )
        .otherwise(lit("unknown"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Calculate row-level ratios
# MAGIC
# MAGIC This cell calculates row-level attentive and productive ratios based on the cumulative counters available in the source data.
# MAGIC
# MAGIC These ratios are useful for quick row-level inspection, but final Gold duration calculations should use increment logic because the duration fields are cumulative counters.

# COMMAND ----------

# DBTITLE 1,Calculate row-level ratios
df_silver = (
    df_silver

    .withColumn(
        "attentive_ratio",
        when(
            col("session_seconds") > 0,
            round(col("attentive_seconds") / col("session_seconds"), 4)
        ).otherwise(lit(None))
    )

    .withColumn(
        "productive_ratio",
        when(
            col("session_seconds") > 0,
            round(col("productive_seconds") / col("session_seconds"), 4)
        ).otherwise(lit(None))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Create a technical record hash
# MAGIC
# MAGIC This cell creates a deterministic hash for each record.
# MAGIC
# MAGIC The `record_hash` column is used to identify duplicated records and avoid repeated rows in the Silver layer.

# COMMAND ----------

# DBTITLE 1,Create a technical record hash
df_silver = (
    df_silver
    .withColumn(
        "record_hash",
        sha2(
            concat_ws(
                "||",
                col("_source_file"),
                col("timestamp"),
                col("level").cast("string"),
                col("face_front").cast("string"),
                col("input_active").cast("string"),
                col("session_seconds").cast("string"),
                col("attentive_seconds").cast("string"),
                col("productive_seconds").cast("string"),
                col("activity_category")
            ),
            256
        )
    )

    .withColumn(
        "_silver_processed_timestamp",
        current_timestamp()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Filter invalid or incomplete records
# MAGIC
# MAGIC This cell removes records that should not continue into the Silver layer.
# MAGIC
# MAGIC The filters remove:
# MAGIC - Records without a valid event timestamp.
# MAGIC - Records without an activity category.
# MAGIC - Records with unknown productivity state.
# MAGIC
# MAGIC This keeps Silver focused on valid and analyzable events.

# COMMAND ----------

# DBTITLE 1,Filter invalid or incomplete records
df_silver = (
    df_silver
    .filter(col("event_timestamp").isNotNull())
    .filter(col("activity_category").isNotNull())
    .filter(col("productivity_state") != "unknown")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Select final Silver columns
# MAGIC
# MAGIC This cell selects the final Silver columns in a clean and organized order.
# MAGIC
# MAGIC The output includes:
# MAGIC - Business keys and timestamps.
# MAGIC - Productivity state fields.
# MAGIC - Interaction state fields.
# MAGIC - Duration counters.
# MAGIC - Row-level ratios.
# MAGIC - Activity categories.
# MAGIC - Technical metadata for lineage and traceability.

# COMMAND ----------

# DBTITLE 1,Select final Silver columns
df_silver = df_silver.select(
    "record_hash",

    "event_timestamp",
    "event_date",
    "event_hour",
    "event_minute",
    "event_year_month",

    "level",
    "face_front_bool",
    "input_active_bool",

    "productivity_state",
    "interaction_state",
    "inactive",
    "attentive",
    "fully_active",

    "session_seconds",
    "attentive_seconds",
    "productive_seconds",

    "attentive_ratio",
    "productive_ratio",

    "activity_clean",
    "activity_category",

    "_source_format",
    "_source_file",
    "_source_file_name",
    "_ingestion_timestamp",
    "_ingest_date",
    "_silver_processed_timestamp"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - Validate the Silver DataFrame
# MAGIC
# MAGIC This cell displays the final Silver DataFrame and prints its schema before writing it to ADLS Gen2.
# MAGIC
# MAGIC The validation helps confirm that the new productivity state and interaction state columns were created correctly.

# COMMAND ----------

# DBTITLE 1,Validate the Silver DataFrame
display(df_silver)

df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 - Run basic quality checks
# MAGIC
# MAGIC This cell runs simple quality checks before writing the Silver Delta table.
# MAGIC
# MAGIC It checks:
# MAGIC - Number of Bronze records.
# MAGIC - Number of Silver records.
# MAGIC - Number of records with missing timestamps.
# MAGIC - Number of records with missing activity category.
# MAGIC - Distribution by productivity state.
# MAGIC - Distribution by interaction state.

# COMMAND ----------

# DBTITLE 1,Run basic quality checks
print("Bronze rows:", df_bronze.count())
print("Silver rows:", df_silver.count())

print("Rows with null event_timestamp:", df_silver.filter(col("event_timestamp").isNull()).count())
print("Rows with null activity_category:", df_silver.filter(col("activity_category").isNull()).count())

display(
    df_silver
    .groupBy("productivity_state")
    .count()
    .orderBy("productivity_state")
)

display(
    df_silver
    .groupBy("interaction_state")
    .count()
    .orderBy("interaction_state")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 - Write the Silver Delta table
# MAGIC
# MAGIC This cell writes the cleaned Silver DataFrame as a Delta table.
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

# DBTITLE 1,Write the Silver Delta table
(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_ingest_date = '{p_ingest_date}'")
    .save(silver_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15 - Read the Silver Delta table for validation
# MAGIC
# MAGIC This cell reads the written Silver Delta table from ADLS Gen2 to confirm that the write operation was successful.

# COMMAND ----------

# DBTITLE 1,Read the Silver Delta table for validation
df_check_silver = spark.read.format("delta").load(silver_path)

df_check_silver.printSchema()

display(df_check_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 16 - Finish notebook execution
# MAGIC
# MAGIC This step returns a success message to the orchestrator notebook.
# MAGIC
# MAGIC If this message is returned, the notebook finished successfully.

# COMMAND ----------

# DBTITLE 1,Finish notebook execution
dbutils.notebook.exit("OK")