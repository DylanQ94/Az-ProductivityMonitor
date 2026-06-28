# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 01_landing_to_bronze
# MAGIC
# MAGIC This notebook builds the Bronze layer of the Productivity Monitor project.
# MAGIC
# MAGIC The notebook reads raw files from the Landing layer in ADLS Gen2 and writes them as a Delta table in the Bronze layer.
# MAGIC
# MAGIC The main goals of this notebook are:
# MAGIC
# MAGIC 1. Read CSV and JSON files from the Landing layer.
# MAGIC 2. Search files recursively inside ingestion date folders.
# MAGIC 3. Preserve the source data with minimal transformations.
# MAGIC 4. Add technical metadata columns for lineage and traceability.
# MAGIC 5. Extract the ingestion date from the source folder path.
# MAGIC 6. Combine CSV and JSON files into one Bronze DataFrame.
# MAGIC 7. Write the final Bronze dataset as a Delta table.
# MAGIC
# MAGIC Input:
# MAGIC - Landing folder: `landing/source-desktop_app/`
# MAGIC
# MAGIC Output:
# MAGIC - Bronze Delta table: `bronze/productivity_events/`
# MAGIC
# MAGIC Important design decision:
# MAGIC The Bronze layer should keep the data as close as possible to the original source. Business cleaning, activity standardization, productivity states, and timestamp normalization are handled later in the Silver layer.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Import required PySpark functions
# MAGIC
# MAGIC This cell imports the PySpark functions needed to add technical metadata columns and extract the ingestion date from the source file path.

# COMMAND ----------

# DBTITLE 1,Import required PySpark functions
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    regexp_extract,
    to_date
)

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Define storage access configuration
# MAGIC
# MAGIC This cell configures access to the storage account using the storage account key. If access is already configured at the cluster, workspace, or Unity Catalog level, the key configuration section can be skipped.

# COMMAND ----------

# DBTITLE 1,Define storage access configuration
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
# MAGIC This cell defines the ADLS Gen2 paths used by the notebook.

# COMMAND ----------

# DBTITLE 1,Define storage paths
base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
landing_root_path = landing_path = f"{base_path}/landing/source=desktop_app/ingest_date={p_ingest_date}/"
bronze_path = f"{base_path}/bronze/productivity_events/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Validate the source file for the ingestion date
# MAGIC
# MAGIC This step validates the source file available in the Landing folder for the selected ingestion date.The notebook expects one supported source file inside:
# MAGIC
# MAGIC `landing/source-desktop_app/ingest_date=yyyy-MM-dd/`
# MAGIC
# MAGIC The supported formats are:
# MAGIC
# MAGIC - CSV
# MAGIC - JSON
# MAGIC
# MAGIC This validation is necessary because the source application generates either a CSV file or a JSON file for each processing day, but not both at the same time.
# MAGIC
# MAGIC If no supported file is found, or if more than one supported file is found, the notebook stops to avoid loading incorrect or duplicated data.

# COMMAND ----------

# DBTITLE 1,Create a recursive file listing function
landing_items = dbutils.fs.ls(landing_root_path)

source_files = [
    item.path
    for item in landing_items
    if not item.isDir()
    and (
        item.path.lower().endswith(".csv")
        or item.path.lower().endswith(".json")
    )
]
print(source_files)

if len(source_files) == 0:
    raise FileNotFoundError(f"No CSV or JSON file found in: {landing_root_path}")

if len(source_files) > 1:
    raise ValueError(f"More than one source file found: {source_files}")

source_file_path = source_files[0]
source_file_name = source_file_path.split("/")[-1]

if p_source_file_date not in source_file_name:
    raise ValueError(
        f"Invalid source file date. Expected {p_source_file_date}, found file {source_file_name}"
    )

if source_file_path.lower().endswith(".csv"):
    source_format = "csv"
elif source_file_path.lower().endswith(".json"):
    source_format = "json"

print(f"Source file path: {source_file_path}")
print(f"Source file name: {source_file_name}")
print(f"Source format: {source_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Read the source file
# MAGIC
# MAGIC This step reads the validated source file from the Landing layer.
# MAGIC
# MAGIC The read logic depends on the detected source format:
# MAGIC
# MAGIC - If the file is CSV, the notebook reads it using the CSV reader.
# MAGIC - If the file is JSON, the notebook reads it using the JSON reader.
# MAGIC
# MAGIC The result is stored in a single DataFrame called `df_source`, which allows the next steps of the notebook to continue with the same logic regardless of the original file format.

# COMMAND ----------

# DBTITLE 1,Find and read all source files
if source_format == "csv":
    df_source = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(source_file_path)
    )

elif source_format == "json":
    df_source = (
        spark.read
        .format("json")
        .option("multiLine", "true")
        .load(source_file_path)
    )

else:
    raise ValueError(f"Unsupported source format: {source_format}")

display(df_source)

df_source.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Creating bronze DataFrame
# MAGIC
# MAGIC This cell searches all files inside the Landing source folder and separates them by file format.
# MAGIC
# MAGIC The notebook currently supports CSV and JSON files because the source app can generate either format.
# MAGIC
# MAGIC then adds technical metadata columns to the Bronze DataFrame.
# MAGIC
# MAGIC The metadata includes:
# MAGIC - The timestamp when Databricks processed the record into Bronze.
# MAGIC - The ingestion date extracted from the source folder path.
# MAGIC
# MAGIC The ingestion date is extracted from paths like `ingest_date=YYYY-MM-DD`.

# COMMAND ----------

# DBTITLE 1,Creating bronze DataFrame
df_bronze = (
    df_source
    .withColumn("timestamp", F.col("timestamp").cast("string"))
    .withColumn("level", F.col("level").cast("long"))
    .withColumn("face_front", F.col("face_front").cast("long"))
    .withColumn("input_active", F.col("input_active").cast("long"))
    .withColumn("session_seconds", F.col("session_seconds").cast("long"))
    .withColumn("attentive_seconds", F.col("attentive_seconds").cast("long"))
    .withColumn("productive_seconds", F.col("productive_seconds").cast("long"))
    .withColumn("activity", F.col("activity").cast("string"))

    # Technical metadata
    .withColumn("_source_format", F.lit(source_format))
    .withColumn("_source_file", F.lit(source_file_path))
    .withColumn("_source_file_name", F.lit(source_file_name))
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_ingest_date", F.to_date(F.lit(p_ingest_date)))
)

display(df_bronze)

df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Run basic Bronze quality checks
# MAGIC
# MAGIC This cell runs basic checks before writing the Bronze Delta table.
# MAGIC
# MAGIC It counts the number of records by source format and source file name to validate that all expected files were loaded.

# COMMAND ----------

# DBTITLE 1,Run basic Bronze quality checks
print(df_bronze.printSchema())
print("--------------------")
print("Bronze rows:", df_bronze.count())

display(
    df_bronze
    .groupBy("_source_format")
    .count()
    .orderBy("_source_format")
)

display(
    df_bronze
    .groupBy("_source_file_name")
    .count()
    .orderBy("_source_file_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Write the Bronze Delta table
# MAGIC
# MAGIC This step writes the Bronze DataFrame as a Delta table in the Bronze layer.
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

# DBTITLE 1,Write the Bronze Delta table
(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_ingest_date = '{p_ingest_date}'")
    .save(bronze_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Validate final Bronze
# MAGIC
# MAGIC This cell validates the final Bronze table by checking total records and record counts by source file.
# MAGIC
# MAGIC This confirms that the Delta table contains the expected loaded data.

# COMMAND ----------

# DBTITLE 1,Validate final Bronze
df_check = spark.read.format("delta").load(bronze_path)

print(df_check.printSchema())
print("--------------------")
print("Df_Check_Rows:", df_check.count())

display(df_check)

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