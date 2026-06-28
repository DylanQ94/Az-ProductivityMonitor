# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 00_orchestrate_transformations
# MAGIC
# MAGIC This notebook orchestrates the full Databricks transformation flow for the Productivity Monitor Lakehouse project.
# MAGIC
# MAGIC The notebook is designed to be executed from Azure Data Factory after the ingestion pipeline finishes successfully.
# MAGIC
# MAGIC Azure Data Factory passes the ingestion date to this notebook using the parameter:
# MAGIC
# MAGIC - `p_ingest_date`
# MAGIC
# MAGIC This notebook receives that parameter and forwards it to each transformation notebook using `dbutils.notebook.run()`.
# MAGIC
# MAGIC The orchestration flow runs the Medallion transformation notebooks in the correct order:
# MAGIC
# MAGIC 1. `01_landing_to_bronze`
# MAGIC 2. `02_bronze_to_silver`
# MAGIC 3. `03_silver_to_gold_fact_interval`
# MAGIC 4. `04_gold_dimensions`
# MAGIC 5. `05_gold_aggregate_facts`
# MAGIC
# MAGIC Input:
# MAGIC - `p_ingest_date`, received from Azure Data Factory.
# MAGIC - Raw files already ingested into the ADLS Gen2 Landing layer.
# MAGIC
# MAGIC Outputs:
# MAGIC - Bronze Delta table.
# MAGIC - Silver Delta table.
# MAGIC - Gold interval fact table.
# MAGIC - Gold dimension tables.
# MAGIC - Gold aggregate fact tables.
# MAGIC
# MAGIC Important execution rule:
# MAGIC Each notebook depends on the previous one. If one notebook fails, the orchestration stops and the error is returned to Azure Data Factory.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Receive and validate the ingestion date parameter
# MAGIC
# MAGIC This step receives the `p_ingest_date` parameter from Azure Data Factory.
# MAGIC
# MAGIC The expected format is: `yyyy-MM-dd`
# MAGIC
# MAGIC Example: `2026-06-26`
# MAGIC
# MAGIC This parameter controls which ingestion date will be processed by the transformation flow.

# COMMAND ----------

# DBTITLE 1,Receive and validate the ingestion date parameter
from datetime import datetime, timedelta

dbutils.widgets.text("p_file_date", "")

p_source_file_date = dbutils.widgets.get("p_file_date").strip()

if p_source_file_date == "":
    raise ValueError("Parameter p_source_file_date is required. Expected format: yyyy-MM-dd")

file_date_obj = datetime.strptime(p_source_file_date, "%Y-%m-%d")

p_ingest_date = (file_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

print(f"Landing ingest date: {p_ingest_date}")
print(f"Expected source file date: {p_source_file_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Define notebook execution parameters
# MAGIC
# MAGIC This step creates the argument dictionary that will be passed to each child notebook.
# MAGIC
# MAGIC Azure Data Factory only passes the parameter directly to this orchestrator notebook. Because of that, this notebook must explicitly forward the same parameter to each transformation notebook.

# COMMAND ----------

# DBTITLE 1,Define notebook execution parameters
notebook_args = {
    "p_ingest_date": p_ingest_date,
    "p_source_file_date": p_source_file_date
}

print("Notebook arguments:")
print(notebook_args)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Define the transformation notebooks
# MAGIC
# MAGIC This step defines the Databricks notebooks that will be executed by the orchestrator.
# MAGIC
# MAGIC The notebooks are executed in the same order as the Medallion transformation flow.
# MAGIC
# MAGIC This version assumes that all notebooks are located in the same Databricks workspace folder as this orchestrator notebook.

# COMMAND ----------

# DBTITLE 1,Define the transformation notebooks
transformation_notebooks = [
    {
        "step": 1,
        "name": "Landing to Bronze",
        "path": "./01_landing_to_bronze"
    },
    {
        "step": 2,
        "name": "Bronze to Silver",
        "path": "./02_bronze_to_silver"
    },
    {
        "step": 3,
        "name": "Silver to Gold Interval Fact",
        "path": "./03_silver_to_gold_fact_interval"
    },
    {
        "step": 4,
        "name": "Gold Dimensions",
        "path": "./04_gold_dimensions"
    },
    {
        "step": 5,
        "name": "Gold Aggregate Facts",
        "path": "./05_gold_aggregate_facts"
    }
]

display(spark.createDataFrame(transformation_notebooks))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Run the transformation notebooks
# MAGIC
# MAGIC This step executes each transformation notebook in sequence.
# MAGIC
# MAGIC If one notebook fails, the orchestration stops immediately and the error is returned to the caller.

# COMMAND ----------

# DBTITLE 1,Run the transformation notebooks
execution_results = []

for notebook in transformation_notebooks:
    step = notebook["step"]
    name = notebook["name"]
    path = notebook["path"]

    try:
        result = dbutils.notebook.run(
            path,
            timeout_seconds=0,
            arguments=notebook_args
        )

        execution_results.append({
            "step": step,
            "name": name,
            "path": path,
            "status": "SUCCESS",
            "result": result
        })

        print(f"Completed Step {step}: {name}")
        print(f"Result: {result}")

    except Exception as e:
        execution_results.append({
            "step": step,
            "name": name,
            "path": path,
            "status": "FAILED",
            "result": str(e)
        })

        print(f"Failed Step {step}: {name}")
        print(f"Error: {str(e)}")

        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Display orchestration results
# MAGIC
# MAGIC This step displays the result of each notebook execution.
# MAGIC
# MAGIC If this step is reached, all transformation notebooks finished successfully.

# COMMAND ----------

# DBTITLE 1,Display orchestration results
df_execution_results = spark.createDataFrame(execution_results)

display(df_execution_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Finish orchestration
# MAGIC
# MAGIC This step returns a success message to Azure Data Factory.
# MAGIC
# MAGIC The returned value can be used by ADF for monitoring or downstream control flow.

# COMMAND ----------

# DBTITLE 1,Finish orchestration
success_message = f"Productivity Monitor transformation flow completed successfully for ingest date {p_ingest_date}"

print(success_message)

dbutils.notebook.exit(success_message)

# COMMAND ----------

# MAGIC %md
# MAGIC