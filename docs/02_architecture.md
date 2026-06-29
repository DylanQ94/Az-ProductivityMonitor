# Architecture

## Architecture Overview

The Productivity Monitor Lakehouse on Azure project follows a layered data engineering architecture.

The solution separates ingestion, storage, processing, modeling, and serving responsibilities across different Azure services.

```text
Local Productivity Monitor files
        ↓
Self-hosted Integration Runtime
        ↓
Azure Data Factory
        ↓
ADLS Gen2 Landing Zone
        ↓
Azure Databricks
        ↓
Bronze Delta Layer
        ↓
Silver Delta Layer
        ↓
Gold Analytical Delta Layer
        ↓
Azure Synapse Analytics Serverless SQL
        ↓
SQL Views
```

## Architecture Diagram


## Layer Responsibilities

### Source Layer

The source layer is the local folder where the Productivity Monitor desktop application writes daily CSV or JSON files.

Confirmed source folder:

```text
C:\ProductivityMonitor\Reports
```

### Ingestion Layer

Azure Data Factory is responsible for moving raw files from the local machine into ADLS Gen2.

Because the files are located in a local folder, the pipeline uses a Self-hosted Integration Runtime.

ADF does not parse, clean, cast, or transform business data. It only copies supported raw files as-is.

Implemented pipeline:

```text
PL_Ingest_ProductivityMonitor_DailyFile
```

Pipeline flow:

```text
Get Metadata
    ↓
Filter
    ↓
ForEach
    ↓
Copy Data
```

### Storage Layer

Azure Data Lake Storage Gen2 stores the project data across lakehouse layers.

Confirmed storage account:

```text
saproductivitymonitor
```

Confirmed container:

```text
productivity
```

Confirmed or documented paths:

```text
landing/source=desktop_app/ingest_date=YYYY-MM-DD/
bronze/productivity_events/
silver/productivity_events/
gold/
```

Pending confirmation:

```text
The ADF summary uses source=desktop_app.
The Databricks summary mentions source-desktop_app.
The final Landing naming convention should be confirmed.
```

### Processing Layer

Azure Databricks is responsible for implementing the Medallion architecture.

Processing flow:

```text
ADLS Gen2 Landing
        ↓
Bronze Delta Layer
        ↓
Silver Delta Layer
        ↓
Gold Analytical Layer
```

Databricks reads the raw files, creates Bronze and Silver Delta datasets, calculates real interval-level duration metrics, and builds Gold fact and dimension tables.

### Serving Layer

Azure Synapse Analytics Serverless SQL exposes the Gold layer through external SQL views.

Synapse does not copy or materialize the data into a dedicated SQL warehouse. The data remains stored in ADLS Gen2 as Delta Lake folders.

Implemented Synapse flow:

```text
ADLS Gen2 Gold Delta folders
        ↓
Azure Synapse Analytics Serverless SQL
        ↓
SQL views
```

## Why This Architecture Was Used

This architecture was selected because it provides a clear separation of concerns:

- ADF handles orchestration and ingestion.
- ADLS Gen2 stores raw and curated data.
- Databricks performs scalable data processing with PySpark and Delta Lake.
- Synapse Serverless SQL exposes curated files using SQL without provisioning a dedicated warehouse.

For a portfolio project, this architecture also helps demonstrate common Azure data engineering patterns, including local ingestion, Medallion layers, Delta Lake processing, and SQL-based serving over the lake.