# Azure Synapse Analytics Serving Layer

## Overview

Azure Synapse Analytics Serverless SQL was implemented as the serving layer for the Productivity Monitor Lakehouse on Azure project.

The purpose of this phase was to expose the curated Gold layer stored in Azure Data Lake Storage Gen2 through SQL views, without copying or materializing the data into a dedicated SQL warehouse.

Synapse Serverless SQL provides a lightweight query layer over the Gold Delta Lake folders generated in the previous processing stage.

## Role in the Architecture

Synapse Serverless SQL sits between the Gold layer in ADLS Gen2 and the consumption layer.

Implemented flow:

```text
ADLS Gen2 Gold Delta folders
        ↓
Azure Synapse Analytics Serverless SQL
        ↓
SQL views
        ↓
Reporting / consumption layer
```

The data remains stored in ADLS Gen2. Synapse uses external SQL views over Delta Lake folders, allowing analytical tools to query curated Gold data using SQL without physically loading the data into Synapse-managed tables.

## Synapse Resources

Synapse workspace:

```text
syn-productivitymonitor-dev
```

Serverless SQL database:

```text
productivity_monitor_db
```

External data source:

```text
GoldLake
```

Database scoped credential:

```text
SynapseWorkspaceIdentity
```

Credential identity:

```sql
WITH IDENTITY = 'Managed Identity'
```

Serverless SQL endpoint:

```text
syn-productivitymonitor-dev-ondemand.sql.azuresynapse.net
```

## Gold Layer Source

The Gold layer is stored in Azure Data Lake Storage Gen2 using the following location:

```text
Storage Account: saproductivitymonitor
Container: productivity
Base path: abfss://productivity@saproductivitymonitor.dfs.core.windows.net/gold/
```

Synapse Serverless SQL queries the Gold layer directly using `OPENROWSET` with Delta Lake format:

```sql
FORMAT = 'DELTA'
```

## SQL Views

The serving layer was designed to expose the final Gold fact and dimension tables as SQL views.

Fact views:

```text
vw_fact_productivity_interval
vw_fact_productivity_monthly
vw_fact_productivity_daily
vw_fact_productivity_hourly
vw_fact_productivity_activity
vw_fact_source_file_audit
```

Dimension views:

```text
vw_dim_date
vw_dim_month
vw_dim_hour
vw_dim_activity_category
vw_dim_source_file
```

Each view follows the same pattern: it reads from a corresponding Gold Delta folder using the `GoldLake` external data source.

Example:

```sql
CREATE VIEW dbo.vw_fact_productivity_daily
AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_productivity_daily/',
    DATA_SOURCE = 'GoldLake',
    FORMAT = 'DELTA'
) AS rows;
```

Because the external data source already points to the `/gold/` folder, each `BULK` path uses the relative folder name, such as:

```text
fact_productivity_daily/
```

## Technical Decisions

Serverless SQL was selected instead of a Dedicated SQL Pool because the project only required a SQL serving layer over files already stored in ADLS Gen2.

This avoids provisioning a dedicated warehouse and keeps the architecture simpler and more cost-efficient for this portfolio project.

SQL views were used instead of external tables or CETAS outputs because the goal was to expose Gold data for consumption without creating additional persisted serving copies.

No external tables, CETAS outputs, dedicated SQL pools, or materialized Synapse tables were created in this phase.

## Issues Resolved

During implementation, Synapse initially failed to list the Delta transaction log folder (`_delta_log`).

The Delta folders were confirmed to exist in ADLS Gen2, and the Synapse workspace Managed Identity already had the required storage role assignment.

The root cause was an incorrect external data source URL that still contained placeholder characters.

The location was corrected from a placeholder-based path to the real ADLS Gen2 URL:

```text
https://saproductivitymonitor.dfs.core.windows.net/productivity/gold/
```

After correcting the path, the external data source was properly associated with the `SynapseWorkspaceIdentity` Managed Identity.

Another issue occurred when connecting from Power BI using Windows authentication. The correct authentication method for the Synapse Serverless SQL endpoint was Microsoft account authentication.

## Final Status

The Synapse Analytics serving layer was configured successfully as a SQL access layer over the Gold Delta Lake folders in ADLS Gen2.

Completed items:

- Synapse Serverless SQL database configured.
- Managed Identity-based access configured.
- External data source created for the Gold layer.
- SQL views designed over Gold fact and dimension folders.
- Serverless endpoint identified for downstream consumption.
- Power BI connection validated through the Synapse Serverless SQL endpoint.

## Pending Confirmation

- Final execution validation of all fact and dimension views.
- Final row count validation for all exposed Gold views.
- Optional future improvement: define explicit SQL columns and data types instead of using `SELECT *` in the views.
