# Results and Conclusions

## Final Result

The Productivity Monitor Lakehouse on Azure project currently implements an end-to-end data engineering flow from local file ingestion to a SQL serving layer.

The final result is a curated Gold Delta Lake analytical model exposed through Azure Synapse Analytics Serverless SQL views.

## Completed Results

### Azure Data Factory

- Local productivity files are ingested from `C:\ProductivityMonitor\Reports`.
- Self-hosted Integration Runtime is used to access the local file system.
- Only `.csv` and `.json` files are processed.
- Files are copied as-is into the ADLS Gen2 Landing zone.
- No business transformations are performed in ADF.

### Azure Databricks

- Bronze Delta layer was created with technical source metadata.
- Silver Delta layer was created with cleaned, typed, and standardized records.
- Gold analytical layer was created with fact and dimension tables.
- Cumulative counters were converted into real interval-level duration increments.
- Aggregated fact tables were built from `fact_productivity_interval`.

### Azure Synapse Analytics Serverless SQL

- Serverless SQL database was configured.
- Managed Identity-based access was configured.
- External data source `GoldLake` was created for the Gold layer.
- SQL views were designed over Gold fact and dimension folders.
- The Serverless SQL endpoint was identified for downstream consumption.
- Power BI connection was validated using Microsoft account authentication.

## Key Technical Learning

The most important technical decision in the project was the creation of an interval-level fact table.

The source files contain cumulative duration counters, so directly summing those fields would overestimate time.

To solve this, the Gold layer calculates real duration increments using window functions and `lag()`. All aggregated facts are then built from this validated interval logic.

## What Was Intentionally Not Created

The following components were not created in the confirmed scope:

- Dedicated SQL Pool.
- Synapse external tables.
- CETAS outputs.
- Materialized Synapse serving tables.
- Final Power BI dashboard.
- DAX measures.

## Pending Items

The following items remain pending confirmation:

- Final execution validation of all Synapse fact and dimension views.
- Final row count validation for all exposed Gold views.
- Final Landing path naming convention between `source=desktop_app` and `source-desktop_app`.
- Optional improvement: define explicit SQL columns and data types instead of using `SELECT *` in Synapse views.
- Final Power BI semantic model and dashboard design.

## Conclusion

The project successfully demonstrates a realistic Azure data engineering architecture for local productivity monitoring data.

It separates ingestion, processing, storage, modeling, and serving responsibilities across Azure services, while keeping the implementation focused and avoiding unnecessary persisted serving copies.
