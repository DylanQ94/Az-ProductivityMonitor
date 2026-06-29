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

## Key Technical Learning

The most important technical decision in the project was the creation of an interval-level fact table.

The source files contain cumulative duration counters, so directly summing those fields would overestimate time.

To solve this, the Gold layer calculates real duration increments using window functions and `lag()`. All aggregated facts are then built from this validated interval logic.

## Conclusion

The project successfully demonstrates a realistic Azure data engineering architecture for local productivity monitoring data that a allow to make conclusions with their views, for example:

## Analysis by Day of the Week

| Day       | Active Days | Session h | Productive h | Productive Ratio | Inactive Ratio |
| --------- | ----------: | --------: | -----------: | ---------------: | -------------: |
| Monday    |           7 |     37.02 |        26.61 |            71.9% |          17.8% |
| Tuesday   |           7 |     43.98 |        32.61 |            74.1% |          16.2% |
| Wednesday |           7 |     50.54 |        34.61 |            68.5% |          18.7% |
| Thursday  |           6 |     21.15 |        13.92 |            65.8% |          15.0% |
| Friday    |           6 |     41.76 |        30.98 |            74.2% |          13.6% |
| Saturday  |           5 |     19.02 |        12.35 |            64.9% |          17.6% |
| Sunday    |           6 |     18.24 |        13.50 |            74.0% |          16.3% |

**Key insight:**

**Friday and Tuesday** show the highest productivity ratios, both close to **74%**.
**Wednesday** concentrates the highest total session hours, but it also shows one of the highest inactive ratios.

