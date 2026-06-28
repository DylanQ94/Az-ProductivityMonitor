# Databricks Artifacts

This folder is reserved for Databricks notebooks used in the processing layer.

Expected subfolder:

```text
databricks/
└── notebooks/
```

The actual exported notebooks were not provided in the current documentation input.

## Confirmed Processing Flow

```text
Landing
    ↓
Bronze
    ↓
Silver
    ↓
Gold
```

## Confirmed Outputs

```text
bronze/productivity_events/
silver/productivity_events/
gold/fact_productivity_interval/
gold/fact_productivity_monthly/
gold/fact_productivity_daily/
gold/fact_productivity_hourly/
gold/fact_productivity_activity/
gold/fact_source_file_audit/
gold/dim_date/
gold/dim_month/
gold/dim_hour/
gold/dim_activity_category/
gold/dim_source_file/
```
