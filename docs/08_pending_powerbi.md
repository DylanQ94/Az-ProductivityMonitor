# Pending Power BI

## Current Status

Power BI is not part of the final confirmed implementation scope documented in this repository.

The only confirmed Power BI-related item is that the connection to the Synapse Serverless SQL endpoint was validated using Microsoft account authentication.

Confirmed endpoint:

```text
syn-productivitymonitor-dev-ondemand.sql.azuresynapse.net
```

Confirmed database:

```text
productivity_monitor_db
```

## Confirmed Connection Note

An issue occurred when attempting to connect from Power BI using Windows authentication.

The correct authentication method for the Synapse Serverless SQL endpoint was Microsoft account authentication.

## Not Yet Implemented

The following Power BI items are pending and should not be documented as completed:

- Final semantic model.
- Final star schema relationships in Power BI.
- DAX measures.
- Dashboard pages.
- Published report.
- Power BI workspace deployment.

## Recommended Future Scope

A future Power BI phase could consume the Synapse Serverless SQL views and build a reporting model over the Gold facts and dimensions.

Potential consumption views:

```text
vw_fact_productivity_interval
vw_fact_productivity_monthly
vw_fact_productivity_daily
vw_fact_productivity_hourly
vw_fact_productivity_activity
vw_fact_source_file_audit
vw_dim_date
vw_dim_month
vw_dim_hour
vw_dim_activity_category
vw_dim_source_file
```
