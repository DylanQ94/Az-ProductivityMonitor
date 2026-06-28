# Gold Data Model

## Overview

The Gold layer is the analytical model of the Productivity Monitor Lakehouse on Azure project.

It was designed to support duration-based productivity analysis using fact and dimension tables stored as Delta Lake folders in ADLS Gen2.

The model prioritizes real time increments over raw record counts because the source files contain cumulative duration counters.

## Gold Layer Structure

```text
gold/
├── fact_productivity_interval/
├── fact_productivity_monthly/
├── fact_productivity_daily/
├── fact_productivity_hourly/
├── fact_productivity_activity/
├── fact_source_file_audit/
├── dim_date/
├── dim_month/
├── dim_hour/
├── dim_activity_category/
└── dim_source_file/
```

## Fact Tables

### fact_productivity_interval

This is the source of truth for downstream Gold aggregations.

It is built from Silver and calculates real row-level duration increments using window functions and `lag()`.

The source duration fields are cumulative counters:

```text
session_seconds
attentive_seconds
productive_seconds
```

Because direct summation would overestimate time, the interval fact compares each row against the previous row from the same source file.

Main content:

- Analytical keys.
- Natural date and category columns.
- Original cumulative counters.
- Real duration increments in seconds and hours.
- Interval-level ratios.
- Record-level flags.
- Data quality indicators.
- Source lineage columns.

### fact_productivity_daily

Daily aggregation of productivity duration metrics.

Main metrics:

```text
total_session_hours
total_attentive_hours
total_productive_hours
total_inactive_hours
total_active_hours
attentive_ratio
productive_ratio
inactive_ratio
active_ratio
```

### fact_productivity_monthly

Monthly aggregation of productivity duration metrics.

### fact_productivity_hourly

Hourly aggregation of productivity duration metrics.

### fact_productivity_activity

Activity-level productivity duration analysis.

This table supports analysis by activity category and productivity state.

### fact_source_file_audit

Source-file-level audit fact used to analyze file ingestion and processing lineage.

## Dimension Tables

### dim_date

Reusable date dimension.

Key:

```text
date_key
```

### dim_month

Reusable month dimension.

Key:

```text
month_key
```

### dim_hour

Reusable hour dimension.

Key:

```text
event_hour
```

### dim_activity_category

Reusable activity category dimension.

Key:

```text
activity_category_key
```

### dim_source_file

Reusable source file dimension.

Key:

```text
source_file_key
```

## Analytical Keys

The model uses the following keys to relate facts and dimensions:

```text
date_key
month_key
event_hour
activity_category_key
source_file_key
```

These keys allow the Gold facts to be used in a star-schema-like analytical model.

## Duration Logic

The final duration model is:

```text
session_duration = attentive_duration + productive_duration + inactive_duration
```

Inactive duration is calculated as:

```text
inactive_duration = session_duration - attentive_duration - productive_duration
```

The use of `fact_productivity_interval` as the source for aggregated fact tables ensures that all duration metrics are calculated consistently.

## Design Decisions

A productivity-state dimension and an activity-state record fact were considered during the design process.

They were removed from the final model because the project prioritizes duration-based productivity analysis over record-count-based state analysis.

## Serving Views

The Gold model is exposed through Synapse Serverless SQL views.

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
