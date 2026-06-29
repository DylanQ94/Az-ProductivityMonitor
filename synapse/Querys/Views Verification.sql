/* ============================================================
   ALL VIEWS
   ============================================================ */
   
SELECT
    name AS vw_name
FROM sys.views
ORDER BY name
GO

SELECT TOP 10 *
FROM dbo.vw_fact_productivity_daily;
GO

SELECT TOP 10 *
FROM dbo.vw_fact_productivity_activity;
GO

SELECT 'vw_fact_productivity_interval' AS view_name, COUNT_BIG(*) AS total_rows
FROM dbo.vw_fact_productivity_interval
UNION ALL
SELECT 'vw_fact_productivity_monthly', COUNT_BIG(*)
FROM dbo.vw_fact_productivity_monthly
UNION ALL
SELECT 'vw_fact_productivity_daily', COUNT_BIG(*)
FROM dbo.vw_fact_productivity_daily
UNION ALL
SELECT 'vw_fact_productivity_hourly', COUNT_BIG(*)
FROM dbo.vw_fact_productivity_hourly
UNION ALL
SELECT 'vw_fact_productivity_activity', COUNT_BIG(*)
FROM dbo.vw_fact_productivity_activity
UNION ALL
SELECT 'vw_fact_source_file_audit', COUNT_BIG(*)
FROM dbo.vw_fact_source_file_audit
UNION ALL
SELECT 'vw_dim_date', COUNT_BIG(*)
FROM dbo.vw_dim_date
UNION ALL
SELECT 'vw_dim_month', COUNT_BIG(*)
FROM dbo.vw_dim_month
UNION ALL
SELECT 'vw_dim_hour', COUNT_BIG(*)
FROM dbo.vw_dim_hour
UNION ALL
SELECT 'vw_dim_activity_category', COUNT_BIG(*)
FROM dbo.vw_dim_activity_category
UNION ALL
SELECT 'vw_dim_source_file', COUNT_BIG(*)
FROM dbo.vw_dim_source_file;