USE productivity_monitor_db;
GO

/* ============================================================
   DROP EXISTING GOLD VIEWS
   ============================================================ */

DROP VIEW IF EXISTS dbo.vw_fact_productivity_interval;
DROP VIEW IF EXISTS dbo.vw_fact_productivity_monthly;
DROP VIEW IF EXISTS dbo.vw_fact_productivity_daily;
DROP VIEW IF EXISTS dbo.vw_fact_productivity_hourly;
DROP VIEW IF EXISTS dbo.vw_fact_productivity_activity;
DROP VIEW IF EXISTS dbo.vw_fact_source_file_audit;

/* ============================================================
   FACT VIEWS
   ============================================================ */

CREATE VIEW dbo.vw_fact_productivity_interval
AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_productivity_interval/',
    DATA_SOURCE = 'GoldLake',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE VIEW dbo.vw_fact_productivity_monthly
AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_productivity_monthly/',
    DATA_SOURCE = 'GoldLake',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE VIEW dbo.vw_fact_productivity_daily
AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_productivity_daily/',
    DATA_SOURCE = 'GoldLake',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE VIEW dbo.vw_fact_productivity_hourly
AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_productivity_hourly/',
    DATA_SOURCE = 'GoldLake',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE VIEW dbo.vw_fact_productivity_activity
AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_productivity_activity/',
    DATA_SOURCE = 'GoldLake',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE VIEW dbo.vw_fact_source_file_audit
AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_source_file_audit/',
    DATA_SOURCE = 'GoldLake',
    FORMAT = 'DELTA'
) AS rows;
GO