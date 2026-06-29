USE productivity_monitor_db;
GO

/* ============================================================
   DROP EXISTING GOLD VIEWS IF EXISTS
   ============================================================ */

DROP VIEW IF EXISTS dbo.vw_dim_date;
DROP VIEW IF EXISTS dbo.vw_dim_month;
DROP VIEW IF EXISTS dbo.vw_dim_hour;
DROP VIEW IF EXISTS dbo.vw_dim_activity_category;
DROP VIEW IF EXISTS dbo.vw_dim_source_file;
GO

/* ============================================================
   DIMENSION VIEWS
   ============================================================ */

CREATE VIEW dbo.vw_dim_date
AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_date/',
    DATA_SOURCE = 'GoldLake',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE VIEW dbo.vw_dim_month
AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_month/',
    DATA_SOURCE = 'GoldLake',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE VIEW dbo.vw_dim_hour
AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_hour/',
    DATA_SOURCE = 'GoldLake',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE VIEW dbo.vw_dim_activity_category
AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_activity_category/',
    DATA_SOURCE = 'GoldLake',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE VIEW dbo.vw_dim_source_file
AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_source_file/',
    DATA_SOURCE = 'GoldLake',
    FORMAT = 'DELTA'
) AS rows;
GO