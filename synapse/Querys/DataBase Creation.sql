CREATE DATABASE productivity_monitor_db;
GO

USE productivity_monitor_db;
GO

ALTER DATABASE CURRENT COLLATE Latin1_General_100_BIN2_UTF8;
GO

USE productivity_monitor_db;
GO

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '7895123Dd@7895123';
GO

CREATE DATABASE SCOPED CREDENTIAL SynapseWorkspaceIdentity
WITH IDENTITY = 'Managed Identity';
GO

CREATE EXTERNAL DATA SOURCE GoldLake
WITH (
    LOCATION = 'https://saproductivitymonitor.dfs.core.windows.net/productivity/gold/',
    CREDENTIAL = SynapseWorkspaceIdentity
);
GO