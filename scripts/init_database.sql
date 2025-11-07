/*
  -------------------------------
  Create database and schemas
  -------------------------------
  Script purpose:
  This script creates a new database named 'Datawarehouse' after checking if it already exists.
  If the database exists, it is droped and recreated. Additionally, the script sets up three schemas
  with the database: 'bronze', 'silver' and 'gold'

Warning
  Running this script will drop the entire Datawarehouse database if it exists.
  All data in the database will be permanently deleted.
  Proceed with caution and ensure you have proper backups before running this script.
*/
USE master;
GO
-- Drop and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
  ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK, IMMEDIATE;
  DROP DATABASE Datawarehouse;
END;
GO

-- Create the 'Datawharehouse' database
CREATE DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO

-- Create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
