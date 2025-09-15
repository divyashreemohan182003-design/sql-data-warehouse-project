/*
============================================================
Create Database and Schemas
=============================================================
Script Purpose:
       This script creates a new database named 'DataWarehouse' after checking if it already exists.
       If the database exists,it is dropped and recreated. Additionally, the script sets up three schemas
       within the database: 'bronze', 'silver', and 'gold'.

WARNING:
       Running this script will drop the entire 'DataWarehouse' database if it exists
       All data in the database will be permanently deleted. Proceed with caution
       and ensure you have proper backups before running this script.
*/
use master;
go
--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.database WHERE name = 'DataWarehouse')
BEGIN 
   ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
   DROP DATABASE datawarehouse;
end;
GO
--create the 'Datawarehouse'database
create database datawarehouse;
go

use datawarehouse;
go

create schema bronze;
go

create schema silver;
go

create schema gold;
go























DROP DATABASE DataWarehouse;

END; GO

Create the 'DataWarehouse database CREATE DATABASE DataWarehouse; GO I

USE DataWarehouse; GO

Create Schemas CREATE SCHEMA bronze; GO

CREATE SCHEMA silver; GO

mtrol Shift to toggle the tab key moving focus. Alternatively, use esc then tab to move to the next interac
