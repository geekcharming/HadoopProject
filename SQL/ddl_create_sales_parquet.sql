--*************************************
--CREATE Parquet TABLES on Sales Data
--*************************************


SET VAR:database_name=zeros_and_ones_salesdb;

SET VAR:source_database=zeros_and_ones_salesdb_raw;

Create Database IF NOT EXISTS ${var:database_name}
COMMENT 'Parquet sales data imported from salesdb database';


--Create Parquet Products Table
CREATE TABLE IF NOT EXISTS ${var:database_name}.products
COMMENT 'Parquet products table'
STORED AS Parquet
AS
SELECT * from ${var:source_database}.products;


--Create Parquet Sales Table
CREATE TABLE IF NOT EXISTS ${var:database_name}.sales
COMMENT 'Parquet sales table'
STORED AS Parquet
AS
SELECT * from ${var:source_database}.sales;


--Create Parquet Customers Table
CREATE TABLE IF NOT EXISTS ${var:database_name}.customers
COMMENT 'Parquet customers table'
STORED AS Parquet
AS
SELECT customer_id, first_name, last_name from ${var:source_database}.customers;


--Create Parquet Employees Table
CREATE TABLE IF NOT EXISTS ${var:database_name}.employees
COMMENT 'Parquet employees table'
STORED AS Parquet
AS
SELECT employee_id, first_name, last_name, lcase(region) as region from ${var:source_database}.employees;

invalidate metadata;
compute stats ${var:database_name}.products;
compute stats ${var:database_name}.sales;
compute stats ${var:database_name}.customers;
compute stats ${var:database_name}.employees;