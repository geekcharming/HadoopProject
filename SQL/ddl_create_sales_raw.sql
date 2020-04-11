--****************************************
--CREATE EXTERNAL TABLES on Raw Sales Data
--****************************************

SET VAR:database_name=zeros_and_ones_salesdb_raw;

Create Database IF NOT EXISTS ${var:database_name}
COMMENT 'Raw sales data imported from SalesDB';


--Create External Customers Table
CREATE EXTERNAL TABLE IF NOT EXISTS ${var:database_name}.customers (
customer_id int,
first_name varchar,
middle_initial varchar,
last_name varchar)
COMMENT 'customers table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/salesdb/Customers2/'
TBLPROPERTIES ("skip.header.line.count"="1");

--Create External Employees Table
CREATE EXTERNAL TABLE IF NOT EXISTS ${var:database_name}.employees (
employee_id int,
first_name varchar,
middle_initial varchar,
last_name varchar,
region varchar)
COMMENT 'employees table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/salesdb/Employees2/'
TBLPROPERTIES ("skip.header.line.count"="1");

--Create External Products Table
CREATE EXTERNAL TABLE IF NOT EXISTS ${var:database_name}.products (
product_id int,
name varchar,
price double)
COMMENT 'products table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/salesdb/Products/'
TBLPROPERTIES ("skip.header.line.count"="1");

--Create External Sales Table
CREATE EXTERNAL TABLE IF NOT EXISTS ${var:database_name}.sales (
order_id int,
sales_person_id int,
customer_id int,
product_id int,
quantity int,
date_ timestamp)
COMMENT 'sales table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/salesdb/Sales2/'
TBLPROPERTIES ("skip.header.line.count"="1");

invalidate metadata;
compute stats ${var:database_name}.customers;
compute stats ${var:database_name}.employees;
compute stats ${var:database_name}.products;
compute stats ${var:database_name}.sales;
