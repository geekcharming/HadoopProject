--**************************************************
--CREATE Partitioned Parquet tables on SalesDB Data
--**************************************************


SET VAR:database_name=zeros_and_ones_salesdb;


--Create an Impala Parquet product and sales materialized table that is partitioned by year and month.
--OrderID, SalesPerson ID, Customer ID, Product ID, Product Name, Product
--Price, Quantity, Total Sales Amount, Order Date, Sales Year, Sales Month
CREATE TABLE IF NOT EXISTS ${var:database_name}.product_sales_partition
PARTITIONED BY (sale_year,sale_month)
COMMENT 'Parquet partitioned product sales table'
STORED AS Parquet
AS
SELECT s.order_id, s.sales_person_id, s.customer_id, s.product_id, p.name, p.price, s.quantity,
       (s.quantity*p.price) as total_sales_amount, s.date_ as order_date, date_part('year', s.date_) as sale_year,
       date_part('month',s.date_) as sale_month from ${var:database_name}.sales s join
       ${var:database_name}.products p on s.product_id = p.product_id;

invalidate metadata;
compute stats ${var:database_name}.product_sales_partition;

