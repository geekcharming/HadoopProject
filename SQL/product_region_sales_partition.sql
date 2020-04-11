--**************************************************
--CREATE Partitioned Parquet tables on SalesDB Data
--**************************************************


SET VAR:database_name=zeros_and_ones_salesdb;


--Create an Impala Parquet sales and region materialized table that is partitioned by region, year, and month.
--OrderID, SalesPerson ID, Customer ID, Product ID, Product Name, Product
--Price, Quantity, Total Sales Amount, Order Date, Region, Sales Year, Sales Month

--Static Partitioning
CREATE TABLE IF NOT EXISTS ${var:database_name}.product_region_sales_partition(
order_id int,
sales_person_id int,
customer_id int,
product_id int,
product_name varchar,
price double,
quantity int,
total_sales_amount double,
order_date timestamp)
PARTITIONED BY (sale_year int , sale_month int, region varchar)
COMMENT 'Parquet partitioned product sales table'
STORED AS Parquet;

insert into ${var:database_name}.product_region_sales_partition partition(sale_year,sale_month,region='east')
select s.order_id, s.sales_person_id, s.customer_id, s.product_id, s.name, s.price, s.quantity,s.total_sales_amount,
       s.order_date, s.sale_year,s.sale_month from ${var:database_name}.product_sales_partition s join
       ${var:database_name}.employees e on s.sales_person_id = e.employee_id where e.region='east';

insert into ${var:database_name}.product_region_sales_partition partition(sale_year,sale_month,region='west')
select s.order_id, s.sales_person_id, s.customer_id, s.product_id, s.name, s.price, s.quantity,s.total_sales_amount,
       s.order_date, s.sale_year,s.sale_month from ${var:database_name}.product_sales_partition s join
       ${var:database_name}.employees e on s.sales_person_id = e.employee_id where e.region='west';

insert into ${var:database_name}.product_region_sales_partition partition(sale_year,sale_month,region='north')
select s.order_id, s.sales_person_id, s.customer_id, s.product_id, s.name, s.price, s.quantity,s.total_sales_amount,
       s.order_date, s.sale_year,s.sale_month from ${var:database_name}.product_sales_partition s join
       ${var:database_name}.employees e on s.sales_person_id = e.employee_id where e.region='north';

insert into ${var:database_name}.product_region_sales_partition partition(sale_year,sale_month,region='south')
select s.order_id, s.sales_person_id, s.customer_id, s.product_id, s.name, s.price, s.quantity,s.total_sales_amount,
       s.order_date, s.sale_year,s.sale_month from ${var:database_name}.product_sales_partition s join
       ${var:database_name}.employees e on s.sales_person_id = e.employee_id where e.region='south';

invalidate metadata;
compute stats ${var:database_name}.product_region_sales_partition;



--Dynamic Partitioning : takes up more memory
--CREATE TABLE IF NOT EXISTS ${var:database_name}.product_region_sales_partition
--PARTITIONED BY (sale_year, sale_month, region)
--COMMENT 'Parquet partitioned product sales table'
--STORED AS Parquet
--AS
--SELECT s.order_id, s.sales_person_id, s.customer_id, s.product_id, s.name, s.price, s.quantity,
--       s.total_sales_amount, s.order_date, s.sale_year,s.sale_month, e.region as region
--       from ${var:database_name}.product_sales_partition s join
--       ${var:database_name}.employees_test e on s.sales_person_id = e.employee_id;
--



