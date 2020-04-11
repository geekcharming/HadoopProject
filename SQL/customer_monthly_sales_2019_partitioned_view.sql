--*********************************************************
--CREATE Views from Partitioned Tables on SalesDB Data
--*********************************************************


SET VAR:database_name=zeros_and_ones_salesdb;

--View: customer_monthly_sales_2019_partitioned_view
--Customer id, customer last name, customer first name,
--year, month, aggregate total amount
--of all products purchased by month for 2019.

CREATE VIEW IF NOT EXISTS ${var:database_name}.customer_monthly_sales_2019_partitioned_view as
Select c.customer_id,c.last_name, c.first_name, max(s.sale_year), s.sale_month, sum(s.quantity*s.price)
From ${var:database_name}.product_sales_partition s
join ${var:database_name}.customers c on c.customer_id = s.customer_id
where sale_year=2019
group by sale_month, c.customer_id, c.first_name, c.last_name;