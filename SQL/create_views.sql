--*************************************
--CREATE Views on SalesDB Data
--*************************************


SET VAR:database_name=zeros_and_ones_salesdb;

--View: customer_monthly_sales_2019_view
--Customer id, customer last name, customer first name,
--year, month, aggregate total amount
--of all products purchased by month for 2019.

CREATE VIEW IF NOT EXISTS ${var:database_name}.customer_monthly_sales_2019_view as
Select c.customer_id,c.last_name, c.first_name, max(date_part('year',s.date_)) as year,
date_part('month',s.date_) as month, sum(s.quantity*p.price) as aggregate_total_amount
From ${var:database_name}.sales s join ${var:database_name}.products p on s.product_id = p.product_id
join ${var:database_name}.customers c on c.customer_id = s.customer_id
where date_part('year', s.date_)=2019
group by date_part('month',s.date_), c.customer_id, c.first_name, c.last_name;


--View: top_ten_customers_amount_view
--Customer id, customer last name, customer first name, total lifetime purchased amount
--This view should only return the top ten customers sorted by total dollar amount in sales from highest to lowest.

CREATE VIEW IF NOT EXISTS ${var:database_name}.top_ten_customers_amount_view as
Select c.customer_id,c.last_name, c.first_name, sum(s.quantity*p.price) as total_lifetime_purchased_amount
From ${var:database_name}.sales s
join ${var:database_name}.products p on s.product_id = p.product_id
join ${var:database_name}.customers c on c.customer_id = s.customer_id
group by c.customer_id, c.first_name, c.last_name
order by sum(s.quantity*p.price) desc
limit 10;