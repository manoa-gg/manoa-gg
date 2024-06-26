select *
from supermarket_sales;

create table supermarket_sales_1
like supermarket_sales;

insert supermarket_sales_1
select *
from supermarket_sales;

select *
from supermarket_sales_1 AS ss1;

with duplicate_cte AS 
(
select *,
row_number () over (
partition by `invoice id`, branch, `customer type`, gender, `product line`,
`time`, payment) AS row_num
from supermarket_sales_1
) 
select *
from duplicate_cte
where row_num > 1;


