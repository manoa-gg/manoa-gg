-- CLEANING

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

create table `supermarket_sales_2` (
	`invoice id` text,
	`branch` text,
	`city` text,
	`customer type` text, 
    `gender` text, 
    `product line` text, 
    `unit price` double DEFAULT NULL,
	`quantity` int default null,
    `tax 5%` double default null,
    `total` double default null,
    `date` text default null,
    `time` text default null,
    `payment` text,
    `cogs` double default null,
    `gross margin percentage` double default null,
	`gross income` double DEFAULT NULL,
    `rating` double default null
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    
  insert into supermarket_sales_2
  select *
  from supermarket_sales_1;
  
  select *
  from supermarket_sales_2
  where `unit price` is null
  or `quantity` is null
  or `tax 5%` is null
  or `total` is null
  or `date` is null
  or `time` is null
  or `cogs` is null
  or `gross margin percentage` is null
  or `gross income` is null 
  or `rating` is null;

select *
from supermarket_sales_2;

select `product line`, trim(`product line`)
from supermarket_sales_2;

select distinct `city`
from supermarket_sales_2
order by 1;

select distinct `product line`
from supermarket_sales_2
order by 1;

select *
from supermarket_sales_2;

select `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') AS converted_date
from supermarket_sales_2;

update supermarket_sales_2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table supermarket_sales_2
MODIFY `date` DATE;

select *
from supermarket_sales_2;

alter table supermarket_sales_2
modify `time` time;

SELECT `time`,
SUBSTRING(`time`, 1, 5) 
FROM supermarket_sales_2;

update supermarket_sales_2
set `time` = SUBSTRING(`time`, 1, 5);

select *
from supermarket_sales_2;

-- Exploratory

select city, sum(total), `product line`
from supermarket_sales_2
group by city, `product line`
order by sum(total) desc;

select city, avg(total), `product line`
from supermarket_sales_2
group by city, `product line`
order by sum(total) desc;

select city, `product line`, sum(quantity), sum(`gross income`), avg(rating)
from supermarket_sales_2
group by city, `product line`
order by sum(quantity) desc;

select `branch`, `product line`, avg(`unit price`)
from supermarket_sales_2
group by `branch`, `product line`
order by avg(`unit price`) desc;

select `customer type`, count(`customer type`)
from supermarket_sales_2
group by `customer type`;

select *
from supermarket_sales_2; 

select `product line`, count(`product line`)
from supermarket_sales_2
group by `product line`
order by count(`product line`) desc;

select `gender`, count(gender)
from supermarket_sales_2
group by `gender`
order by count(gender) desc; 

select `product line`, avg(`tax 5%`)
from supermarket_sales_2
group by `product line`
order by avg(`tax 5%`) desc;

select `city`, `customer type`, `gender`, `payment`, avg(quantity), avg(`gross income`), avg(rating)
from supermarket_sales_2
group by `city`, `customer type`, `gender`, `payment`
order by avg(quantity) desc;

select `invoice id`, (`invoice id`)
from supermarket_sales_2
group by `invoice id`
order by count(`invoice id`) desc;

select `date`, sum(quantity), sum(`gross income`)
from supermarket_sales_2
group by `date`
order by sum(`gross income`) desc;

select day(`date`), sum(quantity) AS SQ
from supermarket_sales_2
where year(`date`) = 2019
group by dayofmonth(`date`)
order by SQ desc;

select day(`date`), avg(quantity) AS SQ, sum(`gross income`) AS TI
from supermarket_sales_2
where year(`date`) = 2019
group by dayofmonth(`date`)
order by TI desc;

select *
from supermarket_sales_2;

select day(`date`), avg(quantity) AS SQ, sum(`gross income`) AS TI
from supermarket_sales_2
where year(`date`) = 2019
group by dayofmonth(`date`)
order by TI desc;

select day(`date`), count(payment)
from supermarket_sales_2
where year(`date`) = 2019
	and payment = 'credit card'
group by dayofmonth(`date`)
order by count(payment) desc;

WITH RS AS (
    SELECT
        payment,
        day(`date`) AS `day`,
        count(payment) AS CP,
        ROW_NUMBER() OVER (PARTITION BY payment ORDER BY count(payment) DESC) AS RS
    FROM supermarket_sales_2
    WHERE YEAR(date) = 2019
    GROUP BY payment, day(`date`)
)
SELECT
    payment,
    `day`,
    CP
FROM RS
WHERE RS <= 3
ORDER BY payment, RS;

select *
from supermarket_sales_2;

select month(`date`), `product line`, min(quantity), min(`gross income`)
from supermarket_sales_2
group by month(`date`), `product line`
order by month(`date`);

select month(`date`), `product line`, max(quantity), max(`gross income`)
from supermarket_sales_2
group by month(`date`), `product line`
order by month(`date`);

WITH MonthlyTotals AS (
    SELECT
        DATE_FORMAT(`date`, '%Y-%m') AS `month`,
        SUM(`quantity`) AS monthly_total
    FROM supermarket_sales_2
    GROUP BY DATE_FORMAT(`date`, '%Y-%m')
)
SELECT
    `month`,
    monthly_total,
    SUM(monthly_total) OVER (ORDER BY `month`) AS rolling_sum
FROM MonthlyTotals
ORDER BY `month`; 

select max(`date`)
from supermarket_sales_2;






-----------------------------------------------------------------------------------------------------------------------------
WITH Rankedpay AS (
    SELECT
        payment,
        day(`date`) AS `day`,
        count(payment) AS CP,
        ROW_NUMBER() OVER (PARTITION BY payment ORDER BY count(payment) DESC) AS RN
    FROM supermarket_sales_2
    WHERE YEAR(date) = 2019
    GROUP BY payment, day(`date`)
),
DayCounts AS (
    SELECT
        payment,
        `day`,
        COUNT(payment) AS CP_per_day
    FROM supermarket_sales_2
    WHERE YEAR(`date`) = 2019
    GROUP BY payment, `day`
),
AverageQuantity AS (
	select 
		payment,
        avg(CP_per_day) AS ACP
	from DayCounts
    group by payment
)
SELECT
    RP.payment,
    RP.`day`,
    RP.CP,
    AQ.ACP
FROM Rankedpay RP
Join AverageQuantity AQ ON RP.payment = AQ.payment
WHERE RP.RN <= 3
ORDER BY RP.payment, RP.RN;

WITH Rankedpay AS (
    SELECT
        payment,
        DAY(`date`) AS `day`,
        COUNT(*) AS CP,
        ROW_NUMBER() OVER (PARTITION BY payment, DAY(`date`) ORDER BY COUNT(*) DESC) AS RN
    FROM supermarket_sales_2
    WHERE YEAR(`date`) = 2019
    GROUP BY payment, DAY(`date`)
),
DayCounts AS (
    SELECT
        payment,
        DAY(`date`) AS `day`,
        COUNT(*) AS CP_per_day
    FROM supermarket_sales_2
    WHERE YEAR(`date`) = 2019
    GROUP BY payment, DAY(`date`)
),
AverageQuantity AS (
    SELECT
        payment,
        AVG(CP_per_day) AS ACP
    FROM DayCounts
    GROUP BY payment
)
SELECT
    RP.payment,
    RP.`day`,
    RP.CP,
    AQ.ACP
FROM Rankedpay RP
JOIN AverageQuantity AQ ON RP.payment = AQ.payment
WHERE RP.RN <= 3
ORDER BY RP.payment, RP.RN;


select day(`date`), payment, count(payment)
from supermarket_sales_2
where day(`date`) = 9
group by day(`date`), payment;
