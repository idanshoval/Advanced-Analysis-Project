-- Analysis of Sales Changes Over Time by Year and Month
select format(DATETRUNC(MONTH,order_date),'yyyy-MMMM') as Date_, sum(sales_amount) Total_Sales, count(distinct customer_key ) as Total_Customers
, sum(quantity) as Total_quantity
from gold.fact_sales
where order_date is NOT NULL
group by DATETRUNC(MONTH,order_date)
order by DATETRUNC(MONTH,order_date)

-- Calculating the total sales per month and the running total of sales over time (years)

select year(order_date) as Year_ ,month(order_date) as Month_, sum(sales_amount) as total_Sales,sum(sum(sales_amount)) over(partition by year(order_date) order by year(order_date), Month(order_date)) as running_total_sales
from gold.fact_sales
where order_date is not null
group by year(order_date),month(order_date)
 go

-- Analyzing Product Performance by Comparing Each Product's Sales to Average Sales and Previous Year's Sales
with cte_result AS ( 

select year(f.order_date) as year_,p.product_name,sum(f.sales_amount) as Sales_per_year_by_product
from gold.fact_sales f left join gold.dim_products p on f.product_key=p.product_key
where f.order_date is not null
group by year(f.order_date),p.product_name
)

select c.year_,c.product_name,c.Sales_per_year_by_product,avg(c.Sales_per_year_by_product) over(partition by c.product_name) as avg_by_product,c.Sales_per_year_by_product- avg(c.Sales_per_year_by_product) over(partition by c.product_name) as diff_avg,case 
 when c.Sales_per_year_by_product- avg(c.Sales_per_year_by_product) over(partition by c.product_name)>0 then 'Above Avg'
 else 'Below Avg'
 end as status_,case when  c.Sales_per_year_by_product-Lag(c.Sales_per_year_by_product) over( partition by product_name order by  year_)>0 then 'Sales Growth'
 when  c.Sales_per_year_by_product-Lag(c.Sales_per_year_by_product) over( partition by product_name order by  year_) is null then 'Previous years do not exist'
 else 'Sales Decline'
 end as 'Year-over-Year Sales Change'
from cte_result c

--Which categories contribute the most to the overall sales.

select t.category	,t.total_sales_by_category, sum(total_sales_by_category) over() as overall_sales,   concat(round( cast(t.total_sales_by_category as float)/sum(total_sales_by_category) over()*100,2),'%' as Sales_Percentage_by_category
from (
select p.category,sum(sales_amount)  as total_sales_by_category
from gold.fact_sales f left join gold.dim_products p on f.product_key=p.product_key
group by p.category ) t


-- segment products into costs range and count the number of products in each segment

with cte_num_pro_to_segment as 
(
select  p.product_key,p.product_name,p.cost, 
case when 
p.cost<100 then 'Below 100'

when p.cost between 100 and 500  then 'between 100-500' 
when 
p.cost between 500 and 1000 then 'between 500-1000'
else 'Above 1000'
end as cost_range
from gold.dim_products p
)

select cost_range, count(*) as num_products
from cte_num_pro_to_segment c 
group by cost_range


--  Group customers into 3 segments based on their spending behavior  :
-- VIP ( customers with at least 12 months of history and spending more then 5000 $ )
-- REGULAR ( customers with at least 12 months of history and spending less then 5000 $)
-- New ( customers with  less then  12 months)
-- and find the total number of customers by each group

WITH CTE_COUNT_CUSTOMERS AS 
(

select t.customer_key,t.Total_sales,t.life_span,case when t.Total_sales>5000 and life_span>=12 then 'VIP'
when t.Total_sales <=5000 AND life_span>12 THEN 'REGULAR'
ELSE 'NEW'
END AS CUSTOMER_STATUS
from (
SELECT  c.customer_key, sum(f.sales_amount) as Total_sales,DATEDIFF(month,min(order_date),max(order_date) ) as life_span
FROM gold.fact_sales f left join gold.dim_customers c on f.customer_key=c.customer_key
group by c.customer_key ) AS t

)
 SELECT C.CUSTOMER_STATUS, count(*) as customers_number
 FROM CTE_COUNT_CUSTOMERS C
 GROUP BY C.CUSTOMER_STATUS

 -- Creating Reports
/*

1.Gather essetential fields such as name, ages,transaction details.
2.Segment customers into categories(VIP,REGULAR,NEW) and age groups.
3.Aggregate customer- level metric:
- total orders
- total sales
- total quantity purchased
- total products
- lifespan (in month)
4.Caclulate valuable KPIs:
- month since last order
- average order value
- average monthly spend
*/

go
--step 1
-- base table
Create view gold.report_customers as
with cte_base_table as (
select f.order_number,f.product_key, f.order_date,f.sales_amount,f.quantity,c.customer_key,c.customer_number,CONCAT(c.first_name,' ',c.last_name) AS customer_name,datediff(YEAR,c.birthdate,GETDATE()) as age
from gold.fact_sales f left join gold.dim_customers c on f.product_key=c.customer_key   
where order_date is not null
)	,	

-- step 2
-- customer aggregations
 customer_agg_cte as (
select customer_key,customer_number,customer_name,
 age, count(DISTINCT order_number) AS total_orders,sum(sales_amount) as total_sales,sum(quantity) as total_quantity, count(product_key) as total_produts, max(order_date) as last_order_date, DATEDIFF(month,min(order_date),max(order_date) ) as life_span
from cte_base_table
group by customer_key,customer_name,customer_number,age )

-- step 3
-- final result

select  customer_key,customer_name,customer_number,age,case when age<20 then 'Under 20' 
when age between 20 and 29 then '20-29' 
when age between 30 and 39 then '30-39'
when age between 40 and 49 then '40-49'
else 'Above 50' 
end  as age_group
, total_orders, total_sales, total_quantity, total_produts, last_order_date, life_span , case when total_sales>5000 and life_span>=12 then 'VIP'
when total_sales <=5000 AND life_span>12 THEN 'REGULAR'
ELSE 'NEW'
--Computing how much time has passed since the last order
end as status_customer, DATEDIFF(month,last_order_date,GETDATE()) as recency, 
-- computing sales average per order 
nullif(total_sales/total_orders,0) as sales_avg__per_order,
-- computing average monthly spend
case when life_span=0 then total_sales 
else total_sales/ life_span
end as  avg_monthly_spand
from customer_agg_cte
 
 go


 -- checking the report
 select *
 from gold.report_customers