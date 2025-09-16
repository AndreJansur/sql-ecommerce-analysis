/*
==========================================================
 SQL Project: E-commerce Data Analysis
 Автор: [AndreJansur]
 Опис: повний пайплайн аналізу e-commerce даних 
       — від дослідження сирих таблиць до бізнес-метрик.

 Структура (оглавлення):
 01. Data exploration         — базове дослідження даних (розмір, пропуски, аномалії)
 02. Data cleaning            — створення retail_cleaned (фільтрація помилок, повернень)
 03. Monthly sales analysis   — тренди місячної виручки та кількості замовлень
 04. Customer summary         — профіль клієнтів (замовлення, дохід, середній чек)
 05. Top products             — топ-10 товарів за виручкою та кількістю
 06. Daily revenue by country — денна виручка по країнах + кумулятивний тренд
 07. Customer segmentation    — Small / Medium / Large за витратами
 08. RFM analysis             — Recency / Frequency / Monetary + сегменти
 09. Frequency analysis       — середній інтервал покупок (Frequent / Occasional / Rare)
 10. Cohort retention         — когортний аналіз (утримання по місяцях)
 11. Retention pivot          — півот-таблиця retention
 12. Pareto & top products    — внесок топ-20% клієнтів, топ-5 товарів, сегменти

 Примітка:
 - Виконувати блоки по черзі, починаючи з 01 → 12.
 - Усі запити працюють у PostgreSQL.
==========================================================
*/




--01. Data exploration — initial data exploration
--Task: understand the scope of the table, the presence of gaps and anomalies.

--How many rows (transaction items/product lines) are there in the raw dataset?
SELECT COUNT(*) FROM retail_raw;

--Counts missing values in key fields. FILTER applies a condition to each aggregation without affecting others. This quickly identifies “broken” columns.
SELECT
  COUNT(*) FILTER (WHERE InvoiceNo IS NULL) AS null_invoiceno,
  COUNT(*) FILTER (WHERE StockCode IS NULL) AS null_stockcode,
  COUNT(*) FILTER (WHERE Description IS NULL) AS null_description,
  COUNT(*) FILTER (WHERE Quantity IS NULL) AS null_quantity,
  COUNT(*) FILTER (WHERE InvoiceDate IS NULL) AS null_invoicedate,
  COUNT(*) FILTER (WHERE UnitPrice IS NULL) AS null_unitprice,
  COUNT(*) FILTER (WHERE CustomerID IS NULL) AS null_customerid,
  COUNT(*) FILTER (WHERE Country IS NULL) AS null_country
FROM retail_raw;

--How many “cancelled”/returned invoices (cancellations are marked with invoices beginning with C).
SELECT 
count(*)
FROM retail_raw
WHERE InvoiceNo LIKE 'C%';

--We catch negative quantities/prices (returns/errors).
SELECT * 
FROM retail_raw
WHERE Quantity < 0 OR UnitPrice < 0;

--We estimate the size of the customer base and its geography.
SELECT COUNT(DISTINCT CustomerID) AS unique_customers,
       COUNT(DISTINCT Country) AS unique_countries
FROM retail_raw;





--02 Data cleaning
--Task: compile a “working” table for analytics.
create table retail_cleaned as
select
    InvoiceNo
   ,StockCode
   ,Description
   ,Quantity
   ,InvoiceDate
   ,UnitPrice
   ,CustomerID
   ,Country
   ,Quantity * UnitPrice as LineTotal
from
    retail_raw
where
    InvoiceNo not like 'C%'                -- eliminating canceled orders
    and Quantity > 0                       -- eliminating returns and errors
    and UnitPrice > 0                      -- exclude test or error values
    and CustomerID is not null
    and customerid <> '';                  -- registered customers only
    
    
    
    
    
--03 Monthly sales analysis
--Task: basic sales trend.
select
    date_trunc('month', invoicedate) as month
    ,count(distinct invoiceno) as total_orders
    ,sum(linetotal) as total_revenue
from retail_cleaned
group by month
order by month;




--04 Customer summary
--Task: compile a customer profile.
--Meaning: provides a basic table “customer → orders/revenue/average check/assortment.”
select
customerid
	,COUNT(DISTINCT InvoiceNo) as uniq_orders 							     -- how many orders
	,SUM(linetotal) as sum_linetotal  										 -- total revenue from the customer
	,ROUND(SUM(LineTotal) / COUNT(DISTINCT InvoiceNo), 2) AS avg_order_value -- average bill
	,COUNT(DISTINCT StockCode) as uniq_products  							 -- variety of purchases
from retail_cleaned
group by customerid



--05 Top 10_products by revenue
--Task: find revenue drivers.
--Meaning: which products generate the majority of revenue and are sold in large volumes.
select
stockcode 
,description 
	,sum(linetotal) as total_revenue
	,sum(quantity) as total_quantity
from retail_cleaned 
where TRIM(description) <> ''       -- discarded empty descriptions       
group by stockcode, description 
order by total_revenue desc
limit 10




--06 Calculated daily revenue per country with window functions to identify top days, average daily revenue, and cumulative revenue growth.
WITH sum_invoice AS (
  SELECT
    SUM(LineTotal) AS LineTotals  -- order amount
,InvoiceNo
,country
,InvoiceDate
  FROM retail_cleaned 
  GROUP BY InvoiceNo, country, InvoiceDate
),
day_rev AS (
  SELECT
	date_trunc('day', InvoiceDate) AS invoice_day
	,country
	,SUM(LineTotals) AS day_revenue   -- daily revenue by country
  FROM sum_invoice
  GROUP BY invoice_day, country
)
SELECT
	RANK() OVER(ORDER BY day_revenue DESC) AS rank_name  -- top days by revenue
	,day_revenue
	,round(AVG(day_revenue) OVER (PARTITION BY country),2) AS avg_day_revenue  -- average daily across the country
	,SUM(day_revenue) OVER (PARTITION BY country ORDER BY invoice_day) AS cum_revenue  -- cumulatively
	,invoice_day
	,country
FROM day_rev;




--07 Customer segmentation by total spending: classify clients into Small/Medium/Large groups and calculate count, revenue, and average check per segment
--Meaning: to understand the contribution of segments to revenue and their volume. This is the basis for “who to offer what to.”
WITH customer_sales AS (
SELECT 
CustomerID
        ,SUM(LineTotal) AS total_spent
FROM retail_cleaned
GROUP BY CustomerID
),


segmentation AS (
SELECT 
CustomerID
,total_spent
        ,CASE  
            WHEN total_spent < 500 THEN 'small'
            WHEN total_spent >= 500 AND total_spent < 2000 THEN 'medium'
            WHEN total_spent >= 2000 THEN 'large'
            ELSE 'unknown'
        END AS segment
FROM customer_sales
)
SELECT 
segment
    ,COUNT(CustomerID) AS count_customers
    ,SUM(total_spent) AS total_sales
    ,ROUND(AVG(total_spent), 2) AS avg_sales
FROM segmentation
GROUP BY segment
ORDER BY total_sales DESC;

-- 08 RFM Analysis:
-- Recency = days since last purchase
-- Frequency = number of unique invoices
-- Monetary = total revenue per customer
-- Customers are segmented into High / Medium / Low Value groups
WITH max_date AS (
SELECT 
max(InvoiceDate) AS dataset_max_date
FROM retail_cleaned
),

customer_features AS (
SELECT
CustomerID
	,max(InvoiceDate) AS last_purchase            -- R 
    ,count(DISTINCT InvoiceNo) AS count_invoice   -- F
    ,sum(LineTotal) AS sum_linetotal              -- M
FROM retail_cleaned
GROUP BY CustomerID
)

select
c.CustomerID
,m.dataset_max_date - c.last_purchase as diff_data_purchase -- Recency as an interval
,c.count_invoice
,c.sum_linetotal
,CASE 
      WHEN segment = 1 THEN 'High Value'
      WHEN segment = 2 THEN 'Medium Value'
      WHEN segment = 3 THEN 'Low Value'
  END AS value_segment
from (
select
CustomerID
,last_purchase
,count_invoice
,sum_linetotal
,ntile(3) over(order by sum_linetotal desc ) as segment     -- 3rd quantile for spending
from customer_features
where customerid <> ''
) as c
cross join max_date as m





-- 09 Performed frequency analysis: calculated average purchase intervals per customer 
-- and segmented them into Frequent, Occasional, and Rare buyers.
with lag_date as (
select
customerid
,lag(invoicedate) over(partition by customerid order by invoicedate) as PreviousPurchaseDate
,InvoiceDate - lag(InvoiceDate) OVER(PARTITION BY CustomerID ORDER BY InvoiceDate) AS DaysBetweenPurchases
from retail_cleaned
where customerid <> ''
	and invoicedate is not null
),

avg_diff as (
select 
customerid
,AVG(EXTRACT(DAY FROM DaysBetweenPurchases)) AS avg_days_between
from lag_date
group by customerid
)

select 
customerid
,case
	WHEN avg_days_between < 30 THEN 'Frequent'
    WHEN avg_days_between BETWEEN 30 AND 90 THEN 'Occasional'
    ELSE 'Rare'
end as segment
from avg_diff





--10 Сohort retention
--Pipeline:
--1. Find the month of the first purchase (cohort_month) for each customer.
--2. Take all purchases and their months.
--3. Calculate the offset in months (month_offset).
--4. Calculate how many customers are active in each month_offset.
--5. Divide by the base (month_offset = 0) → retention %.
with first_purchase as (
select
customerid
    ,min(invoicedate) as first_purchase_date
    ,date_trunc('month', min(invoicedate)) as cohort_month
from retail_cleaned
where customerid <> ''
group by customerid
),

purchases as (
select
customerid
	,invoicedate
	,date_trunc('month',invoicedate) as purchase_month
from retail_cleaned
where customerid <> ''
),

cohort_analysis as (
select
f.customerid
,f.first_purchase_date
,f.cohort_month
,p.purchase_month
from first_purchase as f
join purchases as p
on f.customerid = p.customerid
),

with_offset as (
select 
customerid
,cohort_month
,purchase_month
,(DATE_PART('year', purchase_month) - DATE_PART('year', cohort_month)) * 12
        + (DATE_PART('month', purchase_month) - DATE_PART('month', cohort_month)) as month_offset
from cohort_analysis
),

cohort_counts as (
select
cohort_month
,month_offset
        ,count(distinct customerid) as customers_count
from with_offset
group by cohort_month, month_offset
),

base_counts as (
select
cohort_month
,customers_count as base_count
from cohort_counts
where month_offset = 0
)

select
	c.cohort_month
	,c.month_offset
	,c.customers_count
	,b.base_count
    ,round(c.customers_count * 100.0 / b.base_count, 2) as retention_rate
from cohort_counts c
join base_counts b
    on c.cohort_month = b.cohort_month
order by c.cohort_month, c.month_offset;




--11 Pivot retention table
with first_purchase as (
select
customerid
        ,min(invoicedate) as first_purchase_date
        ,date_trunc('month', min(invoicedate)) as cohort_month
from retail_cleaned
where customerid <> ''
group by customerid
),


purchases as (
select
customerid
,invoicedate
,date_trunc('month', invoicedate) as purchase_month
from retail_cleaned
where customerid <> ''
),


cohort_analysis as (
select
f.customerid
,f.cohort_month
,p.purchase_month
from first_purchase f
    join purchases p
        on f.customerid = p.customerid
),


with_offset as (
select 
customerid
,cohort_month
,purchase_month
        ,(date_part('year', purchase_month) - date_part('year', cohort_month)) * 12
            + (date_part('month', purchase_month) - date_part('month', cohort_month)) as month_offset
    from cohort_analysis
),


cohort_counts as (
select
cohort_month
,month_offset
        ,count(distinct customerid) as customers_count
from with_offset
group by cohort_month, month_offset
),


base_counts as (
    select
        cohort_month,
        customers_count as base_count
    from cohort_counts
    where month_offset = 0
),
retention as (
select
        c.cohort_month
        ,c.month_offset
        ,round(c.customers_count * 100.0 / b.base_count, 2) as retention_rate
from cohort_counts c
    join base_counts b
        on c.cohort_month = b.cohort_month
)
select
cohort_month
    ,max(case when month_offset = 0 then retention_rate end) as m0
    ,max(case when month_offset = 1 then retention_rate end) as m1
    ,max(case when month_offset = 2 then retention_rate end) as m2
    ,max(case when month_offset = 3 then retention_rate end) as m3
    ,max(case when month_offset = 4 then retention_rate end) as m4
from retention
group by cohort_month
order by cohort_month;




--12. Pareto: share of top 20% of customers
with customer_revenue as (
    select customerid, sum(linetotal) as revenue
    from retail_cleaned
    group by customerid
),
ordered as (
    select customerid, revenue,
           row_number() over(order by revenue desc) as rn,
           sum(revenue) over(order by revenue desc 
                             rows between unbounded preceding and current row) as cum_revenue,
           sum(revenue) over() as total_revenue
    from customer_revenue
)
select 
    customerid,
    revenue,
    cum_revenue,
    cum_revenue * 1.0 / total_revenue * 100 as cum_share
from ordered
order by revenue desc;

-- 1. Share of top 5 products in total revenue
with product_revenue as (
select 
stockcode
,description 
	,sum(linetotal) as revenue
from retail_cleaned
group by stockcode, description
),

top5 as (
select 
	sum(revenue) as top5_revenue
from product_revenue
order by revenue desc
limit 5
),


total as (
select 
	sum(revenue) as total_revenue
from product_revenue
)


select 
	round(top5_revenue * 100.0 / total_revenue, 2) as top5_share
from top5, total;


-- 2. Revenue segments (Small/Medium/Large) – contribution to total revenue
with customer_sales as (
select 
customerid 
	,sum(linetotal) as total_spent
from retail_cleaned
group by customerid
),


segmentation as (
select
customerid
        ,case  
            when total_spent < 500 then 'small'
            when total_spent >= 500 and total_spent < 2000 then 'medium'
            when total_spent >= 2000 then 'large'
        end as segment
,total_spent
from customer_sales
)


select
segment
    ,round(sum(total_spent) * 100.0 / (select sum(total_spent) from segmentation), 2) as revenue_share
from segmentation
group by segment
order by revenue_share desc;




