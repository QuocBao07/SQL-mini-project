-- Big project for SQL
-- Using Bigquery base on Google Analytics dataset.
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0
-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
select     
format_date("%Y%m",PARSE_DATE("%Y%m%d", date)) as month,
  	sum(totals.visits) as visits,
       sum(totals.pageviews) as pageviews,
       sum(totals.transactions) as transactions,
       sum(totals.totalTransactionRevenue)/1000000 as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where _table_suffix between '20170101' and '20170331'
group by month
order by month;



-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
select
    trafficSource.source as source,
    SUM(totals.visits) as total_visits,
    SUM(totals.bounces) as total_no_of_bounces,
    SUM(totals.bounces) *100/ SUM(totals.visits) as bounce_rate,
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by source
order by total_visits desc;


-- Query 3: Revenue by traffic source by week, by month in June 2017
select  
      "Month" as time_type,
     format_date("%Y%m",PARSE_DATE("%Y%m%d",date)) as Month,
            trafficSource.source as source, 
            sum(totals.totalTransactionRevenue)/1000000 as revenue
    from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
    group by 1,2,3
union all
    select  
        "Week" as time_type,    
        format_date("%Y%W", PARSE_DATE("%Y%m%d",date)) as time,
        trafficSource.source as source, 
        sum(totals.totalTransactionRevenue)/1000000 as revenue
    from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
    group by 1,2,3
    order by revenue DESC;



--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with purchase as(
SELECT month,
      (SUM(total_pagesviews_per_userID)/COUNT(usersID)) AS avg_pageviews_purchase
FROM
(SELECT 
    FORMAT_DATE ("%Y%m",PARSE_DATE("%Y%m%d", date)) AS month,
    fullVisitorId AS usersID,
    SUM(totals.pageviews) AS total_pagesviews_per_userID
    FROM`bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170731'
    AND totals.transactions >=1
    GROUP BY month, users)
GROUP BY month),

non_purchase as(
 SELECT month,
      (SUM(total_pagesviews_per_userID)/COUNT(usersID)) AS avg_pageviews_non_purchase
 FROM
 (SELECT
     FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d", date)) AS month,
     fullVisitorId AS usersID,
     SUM(totals.pageviews) AS total_pagesviews_per_userID
     FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
     WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170731'
     AND totals.transactions is null
     GROUP BY month, users)
GROUP BY month)

SELECT month, avg_pageviews_purchase, avg_pageviews_non_purchase
FROM purchase 
FULL JOIN non_purchase 
USING (month);



-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
select 
    Month,
    total_transactions/count_userID as Avg_total_transactions_per_user
from (
    select
        format_date('%Y%m', PARSE_DATE('%Y%m%d', date)) as Month,
        count(distinct (case when totals.transactions >= 1 then fullVisitorId
            end)) as count_userID,
        sum(case when totals.transactions >= 1 then totals.transactions
            end) as total_transactions
    from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    group by 1
)
group by Month, total_transactions, count_userID;


-- Query 06: Average amount of money spent per session
#standardSQL
SELECT
 	FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d", date)) AS Month,
(SUM(totals.totaltransactionRevenue)/COUNT(fullVisitorId)) AS
avg_revenue_by_user_per_visit
FROM  
   	`bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions IS NOT NULL
AND totals.totaltransactionRevenue IS NOT NULL
GROUP BY Month;


-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL

select 
    product.v2ProductName AS other_purchased_products,
    sum(product.productQuantity) as quantity
from 
    `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
where fullVisitorId in (   
    select fullVisitorId
    from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
        unnest(hits) as hits,
        unnest(hits.product) as product
    where product.v2ProductName = "YouTube Men's Vintage Henley"
        and product.productRevenue is not null
    group by fullVisitorId
    )
AND  product.v2ProductName!= "YouTube Men's Vintage Henley"
AND product.productRevenue is not null
group by other_purchased_products
order by quantity desc;



--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
WITH product_view AS(
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d", date)) AS Month,
        COUNT(hits.eCommerceAction.action_type) AS num_product_view
	    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST (hits) AS hits,
        UNNEST (hits.product) AS product
        WHERE _table_suffix between '20170101' and '20170331'
        AND  hits.eCommerceAction.action_type = '2'
        GROUP BY Month
        Order by Month),
     addtocart AS(
          SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d", date)) AS Month,
        COUNT(hits.eCommerceAction.action_type) AS num_addtocart
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST (hits) AS hits,
        UNNEST (hits.product) AS product
        WHERE _table_suffix between '20170101' and '20170331'
        AND  hits.eCommerceAction.action_type = '3'
        GROUP BY Month
        Order by Month),
     purchase AS(
        SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d", date)) AS Month,
        COUNT(hits.eCommerceAction.action_type) AS num_purchase
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST (hits) AS hits,
        UNNEST (hits.product) AS product
        WHERE _table_suffix between '20170101' and '20170331'
        AND  hits.eCommerceAction.action_type = '6'
        GROUP BY Month
        Order by Month)
    SELECT *,
          round(num_addtocart/num_product_view*100,2) AS add_to_cart_rate,
          round(num_purchase/num_product_view*100,2)AS purchase_rate
    FROM product_view
    JOIN addtocart
    USING(Month)
    JOIN purchase 
    USING(Month)
    ORDER BY Month;
