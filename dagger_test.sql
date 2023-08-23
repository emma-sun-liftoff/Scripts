-- I was using this query to pull results (~4 mins)
WITH a AS (
SELECT
COALESCE(install__ad_click__impression__auction_id,reeng_click__impression__auction_id) AS auction_id
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(COALESCE(install__ad_click__impression__at, reeng_click__impression__at)/1000, 'UTC'))),1,19),'Z') AS impression_at
, COALESCE(install__ad_click__impression__bid__campaign_id,reeng_click__impression__bid__campaign_id) AS campaign_id
, "group".id AS ab_test_group_id
, sum(CASE WHEN at - COALESCE(install__ad_click__impression__at,reeng_click__impression__at) < 604800000 
			AND for_reporting 
			AND customer_revenue_micros > -100000000000 
			AND customer_revenue_micros < 100000000000 
			AND customer_revenue_micros != 0
	THEN customer_revenue_micros ELSE 0 END) AS customer_revenue_micros_d7

FROM rtb.app_events ae
CROSS JOIN UNNEST(COALESCE(
	          install__ad_click__impression__bid__bid_request__ab_test_assignments,
	          reeng_click__impression__bid__bid_request__ab_test_assignments)) t

WHERE
dt >= '2023-08-05T00'
AND is_uncredited <> TRUE
AND at - COALESCE(install__ad_click__impression__at, reeng_click__impression__at) < 604800000
AND t.id = 1100
AND COALESCE(install__ad_click__impression__bid__ad_group_type,reeng_click__impression__bid__ad_group_type) = 'user-acquisition'
AND COALESCE(install__ad_click__impression__bid__app_platform, reeng_click__impression__bid__app_platform) = 'IOS'
AND COALESCE(install__ad_click__impression__bid__exploratory,reeng_click__impression__bid__exploratory) = FALSE
GROUP BY 1,2,3,4
) 
, funnel AS (
select 
	impression_at
	, ab_test_group_id 
	, campaign_id
	, sum(least(customer_revenue_micros_d7, 500000000)) as capped_cust_rev_d7 
from a 
group by 1,2,3)
, dagger AS (
SELECT ab_test_group_id
, impression_at
, campaign_id
, sum(capped_cust_rev_d7) AS capped_cust_rev_d7
FROM funnel 
GROUP BY 1,2,3
)
SELECT ab_test_group_id
, sum(capped_cust_rev_d7)/1e6
FROM dagger 
WHERE impression_at >= '2023-08-05T00'
AND impression_at <'2023-08-13T00'
--AND campaign_id = 27890
GROUP BY 1

-- I was using this query to pull results from dagger table (~2 secs)
SELECT 
ab_test_group_id
, sum(capped_cust_rev_d7)/1e6 AS capp_cus_rev
FROM product_analytics.dagger_test
WHERE 
dt >= '2023-08-05T00' 
AND impression_at >='2023-08-05T00'
AND impression_at < '2023-08-13T00'
--AND campaign_id IN (27890)
GROUP BY 1
ORDER BY 1

-- dagger pipeline: https://dagger.liftoff.io/pipelines/1624
with a as (
SELECT
COALESCE(install__ad_click__impression__auction_id,reeng_click__impression__auction_id) AS auction_id
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(COALESCE(install__ad_click__impression__at, reeng_click__impression__at)/1000, 'UTC'))),1,19),'Z') AS impression_at
--, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
, COALESCE(install__ad_click__impression__bid__campaign_id,reeng_click__impression__bid__campaign_id) AS campaign_id
, "group".id AS ab_test_group_id
, sum(CASE WHEN at - COALESCE(install__ad_click__impression__at,reeng_click__impression__at) < 604800000 
AND for_reporting 
AND customer_revenue_micros > -100000000000 
AND customer_revenue_micros < 100000000000 
AND customer_revenue_micros != 0
	then customer_revenue_micros else 0 end) as customer_revenue_micros_d7

FROM rtb.app_events ae
CROSS JOIN UNNEST(COALESCE(
	          install__ad_click__impression__bid__bid_request__ab_test_assignments,
	          reeng_click__impression__bid__bid_request__ab_test_assignments)) t

WHERE
dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours = 1) }}'
AND is_uncredited <> TRUE
AND at - COALESCE(install__ad_click__impression__at, reeng_click__impression__at) < 604800000
AND t.id = 1100
AND COALESCE(install__ad_click__impression__bid__ad_group_type,reeng_click__impression__bid__ad_group_type) = 'user-acquisition'
AND COALESCE(install__ad_click__impression__bid__app_platform, reeng_click__impression__bid__app_platform) = 'IOS'
AND COALESCE(install__ad_click__impression__bid__exploratory,reeng_click__impression__bid__exploratory) = FALSE
--AND COALESCE(install__ad_click__impression__bid__app_id,reeng_click__impression__bid__app_id) = 1226
--AND COALESCE(install__ad_click__impression__bid__campaign_id,reeng_click__impression__bid__campaign_id) = 20186
GROUP BY 1,2,3,4
) 
, funnel AS (
select 
	ab_test_group_id 
	, impression_at
	, campaign_id
	, sum(least(customer_revenue_micros_d7, 500000000)) as capped_cust_rev_d7 
from a 
group by 1,2,3
)

SELECT 
ab_test_group_id 
, impression_at
, campaign_id
, sum(capped_cust_rev_d7) as capped_cust_rev_d7 
FROM funnel 
GROUP BY 1,2,3
