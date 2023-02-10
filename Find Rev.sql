SELECT Day
, geo
, source_app_app_store_id
, ad_format
, extention_value
, sum(spend) AS Spend
, sum(revenue) AS Revenue
, sum(approx_bids/sample_rate) AS Estimated_bids
, sum(impressions) AS Impressions
, sum(clicks) AS Clicks
, sum(installs) AS Installs
FROM (
SELECT date_trunc('day', from_iso8601_timestamp(dt)) AS Day
, bid_request__device__geo__country AS geo
, bid_request__app__app_store_id AS source_app_app_store_id
, creative__ad_format AS ad_format
, json_extract(FROM_UTF8(bid_request__raw), '$.imp[0].ext.atc') AS extention_value
, 0 AS spend
, 0 AS revenue
, count(dt) AS approx_bids
, sample_rate
, 0 AS impressions
, 0 AS clicks
, 0 AS installs
FROM bids  
WHERE dt BETWEEN '2021-07-15T00' AND '2021-08-15T00'
AND bid_request__exchange = 'INMOBI'
GROUP BY 1,2,3,4,5,9
UNION ALL
SELECT date_trunc('day', from_iso8601_timestamp(dt)) AS Day
, bid__bid_request__device__geo__country AS geo
, bid__bid_request__app__app_store_id AS source_app_app_store_id
, bid__creative__ad_format AS ad_format
, json_extract(FROM_UTF8(bid__bid_request__raw), '$.imp[0].ext.atc') AS extention_value
, sum(spend_micros)/1000000.0 AS spend
, sum(revenue_micros)/1000000.0 AS revenue
, 0 AS approx_bids
, 1 AS sample_rate
, count(dt) AS impressions
, 0 AS clicks
, 0 AS installs
FROM impressions_with_bids  
WHERE dt BETWEEN '2021-07-15T00' AND '2021-08-15T00'
AND bid__bid_request__exchange = 'INMOBI'
GROUP BY 1,2,3,4,5
UNION ALL
SELECT date_trunc('day', from_iso8601_timestamp(dt)) AS Day
, impression__bid__bid_request__device__geo__country AS geo
, impression__bid__bid_request__app__app_store_id AS source_app_app_store_id
, impression__bid__creative__ad_format AS ad_format
, json_extract(FROM_UTF8(impression__bid__bid_request__raw), '$.imp[0].ext.atc') AS extention_value
, 0 AS spend
, 0 AS revenue
, 0 AS approx_bids
, 1 AS sample_rate
, 0 AS impressions
, count(dt) AS clicks
, 0 AS installs
FROM ad_clicks  
WHERE dt BETWEEN '2021-07-15T00' AND '2021-08-15T00'
AND impression__bid__bid_request__exchange = 'INMOBI'
GROUP BY 1,2,3,4,5
UNION ALL
SELECT date_trunc('day', from_iso8601_timestamp(dt)) AS Day
, ad_click__impression__bid__bid_request__device__geo__country AS geo
, ad_click__impression__bid__bid_request__app__app_store_id AS source_app_app_store_id
, ad_click__impression__bid__creative__ad_format AS ad_format
, json_extract(FROM_UTF8(ad_click__impression__bid__bid_request__raw), '$.imp[0].ext.atc') AS extention_value
, 0 AS spend
, 0 AS revenue
, 0 AS approx_bids
, 1 AS sample_rate
, 0 AS impressions
, 0 AS clicks
, count(dt) AS installs
FROM installs  
WHERE dt BETWEEN '2021-07-15T00' AND '2021-08-15T00'
AND ad_click__impression__bid__bid_request__exchange = 'INMOBI'
GROUP BY 1,2,3,4,5)
GROUP BY 1,2,3,4,5