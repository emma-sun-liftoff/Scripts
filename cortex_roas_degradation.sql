-- count total distinct user_id
WITH test_info AS (
 SELECT
  1154 AS ab_test_id,
  2542 AS control_group,
  2543 AS test_group,
  'IOS' AS ab_test_platform,
  '2023-09-09 00:00:00' AS dt_start_ts,
  '2023-09-10 00:00:00' AS dt_end_ts, 
  '2023-09-09T00' AS dt_start,
  '2023-09-10T00' AS dt_end
)

, test_time AS (
 SELECT 
  date_add('hour', -3, CAST(dt_start_ts AS TIMESTAMP)) AS dt_start_3h,
  date_add('hour', 3, CAST(dt_end_ts AS TIMESTAMP)) AS dt_end_3h,
  date_add('day', 7, CAST(dt_end_ts AS TIMESTAMP)) AS dt_end_7d,
  date_add('day', 30, CAST(dt_end_ts AS TIMESTAMP)) AS dt_end_30d,
  to_unixtime(from_iso8601_timestamp(dt_start)) * 1000 AS start_ms,
  to_unixtime(from_iso8601_timestamp(dt_end)) * 1000 AS end_ms
 FROM test_info
)

-- imp
SELECT
--CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(at/1000, 'UTC'))),1,19),'Z') AS impression_at
"group".id AS ab_test_group_id
--, bid__price_data__model_type AS bid_type
--, cc.current_optimization_state AS  current_optimization_state
, count(DISTINCT COALESCE(bid__bid_request__device__platform_specific_id_sha1
		 , bid__bid_request__device__idfv
		 , bid__bid_request__pods__app_specific_id__id
		 , bid__bid_request__device__model_data__name
		 , bid__bid_request__device__user_agent,'')) AS user_w_imp
, sum(1) AS impressions
, sum(CAST(revenue_micros AS double)/1000000) AS revenue
, sum(CAST(bid__auction_result__winner__price_cpm_micros AS double)/1000000) AS preshaded_cpm
, sum(CAST(bid__price_cpm_micros AS double)/1000000) AS shaded_cpm
, sum(CASE WHEN COALESCE(bid__bid_request__device__platform_specific_id_sha1,'') <> '' THEN 1
           WHEN COALESCE(bid__bid_request__device__idfv, '') <>'' THEN 1
           WHEN COALESCE(bid__bid_request__pods__app_specific_id__id,'') <> '' THEN 1
           WHEN COALESCE(bid__bid_request__device__model_data__name, bid__bid_request__device__user_agent,'') <> '' THEN 1
     ELSE NULL END) AS num_users
   
FROM rtb.impressions_with_bids a
CROSS JOIN UNNEST(bid__bid_request__ab_test_assignments) t
LEFT JOIN pinpoint.public.campaigns cc
  ON a.bid__campaign_id  = cc.id 
LEFT JOIN pinpoint.public.campaign_types ct
  ON cc.campaign_type_id = ct.id 
LEFT JOIN pinpoint.public.trackers trackers
  ON trackers.id = cc.tracker_id
WHERE --dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours = 1) }}'
from_iso8601_timestamp(a.dt) >= (SELECT dt_start_3h FROM test_time)
AND from_iso8601_timestamp(a.dt) < (SELECT dt_end_3h FROM test_time)
AND at >= (SELECT start_ms FROM test_time)
AND at < (SELECT end_ms FROM test_time)
AND t.id = (SELECT ab_test_id FROM test_info)
AND bid__app_platform = (SELECT ab_test_platform FROM test_info)
AND bid__ad_group_type = 'user-acquisition'
AND bid__exploratory = FALSE 
AND IF(trackers.name = 'apple-skan', 'SKAN', IF(trackers.name = 'no-tracker', 'NON-MEASURABLE', IF(a.bid__campaign_id  is null, 'N/A', 'MMP'))) != 'SKAN'
AND ct.name = 'user-acquisition'
AND cc.current_optimization_state IN ('cpr','cpa', 'cpi')
AND bid__price_data__model_type IN ('install','revenue','preferred-app-event')
GROUP BY 1
ORDER BY 1
