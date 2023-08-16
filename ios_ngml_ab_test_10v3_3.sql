-- dagger pipeline: https://dagger.liftoff.io/pipelines/1557
WITH test_info AS
(SELECT
	1100 AS ab_test_id,
	2455 AS control_group,
	2456 AS test_group)

, latest_partition AS 
(SELECT 'customer_campaign__c' AS table_name
	, max(dt) AS latest_dt 
 FROM salesforce_daily."customer_campaign__c$partitions" 
 WHERE from_iso8601_timestamp(dt) >= CURRENT_TIMESTAMP - INTERVAL '4' DAY 
 GROUP BY 1

 UNION ALL

 SELECT 'opportunity' AS table_name
	, max(dt) AS latest_dt 
 FROM salesforce_daily."opportunity$partitions" 
 WHERE from_iso8601_timestamp(dt) >= CURRENT_TIMESTAMP - INTERVAL '4' DAY 
 GROUP BY 1
)

, saleforce_data AS (
 SELECT customer_id
  , c.id AS campaign_id
  , cc.sales_region__c AS sales_region
  , cc.sales_sub_region__c AS sales_sub_region
  , o.scale_zone__c AS scale_zone
 FROM pinpoint.public.campaigns c 
 LEFT JOIN salesforce_daily.customer_campaign__c cc
 	ON cc.campaign_id_18_digit__c = c.salesforce_campaign_id
 LEFT JOIN salesforce_daily.opportunity o
 	ON o.opportunity_id_18_digit__c = cc.opportunity__c
 WHERE cc.dt = (SELECT latest_dt FROM latest_partition WHERE table_name = 'customer_campaign__c')
 	AND o.dt = (SELECT latest_dt FROM latest_partition WHERE table_name = 'opportunity')
 	AND c.state = 'enabled'
)
, goals AS (
     SELECT 
     campaign_id
     , priority
     , type
     , target_value
     FROM (SELECT campaign_id, priority, type, target_value, ROW_NUMBER()
        OVER
        (PARTITION BY campaign_id
        ORDER BY campaign_id, priority ASC) as rn 
    FROM pinpoint.public.goals goals
    WHERE priority IS NOT NULL
    AND type <> 'pacing-model'
    ORDER BY 1,2 ASC)
    WHERE rn = 1
)
, uncapped_cohorted_rev_per_auction AS
(SELECT
COALESCE(install__ad_click__impression__auction_id,reeng_click__impression__auction_id) AS auction_id
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(COALESCE(install__ad_click__impression__at, reeng_click__impression__at)/1000, 'UTC'))),1,19),'Z') AS impression_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(COALESCE(install__ad_click__at, reeng_click__at)/1000, 'UTC'))),1,19),'Z') AS click_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(install__at/1000, 'UTC'))),1,19),'Z') AS install_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
, "group".id AS ab_test_group_id
, COALESCE(install__ad_click__impression__bid__customer_id,reeng_click__impression__bid__customer_id) AS customer_id
, COALESCE(install__ad_click__impression__bid__app_id,reeng_click__impression__bid__app_id) AS dest_app_id
, CAST(COALESCE(install__ad_click__impression__bid__bid_request__non_personalized,reeng_click__impression__bid__bid_request__non_personalized) AS varchar) AS non_personalized
, CAST(is_viewthrough AS varchar) AS is_viewthrough
, CASE WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format) = 'video' THEN 'VAST'
	   WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format) = 'native' THEN 'native'
	   WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format) in ('320x50', '728x90') THEN 'banner'
	   WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format) = '300x250' THEN 'mrec' 
  ELSE 'html-interstitial' END AS  ad_format
, COALESCE(install__ad_click__impression__bid__creative__type,reeng_click__impression__bid__creative__type ) AS creative_type
, COALESCE(install__ad_click__impression__bid__campaign_id,reeng_click__impression__bid__campaign_id) AS campaign_id
, COALESCE(install__ad_click__impression__bid__ad_group_id,reeng_click__impression__bid__ad_group_id) AS ad_group_id
, COALESCE(install__ad_click__impression__bid__price_data__model_type,reeng_click__impression__bid__price_data__model_type) AS bid_type
, COALESCE(install__ad_click__impression__bid__price_data__vt_bidding_enabled,reeng_click__impression__bid__price_data__vt_bidding_enabled) AS vt_bidding
, install__ad_click__impression__bid__price_data__imp_to_install_model_tag AS imp_to_install_model_tag_ct
, install__ad_click__impression__bid__price_data__imp_to_install_model_tag AS imp_to_install_model_tag_vt 
, CASE WHEN COALESCE(install__ad_click__impression__bid__bid_request__exchange, reeng_click__impression__bid__bid_request__exchange) IN ('VUNGLE','APPLOVIN','INNERACTIVE_DIRECT','DOUBLECLICK','MINTEGRAL','IRONSOURCE','UNITY','APPODEAL','INMOBI','VERVE') 
	   THEN COALESCE(install__ad_click__impression__bid__bid_request__exchange, reeng_click__impression__bid__bid_request__exchange) ELSE 'others' END AS exchange_group
, CASE WHEN COALESCE(install__geo__country, reeng_click__geo__country) IN ('US','GB','IN','JP','BR') 
       THEN COALESCE(install__geo__country, reeng_click__geo__country) ELSE 'others' END AS country_group
--, COALESCE(install__ad_click__impression__bid__exploratory,reeng_click__impression__bid__exploratory,attribution_event__click__impression__bid__exploratory) AS exploratory
--, install__ad_click__impression__bid__use_skan_pod AS is_skan
--, COALESCE(install__ad_click__impression__bid__app_platform, reeng_click__impression__bid__app_platform,attribution_event__click__impression__bid__app_platform) AS platform
--, COALESCE(install__ad_click__impression__bid__ad_group_type,reeng_click__impression__bid__ad_group_type,attribution_event__click__impression__bid__ad_group_type) AS ad_group_type

, sum(IF(for_reporting AND at - COALESCE(install__ad_click__impression__at,reeng_click__impression__at) < 604800000 AND custom_event_id = COALESCE(install__ad_click__impression__bid__campaign_target_event_id, reeng_click__impression__bid__campaign_target_event_id),1,0)) AS target_events_d7_imp
, sum(IF(for_reporting AND at - COALESCE(install__ad_click__impression__at,reeng_click__impression__at) < 604800000 AND custom_event_id = COALESCE(install__ad_click__impression__bid__campaign_target_event_id, reeng_click__impression__bid__campaign_target_event_id) AND first_occurrence,1,0)) AS target_events_first_d7_imp
, CAST(sum(CASE WHEN at - COALESCE(install__ad_click__impression__at,reeng_click__impression__at) < 604800000 AND for_reporting AND customer_revenue_micros > -100000000000 AND customer_revenue_micros < 100000000000 AND customer_revenue_micros != 0 
			    THEN customer_revenue_micros ELSE 0 END) AS double) / 1e6 AS customer_revenue_d7_imp	    
FROM rtb.matched_app_events ae
CROSS JOIN UNNEST(COALESCE(install__ad_click__impression__bid__bid_request__ab_test_assignments,
						   reeng_click__impression__bid__bid_request__ab_test_assignments)) t
WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours = 1) }}'
--dt >= '2023-08-14T00' AND dt < '2023-08-14T02'
AND is_uncredited <> TRUE
AND at - COALESCE(install__ad_click__impression__at, reeng_click__impression__at) < 604800000
AND t.id = (SELECT ab_test_id FROM test_info)            
AND COALESCE(install__ad_click__impression__bid__ad_group_type,reeng_click__impression__bid__ad_group_type) = 'user-acquisition'
AND COALESCE(install__ad_click__impression__bid__app_platform, reeng_click__impression__bid__app_platform) = 'IOS'
AND COALESCE(install__ad_click__impression__bid__exploratory,reeng_click__impression__bid__exploratory) = FALSE 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
)
,
funnel AS
(
-- imp
SELECT
CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(at/1000, 'UTC'))),1,19),'Z') AS impression_at
, NULL AS click_at
, NULL AS install_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
, "group".id AS ab_test_group_id
, bid__customer_id AS customer_id
, bid__app_id AS dest_app_id
, CAST(bid__bid_request__non_personalized AS varchar) AS non_personalized
, 'N/A' AS is_viewthrough
, CASE WHEN bid__creative__ad_format = 'video' THEN 'VAST'
	WHEN bid__creative__ad_format = 'native' THEN 'native'
	WHEN bid__creative__ad_format in ('320x50', '728x90') THEN 'banner'
	WHEN bid__creative__ad_format = '300x250' THEN 'mrec'
	ELSE 'html-interstitial' END AS  ad_format
, bid__creative__type AS creative_type
, bid__campaign_id AS campaign_id
, bid__ad_group_id AS ad_group_id
, bid__price_data__model_type AS bid_type
, bid__price_data__vt_bidding_enabled AS vt_bidding
, bid__price_data__imp_to_install_ct_model_tag AS imp_to_install_model_tag_ct
, bid__price_data__imp_to_install_vt_model_tag AS imp_to_install_model_tag_vt
, CASE WHEN bid__bid_request__exchange IN ('VUNGLE','APPLOVIN','INNERACTIVE_DIRECT','DOUBLECLICK','MINTEGRAL','IRONSOURCE','UNITY','APPODEAL','INMOBI','VERVE') THEN bid__bid_request__exchange ELSE 'others' END AS exchange_group
, CASE WHEN bid__bid_request__device__geo__country IN ('US','GB','IN','JP','BR') THEN bid__bid_request__device__geo__country ELSE 'others' END AS country_group
--, bid__use_skan_pod AS is_skan
--, bid__app_platform AS platform
--, bid__ad_group_type AS ad_group_type
--, bid__exploratory AS exploratory

, sum(1) AS impressions
, sum(spend_micros) AS spend_micros
, sum(revenue_micros) AS revenue_micros
, sum(0) AS clicks
, sum(0) AS installs
, sum(0) AS target_events_d7_imp
, sum(0) AS target_events_first_d7_imp
, sum(0) AS customer_revenue_d7_imp
, sum(0) AS capped_customer_revenue_d7_imp
, sum(0) AS squared_capped_customer_revenue_d7_imp
, sum(CASE WHEN COALESCE(bid__bid_request__device__platform_specific_id_sha1,'') <> '' THEN 1
           WHEN COALESCE(bid__bid_request__device__idfv, '') <>'' THEN 1
           WHEN COALESCE(bid__bid_request__pods__app_specific_id__id,'') <> '' THEN 1
           WHEN COALESCE(bid__bid_request__device__model_data__name, bid__bid_request__device__user_agent,'') <> '' THEN 1
     ELSE NULL END) AS num_users
, sum(CAST(CASE WHEN bid__price_data__model_type IN ('revenue', 'revenue-v3') THEN bid__price_data__conversion_likelihood/1000000 ELSE bid__price_data__conversion_likelihood END AS double)) AS predicted_conversion_likelihood
, sum(CAST(bid__auction_result__winner__price_cpm_micros AS double)/1000000) AS preshaded_cpm
, sum(CAST(bid__auction_result__winner__price_cpm_micros AS double)/1000000/CAST(bid__price_data__compensated_margin_bid_multiplier AS double)) AS private_value
, sum(CAST(bid__price_cpm_micros AS double)/1000000) AS shaded_cpm
, sum(bid__price_data__predicted_imp_to_click_rate) AS predicted_clicks
, sum(COALESCE(bid__price_data__predicted_imp_to_click_rate * bid__price_data__predicted_click_to_install_rate, 0)
    + COALESCE(bid__price_data__predicted_imp_to_install_ct_rate, 0)) AS predicted_installs_ct
, sum(bid__price_data__predicted_imp_to_install_vt_rate) AS predicted_installs_vt
, sum(CASE WHEN bid__ad_group_type = 'user-acquisition' 
		   THEN ((COALESCE(bid__price_data__predicted_imp_to_click_rate * bid__price_data__predicted_click_to_install_rate, 0)
    		+ COALESCE(bid__price_data__predicted_imp_to_install_ct_rate, 0)) * bid__price_data__predicted_install_to_preferred_app_event_rate)
    	   ELSE (bid__price_data__predicted_imp_to_click_rate * bid__price_data__predicted_click_to_preferred_app_event_rate)
      END) AS predicted_target_events_ct
, sum(bid__price_data__predicted_imp_to_install_vt_rate * bid__price_data__predicted_install_to_preferred_app_event_vt_rate) AS predicted_target_events_vt
, sum(CASE WHEN bid__ad_group_type = 'user-acquisition'
		   THEN ((COALESCE(bid__price_data__predicted_imp_to_click_rate * bid__price_data__predicted_click_to_install_rate, 0)
    		+ COALESCE(bid__price_data__predicted_imp_to_install_ct_rate, 0)) * LEAST(bid__price_data__predicted_install_to_revenue_rate,500000000))
    	   ELSE (bid__price_data__predicted_imp_to_click_rate * bid__price_data__predicted_click_to_revenue_rate)      
    END) AS predicted_customer_revenue_micros_ct
, sum(bid__price_data__predicted_imp_to_install_vt_rate * LEAST(bid__price_data__predicted_install_to_revenue_rate,500000000)) AS predicted_customer_revenue_micros_vt
   
FROM rtb.impressions_with_bids i
CROSS JOIN UNNEST(bid__bid_request__ab_test_assignments) t
WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours = 1) }}'
--dt >= '2023-08-14T00' AND dt < '2023-08-14T02'
AND t.id = (SELECT ab_test_id FROM test_info)
AND bid__app_platform = 'IOS'
AND bid__ad_group_type = 'user-acquisition'
AND bid__exploratory = FALSE 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19

UNION ALL

-- ad_clicks
SELECT
CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(impression__at/1000, 'UTC'))),1,19),'Z') AS impression_at
, CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS click_at
, NULL AS install_at
, CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
, "group".id AS ab_test_group_id
, impression__bid__customer_id as customer_id
, impression__bid__app_id as dest_app_id
, CAST(impression__bid__bid_request__non_personalized AS varchar) AS is_nonpersonalized
, 'N/A' AS is_viewthrough
, CASE WHEN impression__bid__creative__ad_format = 'video' THEN 'VAST'
	   WHEN impression__bid__creative__ad_format = 'native' THEN 'native'
       WHEN impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
       WHEN impression__bid__creative__ad_format = '300x250' THEN 'mrec'
  ELSE 'html-interstitial' END AS ad_format
, impression__bid__creative__type AS creative_type
, impression__bid__campaign_id as campaign_id
, impression__bid__ad_group_id AS ad_group_id
, impression__bid__price_data__model_type AS bid_type
, impression__bid__price_data__vt_bidding_enabled AS vt_bidding
, impression__bid__price_data__imp_to_install_ct_model_tag AS imp_to_install_model_tag_ct
, impression__bid__price_data__imp_to_install_vt_model_tag AS imp_to_install_model_tag_vt
, CASE WHEN impression__bid__bid_request__exchange IN ('VUNGLE','APPLOVIN','INNERACTIVE_DIRECT','DOUBLECLICK','MINTEGRAL','IRONSOURCE','UNITY','APPODEAL','INMOBI','VERVE') THEN impression__bid__bid_request__exchange ELSE 'others' END AS exchange_group
, CASE WHEN geo__country IN ('US','GB','IN','JP','BR') THEN geo__country ELSE 'others' END AS country_group
--, impression__bid__use_skan_pod AS is_skan
--, impression__bid__app_platform AS platform
--, impression__bid__ad_group_type AS ad_group_type
--, impression__bid__exploratory AS exploratory

, sum(0) AS impressions
, sum(0) AS spend_micros
, sum(0) AS revenue_micros
, sum(1) AS clicks
, sum(0) AS installs
, sum(0) AS target_events_d7_imp
, sum(0) AS target_events_first_d7_imp
, sum(0) AS customer_revenue_d7_imp
, sum(0) AS capped_customer_revenue_d7_imp
, sum(0) AS squared_capped_customer_revenue_d7_imp
, sum(0) AS num_users
, sum(0) AS predicted_conversion_likelihood
, sum(0) AS preshaded_cpm
, sum(0) AS private_value
, sum(0) AS shaded_cpm
, sum(0) AS predicted_clicks
, sum(0) AS predicted_installs_ct
, sum(0) AS predicted_installs_vt
, sum(0) AS predicted_target_events_ct
, sum(0) AS predicted_target_events_vt
, sum(0) AS predicted_customer_revenue_micros_ct
, sum(0) AS predicted_customer_revenue_micros_vt
FROM rtb.ad_clicks ac
CROSS JOIN UNNEST(impression__bid__bid_request__ab_test_assignments) t
WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours = 1) }}'
--dt >= '2023-08-14T00' AND dt < '2023-08-14T02'
AND t.id = (SELECT ab_test_id FROM test_info)
AND at - impression__at < 2592000000
AND impression__bid__app_platform = 'IOS'
AND impression__bid__ad_group_type = 'user-acquisition'
AND impression__bid__exploratory = FALSE 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19

UNION ALL

-- installs
SELECT
CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(ad_click__impression__at/1000, 'UTC'))),1,19),'Z') AS impression_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(ad_click__at/1000, 'UTC'))),1,19),'Z') AS click_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS install_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
, "group".id AS ab_test_group_id
, ad_click__impression__bid__customer_id AS customer_id
, ad_click__impression__bid__app_id AS dest_app_id
, CAST(ad_click__impression__bid__bid_request__non_personalized AS varchar) AS non_personalized
, CAST(is_viewthrough AS VARCHAR) AS is_viewthrough
, CASE WHEN ad_click__impression__bid__creative__ad_format = 'video' THEN 'VAST'
WHEN ad_click__impression__bid__creative__ad_format = 'native' THEN 'native'
WHEN ad_click__impression__bid__creative__ad_format in ('320x50', '728x90') THEN 'banner'
WHEN ad_click__impression__bid__creative__ad_format = '300x250' THEN 'mrec'
ELSE 'html-interstitial' end AS ad_format
, ad_click__impression__bid__creative__type AS creative_type
, ad_click__impression__bid__campaign_id AS campaign_id
, ad_click__impression__bid__ad_group_id AS ad_group_id
, ad_click__impression__bid__price_data__model_type AS bid_type
, ad_click__impression__bid__price_data__vt_bidding_enabled AS vt_bidding
, ad_click__impression__bid__price_data__imp_to_install_ct_model_tag AS imp_to_install_model_tag_ct
, ad_click__impression__bid__price_data__imp_to_install_vt_model_tag AS imp_to_install_model_tag_vt
, CASE WHEN ad_click__impression__bid__bid_request__exchange IN ('VUNGLE','APPLOVIN','INNERACTIVE_DIRECT','DOUBLECLICK','MINTEGRAL','IRONSOURCE','UNITY','APPODEAL','INMOBI','VERVE') THEN ad_click__impression__bid__bid_request__exchange ELSE 'others' END AS exchange_group
, CASE WHEN geo__country IN ('US','GB','IN','JP','BR') THEN geo__country ELSE 'others' END AS country_group
--, ad_click__impression__bid__use_skan_pod AS is_skan
--, ad_click__impression__bid__app_platform AS platform
--, ad_click__impression__bid__ad_group_type AS ad_group_type
--, ad_click__impression__bid__exploratory AS exploratory

, sum(0) AS impressions
, sum(0) AS spend_micros
, sum(0) AS revenue_micros
, sum(0) AS clicks
, sum(IF(for_reporting, 1, 0)) AS installs
, sum(0) AS target_events_d7_imp
, sum(0) AS target_events_first_d7_imp
, sum(0) AS customer_revenue_d7_imp
, sum(0) AS capped_customer_revenue_d7_imp
, sum(0) AS squared_capped_customer_revenue_d7_imp
, sum(0) AS num_users
, sum(0) AS predicted_conversion_likelihood
, sum(0) AS preshaded_cpm
, sum(0) AS private_value
, sum(0) AS shaded_cpm
, sum(0) AS predicted_clicks
, sum(0) AS predicted_installs_ct
, sum(0) AS predicted_installs_vt
, sum(0) AS predicted_target_events_ct
, sum(0) AS predicted_target_events_vt
, sum(0) AS predicted_customer_revenue_micros_ct
, sum(0) AS predicted_customer_revenue_micros_vt
FROM rtb.matched_installs mi
CROSS JOIN UNNEST(ad_click__impression__bid__bid_request__ab_test_assignments) t
WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours = 1) }}'
--dt >= '2023-08-14T00' AND dt < '2023-08-14T02'
AND is_uncredited <> TRUE
AND t.id = (SELECT ab_test_id FROM test_info)
AND at - ad_click__impression__at < 2592000000
AND ad_click__impression__bid__ad_group_type = 'user-acquisition'
AND ad_click__impression__bid__app_platform = 'IOS'
AND ad_click__impression__bid__exploratory = FALSE 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19

UNION ALL

SELECT
impression_at
, click_at
, install_at
, at
, ab_test_group_id
, customer_id
, dest_app_id
, non_personalized
, is_viewthrough
, ad_format
, creative_type
, campaign_id
, ad_group_id
, bid_type
, vt_bidding
, imp_to_install_model_tag_ct
, imp_to_install_model_tag_vt
, exchange_group
, country_group
--, is_skan
--, platform
--, ad_group_type
--, exploratory

, sum(0) AS impressions
, sum(0) AS spend_micros
, sum(0) AS revenue_micros
, sum(0) AS clicks
, sum(0) AS installs
, sum(target_events_d7_imp) AS target_events_d7_imp
, sum(target_events_first_d7_imp) AS target_events_first_d7_imp
, sum(customer_revenue_d7_imp) AS customer_revenue_d7_imp
, sum(LEAST(customer_revenue_d7_imp, 500)) AS capped_customer_revenue_d7_imp
, sum(LEAST(customer_revenue_d7_imp, 500) * LEAST(customer_revenue_d7_imp, 500)) AS squared_capped_customer_revenue_d7_imp
, sum(0) AS num_users
, sum(0) AS predicted_conversion_likelihood
, sum(0) AS preshaded_cpm
, sum(0) AS private_value
, sum(0) AS shaded_cpm
, sum(0) AS predicted_clicks
, sum(0) AS predicted_installs_ct
, sum(0) AS predicted_installs_vt
, sum(0) AS predicted_target_events_ct
, sum(0) AS predicted_target_events_vt
, sum(0) AS predicted_customer_revenue_micros_ct
, sum(0) AS predicted_customer_revenue_micros_vt
FROM uncapped_cohorted_rev_per_auction u
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)

SELECT
impression_at
, click_at
, install_at
, ab_test_group_id
, atg.name AS test_group_name
, a.customer_id
, a.dest_app_id
, non_personalized
, is_viewthrough
, ad_format
, creative_type
, a.campaign_id
, ad_group_id
, bid_type
, vt_bidding
, imp_to_install_model_tag_ct
, imp_to_install_model_tag_vt
, exchange_group
, country_group
--, is_skan
--, platform
--, ad_group_type
--, exploratory
, b.company AS customer_name
, c.display_name AS dest_app_name
, c.salesforce_account_level AS account_level
, cc.current_optimization_state AS current_optimization_state
, cc.display_name AS campaign_name
, ct.name AS campaign_type
, sd.sales_region
, sd.sales_sub_region
, sd.scale_zone
, IF(trackers.name = 'apple-skan', 'SKAN', IF(trackers.name = 'no-tracker', 'NON-MEASURABLE', IF(a.campaign_id is null, 'N/A', 'MMP'))) AS campaign_tracker_type
, goals.type AS goal_type_3
, goals.target_value AS goal_3_value
    
, sum(impressions) AS impressions
, sum(spend_micros) AS spend_micros
, sum(revenue_micros) AS revenue_micros
, sum(clicks) AS clicks
, sum(installs) AS installs
, sum(target_events_d7_imp) AS target_events_d7_imp
, sum(target_events_first_d7_imp) AS target_events_first_d7_imp
, sum(customer_revenue_d7_imp) AS customer_revenue_d7_imp
, sum(capped_customer_revenue_d7_imp) AS capped_customer_revenue_d7_imp
, sum(squared_capped_customer_revenue_d7_imp) AS squared_capped_customer_revenue_d7_imp
, sum(num_users) AS num_users
, sum(predicted_conversion_likelihood) AS predicted_conversion_likelihood
, sum(preshaded_cpm) AS preshaded_cpm
, sum(private_value) AS private_value
, sum(shaded_cpm) AS shaded_cpm
, sum(predicted_clicks) AS predicted_clicks
, sum(predicted_installs_ct) AS predicted_installs_ct
, sum(predicted_installs_vt) AS predicted_installs_vt
, sum(predicted_target_events_ct) AS predicted_target_events_ct
, sum(predicted_target_events_vt) AS predicted_target_events_vt
, sum(predicted_customer_revenue_micros_ct) AS predicted_customer_revenue_micros_ct
, sum(predicted_customer_revenue_micros_vt) AS predicted_customer_revenue_micros_vt
FROM funnel a
LEFT JOIN pinpoint.public.customers b
  ON a.customer_id = b.id
LEFT JOIN pinpoint.public.apps c
  ON a.dest_app_id = c.id AND  a.customer_id = c.customer_id
LEFT JOIN pinpoint.public.ab_test_groups atg
ON ab_test_group_id = atg.id
LEFT JOIN pinpoint.public.campaigns cc
  ON a.campaign_id = cc.id
LEFT JOIN pinpoint.public.campaign_types ct
  ON cc.campaign_type_id = ct.id
LEFT JOIN saleforce_data sd 
  ON a.campaign_id = sd.campaign_id AND a.customer_id = sd.customer_id
LEFT JOIN goals
  ON a.campaign_id = goals.campaign_id
LEFT JOIN pinpoint.public.trackers trackers
  ON trackers.id = cc.tracker_id
WHERE IF(trackers.name = 'apple-skan', 'SKAN', IF(trackers.name = 'no-tracker', 'NON-MEASURABLE', IF(a.campaign_id is null, 'N/A', 'MMP'))) != 'SKAN'
AND ct.name = 'user-acquisition'
AND impression_at > '2023-08-04T23' -- depending on tests
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
