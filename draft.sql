SELECT 
app_id
, count(DISTINCT device_id_sha1) AS total_users
, count(DISTINCT IF(is_uncredited, null, device_id_sha1)) AS attributed_users
, count(DISTINCT IF(is_uncredited, device_id_sha1, null)) AS unattributed_users

FROM rtb.raw_installs
WHERE concat(substr(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') BETWEEN '2023-02-26' AND '2023-02-27'
	AND app_id IN (4379)
GROUP BY 1


-- total addressable users by using users table 
SELECT
try(filter(dest_apps, a -> a."id" = 1552109)[1])."attributed"
--, count(DISTINCT (CASE WHEN bid_requests <= 0 THEN id END)) AS user_wo_bid_requests
--, count(DISTINCT (CASE WHEN bid_requests > 0 THEN id END)) AS user_with_bid_requests
--, count(DISTINCT (CASE WHEN bids > 0 AND bid_requests > 0 THEN id END)) AS user_with_bids
--, count(DISTINCT (CASE WHEN bid_requests > 0 AND bids > 0 AND impressions > 0 THEN id END)) AS user_with_imp
--, count(DISTINCT (CASE WHEN bid_requests > 0 AND bids > 0 AND impressions > 0 AND installs > 0 THEN id END)) AS user_with_installs
FROM proto2parquet.users
WHERE dt = (SELECT dt FROM proto2parquet."users$partitions" ORDER BY dt DESC OFFSET 1 LIMIT 1)
  AND try(filter(dest_apps, a -> a."id" = 1552109)[1])."last_install" BETWEEN to_unixtime(date('2023-02-26')) AND to_unixtime(date('2023-02-27'))
GROUP BY 1
 

-- check users access from rtb.users table
SELECT 
count(DISTINCT id) AS user_counts
, try(filter(dest_apps, a -> a."id" = 1970931)[1])."attributed"
FROM proto2parquet.users
WHERE dt in (SELECT DISTINCT dt FROM proto2parquet."users$partitions" ORDER BY dt DESC OFFSET 1 LIMIT 10)
AND try(filter(dest_apps, a -> a."id" = 1970931)[1])."last_install" BETWEEN to_unixtime(date('2023-02-26')) AND to_unixtime(date('2023-02-27'))
AND id IN (
		SELECT DISTINCT SUBSTRING(device_id_sha1, 1, 16) 
		FROM rtb.raw_installs
		WHERE concat(substr(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') BETWEEN '2023-02-26' AND '2023-02-27'
			AND app_id IN (4379))
GROUP BY 2


-- check how many attributed users more in users table 
SELECT 
count(DISTINCT id) AS user_counts
FROM proto2parquet.users
WHERE dt in (SELECT DISTINCT dt FROM proto2parquet."users$partitions" ORDER BY dt DESC OFFSET 1 LIMIT 10)
AND try(filter(dest_apps, a -> a."id" = 1552109)[1])."last_install" BETWEEN to_unixtime(date('2023-02-26')) AND to_unixtime(date('2023-02-27'))
AND id NOT IN (
		SELECT DISTINCT SUBSTRING(device_id_sha1, 1, 16) 
		FROM rtb.raw_installs
		WHERE concat(substr(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') BETWEEN '2023-02-26' AND '2023-02-27'
			AND app_id IN (350))
AND try(filter(dest_apps, a -> a."id" = 1552109)[1])."attributed" = TRUE 


  
select DISTINCT id, app_store_app_id from pinpoint.public.apps where id IN (4379, 3657, 2898, 350)


WITH temp AS (SELECT 
device_id_sha1 AS device_id
, is_uncredited 
, CASE WHEN try(IF(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) >= 0,
  CAST(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) AS double precision) / power(2, 64), 
  (CAST(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) AS double precision) / power(2, 64))+1)) < 0.000125 THEN 'yes' ELSE 'no' END AS in_no_bid_user_sample
, CASE WHEN try(IF(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) >= 0,
  CAST(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) AS double precision) / power(2, 64), 
  (CAST(from_big_endian_64(reverse(substr(from_hex(device_id_sha1), 1, 8))) AS double precision) / power(2, 64))+1)) < 0.0002   THEN 'yes' ELSE 'no' END AS in_bid_user_sample
FROM rtb.raw_installs 
WHERE  dt BETWEEN '2023-02-26' AND '2023-02-27'
	AND app_id = 1579)


SELECT 
is_uncredited
--, in_bid_user_sample
, in_no_bid_user_sample
, count(DISTINCT device_id) AS user_count
FROM temp
GROUP BY 1,2





SELECT 
--CAST(CASE WHEN bid__price_data__model_type IN ('revenue', 'revenue-v3') THEN bid__price_data__conversion_likelihood/1000000 ELSE bid__price_data__conversion_likelihood END AS double) AS p_cvr
--, CAST(bid__price_data__ad_group_cpx_bid_micros AS double)/1000000  as bid_target
--, CAST(CASE WHEN bid__price_data__model_type IN ('revenue', 'revenue-v3') THEN bid__price_data__conversion_likelihood/1000000 ELSE bid__price_data__conversion_likelihood END AS double) * bid__price_data__effective_cpx_bid_micros/1000000 AS preshaded_w_multiplier
sum(CAST(bid__auction_result__winner__price_cpm_micros AS double)/1000000000) AS preshaded_value 
, sum(CAST(bid__auction_result__winner__price_cpm_micros AS double)/1000000000/CAST(bid__price_data__compensated_margin_bid_multiplier AS double)) AS private_value
--, CAST(bid__price_cpm_micros AS double)/1000000000 AS CPM_per_impression 
--, CAST(revenue_micros AS double)/1000000 AS revenue
--, CAST(spend_micros AS double)/1000000 AS spend
--, (COALESCE(bid__price_data__predicted_imp_to_click_rate * bid__price_data__predicted_click_to_install_rate, 0)
        --+ COALESCE(bid__price_data__predicted_imp_to_install_ct_rate, 0)) * bid__price_data__predicted_install_to_preferred_app_event_rate AS predicted_target_events_ct
--, bid__price_data__predicted_imp_to_install_vt_rate * bid__price_data__predicted_install_to_preferred_app_event_vt_rate AS predicted_target_events_vt
, sum((COALESCE(bid__price_data__predicted_imp_to_click_rate * bid__price_data__predicted_click_to_install_rate, 0)
        + COALESCE(bid__price_data__predicted_imp_to_install_ct_rate, 0)) * LEAST(bid__price_data__predicted_install_to_revenue_rate,500000000)/1000000) AS predicted_customer_revenue_micros_ct
, sum(bid__price_data__predicted_imp_to_install_vt_rate * LEAST(bid__price_data__predicted_install_to_revenue_rate,500000000)) AS predicted_customer_revenue_micros_vt
--, 1 - CAST(spend_micros AS double)/CAST(revenue_micros AS double) AS nrm_by_cal
, max(CAST(bid__price_data__compensated_margin_bid_multiplier AS double)) AS margin_multiplier

FROM rtb.impressions_with_bids f 
WHERE dt between '2023-06-01T00' AND '2023-06-26T00'
-- AND ag.bid_type = 'cpa'
--AND ag.viewthrough_optimization_enabled <> FALSE
AND f.bid__campaign_id IN (5320)
AND spend_micros > 0 

