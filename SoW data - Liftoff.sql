WITH campaign_installs AS (
	SELECT 	from_iso8601_timestamp(dt) as day 
			, campaign_id 
			, SUM(installs) as installs 
			, SUM(spend_micros) / power(10,6) as spend 
			, SUM(revenue_micros) / power(10,6) as gr 
	FROM analytics.trimmed_daily 
	WHERE from_iso8601_timestamp(dt) >= DATE_TRUNC('month',CURRENT_DATE) - interval '<Parameters.Months of Data>' month
		AND from_iso8601_timestamp(dt) < DATE_TRUNC('day',CURRENT_DATE - interval '2' day)
		AND is_uncredited <> 'true'
		AND (installs > 0 OR spend_micros > 0 OR revenue_micros > 0)
	GROUP BY 1,2),
--pull the max dt from analytics table so that the unattributed install data is from the same time range
max_dt_in_daily AS (
	SELECT max(dt) as max_dt 
	FROM analytics.trimmed_daily 
	WHERE from_iso8601_timestamp(dt) >= CURRENT_DATE - interval '8' day),
unattributed_installs AS (
	SELECT  DATE_TRUNC('day',from_iso8601_timestamp(dt)) as day 
			, app_id
     		, SUM(if(is_uncredited, 1, 0)) AS installs_unattributed
     		, SUM(if(is_uncredited, if(traffic_source='ORGANIC',1,0), 0)) AS installs_unattributed_organic
    FROM rtb.raw_installs
    WHERE from_iso8601_timestamp(dt) >= DATE_TRUNC('month',CURRENT_DATE) - interval '<Parameters.Months of Data>' month
		AND from_iso8601_timestamp(dt) < DATE_TRUNC('day',CURRENT_DATE - interval '2' day)
    	AND DATE_TRUNC('day',from_iso8601_timestamp(dt)) <= (SELECT DATE_TRUNC('day',from_iso8601_timestamp(max_dt)) FROM max_dt_in_daily)
	GROUP BY 1,2),
min_unattr_install_day AS (
	SELECT MIN(day) as min_day FROM unattributed_installs)

SELECT  CAST(ci.day as DATE) as day 
		, ci.campaign_id 
		, campaigns.salesforce_campaign_id as salesforce_campaign_id
		, apps.id as app_id 
		, apps.display_name as app_name
		, apps.receiving_unattributed_installs as receiving_unattributed_installs
		, apps.tracker
		, asa.app_store_id as market_id_liftoff
		, customers.id as customer_id 
		, customers.company as customer_name 
		, cat.name as vertical
		, sub.name as subvertical
		, csms.first_name || ' ' || csms.last_name as csm
		, aes.first_name || ' ' || aes.last_name as ae
		, platforms.display_name as platform 
		, SUM(ci.installs) as installs_liftoff_campaign
		, SUM(ci.spend) as spend_liftoff_campaign
		, SUM(ci.gr) as gr_liftoff_campaign
		, MAX(ui.installs_unattributed) as unattributed_installs
		, MAX(ui.installs_unattributed_organic) as installs_unattributed_organic
FROM campaign_installs ci 
	LEFT JOIN pinpoint.public.campaigns campaigns
  		ON ci.campaign_id = campaigns.id
	LEFT JOIN unattributed_installs ui 
		ON campaigns.app_id = ui.app_id 
		AND ci.day = ui.day
	LEFT JOIN pinpoint.public.apps apps
		ON campaigns.app_id = apps.id
	LEFT JOIN pinpoint.public.customers customers
		ON apps.customer_id = customers.id
	LEFT JOIN pinpoint.public.platforms platforms 
		ON apps.platform_id = platforms.id
	LEFT JOIN pinpoint.public.liftoff_categories cat 
		ON apps.liftoff_category_id = cat.id
	LEFT JOIN pinpoint.public.liftoff_subcategories sub 
		ON apps.liftoff_subcategory_id = sub.id
	LEFT JOIN pinpoint.public.campaign_groups campaign_groups
    	ON campaigns.campaign_group_id = campaign_groups.id
    LEFT JOIN pinpoint.public.users csms
  		ON COALESCE(campaign_groups.customer_success_manager_id,customers.customer_success_manager_id) = csms.id
	LEFT JOIN pinpoint.public.users aes
  		ON COALESCE(campaign_groups.account_executive_id,customers.account_executive_id) = aes.id
  	LEFT JOIN pinpoint.public.app_store_apps asa 
  		ON apps.app_store_app_id = asa.id 
  WHERE ci.day >= (SELECT min_day + interval '1' day FROM min_unattr_install_day)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15