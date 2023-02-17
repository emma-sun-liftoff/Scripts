with latest_sf_partition AS (select max(dt) as latest_dt from salesforce_daily.customer_campaign__c where from_iso8601_timestamp(dt) >= CURRENT_TIMESTAMP - interval '3' day)

, sf_data as (select c.campaign_id_18_digit__c as salesforce_campaign_id
  , o.sales_region__c as sales_region
  , o.sales_sub_region__c as sales_sub_region
  , a.prospect_tier__c as prospect_tier
  , a.account_level__c as account_level
  , a.customer_tier__c as customer_tier
  , app.liftoff_category_app__c as liftoff_category
  , app.liftoff_subcategory_app__c as liftoff_subcategory
  , app.class__c as class
  , app.genre__c as genre
  , app.subgenre__c as subgenre
FROM salesforce_daily.customer_campaign__c c
  LEFT JOIN salesforce_daily.opportunity o
  ON c.dt = o.dt
  AND c.opportunity__c = o.opportunity_id_18_digit__c
  LEFT JOIN salesforce_daily.account a
  ON c.dt = a.dt
  AND o.accountid = a.account_id_18_digit__c
  LEFT JOIN salesforce_daily.app__c app
  ON c.dt = app.dt
  AND c.app__c = app.app_id_18_digit__c
WHERE c.dt = (select latest_dt from latest_sf_partition) -- filter ui if not app,daycountrycombo in ccampaigns
AND o.dt = (select latest_dt from latest_sf_partition)  -- join preaggregated salesforce data onto select
AND a.dt = (select latest_dt from latest_sf_partition)
AND app.dt = (select latest_dt from latest_sf_partition))

, campaign_installs AS (SELECT 	from_iso8601_timestamp(d.dt) as ci_day
        , campaign_id
        , campaigns.salesforce_campaign_id as salesforce_campaign_id
        , dest_app_id as app_id
        , d.country as country
        , SUM(installs) as installs
        , SUM(spend_micros) / power(10,6) as spend
        , SUM(revenue_micros) / power(10,6) as gr
    FROM analytics.daily d
    LEFT JOIN pinpoint.public.campaigns campaigns
        on d.campaign_id = campaigns.id
  WHERE from_iso8601_timestamp(d.dt) >= DATE_TRUNC('month',CURRENT_DATE - interval '<Parameters.Months of Data>' month)
    AND from_iso8601_timestamp(d.dt) < DATE_TRUNC('day',CURRENT_DATE - interval '2' day)
    AND is_uncredited <> 'true'
    AND (installs > 0 OR spend_micros > 0 OR revenue_micros > 0)
  GROUP BY 1,2,3,4,5),

max_dt_in_daily AS (
  SELECT max(dt) as max_dt
  FROM analytics.daily
  WHERE from_iso8601_timestamp(dt) >= CURRENT_DATE - interval '8' day),

unattributed_installs AS (
  SELECT  DATE_TRUNC('day',from_iso8601_timestamp(dt)) as day
      , app_id
      , geo__country as country
        , SUM(if(is_uncredited, 1, 0)) AS installs_unattributed
        , SUM(if(is_uncredited, if(traffic_source='ORGANIC',1,0), 0)) AS installs_unattributed_organic
    FROM rtb.raw_installs
    WHERE from_iso8601_timestamp(dt) >= DATE_TRUNC('month',CURRENT_DATE) - interval '<Parameters.Months of Data>' month
    AND from_iso8601_timestamp(dt) < DATE_TRUNC('day',CURRENT_DATE - interval '2' day)
      AND DATE_TRUNC('day',from_iso8601_timestamp(dt)) <= (SELECT DATE_TRUNC('day',from_iso8601_timestamp(max_dt)) FROM max_dt_in_daily)
  GROUP BY 1,2,3),

min_unattr_install_day AS (
  SELECT MIN(day) as min_day FROM unattributed_installs)

SELECT  COALESCE(CAST(ui.day as DATE),CAST(ci_day as DATE))as day
    , COALESCE(ui.country, ci.country) as country
   , COALESCE(ui.app_id, ci.app_id) as app_id
   , apps.display_name as app_name
   , platforms.display_name as platform
   , apps.receiving_unattributed_installs as receiving_unattributed_installs
   , apps.tracker
   , asa.app_store_id as market_id_liftoff
   , customers.id as customer_id
   , customers.company as customer_name
   , cat.name as vertical
   , sub.name as subvertical
   , sf.sales_region
   , sf.sales_sub_region
   , sf.prospect_tier
   , sf.account_level
   , sf.customer_tier
   , sf.liftoff_category
   , sf.liftoff_subcategory
   , sf.class
   , sf.genre
   , sf.subgenre
   , csms.first_name || ' ' || csms.last_name as csm 
   , aes.first_name || ' ' || aes.last_name as ae 
   , SUM(ci.installs) as installs_liftoff_campaign
   , SUM(ci.spend) as spend_liftoff_campaign
   , SUM(ci.gr) as gr_liftoff_campaign
   , MAX(ui.installs_unattributed) as unattributed_installs
   , MAX(ui.installs_unattributed_organic) as installs_unattributed_organic
FROM unattributed_installs ui
  FULL OUTER JOIN campaign_installs ci
      ON ui.day = ci.ci_day
      AND ui.app_id = ci.app_id
      AND ui.country = ci.country
    LEFT JOIN pinpoint.public.campaigns campaigns
      on ci.campaign_id = campaigns.id
    LEFT JOIN pinpoint.public.apps apps
      ON COALESCE(ui.app_id, ci.app_id) = apps.id
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
    LEFT JOIN sf_data sf
      ON ci.salesforce_campaign_id = sf.salesforce_campaign_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
