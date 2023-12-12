
WITH scale_zones AS (
  SELECT
    ppc.id AS campaign_id,
    o.dt,
    o.scale_zone__c AS scale_zone, -- customers can increase budget
    cc.sales_region__c AS sales_region -- customers' origin
  FROM
    salesforce_daily.opportunity o
    LEFT JOIN salesforce_daily.customer_campaign__c cc
      ON o.dt = cc.dt
      AND o.opportunity_id_18_digit__c = cc.opportunity__c
    LEFT JOIN pinpoint.public.campaigns ppc
      ON cc.campaign_id_18_digit__c = ppc.salesforce_campaign_id
  WHERE
  o.dt >= '2023-09-25T21:00:00Z'
  AND o.dt < '2024-12-18T13:00:20Z'
),
upper_funnel_reduced AS (
  SELECT
    u.campaign_tracker, -- Adjust, etc
    u.campaign_tracker_type, -- MMP, Direct, etc
    u.exchange,
    u.test_id,
    u.test_group_id,
    u.test_group,
    u.bid_date,
    u.dest_app_id,
    u.campaign_id,
    a.salesforce_account_level AS salesforce_account_level, -- or customer tiers, Platinum, Silver, etc
    s.scale_zone AS scale_zone,
    s.sales_region AS sales_region,
    u.platform,
    u.customer_id,
    u.logical_size, 
    u.app_event_category,
    u.non_personalized,
    u.model_type,
    u.vt_bidding,  -- using vt funnel model to predict vt impression-to-install
    u.ad_group_type, -- UA, RE
    c.current_optimization_state AS campaign_bidding_strategy, -- cpi, CPA, cpr
    u.liftoff_category,  
    u.exploratory, 

    sum(impressions) as impressions,
    sum(spend) as spend,
    sum(spend_micros) as spend_micros,
    sum(revenue) as revenue,
    sum(revenue_micros) as revenue_micros,
    sum(clicks) as clicks,
    sum(installs) as installs,
    sum(ct_installs) as ct_installs,
    sum(vt_installs) as vt_installs,  -- view through installs
    sum(app_events) as app_events, 
    sum(app_events_total) as app_events_total, 
    sum(predicted_clicks) as predicted_clicks,
    sum(predicted_vt_installs) as predicted_vt_installs, -- only vt_bidding is true
    sum(predicted_ct_installs) as predicted_ct_installs,
    sum(predicted_app_events) as predicted_app_events,
    sum(predicted_customer_revenue) as predicted_customer_revenue,
    sum(click_to_install_time_sum) as click_to_install_time_sum,
    sum(num_installs_within_1h) as num_installs_within_1h,
    sum(num_installs_within_1d) as num_installs_within_1d
  FROM abacus.cortex_ios_iti_unified_v0_1_v2_upperfunnel u
  LEFT JOIN pinpoint_hourly.campaigns c
    ON u.campaign_id = c.id
    AND u.dt = c.dt
  LEFT JOIN scale_zones s
    ON u.campaign_id = s.campaign_id
    AND substring(u.bid_date, 1, 10) = substring(s.dt, 1, 10)
  LEFT JOIN pinpoint_hourly.apps a
    ON u.dest_app_id = a.id
    AND u.dt = a.dt
  WHERE u.dt >= '2023-12-06T00:00:00Z'
    AND c.dt >= '2023-12-06T00:00:00Z'
    AND u.bid_date >= '2023-12-06T00:00:00Z'
    AND u.dt < '2023-12-30T15:00:20Z'
    AND u.bid_date < '2023-12-30T15:00:20Z'
    AND c.dt < '2023-12-30T15:00:20Z'
  GROUP BY u.campaign_tracker, u.campaign_tracker_type, u.exchange, u.test_id, u.test_group_id, u.test_group, u.bid_date, u.dest_app_id, u.campaign_id, a.salesforce_account_level, s.scale_zone, s.sales_region, u.platform, u.customer_id, u.logical_size, u.app_event_category, u.non_personalized, u.model_type, u.vt_bidding, u.ad_group_type, c.current_optimization_state, u.liftoff_category, u.exploratory
),
lower_funnel_per_user AS (
  SELECT
    l.campaign_tracker,
    l.campaign_tracker_type,
    l.exchange,
    l.test_id,
    l.test_group_id,
    l.test_group,
    l.bid_date,
    l.dest_app_id,
    l.campaign_id,
    a.salesforce_account_level AS salesforce_account_level,
    s.scale_zone AS scale_zone,
    s.sales_region AS sales_region,
    l.platform,
    l.customer_id,
    l.logical_size,
    l.app_event_category,
    l.non_personalized,
    l.model_type,
    l.vt_bidding,
    l.ad_group_type,
    c.current_optimization_state AS campaign_bidding_strategy,
    l.liftoff_category,
    l.exploratory,
    auction_id, -- we use to get auction-level data. this is our capping and cohorting logic. We pull 7-day cohorted customer revenue of each auction and cap it at $500 
    sum(if(attribution_delay_millis < 604800000, app_events, 0)) AS app_events_7d,  -- we only use this event for performance report
    sum(if(attribution_delay_millis < 2592000000, app_events, 0)) AS app_events_30d,
    sum(if(attribution_delay_millis < 604800000, 1, 0)) AS app_events_total_7d, -- including all recurring events. We do not use this to report performance
    sum(if(attribution_delay_millis < 2592000000, 1, 0)) AS app_events_total_30d,
    least(sum(if(attribution_delay_millis < 604800000, customer_revenue_micros, 0)), 500 * 1.0e6) AS customer_revenue_micros_7d,
    least(sum(if(attribution_delay_millis < 2592000000, customer_revenue_micros, 0)), 500 * 1.0e6) AS customer_revenue_micros_30d,
    least(sum(if(attribution_delay_millis < 604800000, customer_revenue, 0)), 500) AS customer_revenue_7d,
    sum(if(attribution_delay_millis < 604800000, customer_revenue, 0)) AS non_capped_customer_revenue_7d
  FROM abacus.cortex_ios_iti_unified_v0_1_v2_lowerfunnel l
  LEFT JOIN pinpoint_hourly.campaigns c
    ON l.campaign_id = c.id
    AND l.dt = c.dt
  LEFT JOIN scale_zones s
    ON l.campaign_id = s.campaign_id
    AND substring(l.bid_date, 1, 10) = substring(s.dt, 1, 10)
  LEFT JOIN pinpoint_hourly.apps a
    ON l.dest_app_id = a.id
    AND l.dt = a.dt
  WHERE l.dt >= '2023-12-06T00:00:00Z'
    AND c.dt >= '2023-12-06T00:00:00Z'
    AND l.bid_date >= '2023-12-06T00:00:00Z'
    AND l.dt < '2023-12-30T15:00:20Z'
    AND c.dt < '2023-12-30T15:00:20Z'
    AND l.bid_date < '2023-12-30T15:00:20Z'
  GROUP BY l.campaign_tracker, l.campaign_tracker_type, l.exchange, l.test_id, l.test_group_id, l.test_group, l.bid_date, l.dest_app_id, l.campaign_id, a.salesforce_account_level, s.scale_zone, s.sales_region, l.platform, l.customer_id, l.logical_size, l.app_event_category, l.non_personalized, l.model_type, l.vt_bidding, l.ad_group_type, c.current_optimization_state, l.liftoff_category, l.exploratory, auction_id
),

lower_funnel_reduced AS (
  SELECT
    campaign_tracker,
    campaign_tracker_type,
    exchange,
    test_id,
    test_group_id,
    test_group,
    bid_date,
    dest_app_id,
    campaign_id,
    salesforce_account_level,
    scale_zone,
    sales_region,
    platform,
    customer_id,
    logical_size,
    app_event_category,
    non_personalized,
    model_type,
    vt_bidding,
    ad_group_type,
    campaign_bidding_strategy,
    liftoff_category,
    exploratory,
    sum(app_events_7d) AS app_events_7d,
    sum(app_events_total_7d) AS app_events_total_7d,
    sum(customer_revenue_micros_7d) AS customer_revenue_micros_7d,
    sum(pow(customer_revenue_micros_7d, 2)) AS sum_squared_customer_revenue_micros_7d,
    sum(customer_revenue_7d) AS customer_revenue_7d,
    sum(non_capped_customer_revenue_7d) AS non_capped_customer_revenue_7d,
    sum(pow(customer_revenue_7d, 2)) AS sum_squared_customer_revenue_7d,
    sum(cast(customer_revenue_7d > 0 AS integer)) AS installs_with_revenue_7d  -- we only report performance among CPR campaigns with >= 5 install with revenue.
  FROM lower_funnel_per_user
  GROUP BY campaign_tracker, campaign_tracker_type, exchange, test_id, test_group_id, test_group, bid_date, dest_app_id, campaign_id, salesforce_account_level, scale_zone, sales_region, platform, customer_id, logical_size, app_event_category, non_personalized, model_type, vt_bidding, ad_group_type, campaign_bidding_strategy, liftoff_category, exploratory
)
SELECT
  from_iso8601_timestamp(u.bid_date) AS bid_at,
  u.campaign_tracker,
  u.campaign_tracker_type, 
  u.test_group_id,
  u.test_group,
  u.customer_id,
  u.dest_app_id,
  u.campaign_id,
  u.salesforce_account_level AS account_level,
  u.scale_zone,
  u.sales_region,
  CASE WHEN u.logical_size = 'L0x0'THEN 'native'
       WHEN u.logical_size = 'L300x250' THEN 'mrec'
       WHEN u.logical_size IN ('L320x50', 'L728x90') THEN 'banner'
       ELSE 'interstitial' END AS ad_format,
  u.logical_size,
  u.non_personalized,
  u.model_type AS bid_type,
  u.vt_bidding,
  u.campaign_bidding_strategy AS current_optimization_state,
  b.company AS customer_name,
  c.display_name AS dest_app_name,
  cc.display_name AS campaign_name,
  CASE WHEN u.exchange IN ('VUNGLE','APPLOVIN','INNERACTIVE_DIRECT','DOUBLECLICK','MINTEGRAL','IRONSOURCE') THEN u.exchange ELSE 'others' END AS exchange_group,
  cc.vt_cap AS vt_cap,
  
  u.impressions,
  u.spend,
  u.spend_micros,
  u.revenue,
  u.revenue_micros,
  u.clicks,
  u.installs,
  u.ct_installs,
  u.vt_installs,
  u.app_events,
  u.app_events_total,
  l.app_events_7d AS target_events_first_d7_imp,
  l.app_events_total_7d,
  l.customer_revenue_7d AS capped_customer_revenue_d7_imp,
  l.non_capped_customer_revenue_7d AS non_capped_customer_revenue_7d,
  l.sum_squared_customer_revenue_7d AS squared_capped_customer_revenue_d7_imp, -- this is part of Confidence Interval formula
  u.predicted_clicks,
  u.predicted_vt_installs AS predicted_installs_vt,
  u.predicted_ct_installs AS predicted_installs_ct,
  u.predicted_app_events,
  u.predicted_customer_revenue,
  l.installs_with_revenue_7d AS installs_with_d7_revenue
FROM upper_funnel_reduced u
LEFT JOIN lower_funnel_reduced l
    ON u.campaign_tracker = l.campaign_tracker
    AND u.campaign_tracker_type = l.campaign_tracker_type
    AND u.exchange = l.exchange
    AND u.test_id = l.test_id
    AND u.test_group_id = l.test_group_id
    AND u.test_group = l.test_group
    AND u.bid_date = l.bid_date
    AND u.dest_app_id = l.dest_app_id
    AND u.campaign_id = l.campaign_id
    AND u.salesforce_account_level = l.salesforce_account_level
    AND u.scale_zone = l.scale_zone
    AND u.sales_region = l.sales_region
    AND u.platform = l.platform
    AND u.customer_id = l.customer_id
    AND u.logical_size = l.logical_size
    AND u.app_event_category = l.app_event_category
    AND u.non_personalized = l.non_personalized
    AND u.model_type = l.model_type
    AND u.vt_bidding = l.vt_bidding
    AND u.ad_group_type = l.ad_group_type
    AND u.campaign_bidding_strategy = l.campaign_bidding_strategy
    AND u.liftoff_category = l.liftoff_category
    AND u.exploratory = l.exploratory
LEFT JOIN pinpoint.public.customers b
    ON u.customer_id = b.id
LEFT JOIN pinpoint.public.apps c
    ON u.dest_app_id = c.id AND  u.customer_id = c.customer_id
LEFT JOIN pinpoint.public.campaigns cc 
    ON u.campaign_id = cc.id 
WHERE   
  u.campaign_tracker_type <> 'ANONYMIZED'
  AND u.ad_group_type = 'user-acquisition'
  AND u.platform = 'IOS'
  AND u.exploratory = FALSE
