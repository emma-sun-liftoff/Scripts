SELECT
  install.ad_click.impression.auction_id AS auction_id,
  COALESCE(install.ad_click.impression.bid.campaign_tracker,reeng_click.impression.bid.campaign_tracker) AS campaign_tracker,
  COALESCE(install.ad_click.impression.bid.campaign_tracker_type,reeng_click.impression.bid.campaign_tracker_type) AS campaign_tracker_type,
  COALESCE(install.ad_click.impression.bid.bid_request.exchange,reeng_click.impression.bid.bid_request.exchange) AS exchange,
  ab_test.id AS test_id,
  ab_test."group".id AS test_group_id,
  ab_test."group".name AS test_group,
  COALESCE(to_iso8601(date_trunc('hour', from_unixtime(install.ad_click.impression.bid.at / 1000) AT TIME ZONE 'UTC')),to_iso8601(date_trunc('hour', from_unixtime(reeng_click.impression.bid.at / 1000) AT TIME ZONE 'UTC'))) AS bid_date,
  COALESCE(install.ad_click.impression.bid.app_id,reeng_click.impression.bid.app_id) AS dest_app_id,
  COALESCE(install.ad_click.impression.bid.campaign_id,reeng_click.impression.bid.campaign_id) AS campaign_id,
  COALESCE(install.ad_click.impression.bid.app_platform,reeng_click.impression.bid.app_platform) AS platform,
  COALESCE(install.ad_click.impression.bid.customer_id,reeng_click.impression.bid.customer_id) AS customer_id,
  COALESCE(install.ad_click.impression.bid.bid_request.impressions[1].logical_size,reeng_click.impression.bid.bid_request.impressions[1].logical_size) AS logical_size,
  COALESCE(install.ad_click.impression.bid.cpa_model_event_category,reeng_click.impression.bid.cpa_model_event_category) AS app_event_category,
  COALESCE(install.ad_click.impression.bid.bid_request.non_personalized,reeng_click.impression.bid.bid_request.non_personalized) AS non_personalized,
  COALESCE(install.ad_click.impression.bid.price_data.model_type,reeng_click.impression.bid.price_data.model_type) AS model_type,
  COALESCE(install.ad_click.impression.bid.price_data.vt_bidding_enabled,reeng_click.impression.bid.price_data.vt_bidding_enabled) AS vt_bidding,
  COALESCE(install.ad_click.impression.bid.ad_group_type,reeng_click.impression.bid.ad_group_type) AS ad_group_type,
  COALESCE(install.ad_click.impression.bid.app_liftoff_category,reeng_click.impression.bid.app_liftoff_category) AS liftoff_category,
  COALESCE(install.ad_click.impression.bid.exploratory,reeng_click.impression.bid.exploratory) AS exploratory,
  at - COALESCE(install.ad_click.impression.bid.at, reeng_click.impression.bid.at) AS attribution_delay_millis,
  if(first_occurrence AND custom_event_id = COALESCE(install.ad_click.impression.bid.campaign_target_event_id, reeng_click.impression.bid.campaign_target_event_id), 1, 0) as app_events,
  if(custom_event_id = COALESCE(install.ad_click.impression.bid.campaign_target_event_id, reeng_click.impression.bid.campaign_target_event_id), 1, 0) as app_events_total,
  customer_revenue_micros,
  customer_revenue_micros / 1.0e6 AS customer_revenue
FROM proto2parquet.app_events
  CROSS JOIN UNNEST(install.ad_click.impression.bid.bid_request.ab_test_assignments) AS ab_test
WHERE
  ab_test.id = 1196
  AND dt = '{{ dt }}'
  AND at - COALESCE(install.ad_click.impression.bid.at, reeng_click.impression.bid.at) < 2592000000 -- 30 days
  AND (customer_revenue_micros != 0 OR custom_event_id = COALESCE(install.ad_click.impression.bid.campaign_target_event_id, reeng_click.impression.bid.campaign_target_event_id))
  AND is_uncredited = false
  AND for_reporting = true
