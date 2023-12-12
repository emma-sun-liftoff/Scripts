WITH imps AS (
    SELECT
      -- Dimensions
      bid.campaign_tracker AS campaign_tracker,
      bid.campaign_tracker_type AS campaign_tracker_type,
      bid.bid_request.exchange AS exchange,
      ab_test.id AS test_id,
      ab_test."group".id AS test_group_id,
      ab_test."group".name AS test_group,
      to_iso8601(date_trunc('hour', from_unixtime(bid.at / 1000) AT TIME ZONE 'UTC')) AS bid_date,
      bid.app_id AS dest_app_id,
      bid.campaign_id AS campaign_id,
      bid.app_platform AS platform,
      bid.customer_id AS customer_id,
      bid.bid_request.impressions[1].logical_size AS logical_size,
      bid.cpa_model_event_category AS app_event_category,
      bid.bid_request.non_personalized AS non_personalized,
      bid.price_data.model_type AS model_type,
      bid.price_data.vt_bidding_enabled AS vt_bidding,
      bid.ad_group_type AS ad_group_type,
      bid.app_liftoff_category AS liftoff_category,
      bid.exploratory AS exploratory,
      -- Metrics
      count(*) AS impressions,
      sum(spend_micros) / 1.0e6 AS spend,
      sum(spend_micros) AS spend_micros,
      sum(revenue_micros) / 1.0e6 AS revenue,
      sum(revenue_micros) AS revenue_micros,
      sum(bid.price_data.predicted_imp_to_click_rate) AS predicted_clicks,
      sum(bid.price_data.predicted_imp_to_install_vt_rate) AS predicted_vt_installs,
      sum(bid.price_data.predicted_imp_to_click_rate * bid.price_data.predicted_click_to_install_rate + bid.price_data.predicted_imp_to_install_ct_rate) AS predicted_ct_installs,
      sum((bid.price_data.predicted_imp_to_click_rate * bid.price_data.predicted_click_to_install_rate + bid.price_data.predicted_imp_to_install_ct_rate) * bid.price_data.predicted_install_to_preferred_app_event_rate + bid.price_data.predicted_imp_to_install_ct_rate * bid.price_data.predicted_install_to_preferred_app_event_vt_rate) AS predicted_app_events,
      sum((bid.price_data.predicted_imp_to_click_rate * bid.price_data.predicted_click_to_install_rate + bid.price_data.predicted_imp_to_install_ct_rate + bid.price_data.predicted_imp_to_install_vt_rate) * bid.price_data.predicted_install_to_revenue_rate) / 1.0e6 AS predicted_customer_revenue
    FROM proto2parquet.impressions_with_bids
      CROSS JOIN UNNEST(bid.bid_request.ab_test_assignments) AS ab_test
    WHERE
      ab_test.id = 1196
      AND dt = '{{ dt }}'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
), clicks AS (
    SELECT
      -- Dimensions
      impression.bid.campaign_tracker AS campaign_tracker,
      impression.bid.campaign_tracker_type AS campaign_tracker_type,
      impression.bid.bid_request.exchange AS exchange,
      ab_test.id AS test_id,
      ab_test."group".id AS test_group_id,
      ab_test."group".name AS test_group,
      to_iso8601(date_trunc('hour', from_unixtime(impression.bid.at / 1000) AT TIME ZONE 'UTC')) AS bid_date,
      impression.bid.app_id AS dest_app_id,
      impression.bid.campaign_id AS campaign_id,
      impression.bid.app_platform AS platform,
      impression.bid.customer_id AS customer_id,
      impression.bid.bid_request.impressions[1].logical_size AS logical_size,
      impression.bid.cpa_model_event_category AS app_event_category,
      impression.bid.bid_request.non_personalized AS non_personalized,
      impression.bid.price_data.model_type AS model_type,
      impression.bid.price_data.vt_bidding_enabled AS vt_bidding,
      impression.bid.ad_group_type AS ad_group_type,
      impression.bid.app_liftoff_category AS liftoff_category,
      impression.bid.exploratory AS exploratory,
      -- Metrics
      count(*) AS clicks
    FROM proto2parquet.ad_clicks
      CROSS JOIN UNNEST(impression.bid.bid_request.ab_test_assignments) AS ab_test
    WHERE
      ab_test.id = 1196
      AND dt = '{{ dt }}'
      AND at - impression.bid.at < 604800000 -- 7 days
      AND has_prior_click = false
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
), installs AS (
    SELECT
      -- Dimensions
      ad_click.impression.bid.campaign_tracker AS campaign_tracker,
      ad_click.impression.bid.campaign_tracker_type AS campaign_tracker_type,
      ad_click.impression.bid.bid_request.exchange AS exchange,
      ab_test.id AS test_id,
      ab_test."group".id AS test_group_id,
      ab_test."group".name AS test_group,
      to_iso8601(date_trunc('hour', from_unixtime(ad_click.impression.bid.at / 1000) AT TIME ZONE 'UTC')) AS bid_date,
      ad_click.impression.bid.app_id AS dest_app_id,
      ad_click.impression.bid.campaign_id AS campaign_id,
      ad_click.impression.bid.app_platform AS platform,
      ad_click.impression.bid.customer_id AS customer_id,
      ad_click.impression.bid.bid_request.impressions[1].logical_size AS logical_size,
      ad_click.impression.bid.cpa_model_event_category AS app_event_category,
      ad_click.impression.bid.bid_request.non_personalized AS non_personalized,
      ad_click.impression.bid.price_data.model_type AS model_type,
      ad_click.impression.bid.price_data.vt_bidding_enabled AS vt_bidding,
      ad_click.impression.bid.ad_group_type AS ad_group_type,
      ad_click.impression.bid.app_liftoff_category AS liftoff_category,
      ad_click.impression.bid.exploratory AS exploratory,
      -- Metrics
      count(*) AS installs,
      sum(cast(NOT is_viewthrough AS integer)) AS ct_installs,
      sum(cast(is_viewthrough AS integer)) AS vt_installs,
      SUM(IF(is_viewthrough, 0, at - ad_click.at)) AS click_to_install_time_sum,
      SUM(CAST(at - ad_click.at < 3600000 AS integer)) AS num_installs_within_1h,
      SUM(CAST(at - ad_click.at < 86400000 AS integer)) AS num_installs_within_1d
    FROM proto2parquet.installs
      CROSS JOIN UNNEST(ad_click.impression.bid.bid_request.ab_test_assignments) AS ab_test
    WHERE
      ab_test.id = 1196
      AND dt = '{{ dt }}'
      AND at - ad_click.impression.bid.at < 604800000 -- 7 days
      AND is_uncredited = false
      AND for_reporting = true
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
), events AS (
    SELECT
      -- Dimensions
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
      -- Metrics
      sum(cast(first_occurrence AS integer)) AS app_events,
      count(*) AS app_events_total
    FROM proto2parquet.app_events
      CROSS JOIN UNNEST(COALESCE(install.ad_click.impression.bid.bid_request.ab_test_assignments,reeng_click.impression.bid.bid_request.ab_test_assignments)) AS ab_test
    WHERE
      ab_test.id = 1196
      AND dt = '{{ dt }}'
      AND is_uncredited = false
      AND for_reporting = true
      AND custom_event_id = COALESCE(install.ad_click.impression.bid.campaign_target_event_id, reeng_click.impression.bid.campaign_target_event_id)
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
)

SELECT
  -- Dimensions
  campaign_tracker,
  campaign_tracker_type,
  exchange,
  test_id,
  test_group_id,
  test_group,
  bid_date,
  dest_app_id,
  campaign_id,
  platform,
  customer_id,
  logical_size,
  app_event_category,
  non_personalized,
  model_type,
  vt_bidding,
  ad_group_type,
  liftoff_category,
  exploratory,
  -- Metrics
  impressions,
  spend,
  spend_micros,
  revenue,
  revenue_micros,
  clicks,
  installs,
  ct_installs,
  vt_installs,
  app_events,
  app_events_total,
  predicted_clicks,
  predicted_vt_installs,
  predicted_ct_installs,
  predicted_app_events,
  predicted_customer_revenue,
  click_to_install_time_sum,
  num_installs_within_1h,
  num_installs_within_1d
FROM
  imps
  FULL OUTER JOIN clicks USING (campaign_tracker, campaign_tracker_type, exchange, test_id, test_group_id, test_group, bid_date, dest_app_id, campaign_id, platform, customer_id, logical_size, app_event_category, non_personalized, model_type, vt_bidding, ad_group_type, liftoff_category, exploratory)
  FULL OUTER JOIN installs USING (campaign_tracker, campaign_tracker_type, exchange, test_id, test_group_id, test_group, bid_date, dest_app_id, campaign_id, platform, customer_id, logical_size, app_event_category, non_personalized, model_type, vt_bidding, ad_group_type, liftoff_category, exploratory)
  FULL OUTER JOIN events USING (campaign_tracker, campaign_tracker_type, exchange, test_id, test_group_id, test_group, bid_date, dest_app_id, campaign_id, platform, customer_id, logical_size, app_event_category, non_personalized, model_type, vt_bidding, ad_group_type, liftoff_category, exploratory)
