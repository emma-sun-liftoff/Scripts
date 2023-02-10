select distinct exchange
  , try(filter( bid__bid_request__ab_test_assignments , t -> t.id = 731)[1]."group"."name") as test_group_name 
  , bid__margin_data__channel_margin_multiplier 
from rtb.impressions_with_bids
where dt >= '2022-11-14T00' AND dt <= '2022-11-14T01'
 AND exchange IN ('VUNGLE', 'APPLOVIN')
 AND bid__campaign_id = 12688




SELECT DISTINCT exchange,
bid__margin_data__channel_margin_multiplier,
CASE WHEN COALESCE(ab_test."group".id, 0) = 1665 THEN 'margin-control'
             WHEN COALESCE(ab_test."group".id, 0) = 1666 THEN 'margin-explore'
             ELSE NULL 
             END AS test_group_name

--bid__price_data__source_app_bid_multiplier
from rtb.impressions_with_bids
CROSS JOIN Unnest (
     bid__bid_request__ab_test_assignments) AS ab_test

where date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 1 
 AND ab_test.id = 731
 AND exchange IN ('VUNGLE', 'APPLOVIN')
 AND bid__campaign_id  = 12688



 select distinct exchange
  , dt
  , bid__margin_data__channel_margin_multiplier 
from rtb.impressions_with_bids
where date_diff('day', from_iso8601_timestamp(dt), current_date) <= 30
 AND exchange IN ('VUNGLE', 'APPLOVIN')
 AND bid__campaign_id = 5908
