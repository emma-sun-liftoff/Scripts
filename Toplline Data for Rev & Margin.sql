WITH latest_sfdc_partition AS (
    SELECT max(dt) as latest_dt 
    FROM salesforce_daily.account 
    WHERE from_iso8601_timestamp(dt) >= CURRENT_TIMESTAMP - interval '3' day),

sfdc_data AS (
    SELECT c.campaign_id_18_digit__c as salesforce_campaign_id
        , o.sales_region__c as sales_region
        , o.sales_sub_region__c as sales_sub_region
    FROM salesforce_daily.customer_campaign__c c 
      LEFT JOIN salesforce_daily.opportunity o 
        ON c.dt = o.dt 
        AND c.opportunity__c = o.opportunity_id_18_digit__c
    WHERE c.dt = (select latest_dt from latest_sfdc_partition)
      AND o.dt = (select latest_dt from latest_sfdc_partition))

SELECT
  h.dt
, from_iso8601_timestamp(h.dt) as incremental_timestamp
, h.customer_id
, cust.company as customer_name
, h.dest_app_id
, a.display_name as dest_app_name
, h.campaign_id
, c.display_name as campaign_name
, h.platform
, h.ad_format
, h.creative_type
, h.is_video_creative
, h.is_interactive
, h.exchange
, h.model_type
, sfdc_data.sales_region as sales_region
, sfdc_data.sales_sub_region as sales_sub_region
   ,  SUM(revenue_micros) / power(10,6) AS revenue
   , SUM(spend_micros)/power(10,6)  AS spend

FROM analytics.hourly_data h
LEFT JOIN pinpoint.public.campaigns c  
ON h.campaign_id = c.id  
LEFT JOIN sfdc_data
ON sfdc_data.salesforce_campaign_id = c.salesforce_campaign_id
LEFT JOIN pinpoint.public.apps a 
ON c.app_id = a.id
LEFT JOIN pinpoint.public.customers cust
ON cust.id = c.customer_id
WHERE from_iso8601_timestamp(dt) >= now() - interval '<Parameters.Days of Data>' day
    AND (spend_micros > 0 OR revenue_micros > 0)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17