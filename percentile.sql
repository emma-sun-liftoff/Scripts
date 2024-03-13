---misconfigured amounts data source
select distinct COALESCE(attribution_event__click__impression__bid__app_id, reeng_click__impression__bid__app_id, install__ad_click__impression__bid__app_id) AS dest_app_id
, custom_event_id
, e.name as custom_event_name
, c.name as event_category
,  SUM(IF(for_reporting,customer_revenue_micros))/power(10,6) AS customer_revenue
,  SUM(IF(for_reporting AND customer_revenue_micros > 200000000,customer_revenue_micros))/power(10,6) AS customer_revenue_200
, approx_percentile(customer_revenue_micros, 0.95)/power(10,6) as percentile_95
, approx_percentile(customer_revenue_micros, 0.5)/power(10,6) as percentile_50
FROM rtb.matched_app_events 
LEFT JOIN pinpoint.public.custom_events e 
	ON custom_event_id = e.id
LEFT JOIN pinpoint.public.event_categories c 
	ON e.event_category_id = c.id
where is_uncredited <> true
AND include_in_customer_revenue = true
AND customer_revenue_micros > 0 
AND from_iso8601_timestamp(dt) >= CURRENT_DATE - interval <Parameters.Days> day 
GROUP BY 1,2,3,4
