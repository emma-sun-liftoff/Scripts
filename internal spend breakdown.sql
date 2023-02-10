SELECT 

CASE 
  WHEN sum(CAST(spend_micros as DOUBLE)/1000000) >= 0 AND sum(CAST(spend_micros as DOUBLE)/1000000) < 100 THEN '[0,100)'
  WHEN sum(CAST(spend_micros as DOUBLE)/1000000) >= 100 AND sum(CAST(spend_micros as DOUBLE)/1000000) < 200 THEN '[100,200)'
  WHEN sum(CAST(spend_micros as DOUBLE)/1000000) >= 200 AND sum(CAST(spend_micros as DOUBLE)/1000000) < 300 THEN '[200,300)'
  WHEN sum(CAST(spend_micros as DOUBLE)/1000000) >= 300 AND sum(CAST(spend_micros as DOUBLE)/1000000) < 400 THEN '[300,400)'
  WHEN sum(CAST(spend_micros as DOUBLE)/1000000) >= 400 AND sum(CAST(spend_micros as DOUBLE)/1000000) < 500 THEN '[400,500)'
  WHEN sum(CAST(spend_micros as DOUBLE)/1000000) >= 500 THEN '[500,+)'
  ELSE 'N/A'
END AS internal_spend_breakdown

, sum(CAST(revenue_micros as DOUBLE)/1000000)  as GR 
, sum(installs) as installs 
from product_analytics.supply_analytics_hourly_v2 
where dt  < '2022-09-24'